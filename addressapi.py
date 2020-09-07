#!/flask/bin/python
from decimal import Decimal
import time
import os

from flask import Flask, jsonify
from werkzeug.contrib.fixers import ProxyFix

# Custom libraries:
from include import dbutils
from include import utils
from include import rpc
from include import globals
import settings


app = Flask(__name__)
app.config['JSON_SORT_KEYS'] = False

@app.before_request
def cleanup_globals():
    globals.requests += 1
    globals.start = time.time()
    globals.debug = [{
        'initialized': globals.start,
        'requests': globals.requests,
        'debug_level': utils.get_debug_level(),
        'pid': os.getpid(),
    }]
    try:
        for coin in globals.db_connection:
            dbutils.close_database_connection(coin=coin)
    except:
        pass

def validate_type(type):
    if not type:
        status_code = 400
        return jsonify({
            'status': 'ERROR',
            'code': status_code,
            'error': 'type not set',
            'details': 'type is required',
            'data': {
                'coin': 'unknown',
                'symbol': 'unknown',
                'address': 'unknown',
            },
        }), status_code

    supported_coins = utils.supported_coins(settings)
    if type not in supported_coins:
        status_code = 400
        return jsonify({
            'status': 'ERROR',
            'code': status_code,
            'error': 'unrecognized coin type',
            'details': 'must be one of: %s' % supported_coins,
            'data': {
                'coin': type,
                'symbol': 'unknown',
                'address': 'unknown',
            },
        }), status_code

    globals.args.type = type

    return None, True

def validate_address(type, address):
    if not address:
        status_code = 400
        return jsonify({
            'status': 'ERROR',
            'code': status_code,
            'error': 'address not sent',
            'details': 'address is required',
            'data': {
                'coin': type,
                'symbol': settings.coins[type]['symbol'],
                'address': 'unknown',
            },
        }), status_code

    try:
        timestamp = time.time()
        validated = rpc.rpc_request(method='validateaddress', parameters=[address])
        utils.debug({
            'activity': 'RPC query',
            'rpc_method': 'validateaddress',
            'parameters': [address],
            'elapsed': utils.elapsed(timestamp, 5),
        }, level=2)
    except:
        status_code = 503
        return jsonify({
            'status': 'ERROR',
            'code': status_code,
            'error': 'failed to communicate with daemon',
            'data': {
                'coin': type,
                'symbol': settings.coins[type]['symbol'],
                'address': address,
            },
        }), status_code

    if not validated['isvalid']:
        status_code = 400
        return jsonify({
            'status': 'ERROR',
            'code': status_code,
            'error': 'address is invalid',
            'debug': utils.debug(),
            'data': {
                'coin': type,
                'symbol': settings.coins[type]['symbol'],
                'address': address,
                'rpc': validated,
            },
        }), status_code

    return validated, True

@app.route('/api/address/<type>/<address>', methods=['GET'])
def get_address(type, address):
    message, validated = validate_type(type)
    if validated is not True:
        return message

    details, validated = validate_address(type, address)
    if validated is not True:
        return details
    else:
        # Remove superfluous details, if existing.
        try:
            del details["ismine"]
        except:
            pass
        try:
            del details["iswatchonly"]
        except:
            pass

    address_json = dbutils.select(type, 'address', address)
    utils.debug(message={'address_json': address_json}, level=3)
    address_txids = None
    error_count = 0
    if address_json:
        status_code = 200
        timestamp = time.time()
        blockcount = rpc.rpc_request(method='getblockcount')
        utils.debug({
            'activity': 'RPC query',
            'rpc_method': 'getblockcount',
            'parameters': None,
            'elapsed': utils.elapsed(timestamp, 5),
        }, level=2)
        if len(address_json[address]) > 15000:
            transactions = []
            utils.debug(message="ERROR: too many transactions (%d or more), not calculating" % len(address_json[address]))
            sent_total = received_total = vin_count = vout_count = is_spent = "not calculated"
            error_count += 1
        else:
            transactions = []
            sent_total = received_total = vin_count = vout_count = txid_height = txid_timestamp = 0

            # Generate a list of all txids including the address
            address_txids = {}
            for txid in address_json[address]:
                # Get the height and timestamp of the transaction
                key = next(iter(address_json[address][txid]))
                txid_height = address_json[address][txid][key]['height']

                # Build object allowing us to sort transactions by height
                if txid_height in address_txids:
                    address_txids[txid_height].append(txid)
                else:
                    address_txids.update({
                        txid_height: [txid],
                    })

                # Load to details for each transaction
                tx_vout_json = dbutils.select(type, 'vout', txid)
                utils.debug(message={'tx_vout_json': tx_vout_json}, level=3)
                for to_address in tx_vout_json[txid]['addresses']:
                    if address == to_address:
                        for vout_sent in tx_vout_json[txid]['addresses'][to_address]:
                            # Search for received that have subsequently been spent
                            tx_vin_spent_json = dbutils.select(type, 'vin_spent', txid)
                            utils.debug(message={'tx_vin_spent_json': tx_vin_spent_json}, level=3)
                            if tx_vin_spent_json and vout_sent in tx_vin_spent_json[txid]:
                                height = tx_vin_spent_json[txid][vout_sent]['height']
                                if height in address_txids:
                                    address_txids[height].append(tx_vin_spent_json[txid][vout_sent]['txid'])
                                else:
                                    address_txids.update({
                                        height: [tx_vin_spent_json[txid][vout_sent]['txid']],
                                    })

            # Loop through all transactions involving this address, newest first.
            for height in sorted(address_txids, reverse=True):
                txids = address_txids[height]
                for txid in set(txids):
                    received_from_count = 0
                    sent_to_count = 0
                    received = sent = False
                    value_in = value_out = 0

                    # Load to details for each transaction
                    tx_vout_json = dbutils.select(type, 'vout', txid)
                    utils.debug(message={'tx_vout_json': tx_vout_json}, level=3)
                    txid_height = tx_vout_json[txid]['height']
                    txid_timestamp = tx_vout_json[txid]['timestamp']
                    to_details = []
                    total_vout_value = 0
                    for to_address in tx_vout_json[txid]['addresses']:
                        for vout_sent in tx_vout_json[txid]['addresses'][to_address]:
                            vout_count += 1
                            sent_to_count += 1
                            total_vout_value += tx_vout_json[txid]['addresses'][to_address][vout_sent]['value']
                            if address == to_address:
                                received_total += tx_vout_json[txid]['addresses'][to_address][vout_sent]['value']
                                value_in += tx_vout_json[txid]['addresses'][to_address][vout_sent]['value']
                                received = True

                            # Determine if this vout has subsequently been spent
                            tx_vin_spent_json = dbutils.select(type, 'vin_spent', txid)
                            utils.debug(message={'tx_vin_spent_json': tx_vin_spent_json}, level=3)
                            if tx_vin_spent_json and vout_sent in tx_vin_spent_json[txid]:
                                is_spent = True
                            else:
                                is_spent = False

                            to_details.append({
                                'address': to_address,
                                'value': tx_vout_json[txid]['addresses'][to_address][vout_sent]['value'],
                                'is_spent': is_spent,
                            })

                    tx_vin_json = dbutils.select(type, 'vin_txid', txid)
                    utils.debug(message={'tx_vin_json': tx_vin_json}, level=3)
                    from_details = []
                    total_vin_value = 0
                    if tx_vin_json and txid in tx_vin_json:
                        for vin in tx_vin_json[txid]['vin']:
                            received_from_count += 1
                            spent_txid = tx_vin_json[txid]['vin'][vin]['spent']
                            spent_vout = tx_vin_json[txid]['vin'][vin]['vout']

                            # Finally, look up the address associated with this spent txid/vout
                            spent_json = dbutils.select(type, 'vout', spent_txid)
                            utils.debug(message={'spent_json': spent_json}, level=3)
                            for from_address in spent_json[spent_txid]['addresses']:
                                if spent_vout in spent_json[spent_txid]['addresses'][from_address]:
                                    total_vin_value += spent_json[spent_txid]['addresses'][from_address][spent_vout]['value']
                                    vin_count += 1
                                    from_details.append({
                                        'address': from_address,
                                        'value': spent_json[spent_txid]['addresses'][from_address][spent_vout]['value'],
                                        # We can easily provide more details here:
                                        #'spent': {
                                        #    'txid': spent_txid,
                                        #    'vout': spent_vout,
                                        #}
                                    })
                                    if address == from_address:
                                        sent_total += spent_json[spent_txid]['addresses'][from_address][spent_vout]['value']
                                        value_out += spent_json[spent_txid]['addresses'][from_address][spent_vout]['value']
                                        sent = True
                        fee = int(total_vin_value - total_vout_value)
                    else:
                        coinbase_json = dbutils.select(type, 'coinbase', txid)
                        utils.debug(message={'coinbase_json': coinbase_json}, level=3)
                        if coinbase_json and txid in coinbase_json:
                            received_from_count += 1
                            from_details.append({
                                'address': False,
                                'coinbase': coinbase_json[txid]['coinbase'],
                                'value': coinbase_json[txid]['value'],
                            })
                        fee = 0

                    if len(from_details) < 1 or len(from_details) != received_from_count:
                        utils.debug(message='ERROR: length of from array: %d, reported length: %d' % (len(from_details),
                                                                                        received_from_count))
                        error_count += 1

                    if len(to_details) < 1:
                        utils.debug(message='ERROR: empty to array on txid %s' % txid)
                        error_count += 1

                    transactions.append({
                        'txid': txid,
                        'block': int(txid_height),
                        'confirmations': int(blockcount) - int(txid_height),
                        'timestamp': int(txid_timestamp),
                        'received': received,
                        'value_in': value_in,
                        'sent': sent,
                        'value_out': value_out,
                        'from_count': int(received_from_count),
                        'to_count': int(sent_to_count),
                        'fee': fee,
                        'from':  from_details,
                        'to': to_details,
                    })

        try:
            balance = received_total - sent_total
            if balance < 0:
                utils.debug(message='ERROR: negative balance')
                error_count += 1
        except:
            balance = 'not calculated'

        utils.debug(message={'address_txids': address_txids}, level=3)

        dbutils.close_database_connection(type)
        utils.debug(message={'total elapsed': utils.elapsed(globals.start, 3)})
        return jsonify({
            'status': 'OK',
            'code': status_code,
            'debug': utils.debug(),
            'errors': error_count,
            'data': {
                'coin': type,
                'symbol': settings.coins[type]['symbol'],
                'address': details,
                'balance': balance,
                'total': {
                    'received': received_total,
                    'sent': sent_total,
                    'vin': vin_count,
                    'vout': vout_count,
                    'blockcount': blockcount,
                },
                'transactions': transactions,
            }
        }), status_code

    else:
        status_code = 404
        dbutils.close_database_connection(type)
        utils.debug(message={'total elapsed': utils.elapsed(globals.start, 3)})
        return jsonify({
            'status': 'ERROR',
            'code': status_code,
            'error': 'address not found',
            'debug': utils.debug(),
            'data': {
                'coin': type,
                'symbol': settings.coins[type]['symbol'],
                'address': details,
            },
        }), status_code

@app.route('/api/address/<type>/<address>/unspent', methods=['GET'])
def get_address_unspent(type, address):
    message, validated = validate_type(type)
    if validated is not True:
        return message

    details, validated = validate_address(type, address)
    if validated is not True:
        return details
    else:
        # Remove superfluous details, if existing.
        try:
            del details["ismine"]
        except:
            pass
        try:
            del details["iswatchonly"]
        except:
            pass

    address_json = dbutils.select(type, 'address', address)
    utils.debug(message={'address_json': address_json}, level=3)
    error_count = 0
    if address_json:
        status_code = 200
        if len(address_json[address]) > 15000:
            unspent = {}
            balance = 'unknown'
            utils.debug(message='too many transactions (%d or more), not calculating' % len(address_json[address]))
            error_count += 0
        else:
            unspent = {}
            balance = 0
            for txid in address_json[address]:
                tx_vin_spent_json = dbutils.select(type, 'vin_spent', txid)
                utils.debug(message={'tx_vin_spent_json': tx_vin_spent_json}, level=3)
                # Determine which vout are spent, and which are unspent.
                for vout in address_json[address][txid]:
                    # If this txid:vout pair exists in the vin_spent table, it has been spent
                    if not tx_vin_spent_json or vout not in tx_vin_spent_json[txid]:
                        value = int(Decimal(address_json[address][txid][vout]['value']) * 100000000)
                        balance += value
                        if txid in unspent:
                            unspent[txid][vout] = value,
                        else:
                            unspent[txid] = {
                                vout: value,
                                'height': address_json[address][txid][vout]['height'],
                            }

        dbutils.close_database_connection(type)
        utils.debug(message={'total elapsed': utils.elapsed(globals.start, 3)})
        return jsonify({
            'status': 'OK',
            'code': status_code,
            'debug': utils.debug(),
            'data': {
                'coin': type,
                'symbol': settings.coins[type]['symbol'],
                'address': details,
                'balance': balance,
                'unspent': unspent,
            }
        }), status_code
    else:
        status_code = 404
        dbutils.close_database_connection(type)
        utils.debug(message={'total elapsed': utils.elapsed(globals.start, 3)})
        return jsonify({
            'status': 'ERROR',
            'code': status_code,
            'error': 'address not found',
            'debug': utils.debug(),
            'data': {
                'coin': type,
                'symbol': settings.coins[type]['symbol'],
                'address': details,
            },
        }), status_code

@app.route('/api/tx/<type>/<txid>', methods=['GET'])
def get_tx(type, txid):
    message, validated = validate_type(type)
    if validated is not True:
        return message

    tx = {}
    vout = dbutils.select(type, 'vout', txid)
    utils.debug(message={'vout': vout}, level=3)
    if vout:
        tx['coinbase'] = dbutils.select(type, 'coinbase', txid)
        tx['vin'] = dbutils.select(type, 'vin_txid', txid)
        tx['vout'] = vout
        status_code = 200
        return jsonify({
            'status': 'OK',
            'code': status_code,
            'data': {
                'coin': type,
                'symbol': settings.coins[type]['symbol'],
                'tx': tx,
            }
        }), status_code
    else:
        status_code = 404
        return jsonify({
            'status': 'ERROR',
            'code': status_code,
            'error': 'txid not found',
            'data': {
                'coin': type,
                'symbol': settings.coins[type]['symbol'],
                'txid': txid,
            }
        }), status_code

@app.route('/api/block/<type>/<hash>', methods=['GET'])
def get_block(type, hash):
    message, validated = validate_type(type)
    if validated is not True:
        return message

    block_json = dbutils.select(type, 'block', hash)
    if block_json:
        status_code = 200
        return jsonify({
            'status': 'OK',
            'code': status_code,
            'data': {
                'coin': type,
                'symbol': settings.coins[type]['symbol'],
                'block': block_json,
            }
        }), status_code
    else:
        status_code = 404
        return jsonify({
            'status': 'ERROR',
            'code': status_code,
            'error': 'block hash not found',
            'data': {
                'coin': type,
                'symbol': settings.coins[type]['symbol'],
                'hash': hash,
            }
        }), status_code

# Create a generic object, and populate with parameters expected by our helper functions.
globals.args = type('', (), {})()
globals.args.db = None
globals.args.verbose = 0
globals.settings = settings
globals.db_connection = {}
globals.db = {}
globals.debug = []
globals.requests = 0
app.wsgi_app = ProxyFix(app.wsgi_app)
