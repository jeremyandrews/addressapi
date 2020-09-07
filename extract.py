import argparse
import os
import gzip
import csv
import json
import pathlib
import logging
import time
from decimal import Decimal

# Libraries that must be installed:
from tqdm import tqdm
import requests

# Custom libraries:
from include import utils
from include import dbutils
from include import rpc
from include import globals
import settings


# Work around tqdm locking bug which can cause progress bar to hang.
# https://github.com/tqdm/tqdm/issues/461
tqdm.get_lock().locks = []

def read_metadata():
    '''
    Read metadata about the CSV files we extracted from the coin daemon rest server. The metadata:
     - Allows us to re-run processing without re-downloading the data
     - Allows us to provide progress updates as we process the CSV files

    :return: contents of metadata cache if exists, or False
    '''
    if not os.path.isfile(globals.metadata_file):
        utils.vprint("metadata file %s does not exist" % globals.metadata_file)
        return {}

    with open(globals.metadata_file, 'r') as f_metadata_file:
        utils.vprint("reading metadata from %s" % globals.metadata_file, level=2)
        return json.load(f_metadata_file)

def write_metadata():
    '''
    Writes metadata about the CSV files we extracted from the coin daemon rest server.

    @TODO: un-hide file and move outside cache

    :param metadata_file: path to the metadata cache file
    :param data: metadata to write into the cache file
    :return: None
    '''
    with open(globals.metadata_file, 'w') as f_cache_metadata:
        utils.vprint("writing cache metadata to %s" % globals.metadata_file, level=2)
        json.dump(globals.metadata, f_cache_metadata, indent=2)

def extract_blockchain(args):
    '''
    Connect to the coin daemon and extract all blocks via REST requests.

    This is the first step in building a searchable database of all addresses in the blockchain. It steps through the
    entire blockchain and writes data to several CSV files which we later import into a key-value store.

    :param args: Arguments used to invoke extract script.
    :return: True or False, indicating success.
    '''
    utils.vprint("extract %s blockchain from REST server: %s" % (args.type, settings.coins[args.type]['server']))
    logging.info("extract %s blockchain from REST server: %s" % (args.type, settings.coins[args.type]['server']))

    utils.memory_snapshot()
    timer = time.time()

    if args.limit:
        blocks = args.limit
    else:
        chaininfo = utils.request_chaininfo(settings)
        blocks = chaininfo['blocks']
    # tx-vin, address-vout, tx-vout and block are used to build a local database of blockchain data.
    # types simply tracks all the different script-types we see in the blockchain.
    # segwit logs all segregated witness transactions seen in the blockchain.
    with gzip.open(args.working_path + "vin_spent.csv.gz", "wt", compresslevel=args.compress_level) as f_tx_vin_spent, \
            gzip.open(args.working_path + "vin_txid.csv.gz", "wt", compresslevel=args.compress_level) as f_tx_vin_txid, \
            gzip.open(args.working_path + "coinbase.csv.gz", "wt", compresslevel=args.compress_level) as f_tx_coinbase, \
            gzip.open(args.working_path + "vout.csv.gz", "wt", compresslevel=args.compress_level) as f_tx_vout, \
            gzip.open(args.working_path + "address.csv.gz", "wt", compresslevel=args.compress_level) as f_address, \
            gzip.open(args.working_path + "block.csv.gz", "wt", compresslevel=args.compress_level) as f_block:
        txvin_lines = txcoinbase_lines = address_lines = txvout_lines = block_lines = 0
        total_tx = 0
        csv_tx_vin_spent = csv.writer(f_tx_vin_spent)
        csv_tx_vin_txid = csv.writer(f_tx_vin_txid)
        csv_tx_coinbase = csv.writer(f_tx_coinbase)
        csv_address = csv.writer(f_address)
        csv_tx_vout = csv.writer(f_tx_vout)
        csv_block = csv.writer(f_block)

        next = globals.next_block
        print("starting with: ", next)
        request_counter = 0
        try:
            total = blocks - globals.last_processed_block_height
        except:
            total = blocks
        # use tqdm to provide a progress bar: it's based on blocks so not generally accurate for time estimates
        # because blocks can be big or small, containing lots or few transactions.
        with tqdm(total=total, desc=args.type, unit='blk', unit_scale=True, dynamic_ncols=True, smoothing=0,
                  miniters=1, mininterval=1.0) as pbar:
            while next:
                if utils.elapsed(timer) >= globals.snapshot_timer:
                    utils.memory_snapshot("memory snapshot loop")
                    timer = time.time()
                last_processed_block = next
                block = utils.request_block(next, settings)
                if block is None:
                    break
                else:
                    assert block['hash'] == next
                    block_lines += 1
                    # track and visualize how many transactions we've extracted so far
                    total_tx += len(block['tx'])
                    pbar.set_postfix(tx=total_tx, refresh=False)
                    pbar.update(1)
                    for tx in block['tx']:
                        vin_count = len(tx['vin'])
                        vout_count = len(tx['vout'])
                        csv_block.writerow([block['hash'], tx['txid'], block['height'], block['time'],
                                            vin_count, vout_count])

                        for vout in tx['vout']:
                            try:
                                for address in vout['scriptPubKey']['addresses']:
                                    # Group by txid, we will use this to process vin (looking up spent).
                                    csv_tx_vout.writerow(
                                        [tx['txid'], vout['n'], address, vout['value'], block['height'],
                                         block['hash'], block['time'], vin_count, vout_count])
                                    # Group by address, we will use this to find txids.
                                    csv_address.writerow([address, tx['txid'], vout['n'], vout['value'], block['height'], block['time']])
                                    txvout_lines += 1
                                    address_lines += 1

                            except Exception as e:
                                # https://bitcoin.stackexchange.com/questions/60463/segwit-bitcoin-json-rpc-and-strange-addresses
                                # https://github.com/bitcoin/bips/blob/master/bip-0173.mediawiki
                                csv_tx_vout.writerow(
                                    [tx['txid'], vout['n'], 'unknown', vout['value'], block['height'], block['hash'],
                                     block['time'], vin_count, vout_count])
                                # I don't think there's much value in lumping all these undecipherable addresses into
                                # a single bucket in the database. Instead we just ignore them, leaving them only in
                                # their specific transactions.
                                #csv_address.writerow(['unknown', tx['txid'], vout['n'], vout['value'], block['height'], block['time']])
                                txvout_lines += 1
                                address_lines += 1

                        vin_n = 0
                        for vin in tx['vin']:
                            try:
                                # Spent are most common, so try them first.
                                spent = vin['txid']
                                vout = vin['vout']
                                csv_tx_vin_spent.writerow([spent, vout, tx['txid'], vin_n, block['time'], block['height']])
                                csv_tx_vin_txid.writerow([tx['txid'], vin_n, spent, vout, block['time'], block['height']])
                                txvin_lines += 1
                            except:
                                coinbase = vin['coinbase']
                                # Coinbase transaction always has exactly one vin.
                                csv_tx_coinbase.writerow([tx['txid'], coinbase, vin_n, block['time'], block['height']])
                                txcoinbase_lines += 1
                                assert len(tx['vin']) == 1
                            vin_n += 1

                    request_counter += 1
                    if args.limit and request_counter >= args.limit:
                        utils.vprint("requested limit of %d blocks, finished" % (args.limit,))
                        break
                    utils.vprint("lines: txvout(%d) txvin(%d) txcoinbase(%d) address(%d) block(%d)" %
                                 (txvout_lines, txvin_lines, txcoinbase_lines, address_lines, block_lines), level=5)

                    if block and "nextblockhash" in block:
                        next = block["nextblockhash"]
                    else:
                        next = False
                        if not globals.args.initial:
                            globals.notify = {
                                'height': block['height'],
                                'hash': block['hash'],
                                'timestamp': block['time'],
                                'addresses': [],
                            }

    # If we got here, this phase completed successfully.
    return {
        'vout': txvout_lines,
        'vin_spent': txvin_lines,
        'vin_txid': txvin_lines,
        'coinbase': txcoinbase_lines,
        'address': address_lines,
        'block': block_lines,
        'last-processed-block': last_processed_block,
        'limit': args.limit,
    }

def sort_files(args):
    '''
    Sort tx-vin and tx-vout CSV files by txid, allowing us to group data per-txid.

    Sorting these files allows us to group them by txid. This allows us to more-efficiently store the data, as well as
    to more efficiently query the data.

    :param args: Arguments used to invoke extract script.
    :return: True or False, indicating success.
    '''
    # Phase 2: sort the extracted transaction CSV files
    utils.vprint("sort transaction CSV files extracted from blockchain")
    for file in ['vin_spent', 'vin_txid', 'vout', 'coinbase', 'address', 'block']:
        source_file = args.working_path + file + ".csv.gz"
        destination_file = args.working_path + file + "_sorted.csv.gz"
        utils.gzipped_sort(source_file, destination_file, lines=globals.metadata["extract_blockchain"][file])
    return True

def load_transaction_vout(args):
    '''
    Load tx-vout-sorted CSV, grouping by txid and address.

    This is sorted by txid, which should be unique with new blocks. If a txid shows up in multiple blocks
    this could be an issue, but this should not happen in modern blocks/blockchains.

    :param args: Arguments used to invoke extract script.
    :return: True or False, indicating success.
    '''
    utils.vprint("load sorted transaction vout data into the database")
    timer = time.time()
    source_file = args.working_path + 'vout_sorted.csv.gz'
    tx_json = {}
    utils.vprint("processing %s, grouping by txid" % (source_file,))
    with open(globals.args.working_path + 'vout.csv', 'wt') as vout_csv:
        csv_writer = csv.writer(vout_csv, delimiter='\t', quoting=csv.QUOTE_NONE, quotechar='', escapechar='\\')
        with gzip.open(source_file, 'rt') as csvfile:
            source_csv = csv.reader(csvfile)
            for row in tqdm(iterable=source_csv, desc='tx-vout', total=globals.metadata["extract_blockchain"]['vout'],
                            unit="txid", unit_scale=True, dynamic_ncols=True, smoothing=0, miniters=1, mininterval=1.0):
                if utils.elapsed(timer) >= globals.snapshot_timer:
                    utils.memory_snapshot("memory snapshot loop")
                    timer = time.time()

                (txid, n, address, value, height, block_hash, timestamp, vin_count, vout_count) = row
                # Convert to integer
                value = int(Decimal(value) * 100000000)
                vout = {
                    'value': value,
                    'timestamp': timestamp,
                }
                if txid in tx_json:
                    if address in tx_json[txid]['addresses']:
                        if n in tx_json[txid]['addresses'][address]:
                            utils.vprint("txid %s, address %s, n %s already exists, ignoring" % (txid, address, n))
                        else:
                            # Add a new vout for an existing address.
                            tx_json[txid]['addresses'][address][n] = vout
                    else:
                        tx_json[txid]['addresses'][address] = {
                            n: vout,
                        }
                else:
                    # New txid, write the old one
                    if tx_json:
                        old_txid, = tx_json.keys()
                        csv_writer.writerow([old_txid, json.dumps(tx_json)])
                        del tx_json
                    tx_json = {
                        txid: {
                            'height': height,
                            'block_hash': block_hash,
                            'timestamp': timestamp,
                            'vin_count': vin_count,
                            'vout_count': vout_count,
                            'addresses': {
                                address: {
                                    n: vout,
                                },
                            },
                        },
                    }
            # Write the last txid to database.
            try:
                last_txid, = tx_json.keys()
                csv_writer.writerow([last_txid, json.dumps(tx_json)])
            except:
                # Empty file, safe to ignore.
                pass
    if globals.args.initial:
        dbutils.truncate_tables(coin=globals.args.type, tables=['vout'])
    dbutils.load_data_infile(coin=globals.args.type, table='vout')
    return True

def load_transaction_vin_coinbase(args):
    '''
    Coinbase shows up one time per block: we can bulk-import this every time we process new blocks in the blockchain.

    :param args: Arguments used to invoke extract script.
    :return: True or False, indicating success.
    '''
    utils.vprint("load sorted transaction vin coinbase data into the database")
    timer = time.time()
    source_file = args.working_path + 'coinbase_sorted.csv.gz'
    utils.vprint("processing %s, grouping by txid" % (source_file,))
    with open(globals.args.working_path + 'coinbase.csv', 'wt') as coinbase_csv:
        csv_writer = csv.writer(coinbase_csv, delimiter='\t', quoting=csv.QUOTE_NONE, quotechar='', escapechar='\\')
        with gzip.open(source_file, 'rt') as csvfile:
            source_csv = csv.reader(csvfile)
            for row in tqdm(iterable=source_csv, desc='coinbase', total=globals.metadata["extract_blockchain"]['coinbase'],
                            unit="txid", unit_scale=True, dynamic_ncols=True, smoothing=0, miniters=1, mininterval=1.0):
                if utils.elapsed(timer) >= globals.snapshot_timer:
                    utils.memory_snapshot("memory snapshot loop")
                    timer = time.time()
                (txid, coinbase, vin_n, timestamp, height) = row
                tx_data = dbutils.select(coin=globals.args.type, table='vout', key=txid)
                try:
                    # A transaction can only have a single txid, so we unwrap from the returned array.
                    (vout_txid,) = tx_data.keys()
                except Exception as e:
                    print('tx_data', json.dumps(tx_data, indent=2, sort_keys=True))  # @DEBUG
                    print("INVALID DATA: tx_data is empty for txid[%s] (%s)" % (txid, e))
                    logging.warning("INVALID DATA: tx_data is empty for txid[%s] (%s)" % (txid, e))
                    '''
                    #exit(1)
                    In litecoin_testnet4 we have to ignore these errors:
                    INVALID DATA: tx_data is empty for txid[ffffc6459cea025951a3eba5bb5fe2d6eb4df03ce9589fcc333553c1748ec683] ('NoneType' object has no attribute 'keys')
                    tx_data null
                    INVALID DATA: tx_data is empty for txid[ffffccd3189eb1eb4052aaa4195aace8f49c664bd589272e448495617aa6f1e0] ('NoneType' object has no attribute 'keys')
                    tx_data null
                    INVALID DATA: tx_data is empty for txid[ffffdc5b5db9a9e2aefba05b9469e49809da50ee36d4a69c8a1310cbdcf4b48b] ('NoneType' object has no attribute 'keys')
                    tx_data null
                    INVALID DATA: tx_data is empty for txid[fffffe73c83ef944a15242bccaa12a4cba160f41dc0eb0c34eb7b8bd661ac3a2] ('NoneType' object has no attribute 'keys')
                    Continuing instead of exiting.
                    '''
                    continue

                # Coinbase can be sent to any number of addresses:
                value = 0
                #print('tx_data', json.dumps(tx_data, indent=2, sort_keys=True)) # @DEBUG
                #print('vout_txid', vout_txid)
                for address in tx_data[vout_txid]['addresses']:
                    for vout in tx_data[vout_txid]['addresses'][address]:
                        value += tx_data[vout_txid]['addresses'][address][vout]['value']

                coinbase_json = {
                    txid: {
                        'value': value,
                        'coinbase': coinbase,
                        'vin_n': vin_n,
                        'timestamp': timestamp,
                        'height': height,
                    }
                }
                txid, = coinbase_json.keys()
                csv_writer.writerow([txid, json.dumps(coinbase_json)])
    if globals.args.initial:
        dbutils.truncate_tables(coin=globals.args.type, tables=['coinbase'])
    dbutils.load_data_infile(coin=globals.args.type, table='coinbase')
    return True

def load_transaction_vin_spent(args):
    '''
    We may spend from a txid that has other spent vout; this requires checking for an existing entry in the
    database so can't be bulk-imported after the --initial pass.

    :param args:
    :return:
    '''
    utils.vprint("load sorted transaction vin data into the database")
    timer = time.time()
    source_file = args.working_path + 'vin_spent_sorted.csv.gz'
    spent_json = {}
    utils.vprint("processing %s, grouping by spent" % (source_file,))
    with open(globals.args.working_path + 'vin_spent.csv', 'wt') as vin_spent_csv:
        csv_writer = csv.writer(vin_spent_csv, delimiter='\t', quoting=csv.QUOTE_NONE, quotechar='',
                                escapechar='\\')
        with gzip.open(source_file, 'rt') as csvfile:
            source_csv = csv.reader(csvfile)
            for row in tqdm(iterable=source_csv, desc='tx-vin', total=globals.metadata["extract_blockchain"]['vin_spent'],
                            unit="spent", unit_scale=True, dynamic_ncols=True, smoothing=0, miniters=1, mininterval=1.0):
                if utils.elapsed(timer) >= globals.snapshot_timer:
                    utils.memory_snapshot("memory snapshot loop")
                    timer = time.time()
                (spent, vout, txid, vin_n, timestamp, height) = row

                if not globals.args.initial and not spent_json:
                    # This is the first row of the CSV, and we're updating an existing database; see if txid already exists.
                    spent_json = dbutils.select(coin=globals.args.type, table='vin_spent', key=spent)
                    if not spent_json:
                        # Not found, restore spent_json from None to empty dictionary.
                        spent_json = {}

                if spent in spent_json:
                    spent_json[spent][vout] = {
                        'timestamp': timestamp,
                        'height': height,
                        'txid': txid,
                        'vin_n': vin_n,
                    }
                else:
                    # New spent, write the previous one to the database.
                    if spent_json:
                        old_spent, = spent_json.keys()
                        if globals.args.initial:
                            # We do bulk inserts on the first pass through the blockchain.
                            csv_writer.writerow([old_spent, json.dumps(spent_json)])
                        else:
                            # We do per-spent inserts on subsequent passes
                            spent_exists = dbutils.select(coin=globals.args.type, table='vin_spent', key=old_spent,
                                                          check_if_exists=True)
                            if spent_exists:
                                dbutils.update(coin=globals.args.type, table='vin_spent', key=old_spent, value=spent_json)
                            else:
                                dbutils.insert(coin=globals.args.type, table='vin_spent', key=old_spent, value=spent_json)
                        del spent_json

                    if not globals.args.initial:
                        spent_json = dbutils.select(coin=globals.args.type, table='vin_spent', key=spent)
                    else:
                        spent_json = None

                    if spent_json:
                        spent_json[spent][vout] = {
                            'timestamp': timestamp,
                            'height': height,
                            'txid': txid,
                            'vin_n': vin_n,
                        }
                    else:
                        spent_json = {
                            spent: {
                                vout: {
                                    'timestamp': timestamp,
                                    'height': height,
                                    'txid': txid,
                                    'vin_n': vin_n,
                                }
                            }
                        }

            try:
                # Write the last vin to the csv.
                last_spent, = spent_json.keys()
                if globals.args.initial:
                    csv_writer.writerow([last_spent, json.dumps(spent_json)])
                else:
                    spent_exists = dbutils.select(coin=globals.args.type, table='vin_spent', key=last_spent,
                                                  check_if_exists=True)
                    if spent_exists:
                        dbutils.update(coin=globals.args.type, table='vin_spent', key=last_spent, value=spent_json)
                    else:
                        dbutils.insert(coin=globals.args.type, table='vin_spent', key=last_spent, value=spent_json)
            except Exception as e:
                # Exception raised when nothing was spent.
                utils.vprint("no spent processed")
                logging.info("no spent processed: %s" % e)
                pass

    if globals.args.initial:
        # Bulk load the entire CSV file on the first pass through the blockchain.
        dbutils.truncate_tables(coin=globals.args.type, tables=['vin_spent'])
        dbutils.load_data_infile(coin=globals.args.type, table='vin_spent')
    return True

def load_transaction_vin_txid(args):
    '''
    Except for where instances of a duplicate txid, this will always be new txid each time we process
    new blocks. In the case of a duplicate txid, it could result in the txid showing up in the database twice,
    which will generate errors when we query it (because our API code throws an exception if a single txid is found
    more than once in the database) -- this shouldn't affect us, as it seems to only happen early in a coin's
    existence (ie when first forking bitcoin) and is a solved issue in all coins we've reviewed.
    '''
    utils.vprint("load sorted transaction vin data into the database")
    timer = time.time()
    source_file = args.working_path + 'vin_txid_sorted.csv.gz'
    txid_json = {}
    utils.vprint("processing %s, grouping by txid" % (source_file,))
    with open(globals.args.working_path + 'vin_txid.csv', 'wt') as txvin_csv:
        csv_writer = csv.writer(txvin_csv, delimiter='\t', quoting=csv.QUOTE_NONE, quotechar='', escapechar='\\')
        with gzip.open(source_file, 'rt') as csvfile:
            source_csv = csv.reader(csvfile)
            for row in tqdm(iterable=source_csv, desc='tx-vin', total=globals.metadata["extract_blockchain"]['vin_txid'],
                            unit="txid", unit_scale=True, dynamic_ncols=True, smoothing=0, miniters=1, mininterval=1.0):
                if utils.elapsed(timer) >= globals.snapshot_timer:
                    utils.memory_snapshot("memory snapshot loop")
                    timer = time.time()
                (txid, vin_n, spent, vout, timestamp, height) = row

                if txid in txid_json:
                    txid_json[txid]['vin'][vin_n] = {
                        'spent': spent,
                        'vout': vout,
                    }
                else:
                    if txid_json:
                        old_txid, = txid_json.keys()
                        csv_writer.writerow([old_txid, json.dumps(txid_json)])
                        del txid_json

                    txid_json = {
                        txid: {
                            'timestamp': timestamp,
                            'height': height,
                            'vin': {
                                vin_n: {
                                    'spent': spent,
                                    'vout': vout,
                                }
                            }
                        }
                    }

            # Write the last vin to database.
            try:
                last_txid, = txid_json.keys()
                csv_writer.writerow([last_txid, json.dumps(txid_json)])
            except Exception as e:
                # Exception raised when nothing was spent.
                utils.vprint("no txid processed")
                logging.info("no txid processed: %s" % e)
                pass

    if globals.args.initial:
        dbutils.truncate_tables(coin=globals.args.type, tables=['vin_txid'])
    dbutils.load_data_infile(coin=globals.args.type, table='vin_txid')
    return True

def load_address(args):
    '''
    Load addresses, grouping by txid and vout (n).

    Addresses can be re-used at any point, so this will require checking if the address already exists each time
    a new block is processed.

    :param args: Arguments used to invoke extract script.
    :return: True or False, indicating success.
    '''
    utils.vprint("load sorted address data into the database")
    timer = time.time()
    source_file = args.working_path + 'address_sorted.csv.gz'
    address_json = {}
    utils.vprint("processing %s, grouping by address, txid" % (source_file,))
    with open(globals.args.working_path + 'address.csv', 'wt') as address_csv:
        csv_writer = csv.writer(address_csv, delimiter='\t', quoting=csv.QUOTE_NONE, quotechar='', escapechar='\\')
        with gzip.open(source_file, 'rt') as csvfile:
            source_csv = csv.reader(csvfile)
            for row in tqdm(iterable=source_csv, desc='addresses', total=globals.metadata["extract_blockchain"]['address'],
                            unit="adr", unit_scale=True, dynamic_ncols=True, smoothing=0, miniters=1, mininterval=1.0):

                # Optional logging/tracing of memory usage
                if utils.elapsed(timer) >= globals.snapshot_timer:
                    utils.memory_snapshot("memory snapshot loop")
                    timer = time.time()

                (address, txid, n, value, height, timestamp) = row

                if not globals.args.initial and not address_json:
                    # This is the first row of the CSV, and we're updating an existing database; see if address already
                    #  exists.
                    address_json = dbutils.select(coin=globals.args.type, table='address', key=address)
                    if not address_json:
                        # Not found, restore address_json from None to empty dictionary.
                        address_json = {}

                if address in address_json:
                    if 'skip' not in address_json[address]:
                        if len(address_json[address]) > 1000000:
                            # For now, completely skip over addresses with 1 million+ transactions.
                            # @TODO FIXME ^^ Optimize handling of addresses with lots of transactions.
                            address_json = {
                                address: {
                                    'skip': True,
                                }
                            }
                            logging.warning('skipping address %s with > 1 million transactions' % address)
                        if txid in address_json[address]:
                            address_json[address][txid][n] = {
                                'value': value,
                                'height': height,
                                'timestamp': timestamp,
                            }
                        else:
                            address_json[address][txid] = {
                                n: {
                                    'value': value,
                                    'height': height,
                                    'timestamp': timestamp,
                                },
                            }
                else:
                    # Save previous address, unless this is the first row of the CSV and there is no previous address.
                    if address_json:
                        old_address, = address_json.keys()
                        if globals.args.initial:
                            # We do bulk inserts on the first pass through the blockchain.
                            csv_writer.writerow([old_address, json.dumps(address_json)])
                        else:
                            # We do per-address inserts on subsequent passes
                            address_exists = dbutils.select(coin=globals.args.type, table='address', key=old_address,
                                                            check_if_exists=True)
                            if address_exists:
                                dbutils.update(coin=globals.args.type, table='address', key=old_address,
                                               value=address_json)
                            else:
                                dbutils.insert(coin=globals.args.type, table='address', key=old_address,
                                               value=address_json)
                            try:
                                globals.notify['addresses'].append(old_address)
                            except:
                                # If this isn't defined, we're running a catch-up job, and aren't
                                # going to bother notifying the UI as it could be a massive
                                # number of addresses.
                                pass
                        del address_json

                    # Previous address saved, start the next
                    if not globals.args.initial:
                        address_json = dbutils.select(coin=globals.args.type, table='address', key=address)
                    else:
                        address_json = False

                    if address_json:
                        if txid in address_json[address]:
                            address_json[address][txid][n] = {
                                'value': value,
                                'height': height,
                                'timestamp': timestamp,
                            }
                        else:
                            address_json[address][txid] = {
                                n: {
                                    'value': value,
                                    'height': height,
                                    'timestamp': timestamp,
                                }
                            }
                    else:
                        address_json = {
                            address: {
                                txid: {
                                    n: {
                                        'value': value,
                                        'height': height,
                                        'timestamp': timestamp,
                                    }
                                }
                            }
                        }

            try:
                address, = address_json.keys()
                if globals.args.initial:
                    csv_writer.writerow([address, json.dumps(address_json)])
                else:
                    address_exists = dbutils.select(coin=globals.args.type, table='address', key=address,
                                                    check_if_exists=True)
                    if address_exists:
                        dbutils.update(coin=globals.args.type, table='address', key=address, value=address_json)
                    else:
                        dbutils.insert(coin=globals.args.type, table='address', key=address, value=address_json)
                    globals.notify['addresses'].append(address)
            except Exception as e:
                # Exception raised when there is no address to process.
                utils.vprint("no addresses processed: %s" % e)
                logging.info("no addresses processed: %s" % e)
                pass

    if globals.args.initial:
        # Bulk load the entire CSV file on the first pass through the blockchain.
        dbutils.truncate_tables(coin=globals.args.type, tables=['address'])
        dbutils.load_data_infile(coin=globals.args.type, table='address')

    return True

def load_block(args):
    '''
    :param args: Arguments used to invoke extract script.
    :return: True or False, indicating success.
    '''
    utils.vprint("load sorted blocks data into the database")
    timer = time.time()
    source_file = args.working_path + 'block_sorted.csv.gz'
    block_json = {}
    utils.vprint("processing %s, grouping by block hash" % (source_file,))
    with open(globals.args.working_path + 'block.csv', 'wt') as block_csv:
        csv_writer = csv.writer(block_csv, delimiter='\t', quoting=csv.QUOTE_NONE, quotechar='', escapechar='\\')
        with gzip.open(source_file, 'rt') as csvfile:
            source_csv = csv.reader(csvfile)
            for row in tqdm(iterable=source_csv, desc='block', total=globals.metadata["extract_blockchain"]['block'],
                            unit="block", unit_scale=True, dynamic_ncols=True, smoothing=0, miniters=1, mininterval=1.0):
                if utils.elapsed(timer) >= globals.snapshot_timer:
                    utils.memory_snapshot("memory snapshot loop")
                    timer = time.time()
                (hash, txid, height, timestamp, vin_count, vout_count) = row

                if hash in block_json:
                    block_json[hash]['tx'][txid] = {
                        'vin_vount': vin_count,
                        'vout_count': vout_count,
                    }
                else:
                    if block_json:
                        old_hash, = block_json.keys()
                        csv_writer.writerow([old_hash, json.dumps(block_json)])
                        del block_json

                    block_json = {
                        hash: {
                            'height': height,
                            'timestamp': timestamp,
                            'tx': {
                                txid: {
                                    'vin_vount': vin_count,
                                    'vout_count': vout_count,
                                }
                            }
                        }
                    }

            # Write the last vin to database.
            try:
                last_hash, = block_json.keys()
                csv_writer.writerow([last_hash, json.dumps(block_json)])
            except:
                # Empty file, safe to ignore.
                pass
    if globals.args.initial:
        dbutils.truncate_tables(coin=globals.args.type, tables=['block'])
    dbutils.load_data_infile(coin=globals.args.type, table='block')
    return True

def get_phases(as_strings=False):
    phases = [extract_blockchain, sort_files, load_transaction_vout, load_transaction_vin_coinbase,
              load_transaction_vin_spent, load_transaction_vin_txid, load_address, load_block]
    if as_strings:
        return [phase.__name__ for phase in phases]
    else:
        return phases

def unwind_orphan_chain(block):
    # Load the entire block, with transactions
    block = utils.request_block(block["hash"], settings)
    while block["confirmations"] < 0:
        utils.vprint("- removing orphaned block: %s" % block["hash"], level=2)
        logging.warning("- removing orphaned block: %s" % block["hash"])
        dbutils.delete(coin=globals.args.type, table='block', key=block["hash"])

        globals.notify = {
            'height': block['height'],
            'hash': block['hash'],
            'timestamp': block['time'],
            'addresses': [],
        }

        for tx in block["tx"]:
            utils.vprint("-- removing tx %s" % tx["txid"], level=2)
            logging.info("-- removing tx %s" % tx["txid"])

            utils.vprint("--- removing %d vin" % len(tx["vin"]), level=2)
            logging.info("--- removing %d vin" % len(tx["vin"]))
            for vin in tx['vin']:
                try:
                    # Spent are most common, so try them first.
                    utils.vprint("--- affected vin spent txid: %s vout %d" % (vin['txid'], vin['vout']), level=2)
                    logging.info("--- affected vin spent txid: %s vout %d" % (vin['txid'], vin['vout']))
                    dbutils.delete(coin=globals.args.type, table='vin_txid', key=tx["txid"])
                    dbutils.delete(coin=globals.args.type, table='vin_spent', key=vin["txid"])
                except:
                    utils.vprint("--- affected vin coinbase %s" % vin['coinbase'], level=2)
                    logging.info("--- affected vin coinbase %s" % vin['coinbase'])
                    dbutils.delete(coin=globals.args.type, table='coinbase', key=tx["txid"])

            utils.vprint("--- removing %d vout" % len(tx["vout"]), level=2)
            logging.info("--- removing %d vout" % len(tx["vout"]))
            dbutils.delete(coin=globals.args.type, table='vout', key=tx["txid"])
            for vout in tx['vout']:
                try:
                    for address in vout['scriptPubKey']['addresses']:
                        utils.vprint("--- affected address: %s" % address, level=2)
                        logging.info("--- affected address: %s" % address)
                        utils.vprint("--- affected vout: n: %d value: %s" % (vout["n"], vout["value"]), level=2)
                        logging.info("--- affected vout: n: %d value: %s" % (vout["n"], vout["value"]))
                        globals.notify['addresses'].append(address)
                        address_json = dbutils.select(coin=globals.args.type, table='address', key=address)
                        need_to_update = True
                        del(address_json[address][tx["txid"]][str(vout["n"])])
                        if len(address_json[address][tx["txid"]]) == 0:
                            del(address_json[address][tx["txid"]])
                            if len(address_json[address]) == 0:
                                dbutils.delete(coin=globals.args.type, table='address', key=address)
                                need_to_update = False
                        if need_to_update:
                            dbutils.update(coin=globals.args.type, table='address', key=address, value=address_json)

                except Exception as e:
                    # https://bitcoin.stackexchange.com/questions/60463/segwit-bitcoin-json-rpc-and-strange-addresses
                    # https://github.com/bitcoin/bips/blob/master/bip-0173.mediawiki
                    utils.vprint("--- affected address: unknown", level=2)
                    logging.info("--- affected address: unknown (%s)" % e)
                    utils.vprint("--- affected vout: n: %s value: %s" % (vout["n"], vout["value"]), level=2)
                    logging.info("--- affected vout: n: %s value: %s" % (vout["n"], vout["value"]))

                    # Remove: we no longer track "unknown" addresses.
                    '''
                    address = 'unknown'
                    address_json = dbutils.select(coin=globals.args.type, table='address', key=address)
                    need_to_update = True
                    if tx["txid"] in address_json[address] and str(vout["n"]) in address_json[address][tx["txid"]]:
                        del(address_json[address][tx["txid"]][str(vout["n"])])
                        if len(address_json[address][tx["txid"]]) == 0:
                            del(address_json[address][tx["txid"]])
                            if len(address_json[address]) == 0:
                                dbutils.delete(coin=globals.args.type, table='address', key=address)
                                need_to_update = False
                        if need_to_update:
                            dbutils.update(coin=globals.args.type, table='address', key=address, value=address_json)
                    '''

        # The orphaned block is now removed, notify backend then load previous block looking for valid chain
        notify_colpo('orphan block')
        block = utils.request_block(block["previousblockhash"], settings)

    # If we got here, we're back on the main blockchain
    utils.vprint("++ found main blockchain", level=2)
    logging.info("++ found main blockchain")
    return block

def notify_colpo(event):
    # Only send notifications for blockchain updates, not initial extraction.
    if not globals.args.initial:
        message = {
            'event': event,
            'type': globals.args.type,
            'symbol': settings.coins[globals.args.type]['symbol'],
            'height': globals.notify['height'],
            'hash': globals.notify['hash'],
            'timestamp': globals.notify['timestamp'],
            'addresses': ",".join(set(globals.notify['addresses'])),
        }
        try:
            requests.post(url=settings.new_block_notification, data=message, timeout=60.0)
            utils.vprint("notified colpo at %s" % settings.new_block_notification)
            logging.info("notified colpo at %s" % settings.new_block_notification)
        except Exception as e:
            utils.vprint("failed to notify colpo at %s: %s" % (settings.new_block_notification, e))
            logging.warning("failed to notify colpo at %s: %s" % (settings.new_block_notification, e))

def main(args):
    try:
        log_file = settings.extract_log['file'].replace('{coin}', args.type)
    except:
        log_file = 'extract.log'

    try:
        log_level = settings.extract_log['level']
    except:
        log_level = 'INFO'

    logging.basicConfig(format='%(asctime)s:%(levelname)s:%(message)s', filename=log_file, level=log_level)
    logging.info(" -- START: extracting %s blockchain" % args.type)

    utils.memory_snapshot("initial memory snapshot")

    utils.vprint("extracting %s blockchain" % args.type)

    # Determine where to read/write working csv files.
    working_path = utils.working_path()
    utils.vprint("writing temporary csv files in %s" % (working_path))
    try:
        pathlib.Path(working_path).mkdir(parents=True, exist_ok=True)
    except Exception as e:
        logging.warning("Failed to create temporary directory [%s]: %s" % (working_path, e))
        print("Failed to create temporary directory [%s]: %s" % (working_path, e))
    args.working_path = working_path

    # Optionally adjust compression level of CSV working files.
    if not args.compress_level:
        args.compress_level = 6
    utils.vprint("csv file compression level: %d" % (args.compress_level), level=2)

    # Store some metadata allowing the process to stop and resume, and to track the number of lines in each file to
    # efficiently provide progress meters.
    globals.metadata_file = working_path + "metadata"
    globals.metadata = read_metadata()

    dbutils.create_tables(globals.args.type)

    # Optionally cleanup all variations of all CSV files
    if globals.args.cleanup:
        for file in ['address', 'block', 'coinbase', 'vin', 'vout']:
            path = pathlib.Path("%s/%s*" % (working_path, file))
            for p in pathlib.Path(path.parent).expanduser().glob(path.name):
                logging.info("deleting file %s" % p)
                p.unlink()

    # List of functions to invoke.
    phases = get_phases()

    phase_counter = 0
    for phase in phases:
        phase_counter += 1

        if args.phase:
            if args.phase != phase.__name__:
                # Skip until we get to the desired phase
                utils.vprint("skipping phase %s (%d)" % (phase.__name__, phase_counter))
                logging.info("skipping phase %s (%d)" % (phase.__name__, phase_counter))
                continue
            else:
                args.phase = None

        if phase_counter == 1 and args.regenerate:
            # Regenerate flag; flush metadata.
            utils.vprint("-r flag set, ignoring metadata and regenerating CSV files")
            globals.metadata = {}
            dbutils.truncate_tables(args.type)

        if phase_counter == 1:
            if globals.args.initial:
                genesis_hash = rpc.rpc_request(method='getblockhash', parameters=[0])
                if not genesis_hash:
                    try:
                        genesis_hash = settings.coins[args.type]['genesis_hash']
                    except:
                        logging.critical("failed to determine genesis hash")
                        print("failed to determine genesis hash")
                        exit(1)
                globals.next_block = genesis_hash
                logging.info("starting with genesis hash (%d: %s)" % (0, genesis_hash))
                utils.vprint("starting with genesis hash (%d: %s)" % (0, genesis_hash))
            else:
                # @TODO wrap this in a transaction so we can restart only new blocks
                # Determine the last block extracted:
                last_processed_hash = globals.metadata["extract_blockchain"]["last-processed-block"]
                last_processed_block = rpc.rpc_request(method='getblock', parameters=[last_processed_hash])
                globals.last_processed_block_height = last_processed_block["height"]
                confirmations = last_processed_block["confirmations"]
                logging.info(">> resuming where we left off (%d: %s)" % (globals.last_processed_block_height,
                                                                         last_processed_hash))
                utils.vprint(">> resuming where we left off (%d: %s)" % (globals.last_processed_block_height,
                                                                         last_processed_hash))

                if confirmations < 0:
                    utils.vprint("!! this is an orphaned block")
                    last_processed_block = unwind_orphan_chain(last_processed_block)
                    logging.info("metadata before: %s" % globals.metadata)
                    globals.metadata["extract_blockchain"]["last-processed-block"] = last_processed_block["hash"]
                    logging.info("metadata after: %s" % globals.metadata)
                    write_metadata()
                try:
                    globals.next_block = last_processed_block["nextblockhash"]
                except Exception as e:
                    utils.vprint("no nextblockhash, end of blockchain")
                    logging.info("no nextblockhash, end of blockchain: %s" % e)
                    exit(0)

        utils.vprint("phase %d, invoking %s()" % (phase_counter, phase.__name__,))
        logging.info("phase %d, invoking %s()" % (phase_counter, phase.__name__,))
        utils.memory_snapshot()
        rc = phase(args)
        if rc:
            globals.metadata.update({
                phase.__name__: rc,
            })
            write_metadata()
        else:
            utils.vprint("phase %d, %s() failed, exiting" % (phase_counter, phase.__name__,))
            exit(1)
        dbutils.close_database_connection(args.type)

        if args.single:
            utils.vprint("--single flag set, exiting now after running a single phase")
            exit(0)
    globals.metadata.update({
        'completed': True,
    })
    write_metadata()

    notify_colpo('new block')

if __name__ == '__main__':
    globals.init()
    parser = argparse.ArgumentParser(description="Extract blockchain")
    parser.add_argument('-t', '--type', help="coin type to extract", type=str, choices=utils.supported_coins(settings), required=True)
    parser.add_argument('-p', '--phase', help="restart specific phase", type=str, choices=get_phases(as_strings=True))
    parser.add_argument('-l', '--limit', help="limit the number of blocks to process", type=int)
    parser.add_argument('--working', help="full path to working directory (defaults to current directory; files can get very large)", type=str)
    parser.add_argument('-r', '--regenerate', help="regenerate temporary CSV files", action="store_true")
    parser.add_argument('--validate', help="perform extra data validations (slower)", action="store_true")
    parser.add_argument('--initial', help="initial pass, optimize import", action="store_true")
    parser.add_argument('--cleanup', help="cleanup files on startup", action="store_true")
    parser.add_argument('--single', help="run only a single phase", action="store_true")
    parser.add_argument('--compress-level', help="compress level for temporary files (0-9, defaults to 6)", type=int)
    parser.add_argument('--host', help="coin daemon host and port (for example 'localhost:8332')", type=str)
    parser.add_argument('-v', dest='verbose', action='count', help="verbose output", default=0)
    globals.args = parser.parse_args()
    globals.settings = settings

    if globals.args.phase and globals.args.regenerate:
        print("--phase and --regenerate are mutually exclusive options, exiting")
        exit(2)

    if globals.args.phase and globals.args.cleanup:
        print("--phase and --cleanup are mutually exclusive options, exiting")
        exit(2)

    if globals.args.regenerate and globals.args.cleanup:
        print("--regenerate and --cleanup are mutually exclusive options, exiting")
        exit(2)

    try:
        globals.snapshot_memory = globals.settings.extract_log['snapshot_memory']
    except:
        globals.snapshot_memory = False
    try:
        globals.snapshot_timer = globals.settings.extract_log['snapshot_timer']
    except:
        globals.snapshot_timer = 300

    utils.vprint("starting ...")
    rc = main(globals.args)
    utils.vprint("done!")
    utils.memory_snapshot("final memory snapshot")
    logging.info(" -- END %s" % globals.args.type)
    exit(rc)
