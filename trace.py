import time
import argparse
import json

# Custom libraries:
from include import dbutils
from include import utils
from include import globals
import settings

start = time.time()

def main(args):
    database = "address"
    utils.vprint(" > querying database '%s'" % database)
    address_json = dbutils.select(globals.args.type, database, args.address)
    if not address_json:
        print("address %s not found" % args.address)
        quit()

    if args.verbose > 1:
        print('address_json:', json.dumps(address_json, indent=2, sort_keys=True))

    try:
        vins = address_json[args.address]['vin']
    except:
        vins = []
    vouts = address_json[args.address]['vout']
    utils.vprint(" >> %d vins" % len(vins))
    utils.vprint(" >> %d vouts" % len(vouts))

    utils.vprint(" >> %s" % address_json, level=2)

    vin_details = []
    vin = 0
    for txid in vins:
        utils.vprint(" > querying database for vin (%s)" % vins[txid])
        tx_json = dbutils.select(globals.args.type, 'tx', txid)
        if tx_json:
            vin_details.append(tx_json)
            vin += 1
        if vin >= 100:
            vin_details.append("first 100 vin txid displayed")
            vin_details.append("total vin txid: %d" % len(vins))
            break

    vout_details = []
    vout = 0
    for txid in vouts:
        utils.vprint(" > querying database for vout (%s)" % vouts[txid])
        tx_json = dbutils.select(globals.args.type, 'tx', txid)
        if tx_json:
            vout_details.append(tx_json)
            vout += 1
        if vout >= 100:
            vout_details.append("first 100 vout txid displayed")
            vout_details.append("total vout txid: %d" % len(vouts))
            break

    print('vin:', json.dumps(vin_details, indent=2, sort_keys=True))
    print('vout:', json.dumps(vout_details, indent=2, sort_keys=True))
    print('unspent:', json.dumps(address_json[args.address]['unspent'], indent=2, sort_keys=True))
    print('balance:', json.dumps(address_json[args.address]['balance'], indent=2, sort_keys=True))

if __name__ == '__main__':
    globals.init()
    globals.settings = settings
    parser = argparse.ArgumentParser(description="Trace address activity.")
    parser.add_argument('-t', '--type', help="coin type to extract", type=str, choices=utils.supported_coins(settings), required=True)
    parser.add_argument('-a', '--address', help="address to trace", type=str, required=True)
    parser.add_argument('-d', '--db', help="full path to rocksdb directory (defaults to current directory; files can get very large)", type=int)
    parser.add_argument('-v', dest='verbose', action='count', help="verbose output")
    globals.args = parser.parse_args()
    globals.dbs = {}
    utils.vprint("starting ...", level=1)
    main(globals.args)
    utils.vprint("done!", level=1)
