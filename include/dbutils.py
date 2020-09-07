import json
import os
import time

import mysql.connector

# Custom libraries:
from include import globals
from include import utils


def lines_in_file(file):
    st = os.stat(file)
    # Optionally display extra information about the file, can be useful for debugging.
    utils.vprint(st, level=2)
    with open(file) as f:
        return sum(1 for _ in f)

def get_tables():
    return ["vin_spent", "vin_txid", "coinbase", "vout", "address", "block"]

def get_database_settings(coin):
    if coin in globals.settings.coins:
        if 'database' in globals.settings.coins[coin]:
            db_settings = globals.settings.coins[coin]['database'].copy()
        else:
            db_settings = globals.settings.database.copy()

        # Replace {coin} variable with actual coin name.
        for key in db_settings:
            db_settings[key] = db_settings[key].replace('{coin}', coin)

        utils.vprint(db_settings, level=2)

        return db_settings
    else:
        # Coin is not configured
        return None

def database_connection(coin):
    start = time.time()
    try:
        _ = globals.db_connection
    except:
        globals.db_connection = {}

    if coin not in globals.db_connection:
        utils.vprint('opening db connection')
        db_settings = get_database_settings(coin)
        globals.db_connection[coin] = mysql.connector.connect(
            host=db_settings['host'], user=db_settings['user'], password=db_settings['passwd'],
            database=db_settings['db'], allow_local_infile=True, connection_timeout=60,
            compress=True)
        globals.db_connection[coin].autocommit = True
        utils.debug(message={
            'activity': 'opened %s db connection to (host=%s user=%s database=%s)' % (coin, db_settings['host'],
                                                                                      db_settings['user'],
                                                                                      db_settings['db']),
            'elapsed': utils.elapsed(start, 5),
        }, level=2)
    return globals.db_connection[coin]

def database_cursor(coin):
    start = time.time()
    try:
        _ = globals.db
    except:
        globals.db = {}

    if coin not in globals.db:
        utils.vprint('getting db cursor')
        globals.db[coin] = database_connection(coin).cursor()
        utils.debug(message={
            'activity': 'got db cursor',
            'elapsed': utils.elapsed(start, 5),
        }, level=2)
    return globals.db[coin]

def close_database_connection(coin):
    try:
        _ = globals.db_connection
    except:
        # No connection to close
        return

    if coin in globals.db_connection:
        utils.vprint('closing db connection')
        globals.db_connection[coin].close()
        del globals.db_connection[coin]
        if coin in globals.db:
            del globals.db[coin]

def create_tables(coin):
    close_database_connection(coin)
    for table in get_tables():
        utils.vprint("creating %s.%s table (if not exists)" % (coin, table))
        query = """
        CREATE TABLE IF NOT EXISTS `%s` (
          `id` bigint(11) unsigned NOT NULL AUTO_INCREMENT,
          `hash` varchar(128) NOT NULL DEFAULT '',
          `data` longtext NOT NULL,
          PRIMARY KEY(`id`),
          KEY `hash` (`hash`(10))
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8;
        """ % table
        utils.vprint(query, level=3)
        database_cursor(coin).execute(query)

def truncate_tables(coin, tables=None):
    close_database_connection(coin)
    if not tables:
        tables = get_tables()
    for table in tables:
        utils.vprint("truncating %s.%s table" % (coin, table))
        database_cursor(coin).execute(
            """
            TRUNCATE TABLE `%s`;
            """ % (table,)
        )

def load_data_infile(coin, table):
    start = time.time()
    close_database_connection(coin)
    os.sync()
    filename = globals.args.working_path + table + '.csv'
    if os.path.isfile(filename):
        print("--> LOAD DATA LOCAL INFILE '%s' INTO TABLE %s" % (filename, table))
        print(" [[ Lines in file: %d ]] " % lines_in_file(filename))
        query = "LOAD DATA LOCAL INFILE '%s' INTO TABLE %s FIELDS TERMINATED BY '\t' OPTIONALLY ENCLOSED BY '' ESCAPED BY '\\\\' LINES TERMINATED BY '\n' (hash, data)" % (filename, table)
        utils.vprint(query, level=2)
        try:
            database_cursor(coin).execute(query)
            utils.debug({
                'activity': 'LOAD DATA INFILE query',
                'query': query,
                'elapsed': utils.elapsed(start, 5)
            }, level=2)
        except Exception as e:
            print("LOAD DATA INFILE query failed: %s" % (e,))
            print(query, table)
            exit(1)
    else:
        utils.vprint("'%s' does not exist, skipping" % filename, level=2)

def insert(coin, table, key, value):
    start = time.time()
    query = "INSERT INTO %s (`hash`, `data`) VALUES('%s', '%s')" % (table, key, json.dumps(value))
    utils.vprint(query, level=3)
    try:
        database_cursor(coin).execute(query)
        utils.debug({
            'activity': 'INSERT query',
            'query': query,
            'elapsed': utils.elapsed(start, 5)
        }, level=2)
    except Exception as e:
        print(query, table, key)
        print("data:")
        print(json.dumps(value))
        print("INSERT query failed (table=%s, key=%s): %s" % (table, key, e))
        exit(1)

def update(coin, table, key, value):
    start = time.time()
    query = "UPDATE %s SET `data` = '%s' WHERE `hash` = '%s'" % (table, json.dumps(value), key)
    utils.vprint(query, level=3)
    try:
        database_cursor(coin).execute(query)
        utils.debug({
            'activity': 'UPDATE query',
            'query': query,
            'elapsed': utils.elapsed(start, 5)
        }, level=2)
    except Exception as e:
        print(query, table, key)
        print("data:")
        print(json.dumps(value))
        print("UPDATE query failed (table=%s, key=%s): %s" % (table, key, e))
        exit(1)

def delete(coin, table, key):
    start = time.time()
    query = "DELETE FROM %s WHERE `hash` = '%s'" % (table, key)
    utils.vprint(query, level=3)
    try:
        database_cursor(coin).execute(query)
        utils.debug({
            'activity': 'DELETE query',
            'query': query,
            'elapsed': utils.elapsed(start, 5)
        }, level=2)
    except Exception as e:
        print(query, table, key)
        print("data:")
        print(json.dumps(value))
        print("DELETE query failed (table=%s, key=%s): %s" % (table, key, str(e)))
        exit(1)


def select(coin, table, key, check_if_exists=False):
    start = time.time()
    if check_if_exists:
        query = "SELECT 1 FROM %s WHERE `hash` = '%s'" % (table, key)
    else:
        query = "SELECT `data` FROM %s WHERE `hash` = '%s'" % (table, key)
    utils.vprint(query, level=3)
    result = []
    result_count = 0
    try:
        database_cursor(coin).execute(query)
        has_result = False
        result = None
        for data in database_cursor(coin):
            result_count += 1
            if has_result:
                print("WARNING: multiple results in %s for %s" % (table, key))
                # We use first matching for now
                continue
            result, = data
            has_result = True
        utils.debug({
            'activity': 'SELECT query',
            'query': query,
            'result_count': result_count,
            'elapsed': utils.elapsed(start, 5)
        }, level=2)
        if has_result:
            if check_if_exists:
                return result
            else:
                return json.loads(result)
    except Exception as e:
        print(query, table, key)
        if check_if_exists:
            result_count = result
        else:
            result_count = len(result)
        print("[%s] number of results: %d" % (key, result_count))
        print("SELECT query failed (table=%s, key=%s): %s" % (table, key, e))
        utils.debug({
            'query': query,
            'error': e,
            'result_count': result_count,
            'elapsed': utils.elapsed(start, 5)
        })
        exit(1)
