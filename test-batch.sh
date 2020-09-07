#!/usr/bin/env bash

DATABASE='litecoin_testnet4'
MYSQL_REMOTE_COMMAND="mysql -uexchange -pexchange -hexchange-db $DATABASE -e"

# Parameters:
#  - none
# Drops and recreates database.
recreate_database() {
    mysql -uexchange -pexchange -hexchange-db -e "DROP DATABASE IF EXISTS $DATABASE"
    mysql -uexchange -pexchange -hexchange-db -e "CREATE DATABASE $DATABASE"
}

# Parameters:
#  - none
# Counts all tables in current database.
count_tables () {
    $MYSQL_REMOTE_COMMAND "SHOW TABLES" | wc -l
}

# Parameters:
#  1) full  path to file
# Counts line in specified file.
length_of_file () {
    if [ -f $1 ]
    then
        wc -l $1 | awk '{print $1}'
    else
        echo 0
    fi
}

# Parameters:
#  1) name of table
#  2) [optional] anything makes query add (DISTINCT hash)
# Counts rows in specified table.
sizeof_table () {
    if [ $2 ]
    then
        $MYSQL_REMOTE_COMMAND "SELECT COUNT(DISTINCT hash) FROM ${1}" | tail -1
    else
        $MYSQL_REMOTE_COMMAND "SELECT COUNT(hash) FROM ${1}" | tail -1
    fi
}

_test_initial() {
    BLOCKS=10000
    recreate_database
    python -u ./extract.py -vv -t litecoin_testnet4 --initial --cleanup -l ${BLOCKS}

    LENGTH_ADDRESS_FILE=$( length_of_file "/app/blockchain_data/${DATABASE}/address.csv" )
    LENGTH_BLOCK_FILE=$( length_of_file "/app/blockchain_data/${DATABASE}/block.csv" )
    LENGTH_COINBASE_FILE=$( length_of_file "/app/blockchain_data/${DATABASE}/coinbase.csv" )
    LENGTH_VIN_SPENT_FILE=$( length_of_file "/app/blockchain_data/${DATABASE}/vin_spent.csv" )
    LENGTH_VIN_TXID_FILE=$( length_of_file "/app/blockchain_data/${DATABASE}/vin_txid.csv" )
    LENGTH_VOUT_FILE=$( length_of_file "/app/blockchain_data/${DATABASE}/vout.csv" )

    assertEquals ${BLOCKS} ${LENGTH_BLOCK_FILE}

    ADDRESS_COUNT=$( sizeof_table 'address' )
    ADDRESS_COUNT_UNIQUE=$( sizeof_table 'address' UNIQUE )
    echo "addresses: ${ADDRESS_COUNT}, unique addresses: ${ADDRESS_COUNT_UNIQUE}"
    assertEquals ${LENGTH_ADDRESS_FILE} ${ADDRESS_COUNT}
    assertEquals ${ADDRESS_COUNT} ${ADDRESS_COUNT_UNIQUE}

    BLOCK_COUNT=$( sizeof_table 'block' )
    BLOCK_COUNT_UNIQUE=$( sizeof_table 'block' UNIQUE )
    echo "blocks: ${BLOCK_COUNT}, unique blocks: ${BLOCK_COUNT_UNIQUE}"
    assertEquals ${BLOCKS} ${BLOCK_COUNT}
    assertEquals ${BLOCK_COUNT} ${BLOCK_COUNT_UNIQUE}

    COINBASE_COUNT=$( sizeof_table 'coinbase' )
    COINBASE_COUNT_UNIQUE=$( sizeof_table 'coinbase' UNIQUE )
    echo "coinbase: ${COINBASE_COUNT}, unique coinbase: ${COINBASE_COUNT_UNIQUE}"
    assertEquals ${LENGTH_COINBASE_FILE} ${COINBASE_COUNT}
    assertEquals ${COINBASE_COUNT} ${COINBASE_COUNT_UNIQUE}

    VIN_SPENT_COUNT=$( sizeof_table 'vin_spent' )
    VIN_SPENT_COUNT_UNIQUE=$( sizeof_table 'vin_spent' UNIQUE )
    echo "vin_spent: ${VIN_SPENT_COUNT}, unique vin_spent: ${VIN_SPENT_COUNT_UNIQUE}"
    assertEquals ${LENGTH_VIN_SPENT_FILE} ${VIN_SPENT_COUNT}
    assertEquals ${VIN_SPENT_COUNT} ${VIN_SPENT_COUNT_UNIQUE}

    VIN_TXID_COUNT=$( sizeof_table 'vin_txid' )
    VIN_TXID_COUNT_UNIQUE=$( sizeof_table 'vin_txid' UNIQUE )
    echo "vin_txid: ${VIN_TXID_COUNT}, unique vin_txid: ${VIN_TXID_COUNT_UNIQUE}"
    assertEquals ${LENGTH_VIN_TXID_FILE} ${VIN_TXID_COUNT}
    assertEquals ${VIN_TXID_COUNT} ${VIN_TXID_COUNT_UNIQUE}

    VOUT_COUNT=$( sizeof_table 'vout' )
    VOUT_COUNT_UNIQUE=$( sizeof_table 'vout' UNIQUE )
    echo "vout: ${VOUT_COUNT}, unique vout: ${VOUT_COUNT_UNIQUE}"
    assertEquals ${LENGTH_VOUT_FILE} ${VOUT_COUNT}
    assertEquals ${VOUT_COUNT} ${VOUT_COUNT_UNIQUE}
}

test_batch() {
    BLOCKS=500000
    recreate_database
    python -u ./extract.py -vv -t litecoin_testnet4 --initial -l ${BLOCKS}

    LENGTH_ADDRESS_FILE=$( length_of_file "/app/blockchain_data/${DATABASE}/address.csv" )
    ADDRESS_COUNT_NOBATCH=$( sizeof_table 'address' )
    LENGTH_BLOCK_FILE=$( length_of_file "/app/blockchain_data/${DATABASE}/block.csv" )
    BLOCK_COUNT_NOBATCH=$( sizeof_table 'block' )
    LENGTH_COINBASE_FILE=$( length_of_file "/app/blockchain_data/${DATABASE}/coinbase.csv" )
    COINBASE_COUNT_NOBATCH=$( sizeof_table 'coinbase' )
    LENGTH_VIN_SPENT_FILE=$( length_of_file "/app/blockchain_data/${DATABASE}/vin_spent.csv" )
    VIN_SPENT_COUNT_NOBATCH=$( sizeof_table 'vin_spent' )
    LENGTH_VIN_TXID_FILE=$( length_of_file "/app/blockchain_data/${DATABASE}/vin_txid.csv" )
    VIN_TXID_COUNT_NOBATCH=$( sizeof_table 'vin_txid' )
    LENGTH_VOUT_FILE=$( length_of_file "/app/blockchain_data/${DATABASE}/vout.csv" )
    VOUT_COUNT_NOBATCH=$( sizeof_table 'vout' )

    recreate_database
    python -u ./extract.py -vv -t litecoin_testnet4 --cleanup --initial -l 100000
    python -u ./extract.py -vv -t litecoin_testnet4 --cleanup -l 100000
    python -u ./extract.py -vv -t litecoin_testnet4 --cleanup -l 100000
    python -u ./extract.py -vv -t litecoin_testnet4 --cleanup -l 100000
    python -u ./extract.py -vv -t litecoin_testnet4 --cleanup -l 100000

    ADDRESS_COUNT=$( sizeof_table 'address' )
    ADDRESS_COUNT_UNIQUE=$( sizeof_table 'address' UNIQUE )
    echo " -- addresses: ${ADDRESS_COUNT}, unique addresses: ${ADDRESS_COUNT_UNIQUE}"
    echo "assert lines in file matches rows in database"
    assertEquals ${LENGTH_ADDRESS_FILE} ${ADDRESS_COUNT}
    echo "assert unique rows in database matches non-unique rows in database"
    assertEquals ${ADDRESS_COUNT_UNIQUE} ${ADDRESS_COUNT}
    echo "assert rows in non-batch database matches rows in batch database"
    assertEquals ${ADDRESS_COUNT_NOBATCH} ${ADDRESS_COUNT}

    BLOCK_COUNT=$( sizeof_table 'block' )
    BLOCK_COUNT_UNIQUE=$( sizeof_table 'block' UNIQUE )
    echo " -- blocks: ${BLOCK_COUNT}, unique blocks: ${BLOCK_COUNT_UNIQUE}"
    echo "assert number of blocks extracted matches rows in database"
    assertEquals ${BLOCKS} ${BLOCK_COUNT}
    echo "assert lines in file matches rows in database"
    assertEquals ${LENGTH_BLOCK_FILE} ${BLOCK_COUNT}
    echo "assert unique rows in database matches non-unique rows in database"
    assertEquals ${BLOCK_COUNT_UNIQUE} ${BLOCK_COUNT}
    echo "assert rows in non-batch database matches rows in batch database"
    assertEquals ${BLOCK_COUNT_NOBATCH} ${BLOCK_COUNT}

    COINBASE_COUNT=$( sizeof_table 'coinbase' )
    COINBASE_COUNT_UNIQUE=$( sizeof_table 'coinbase' UNIQUE )
    echo " -- coinbase: ${COINBASE_COUNT}, unique coinbase: ${COINBASE_COUNT_UNIQUE}"
    echo "assert lines in file matches rows in database"
    assertEquals ${LENGTH_COINBASE_FILE} ${COINBASE_COUNT}
    echo "assert unique rows in database matches non-unique rows in database"
    assertEquals ${COINBASE_COUNT_UNIQUE} ${COINBASE_COUNT}
    echo "assert rows in non-batch database matches rows in batch database"
    assertEquals ${COINBASE_COUNT_NOBATCH} ${COINBASE_COUNT}

    VIN_SPENT_COUNT=$( sizeof_table 'vin_spent' )
    VIN_SPENT_COUNT_UNIQUE=$( sizeof_table 'vin_spent' UNIQUE )
    echo " -- vin_spent: ${VIN_SPENT_COUNT}, unique vin_spent: ${VIN_SPENT_COUNT_UNIQUE}"
    echo "assert lines in file matches rows in database"
    assertEquals ${LENGTH_VIN_SPENT_FILE} ${VIN_SPENT_COUNT}
    echo "assert unique rows in database matches non-unique rows in database"
    assertEquals ${VIN_SPENT_COUNT_UNIQUE} ${VIN_SPENT_COUNT}
    echo "assert rows in non-batch database matches rows in batch database"
    assertEquals ${VIN_SPENT_COUNT_NOBATCH} ${VIN_SPENT_COUNT}

    VIN_TXID_COUNT=$( sizeof_table 'vin_txid' )
    VIN_TXID_COUNT_UNIQUE=$( sizeof_table 'vin_txid' UNIQUE )
    echo " -- vin_txid: ${VIN_TXID_COUNT}, unique vin_txid: ${VIN_TXID_COUNT_UNIQUE}"
    echo "assert lines in file matches rows in database"
    assertEquals ${LENGTH_VIN_TXID_FILE} ${VIN_TXID_COUNT}
    echo "assert unique rows in database matches non-unique rows in database"
    assertEquals ${VIN_TXID_COUNT_UNIQUE} ${VIN_TXID_COUNT}
    echo "assert rows in non-batch database matches rows in batch database"
    assertEquals ${VIN_TXID_COUNT_NOBATCH} ${VIN_TXID_COUNT}

    VOUT_COUNT=$( sizeof_table 'vout' )
    VOUT_COUNT_UNIQUE=$( sizeof_table 'vout' UNIQUE )
    echo " -- vout: ${VOUT_COUNT}, unique vout: ${VOUT_COUNT_UNIQUE}"
    echo "assert lines in file matches rows in database"
    assertEquals ${LENGTH_VOUT_FILE} ${VOUT_COUNT}
    echo "assert unique rows in database matches non-unique rows in database"
    assertEquals ${VOUT_COUNT_UNIQUE} ${VOUT_COUNT}
    echo "assert rows in non-batch database matches rows in batch database"
    assertEquals ${VOUT_COUNT_NOBATCH} ${VOUT_COUNT}
}

test_tablecount() {
    echo "assert number of tables is correct"
    assertEquals $( count_tables ) 7
}

. /usr/bin/shunit2
