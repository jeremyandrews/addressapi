# Global settings

coins = {
    # Support for a bitcoin-style coin is added by defining the genesis_hash (first block in the blockchain) and the
    # server hostname and port of the daemon's rest server.
    'bitcoin': {
        # REST/RPC server and port of blockchain daemon.
        'server': 'bitcoin:8332',
        'rpcauth': 'AFwy9VfUcNWouMG1ufW3EgtavyFJhUJhCRxVnBEBr4t4DeBHCu:qcWFdSLXGPZdzReV3ee7miEXqizoPCVmhDZvgX1oLSZ49WVoyx',
        'symbol': 'BTC',
    },
    'bitcoin_testnet3': {
        'server': 'bitcoin-testnet3:18332',
        'rpcauth': 'bitcointest:testbitcoin',
        'symbol': 'XTN',
    },
    'litecoin': {
        'server': 'litecoin:9332',
        'rpcauth': '4MYPO8lKknVfS3RCDNJ3apoUCR7MYRaJHjBZsNYMvbhMTfPMud:lHS9MTG6SM7ayDaSJtQ4o6odaTfZSdHyNrZWUHgvDSBlqlbsVO',
        'symbol': 'LTC',
    },
    'litecoin_testnet4': {
        # https://github.com/litecoin-project/litecoin/blob/0.13/src/chainparams.cpp#L214
        # https://github.com/litecoin-project/litecoin/blob/master/src/chainparamsbase.cpp#L42
        # https://github.com/litecoin-project/litecoin/blob/master/src/chainparams.cpp#L227
        'server': 'litecoin-testnet4:19332',
        'rpcauth': 'litecointest:litecointest',
        'symbol': 'XLT',
    },
    'dogecoin': {
        'server': 'dogecoin:22555',
        'rpcauth': 'rpcuser:rpcpassword',
        'symbol': 'DOGE',
    },
    'dogecoin_testnet3': {
        # https://github.com/dogecoin/dogecoin/blob/master/src/chainparams.cpp#L243
        'server': 'dogecoin-testnet3:44555',
        'rpcauth': 'dogecointest:testdogecoin',
        'symbol': 'XDT',
    }
}

# This dictionary object can also be set per-coin.
database = {
    'user':   'root',
    'passwd': 'd3vEL',
    'host':   'addressdb',
    'db':     '{coin}',
}

# Optionally modify the sort command for your local environment.
# Requires three variables: %s, %d, %s
#  - The first %s is the name of the compressed file to be sorted.
#  - The %d is the number of lines (uncompressed) to be sorted.
#  - The second %s is the name of the sorted and compressed file that will be generated.
# Things you may need to customize:
#  - Provide full path to gzip or different flags
#  - Adjust the amount of memory that sort uses (defaults to 12G)
#  - Write temporary files to a different path (defaults to /tmp)
#  - Remove pv if unavailable on your OS (it provides progress information)
system_sort_command = \
    "gzip -dc %s | LANG=C sort -u -S 12G -T /tmp/{coin} --compress-program=gzip | pv -l -s %d | gzip -9 > %s"

# Colpo API REST API endpoint where we send blockchain notifications
#  - new_block_notification: each time a block is extracted we push details to this API endpoint
new_block_notification = "http://colpoweb:8000/api/reporting/block/"

# Logger configuration
# The extract script can log information to a file. The following defaults should be overwritten in local_settings.py.
#  file: the name of the log file to write ({coin} will be replaced with the coin name)
#  append: set to false to truncate the log file each time the script starts
#  level: log messages at this level and above: DEBUG, INFO, WARNING, ERROR, CRITICAL
#  snapshot_memory: log memory usage details
#  snapshot_timer: how often to log memory usage inside loops (every n seconds)
extract_log = {
    'file': "blockchain_data/{coin}/extract.log",
    'append': True,
    'level': 'INFO',
    'snapshot_memory': False,
    'snapshot_timer': 300,
}

# Debug levels
#   0 debug disabled
#   1 minimal debug
#   2 verbose debug
#   3 full debug
debug = 1

try:
    # A local_settings.py file can override defaults for local environment.
    from local_settings import *
except Exception as e:
    # No local_settings.py file is defined.
    pass
