# Overview

This is an old project that was never completed, but that I'm releasing as
open source for anyone who may be interested.

This codebase is released under the GPL v3. 
https://opensource.org/licenses/GPL-3.0

To request the codebase under a different license, please explain your
need and intent clearly.


Extract and query blockchains.

# Supported Blockchains

* Bitcoin (and bitcoin-testnet3)
* Litecoin (and litecoin-testnet4)
* Dogecoin (and dogecoin-testnet3)

## Bitcoin (BTC) ₿

### Bitcoin main

Bitcoin main blockchain explorers:

* https://live.blockcypher.com/
* https://chain.so/btc
* https://www.blocktrail.com/BTC
* http://blockchain.info/

### Bitcoin testnet3

Bitcoin testnet3 blockchain explorers:

* https://live.blockcypher.com/btc-testnet/
* https://chain.so/testnet/btc
* https://www.blocktrail.com/tBTC

Bitcoin testnet3 faucet:

* https://testnet.coinfaucet.eu/en/
* https://testnet.manu.backend.hamburg/faucet
* https://tpfaucet.appspot.com/


## Litecoin (LTC) Ł

### Litecoin main

Litecoin main blockchain explorers:

* https://live.blockcypher.com/ltc/
* https://chain.so/ltc


### Litecoin testnet4

Litecoin testnet4 blockchain explorers:

* https://chain.so/testnet/ltc

Litecoin testnet4 faucets:

* http://testnet.thrasher.io/


## Dogecoin (DOGE) Ð

### Dogecoin main

Dogecoin main blockchain explorers:

* https://chain.so/doge

Dogecoin main faucet (none worked when last tested):

* https://faucethut.com/dogecoin-faucet/ (coins never arrived)
* http://indogewetrust.com/faucet
* https://www.dogefaucet.com/en
* http://mydoge.co.in/

Address on dev.colpo.net: DNv7di7ayXWSg7zGVgjcLMjJhMePdEEPwp


### Dogecoin testnet

* https://chain.so/testnet/doge

Dogecoin test faucet:

* https://doge-faucet-testnet.ggcorp.fr/


# Installation

Everything runs from Docker. 

```bash
docker-compose up
```

If the above fails, you may need to rebuild:
```bash
docker-compose build && docker-compose up
```

If the above also fails, you made need to delete and recreate the containers:
```bash
docker-compose rm && docker-compose build && docker-compose up
```

# Stack

Nginx listens for API requests on port 8001, passing the requests to gunicorn
through a socket (`/run/gunicorn.sock`). These requests are processed by Flask
which makes queries to a MySQL database and the appropriate coin daemon.

# API

## Address

Traces all activity associated with a given address. For example, to get full details about
the `litecoin_testnet4` address `n1dB69Ptu1HMt1tRqiueyJ1tsaj59qSjLn`, visit:

 - http://127.0.0.1:8001/api/address/litecoin_testnet4/n1dB69Ptu1HMt1tRqiueyJ1tsaj59qSjLn

## Transaction

Shows all details about a specified txid. For example, to get full details about the
`litecoin_testnet4` txid `63b302aa3e97c29ae01edc5ca80777105efc1164e1dda9a255e9e86f93e3f714`, visit:

 - http://127.0.0.1:8001/api/tx/litecoin_testnet4/63b302aa3e97c29ae01edc5ca80777105efc1164e1dda9a255e9e86f93e3f714
 
In particular, this will show all coinbase, vin and vout contained in the transaction.

## Block

Shows all txid found in a specified block. For example, to get full details about the
`litecoin_testnet4` block with hash `4557827abb0272c7dd3b6fae8485ff3d3d5aad876e207743660000555fc4567b`, visit:

 - http://127.0.0.1:8001/api/block/litecoin_testnet4/4557827abb0272c7dd3b6fae8485ff3d3d5aad876e207743660000555fc4567b
