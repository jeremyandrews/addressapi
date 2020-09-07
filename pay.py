import bitcoin


'''
# CLI example:
[litecoin-test@rs237007 ~]$ litecoin-0.15.1/bin/litecoin-cli createrawtransaction '[{"txid":"2f1dcf81220e47f551878e5b0a30c6bfa653e4539b8802875bba422367ca8630","vout":0}]' '{"mxkRUXKVLjHCDL96qwARVmStGMTWWxntQb":"1.95"}'
02000000013086ca672342ba5b8702889b53e453a6bfc6300a5b8e8751f5470e2281cf1d2f0000000000ffffffff01c0769f0b000000001976a914bd063c274074b1a13d8f43ac5e3097beb912f94188ac00000000
[litecoin-test@rs237007 ~]$ litecoin-0.15.1/bin/litecoin-cli signrawtransaction 02000000013086ca672342ba5b8702889b53e453a6bfc6300a5b8e8751f5470e2281cf1d2f0000000000ffffffff01c0769f0b000000001976a914bd063c274074b1a13d8f43ac5e3097beb912f94188ac00000000
{
  "hex": "02000000013086ca672342ba5b8702889b53e453a6bfc6300a5b8e8751f5470e2281cf1d2f000000006b483045022100a1fa947cb11daae26c4728b5cf9e61f772d9b970eb976162ea93a50945eb952a02205f8a24aedbb39f7511fef89452c5dec695685952f367d389536c7cc7754187300121024cd7aa51d17ec5f93b78c3744f31daa60479589ec1e20f8181081609e4281552ffffffff01c0769f0b000000001976a914bd063c274074b1a13d8f43ac5e3097beb912f94188ac00000000",
  "complete": true
}
[litecoin-test@rs237007 ~]$ litecoin-0.15.1/bin/litecoin-cli sendrawtransaction 02000000013086ca672342ba5b8702889b53e453a6bfc6300a5b8e8751f5470e2281cf1d2f000000006b483045022100a1fa947cb11daae26c4728b5cf9e61f772d9b970eb976162ea93a50945eb952a02205f8a24aedbb39f7511fef89452c5dec695685952f367d389536c7cc7754187300121024cd7aa51d17ec5f93b78c3744f31daa60479589ec1e20f8181081609e4281552ffffffff01c0769f0b000000001976a914bd063c274074b1a13d8f43ac5e3097beb912f94188ac00000000
ff0a4adb572375c9bdc95e5ff3dd625405b32397e64da293c048e5e671db6a10

http://explorer.litecointools.com/address/mxkRUXKVLjHCDL96qwARVmStGMTWWxntQb
http://explorer.litecointools.com/tx/ff0a4adb572375c9bdc95e5ff3dd625405b32397e64da293c048e5e671db6a10
http://explorer.litecointools.com/block/0f6ea945aec5b82b2883e0c5fee818f47d5580f310bba063895d53104de3527e
'''

'''
Documentation:
 - https://en.bitcoin.it/wiki/Raw_Transactions
 - http://www.righto.com/2014/02/bitcoins-hard-way-using-raw-bitcoin.html
'''

''''
[litecoin-test@rs237007 ~]$ litecoin-0.15.1/bin/litecoin-cli createrawtransaction '[{"txid":"ca162a7139fe20ae131628bcfafe59ad1f55fe8881b856a52eb48013a02a745b","vout":614}]'
error code: -1
error message:
createrawtransaction [{"txid":"id","vout":n},...] {"address":amount,"data":"hex",...} ( locktime ) ( replaceable )

Create a transaction spending the given inputs and creating new outputs.
Outputs can be addresses or data.
Returns hex-encoded raw transaction.
Note that the transaction's inputs are not signed, and
it is not stored in the wallet or transmitted to the network.

Arguments:
1. "inputs"                (array, required) A json array of json objects
     [
       {
         "txid":"id",    (string, required) The transaction id
         "vout":n,         (numeric, required) The output number
         "sequence":n      (numeric, optional) The sequence number
       }
       ,...
     ]
2. "outputs"               (object, required) a json object with outputs
    {
      "address": x.xxx,    (numeric or string, required) The key is the litecoin address, the numeric value (can be string) is the LTC amount
      "data": "hex"      (string, required) The key is "data", the value is hex encoded data
      ,...
    }
3. locktime                  (numeric, optional, default=0) Raw locktime. Non-0 value also locktime-activates inputs
4. replaceable               (boolean, optional, default=false) Marks this transaction as BIP125 replaceable.
                             Allows this transaction to be replaced by a transaction with higher fees. If provided, it is an error if explicit sequence numbers are incompatible.

Result:
"transaction"              (string) hex string of the transaction

Examples:
> litecoin-cli createrawtransaction "[{\"txid\":\"myid\",\"vout\":0}]" "{\"address\":0.01}"
> litecoin-cli createrawtransaction "[{\"txid\":\"myid\",\"vout\":0}]" "{\"data\":\"00010203\"}"
> curl --user myusername --data-binary '{"jsonrpc": "1.0", "id":"curltest", "method": "createrawtransaction", "params": ["[{\"txid\":\"myid\",\"vout\":0}]", "{\"address\":0.01}"] }' -H 'content-type: text/plain;' http://127.0.0.1:9332/
> curl --user myusername --data-binary '{"jsonrpc": "1.0", "id":"curltest", "method": "createrawtransaction", "params": ["[{\"txid\":\"myid\",\"vout\":0}]", "{\"data\":\"00010203\"}"] }' -H 'content-type: text/plain;' http://127.0.0.1:9332/

'''