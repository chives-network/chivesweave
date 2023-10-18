# Chivesweave HTTP Interface Documentation

You can run this HTTP interface using Postman [![Postman](https://run.pstmn.io/button.svg)](https://app.getpostman.com/run-collection/8af0090f2db84e979b69) or you can find the documentation [here](https://documenter.getpostman.com/view/5500657/RWgm2g1r).


## GET network information

Retrieve the current network information from a specific node.

- **URL**
  `/info`
- **Method**
  GET

#### Example Response

A JSON array containing the network information for the current node.

```javascript
{
  "network": "chivesweave.mainnet",
  "version": 5,
  "release": 66,
  "height": 1055,
  "current": "-bi3G5YAKq6V4F3iGqs8zlMG17ZE5REq8SedpwuhYy-BQxcmokry5t5corRP4Kz6",
  "blocks": 1056,
  "peers": 4,
  "time": "1697047613",
  "miningtime": "73",
  "weave_size": "3051356160",
  "denomination": "1",
  "diff": "115790107069349985313201188186051774420648095747327904096085788683832520728835",
  "queue_length": 0,
  "node_state_latency": 1
}
```

#### JavaScript Example Request

```javascript
var node = 'http://127.0.0.1:1985';
var path = '/info';
var url = node + path;
var xhr = new XMLHttpRequest();

xhr.open('GET', url);
xhr.onreadystatechange = function() {
  if(xhr.readystate == 4 && xhr.status == 200) {
    // Do something.
  }
};
xhr.send();
```



## GET full transaction via ID

Retrieve a JSON transaction record via the specified ID.

- **URL**
  `/tx/[transaction_id]`
- **Method**
  GET
- **URL Parameters**
  [transaction_id] : Base64 encoded ID associated with the transaction


#### Example Response

A JSON transaction record.

```javascript
{
  "format": 1,
  "id": "AUftwuYRUFTJWk74x9i7ea3ihsK4PRuWnh0hXIZzLos",
  "last_tx": "o1hKkYZx-LfDqtbDT7_hnuxAHFJ5BkNBaX9aJ1eC89n1LIBqhi-eYeqWN4wc1CJi",
  "owner": "8pztX2...oAlIJk",
  "tags": [
    {
      "name": "Q29...XBl",
      "value": "aW1...G5n"
    }
  ],
  "target": "aN5vbSDwwUksFYlT5gUSXWe5PVrd_L1tQLTg44Uq1TU",
  "quantity": "868660046",
  "data": "iVBORw...rkJggg",
  "data_size": "719602",
  "data_tree": [
    
  ],
  "data_root": "",
  "reward": "630923958",
  "signature": "ieDCgm...V5qlSw"
}
```


## GET additional info about the transaction via ID

- **URL**
  `/tx/[transaction_id]/status`
- **Method**
  GET
- **URL Parameters**
  [transaction_id] : base64url encoded ID associated with the transaction


#### Example Response

```javascript
{"block_height": 1055, "block_indep_hash": "-bi3G5YAKq6V4F3iGqs8zlMG17ZE5REq8SedpwuhYy-BQxcmokry5t5corRP4Kz6", "number_of_confirmations": 2 }
```
#### JavaScript Example Request

```javascript
var node = 'http://127.0.0.1:1985';
var path = '/tx/AUftwuYRUFTJWk74x9i7ea3ihsK4PRuWnh0hXIZzLos';
var url = node + path;
var xhr = new XMLHttpRequest();

xhr.open('GET', url);
xhr.onreadystatechange = function() {
  if(xhr.readystate == 4 && xhr.status == 200) {
    // Do something.
  }
};
xhr.send();
```


## GET specific transaction fields via ID

Retrieve a string of the requested field for a given transaction.

- **URL**
  `/tx/[transaction_id]/[field]`
- **Method**
  GET
- **URL Parameters**
  [transaction_id] : Base64url encoded ID associated with the transaction
  [field] : A string containing the name of the data field being requested
- **Fields**
  id | last_tx | owner | target | quantity | data | reward | signature


#### Example Response

A string containing the requested field.

```javascript
"bUfaJN-KKS1LRh_DlJv4ff1gmdbHP4io-J9x7cLY5is"
```


#### JavaScript Example Request

```javascript
var node = 'http://127.0.0.1:1985';
var path = '/tx/AUftwuYRUFTJWk74x9i7ea3ihsK4PRuWnh0hXIZzLos/last_tx';
var url = node + path;
var xhr = new XMLHttpRequest();

xhr.open('GET', url);
xhr.onreadystatechange = function() {
  if(xhr.readystate == 4 && xhr.status == 200) {
    // Do something.
  }
};
xhr.send();
```


## GET transaction body as HTML via ID

Retrieve the data segment of the transaction body decoded from base64url encoding.
If the transaction was an archived website then the result will be browser rendererable HTML.

- **URL**
  `/tx/[transaction_id]/data.html`
- **Method**
  GET
- **URL Parameters**
  [transaction_id] : Base64url encoded ID associated with the transaction


#### Example Response

A string containing the requested field.

```javascript
"Hello World"
```


#### JavaScript Example Request

```javascript
var node = 'http://127.0.0.1:1985';
var path = '/tx/AUftwuYRUFTJWk74x9i7ea3ihsK4PRuWnh0hXIZzLos/data.html'
var url = node + path;
var xhr = new XMLHttpRequest();

xhr.open('GET', url);
xhr.onreadystatechange = function() {
  if(xhr.readystate == 4 && xhr.status == 200) {
    // Do something.
  }
};
xhr.send();
```


## GET estimated transaction price

Returns an estimated cost for a transaction of the given size.
The returned amount is in winston (the smallest division of XWE, 1 XWE = 1000000000000 winston).

The endpoint is pessimistic, it reports the price as if the network difficulty was smaller by one, to account for the possible difficulty change.

- **URL**
  `/price/[byte_size]`
- **Method**
  GET
- **URL Parameters**
  [byte_size] : The size of the transaction's data field in bytes. For financial transactions without associated data, this should be zero.


#### Example Response

A string containing the estimated cost of the transaction in Winston.

```javascript
"3772719798"
```


#### JavaScript Example Request

```javascript
var node = 'http://127.0.0.1:1985';
var path = '/price/2048';
var url = node + path;
var xhr = new XMLHttpRequest();

xhr.open('GET', url);
xhr.onreadystatechange = function() {
  if(xhr.readystate == 4 && xhr.status == 200) {
    // Do something.
  }
};
xhr.send();
```


## GET block via ID

Retrieve a JSON array representing the contents of the block specified via the ID.

- **URL**
  `/block/hash/[block_id]`
- **Method**
  GET
- **URL Parameters**
  [block_id] : Base64url encoded ID associated with the block


#### Example Response

A JSON array detailing the block.

```javascript
{
  "nonce": "kQ",
  "previous_block": "_XPwo6AxA5ydcnHObDTyqNlKPaxq4_FgWojm058bw-K1-fkihxT8QPyogUHasPUN",
  "timestamp": 1697047656,
  "last_retarget": 1697046823,
  "diff": "115790107069349985313201188186051774420648095747327904096085788683832520728835",
  "height": 1055,
  "hash": "-3-oyxTcYAgbbNoFyDz8hqs7KCJHI4qb4VdER9Jotbs",
  "indep_hash": "o7elNx_7woBugKUhYwksZY6jtbI1yi-jLE86ovAQMHlSDLWmOLpW92Fkw0qJgC5I",
  "txs": [...],
  "hash_list": [...],
  "wallet_list": [...],
  "reward_addr": "tUhM1HnjXF1lishDJ1O7sEM-AfXgEKNVm0PimVzNDHA",
  ...
}
```


#### JavaScript Example Request

```javascript
var node = 'http://127.0.0.1:1985';
var path = '/block/hash/o7elNx_7woBugKUhYwksZY6jtbI1yi-jLE86ovAQMHlSDLWmOLpW92Fkw0qJgC5I';
var url = node + path;
var xhr = new XMLHttpRequest();

xhr.open('GET', url);
xhr.onreadystatechange = function() {
  if(xhr.readystate == 4 && xhr.status == 200) {
    // Do something.
  }
};
xhr.send();
```


## GET block via from height and records [New Function]

Retrieve a JSON array representing the contents of the block specified via the block from height and how many blocks your want to fetch.

- **URL**
  `/blocklist/[FromHeight]/[BlockNumber]`
- **Method**
  GET
- **URL Parameters**
  [FromHeight] : The height at which the block is being requested for.
  [BlockNumber] : how many blocks your want to fetch per page, value from 1 ~ 100.
- **Examples**
  `/blocklist/300/100`
- **Use Cases**
  Blockchain Explorers, Wallets, and More.


## GET block via pageid and records [New Function]

Retrieve a JSON array representing the contents of the block specified via the block pageid and how many blocks your want to fetch.

- **URL**
  `/blocklist/[Pageid]/[BlockNumber]`
- **Method**
  GET
- **URL Parameters**
  [Pageid] : The page of the whole blocks, pageid begin with 1, mean the first page in descending order, value from 1 ~ int(CurrentHeight/BlockNumber).
  [BlockNumber] : how many blocks your want to fetch per page, value from 1 ~ 100.
- **Examples**
  `/blocklist/300/100`
- **Use Cases**
  Blockchain Explorers, Wallets, and More.


## GET block via height

Retrieve a JSON array representing the contents of the block specified via the block height.

- **URL**`
  `/block/height/[block_height]`
- **Method**
  GET
- **URL Parameters**
  [block_height] : The height at which the block is being requested for.


#### Example Response

A JSON array detailing the block.

```javascript
{
  "nonce": "c7V-8dLmmqo",
  "previous_block": "yeCiFpWcguWtWRJnJ_XOKhQXw6xtiOHh-rAw-RjX0YE",
  "timestamp": 1517563547,
  "last_retarget": 1517563547,
  "diff": 8,
  "height": 30,
  "hash": "-3-oyxTcYAgbbNoFyDz8hqs7KCJHI4qb4VdER9Jotbs",
  "indep_hash": "oyxTcYAgbbNoFyDz8hqs7KCJHI4qb4VdER9Jotbs",
  "txs": [...],
  "hash_list": [...],
  "wallet_list": [...],
  "reward_addr": "unclaimed"
}
```


## GET current block

Retrieve a JSON array representing the contents of the current block, the network head.

- **URL**
  `/current_block`
- **Method**
  GET


#### Example Response

A JSON array detailing the block.

```javascript
{
  "nonce": "rihlezm7XAc",
  "previous_block": "pc-0MvV6lQOWt0O2L3VcSheOfIdymntOBVcloERVbQQ",
  "timestamp": 1517564276,
  "last_retarget": 1517564044,
  "diff": 24,
  "height": 166,
  "hash": "mGe34a3DcT8HLE0BfaME38XUelENSjPQA-vcYJG6PGs",
  "indep_hash": "ntoWN8DMFSuxPsdF8CelZqP03Gr4GahMBXX8ZkyPA3U",
  "txs": [...],
  "hash_list": [...],
  "wallet_list": [...],
  "reward_addr": "unclaimed"
}
```


## GET wallet balance via address

Retrieve the balance of the wallet specified via the address.
The returned amount is in winston (the smallest division of XWE, 1 XWE = 1000000000000 winston).

- **URL**
  `/wallet/[wallet_address]/balance`
- **Method**
  GET
- **URL Parameters**
  [wallet_address] : A base64url encoded SHA256 hash of the raw RSA modulus.


#### Example Response

A string containing the balance of the wallet.

```javascript
"1249611338095239"
```


## GET last transaction via address

Retrieve the ID of the last transaction made by the given address.

- **URL**
  `/wallet/[wallet_address]/last_tx`
- **Method**
  GET
- **URL Parameters**
  [wallet_address] : A Base64 encoded SHA256 hash of the public key.

#### Example Response

A string containing the ID of the last transaction made by the given address.

```javascript
"bUfaJN-KKS1LRh_DlJv4ff1gmdbHP4io-J9x7cLY5is"
```


## GET transactions made by the given address [New Function]

Retrieve identifiers of transactions made by the given address.

- **URL**
  `/wallet/[wallet_address]/txs/[Pageid]/[TxNumber]`
- **Method**
  GET
- **URL Parameters**
  - [wallet_address] : A Base64 encoded SHA256 hash of the public key.
  - [Pageid] : The page of the whole txs, pageid begin with 0, mean the first page in descending order, value from 1 ~ int(All Txs Relative This Address / Txs Number).
  - [TxNumber] : how many txs your want to fetch per page, value from 1 ~ 100. And will append the relative txs in mempool, so the results will more than [TxNumber].


#### Example Response

A JSON list of base64url encoded transaction identifiers.

```javascript
["bUfaJN-KKS1LRh_DlJv4ff1gmdbHP4io-J9x7cLY5is","b23...xg"]
```


## GET depositing transactions made by the given address [New Function]

Retrieve identifiers of transactions depositing to the given address.

- **URL**
  `/wallet/[wallet_address]/deposits/[Pageid]/[TxNumber]`
- **Method**
  GET
- **URL Parameters**
  - [wallet_address] : A Base64 encoded SHA256 hash of the public key.
  - [Pageid] : The page of the whole txs, pageid begin with 0, mean the first page in descending order, value from 1 ~ int(All Txs Relative This Address / Txs Number).
  - [TxNumber] : how many txs your want to fetch per page, value from 1 ~ 100. And will append the relative txs in mempool, so the results will more than [TxNumber].
- **Use Cases**
  Blockchain Explorers, Wallets, and More.


#### Example Response

A JSON list of base64url encoded transaction identifiers.

```javascript
["bUfaJN-KKS1LRh_DlJv4ff1gmdbHP4io-J9x7cLY5is","b23...xg"]
```

## GET send transactions made by the given address [New Function]

Retrieve identifiers of transactions depositing to the given address.

- **URL**
  `/wallet/[wallet_address]/send/[Pageid]/[TxNumber]`
- **Method**
  GET
- **URL Parameters**
  - [wallet_address] : A Base64 encoded SHA256 hash of the public key.
  - [Pageid] : The page of the whole txs, pageid begin with 0, mean the first page in descending order, value from 1 ~ int(All Txs Relative This Address / Txs Number).
  - [TxNumber] : how many txs your want to fetch per page, value from 1 ~ 100. And will append the relative txs in mempool, so the results will more than [TxNumber].
- **Use Cases**
  Blockchain Explorers, Wallets, and More.

#### Example Response

A JSON list of base64url encoded transaction identifiers.

```javascript
["bUfaJN-KKS1LRh_DlJv4ff1gmdbHP4io-J9x7cLY5is","b23...xg"]
```


## GET a tx structure by give a txid [New Function]

Retrieve identifiers of transactions structure to the given address.

- **URL**
  `/wallet/[transaction_id]/txrecord`
- **Method**
  GET
- **URL Parameters**
  - [transaction_id] : Base64 encoded ID associated with the transaction
- **Use Cases**
  Blockchain Explorers, Wallets, and More.

#### Example Response

A JSON object.

```javascript
{
  "tags": [
    {
      "name": "Content-Type",
      "value": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    },
    {
      "name": "File-Hash",
      "value": "af212aa1fef46859f2c1c9d6bb0a848140434d8f45e1b9267d92e351c160d448"
    }
  ],
  "recipient": "",
  "quantity": {
    "xwe": 0.0,
    "winston": 0
  },
  "owner": {
    "address": "r72kcrbulgboBk6EiuNro-JcLedyANekX-y1OMUQ3dM"
  },
  "id": "ogWreYEenO_pVL7Pe3olhw2Iv9a97i0DU5wxeXRtoP4",
  "fee": {
    "xwe": 0.000212017846,
    "winston": 212017846
  },
  "data": {
    "type": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    "size": 32176
  },
  "block": {
    "timestamp": 1697648297,
    "indep_hash": "fBZ8xXyZzZs_eDTjQ4Rix6ZBoVpzhjiLZFczZpkQNfurVo2Jbr9eZDHdEURmNmNg",
    "height": 5774
  }
}
```


## GET nodes peer list  

Retrieve the list of peers held by the contacted node.

- **URL**
  `/peers`
- **Method**
  GET


#### Example Response

A list containing the IP addresses of all of the nodes peers.

```javascript
[
  "127.0.0.1:1985",
  "127.0.0.1.:1985"
]
```


## GET nodes peer list with details

Retrieve the list with details of peers held by the contacted node.

- **URL**
  `/peersinfo`
- **Method**
  GET


#### Example Response

A list containing the IP addresses and details of all of the nodes peers.

```javascript
[
  {
    "coordinates": {
      "latitude": xxx.xxx,
      "longitude": -xxx.xxx
    },
    "ip": "xxx.xxx.xxx.xxx",
    "isp": "xxx xxx",
    "host": {
      "domain": "xxx.net",
      "ip_address": "xxx.xxx.xxx.xxx",
      "prefix_len": 21
    },
    "status": false,
    "country": "United States",
    "region": "xxx",
    "city": "xxx xxx",
    "location": "United States, xxx, xxx xxx",
    "area_code": "xxx",
    "country_code": "US"
  },
  ...
]
```

## POST transaction to network

Post a transaction to the network.

- **URL**
  `/tx`

- **Method**
  POST


#### Data Parameter (Post body)

```javascript
{
    "last_tx": "",  // Base64 encoded ID of the last transaction made by this wallet. Empty if this is the first transaction.
    "owner": "",    // The public key making this transaction.
    "target": "",   // Base64 encoded SHA256 hash of recipient's public key. Empty for data transactions.
    "quantity": "", // Decimal string representation of the amount of sent XWE in winston. Empty for data transactions.
    "data": "",     // The Base64 encoded data being store in the transaction. Empty for transfer transactions.
    "reward": "",   // Decimal string representation of the mining reward XWE amount in winston.
    "signature": "" // Base64 encoded signature of the transaction
}
```

#### JavaScript Example Request

```javascript
var node = 'http://127.0.0.1:1985';
var path = '/tx';
var url = node + path;
var xhr = new XMLHttpRequest();
var post =
    {
      "id": "AUftwuYRUFTJWk74x9i7ea3ihsK4PRuWnh0hXIZzLos",
      "last_tx": "bUfaJN-KKS1LRh_DlJv4ff1gmdbHP4io-J9x7cLY5is",
      "owner": "1Q7RfP...J2x0xc",
      "tags": [],
      "target": "",
      "quantity": "0",
      "data": "3DduMPkwLkE0LjIxM9o",
      "reward": "1966476441",
      "signature": "RwBICn...Rxqi54"
  };

xhr.open('POST', url);
xhr.onreadystatechange = function() {
  if(xhr.readystate == 4 && xhr.status == 200) {
    // Do something.
  }
};
xhr.send(post);
```


## GET network statistics [New Function]

Retrieve a JSON array representing the contents of the network statistics.

- **URL**
  `/statistics_network`
- **Method**
  GET


## GET data statistics [New Function]

Retrieve a JSON array representing the contents of the data statistics.

- **URL**
  `/statistics_data`
- **Method**
  GET




> Please note that in the JSON transaction records all winston value fields (quantity and reward) are strings. This is to allow for interoperability between environments that do not accommodate arbitrary-precision arithmetic. JavaScript for instance stores all numbers as double precision floating point values and as such cannot natively express the integer number of winston. Providing these values as strings allows them to be directly loaded into most 'bignum' libraries.


# Contact

If you have questions or comments on the Chivesweave HTTP interface you can get in touch by
finding us on [Twitter](https://twitter.com/Chives-network/), [Reddit](https://www.reddit.com/r/Chives-network), [Discord](https://discord.gg/8KrtgBRjZn).

# License
The Chivesweave project is released under GNU General Public License v2.0.
See [LICENSE](LICENSE.md) for full license conditions.
