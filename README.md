# Chivesweave Server

This is the repository for the official Erlang implementation of the Chivesweave
protocol and a gateway implementation.

Chivesweave is a distributed, cryptographically verified permanent archive built
on a cryptocurrency that aims to, for the first time, provide feasible data
permanence. By leveraging our novel Blockweave datastructure, data is stored
in a decentralised, peer-to-peer manner where miners are incentivised to
store rare data.

# Getting Started

Download and extract the latest archive for your platform on the release
page, then run the included `bin/start` script to get started.

For more information, refer to the [mining guide](https://docs.chivesweave.org/info/mining/mining-guide).

# Building from source

## Requirements

- OpenSSL 1.1.1+
- OpenSSL development headers
- GCC or Clang (GCC 8+ recommended)
- Erlang OTP v24, with OpenSSL support
- GNU Make
- CMake (CMake version > 3.10.0)
- SQLite3 headers (libsqlite3-dev on Ubuntu)
- GNU MP (libgmp-dev on Ubuntu)

To install the dependencies on Ubuntu 22 (recommended), run:

```sh
sudo apt install libssl-dev libgmp-dev libsqlite3-dev make cmake gcc g++
```

On some systems you might need to install `libncurses-dev`.

Install the Erlang:

```sh
sudo apt remove erlang
wget https://packages.erlang-solutions.com/erlang-solutions_2.0_all.deb
sudo dpkg -i erlang-solutions_2.0_all.deb
sudo apt update
sudo apt install erlang
```

Download the repo:

```sh
git clone --recursive https://github.com/chives-network/chivesweave
cd chivesweave
```

Increase the [open file
limits](https://docs.chivesweave.org/info/mining/mining-guide#preparation-file-descriptors-limit).

Make a production build:

```sh
./rebar3 as mainnet tar
rm -rf mainnet_data_dir
mkdir mainnet_data_dir
./bin/create-wallet mainnet_data_dir
```
You will get your wallet and address.

You will then find the gzipped tarball at `_build/mainnet/rel/chivesweave/chivesweave-x.y.z.tar.gz`.

```sh
_build/mainnet/rel/chivesweave/bin/start mine data_dir mainnet_data_dir mining_addr [YOUR_WALLET_ADDRESS] storage_module 0,[YOUR_WALLET_ADDRESS] 
```

### Starting New Weave

To start a new weave, create a new data directory

```sh
mkdir -p localnet_data_dir
```
,
create a wallet:

```sh
./bin/create-wallet localnet_data_dir
```
,
and run:

```sh
./bin/start-localnet init data_dir <your-data-dir> mining_addr <your-mining-addr>
storage_module 0,<your-mining-addr> mine
```

The given address (if none is specified, one will be generated for you) will be assigned
`1000_000_000_000` AR in the new weave.

The network name will be `chivesweave.localnet`. You can not start the same node again with the
init option unless you clean the data directory - you need to either restart with the
`start_from_block_index` option or specify a peer from the same Chivesweave network via
`peer <peer>`. Note that the state is only persisted every 50 blocks so if you
restart the node without peers via `start_from_block_index` before reaching the height 50,
it will go back to the genesis block.

As with mainnet peers, each peer must be run in its own physical or virtual environment (e.g. on its own machine or in its own container or virtual machine). If you try to run two nodes within the same environment you will get an error like `Protocol 'inet_tcp': the name chivesweave@127.0.0.1 seems to be in use by another Erlang node`

When POST'ing transactions to your localnet make sure to include the `X-Network: chivesweave.localnet` header. If the header is omitted, the mainnet network will be assumed and the request will fail.

# Contributing

Make sure to have the build requirements installed.

Clone the repo and initialize the Git submodules:

```sh
git clone --recursive https://github.com/chives-network/chivesweave
```

## Running the tests

```sh
bin/test
```

## Running a shell

```sh
bin/shell
```

`bin/test` and `bin/shell` launch two connected Erlang VMs in distributed mode. The master VM runs an HTTP server on the port 1984. The slave VM uses the port 1983. The data folders are `data_test_master` and `data_test_slave` respectively. The tests that do not depend on two VMs are run against the master VM.

Run a specific test (the shell offers autocompletion):

```sh
(master@127.0.0.1)1> eunit:test(ar_fork_recovery_tests:height_plus_one_fork_recovery_test_()).
```

If it fails, the nodes keep running so you can inspect them through Erlang shell or HTTP API.
The logs from both nodes are collected in `logs/`. They are rotated so you probably want to
consult the latest modified `master@127.0.0.1.*` and `slave@127.0.0.1.*` files first - `ls -lat
logs/`.

See [CONTRIBUTING.md](CONTRIBUTING.md) for more information.

# HTTP API

You can find documentation regarding our HTTP interface [here](http_iface_docs.md).

# Contact

If you have questions or comments about Chivesweave you can get in touch by
finding us on [Twitter](https://twitter.com/ArweaveTeam/), [Reddit](https://www.reddit.com/r/chivesweave), [Discord](https://discord.gg/DjAFMJc) or by
emailing us at team@chivesweave.org.


For more information about the Chivesweave project visit [https://www.chivesweave.org](https://www.chivesweave.org/)
or have a look at our [yellow paper](https://yellow-paper.chivesweave.dev).

# License

The Chivesweave project is released under GNU General Public License v2.0.
See [LICENSE](LICENSE.md) for full license conditions.

# Chivesweave Bug Bounty Program

Chivesweave core team has initiated an Chivesweave bug bounty program, with a maximum bounty of up to USD `1,000,000`. The program is focused on discovering potential technical vulnerabilities and strengthening Chivesweave core protocol security.

The Chivesweave core team puts security as its top priority and has dedicated resources to ensure high incentives to attract the community at large to evaluate and safeguard the ecosystem. Whilst building Chivesweave, the team has engaged with industry-leading cybersecurity audit firms specializing in Blockchain Security to help secure the codebase of Chivesweave protocol.

We encourage developers, whitehat hackers to participate, evaluate the code base and hunt for bugs, especially on issues that could potentially put users’ funds or data at risk. In exchange for a responsibly disclosed bug, the bug bounty program will reward up to USD `1,000,000` (paid in `$AR` tokens) based on the vulnerability severity level, at the discretion of the Chivesweave team. Please email us at team@chivesweave.org to get in touch.
