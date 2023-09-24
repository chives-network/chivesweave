# Chivesweave Server

This is the repository for the official Erlang implementation of the Chivesweave
protocol and a gateway implementation.

Chivesweave is a distributed, cryptographically verified permanent archive built
on a cryptocurrency that aims to, for the first time, provide feasible data
permanence. By leveraging our novel Blockweave datastructure, data is stored
in a decentralised, peer-to-peer manner where miners are incentivised to
store rare data.

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

- Disk free space size need more than 200G, 3.6T is perfect.
- HDD and SDD both are support.
- Friendly for Chia and File miners.

To install the dependencies on Ubuntu 22 (recommended), run:

```sh
sudo apt install libssl-dev libgmp-dev libsqlite3-dev make cmake gcc g++
```

On some systems you might need to install `libncurses-dev`.

Step 1: Install the Erlang using the commands one by one:

```sh
sudo apt remove erlang
wget https://packages.erlang-solutions.com/erlang-solutions_2.0_all.deb
sudo dpkg -i erlang-solutions_2.0_all.deb
sudo apt update
sudo apt install erlang

```

Step 2: Download the repo:

```sh
git clone --recursive https://github.com/chives-network/chivesweave
cd chivesweave

```

Step 3: Make a mainnet build:

```sh
./rebar3 as mainnet tar

```

Step 4: You will get your wallet and address.
```sh
mkdir mainnet_data_dir
./_build/mainnet/rel/chivesweave/bin/create-wallet mainnet_data_dir

```
You will get your wallet key file in the directory "mainnet_data_dir/wallets/", and the wallet address will show in the console.

Format as "Created a wallet with address [YOUR_WALLET_ADDRESS]."

You will then find the gzipped tarball at `_build/mainnet/rel/chivesweave/chivesweave-x.y.z.tar.gz`.

Step 5: Running your node:

```sh
./_build/mainnet/rel/chivesweave/bin/start mine data_dir mainnet_data_dir mining_addr [YOUR_WALLET_ADDRESS] storage_module 0,[YOUR_WALLET_ADDRESS] peer node1.chivesweave.net peer node2.chivesweave.net

```

If you have small disk free space, you can use this command to start mine job:

```sh
./_build/mainnet/rel/chivesweave/bin/start mine data_dir mainnet_data_dir mining_addr [YOUR_WALLET_ADDRESS] storage_module 0,[YOUR_WALLET_ADDRESS] max_disk_pool_buffer_mb 10000 peer node1.chivesweave.net peer node2.chivesweave.net

```

If you want to execute mine in the background:

```sh
nohup _build/mainnet/rel/chivesweave/bin/start mine data_dir mainnet_data_dir mining_addr [YOUR_WALLET_ADDRESS] storage_module 0,[YOUR_WALLET_ADDRESS] max_disk_pool_buffer_mb 10000 peer node1.chivesweave.net peer node2.chivesweave.net > output.log 2>&1 &

```

Step 6: View the logs:

```sh
./_build/mainnet/rel/chivesweave/bin/logs -f

```

Step 7: Stop the node:

```sh
./_build/mainnet/rel/chivesweave/bin/stop

```

As with mainnet peers, each peer must be run in its own physical or virtual environment (e.g. on its own machine or in its own container or virtual machine). If you try to run two nodes within the same environment you will get an error like `Protocol 'inet_tcp': the name chivesweave@127.0.0.1 seems to be in use by another Erlang node`


# HTTP API

You can find documentation regarding our HTTP interface [here](http_iface_docs.md).

# Contact

If you have questions or comments about Chivesweave you can get in touch by
finding us on [Twitter](https://twitter.com/chivesweave/), [Reddit](https://www.reddit.com/r/chivesweave), [Discord](https://discord.gg/8KrtgBRjZn).

For more information about the Chivesweave project visit [https://www.chivesweave.org](https://www.chivesweave.org/).

# License

The Chivesweave project is released under GNU General Public License v2.0.
See [LICENSE](LICENSE.md) for full license conditions.
