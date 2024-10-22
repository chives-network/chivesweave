# Chivesweave Server

This is the repository for the official Erlang implementation of the Chivesweave
protocol and a gateway implementation.

Chivesweave is a distributed, cryptographically verified permanent archive built
on a cryptocurrency that aims to, for the first time, provide feasible data
permanence. By leveraging our novel Blockweave datastructure, data is stored
in a decentralised, peer-to-peer manner where miners are incentivised to
store rare data.

# Building from release tar.gz file

Step 1: Install the Erlang using the commands one by one:

```sh
sudo apt remove erlang
wget https://packages.erlang-solutions.com/erlang-solutions_2.0_all.deb
sudo dpkg -i erlang-solutions_2.0_all.deb
sudo apt update
sudo apt install erlang

```
Step 2: Download tar.gz file

```sh
https://github.com/chives-network/Chivesweave/releases

```

Step 3: Extract file

```sh
mkdir /home/[YOUR_UBUNTU_USERNAME]/chivesweave/
cd /home/[YOUR_UBUNTU_USERNAME]/chivesweave/
tar -zxvf chivesweave-2.7.2-Ubuntu20.04.tar.gz
```

Step 4: Create Wallet Address

This directory can be any other directory, with a minimum requirement of at least 300GB of remaining space, and the path should be specified using an absolute path.

```sh
mkdir /home/[YOUR_UBUNTU_USERNAME]/chivesweave/mainnet_data_dir
./bin/create-wallet /home/[YOUR_UBUNTU_USERNAME]/chivesweave/mainnet_data_dir

```

Step 5: Running your node:

```sh
./bin/start mine data_dir /home/[YOUR_UBUNTU_USERNAME]/chivesweave/mainnet_data_dir mining_addr [YOUR_WALLET_ADDRESS] storage_module 0,[YOUR_WALLET_ADDRESS] peer node1.chivesweave.net

```

Step 6: View the logs:

```sh
./bin/logs -f

```

Step 7: Stop the node:

```sh
./bin/stop

```

# Building from source code

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

- AMD 7950 can mining almost 100 ~ 120 xwe every 6 hours. 
- Can setup two VM to mining, total get 200 ~ 240 xwe every 6 hours.

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

Step 2: Download the repo: need add '--recursive'

```sh
git clone --recursive https://github.com/chives-network/chivesweave
cd chivesweave

```

Step 3: Make a mainnet build:

```sh
./rebar3 as mainnet tar

```

Step 4: You will get your wallet and address(Only absolute paths can be used).
```sh
mkdir /home/[YOUR_UBUNTU_USERNAME]/chivesweave/mainnet_data_dir
./_build/mainnet/rel/chivesweave/bin/create-wallet /home/[YOUR_UBUNTU_USERNAME]/chivesweave/mainnet_data_dir

```
You will get your wallet key file in the directory "/home/[YOUR_UBUNTU_USERNAME]/chivesweave/mainnet_data_dir/wallets/", and the wallet address will show in the console.

Format as "Created a wallet with address [YOUR_WALLET_ADDRESS]."

You will then find the gzipped tarball at `_build/mainnet/rel/chivesweave/chivesweave-x.y.z.tar.gz`.

Step 5: Running your node:

```sh
./_build/mainnet/rel/chivesweave/bin/start mine data_dir /home/[YOUR_UBUNTU_USERNAME]/chivesweave/mainnet_data_dir mining_addr [YOUR_WALLET_ADDRESS] storage_module 0,[YOUR_WALLET_ADDRESS] peer node1.chivesweave.net

```

If you have small disk free space, you can use this command to start mine job:

```sh
./_build/mainnet/rel/chivesweave/bin/start mine data_dir /home/[YOUR_UBUNTU_USERNAME]/chivesweave/mainnet_data_dir mining_addr [YOUR_WALLET_ADDRESS] storage_module 0,[YOUR_WALLET_ADDRESS] peer node1.chivesweave.net

```

If you want to execute mine in the background:

```sh
nohup _build/mainnet/rel/chivesweave/bin/start mine data_dir /home/[YOUR_UBUNTU_USERNAME]/chivesweave/mainnet_data_dir mining_addr [YOUR_WALLET_ADDRESS] storage_module 0,[YOUR_WALLET_ADDRESS] peer node1.chivesweave.net > output.log 2>&1 &

```

Step 6: View the logs:

```sh
./_build/mainnet/rel/chivesweave/bin/logs -f

```

Step 7: Stop the node:

```sh
./_build/mainnet/rel/chivesweave/bin/stop

```

# How to check your wallet balance? 

If you are a miner, the block reward you receive will be retained in 30*24 blocks, and you can use it normally after that.
Enter the following url into your local machine's browser to check reserved rewards balance:

```sh
http://127.0.0.1:1985/wallet/[YOUR_WALLET_ADDRESS]/reserved_rewards_total
```

Enter the following url into your local machine's browser to check wallet balance:

```sh
http://127.0.0.1:1985/wallet/[YOUR_WALLET_ADDRESS]/balance
```

You can also use public nodes to check wallet balances:

```sh
https://api.chivesweave.net:1986/wallet/[YOUR_WALLET_ADDRESS]/reserved_rewards_total
https://api.chivesweave.net:1986/wallet/[YOUR_WALLET_ADDRESS]/balance
```


# How to check if my node is mining?

- 1 The hard disk partition where the directory specified (/home/[YOUR_UBUNTU_USERNAME]/chivesweave/mainnet_data_dir) when the mining program is started requires at least 300G space, You can set this directory to another partition or path.
- 2 The smaller the value of VDF, the better. It is usually required to be less than 1.4. Most nodes are around 1.
- 3 When the node first starts, about half an hour later, the following data will appear:
```sh
Mining performance report:
Total avg: 75.71 MiB/s,  302.86 h/s; current: 83.49 MiB/s, 333.97 h/s; VDF: 1.25 s.
Partition 0 avg: 75.71 MiB/s, current: 83.49 MiB/s, optimum: 0.04 MiB/s, 0.09 MiB/s (full weave).

Mining performance report:
Total avg: 75.71 MiB/s,  302.85 h/s; current: 74.09 MiB/s, 296.34 h/s; VDF: 1.11 s.
Partition 0 avg: 75.71 MiB/s, current: 74.09 MiB/s, optimum: 0.05 MiB/s, 0.10 MiB/s (full weave).
```
It means that the mining program is already working.


As with mainnet peers, each peer must be run in its own physical or virtual environment (e.g. on its own machine or in its own container or virtual machine). If you try to run two nodes within the same environment you will get an error like `Protocol 'inet_tcp': the name chivesweave@127.0.0.1 seems to be in use by another Erlang node`, stop and restart your node, will slove this issue.


# HTTP API

You can find documentation regarding our HTTP interface [here](http_iface_docs.md).

# Contact

If you have questions or comments about Chivesweave you can get in touch by
finding us on [Twitter](https://twitter.com/chivesweave/), [Reddit](https://www.reddit.com/r/chivesweave), [Discord](https://discord.gg/8KrtgBRjZn).

For more information about the Chivesweave project visit [https://www.chivesweave.org](https://www.chivesweave.org/).

# License

The Chivesweave project is released under GNU General Public License v2.0.
See [LICENSE](LICENSE.md) for full license conditions.
