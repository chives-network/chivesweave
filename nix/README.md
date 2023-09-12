## Building Chivesweave in Nix

Easiest way to import chivesweave as systemd service, is via flakes

```nix
{
  inputs.chivesweave.url = "github:chives-network/chivesweave";
  outputs = { self, nixpkgs, chivesweave }: {
    nixosSystem = nixpkgs.lib.nixosSystem {
      modules = [ chivesweave.nixosModules."x86_64-linux".chivesweave ];
    };
  }
```

In non nixos system, the package derivation can be accessed and used as standalone.

```nix
{
  inputs.chivesweave.url = "github:chives-network/chivesweave";
  outputs = { self, nixpkgs, chivesweave }:
    let
      system = "x86_64-linux";
      pkgs = import nixpkgs { inherit system; overlays = [ chivesweave.overlay ]; };
    in {
      # your flake here...
      # pkgs.chivesweave should exist
    }
```

Module extraArgs are also a good way to access pkgs.chivesweave for overrides if needed


```nix
{
  inputs.chivesweave.url = "github:chives-network/chivesweave";
  outputs = { self, nixpkgs, chivesweave }:
    let
      system = "x86_64-linux";
      pkgs = import nixpkgs { inherit system; };
      extraArgs = { inherit pkgs; };
     in {
        nixosSystem = nixpkgs.lib.nixosSystem {
         inherit extraArgs system;
         modules = [ chivesweave.nixosModules."${system}".chivesweave ];
        };
     };
  }
```

## Using services.chivesweave

In your configuration.nix you can enable chivesweave node as service.
Note that this is limited to nixos the operating system (as opposed to just nix the package manager).

```nix
{
  config = {
    services.chivesweave = {
      enable = true;
      peer = [
        "188.166.200.45"
        "188.166.192.169"
        "163.47.11.64"
      ];
      # see more options below
    };
  };
}
```

<!--  Generated in nix repl: (builtins.toJSON (builtins.mapAttrs (k: v: if (builtins.typeOf v == "set" && builtins.hasAttr "_type" v && v._type == "option") then {option = k; defaultValue = if (builtins.typeOf v == "set") then if (builtins.hasAttr "defaultText" v) then v.defaultText.text else v.default else v; description = if (builtins.typeOf v == "set") then v.description else v; } else {}) (import ./module.nix (pkgs // {chivesweave = {};})).options.services.chivesweave)) -->

_A schema of the available options as json_

```json
{
  "dataDir": {
    "defaultValue": "/chivesweave-data",
    "description": "Data directory path for chivesweave node.\n",
    "option": "dataDir"
  },
  "enable": {
    "defaultValue": false,
    "description": "Whether to enable Enable chivesweave node as systemd service\n.",
    "option": "enable"
  },
  "featuresDisable": {
    "defaultValue": [],
    "description": "List of features to disable.\n",
    "option": "featuresDisable"
  },
  "group": {
    "defaultValue": "users",
    "description": "Run Chivesweave Node under this group.",
    "option": "group"
  },
  "headerSyncJobs": {
    "defaultValue": 10,
    "description": "The pace for which to sync up with historical data.",
    "option": "headerSyncJobs"
  },
  "maxDiskPoolDataRootBufferMb": {
    "defaultValue": 500,
    "description": "Max disk-pool buffer size in mb.",
    "option": "maxDiskPoolDataRootBufferMb"
  },
  "maxMiners": {
    "defaultValue": 0,
    "description": "Max amount of miners to spawn, 0 means no mining will be performed.",
    "option": "maxMiners"
  },
  "maxParallelBlockIndexRequests": {
    "defaultValue": 2,
    "description": "As semaphore, the max amount of parallel block index requests to perform.",
    "option": "maxParallelBlockIndexRequests"
  },
  "maxParallelGetAndPackChunkRequests": {
    "defaultValue": 10,
    "description": "As semaphore, the max amount of parallel get chunk and pack requests to perform.",
    "option": "maxParallelGetAndPackChunkRequests"
  },
  "maxParallelGetChunkRequests": {
    "defaultValue": 100,
    "description": "As semaphore, the max amount of parallel get chunk requests to perform.",
    "option": "maxParallelGetChunkRequests"
  },
  "maxParallelGetSyncRecord": {
    "defaultValue": 2,
    "description": "As semaphore, the max amount of parallel get sync record requests to perform.",
    "option": "maxParallelGetSyncRecord"
  },
  "maxParallelGetTxDataRequests": {
    "defaultValue": 10,
    "description": "As semaphore, the max amount of parallel get transaction data requests to perform.",
    "option": "maxParallelGetTxDataRequests"
  },
  "maxParallelPostChunkRequests": {
    "defaultValue": 100,
    "description": "As semaphore, the max amount of parallel post chunk requests to perform.",
    "option": "maxParallelPostChunkRequests"
  },
  "maxParallelWalletListRequests": {
    "defaultValue": 2,
    "description": "As semaphore, the max amount of parallel block index requests to perform.",
    "option": "maxParallelWalletListRequests"
  },
  "metricsDir": {
    "defaultValue": "/var/lib/chivesweave/metrics",
    "description": "Directory path for node metric outputs\n",
    "option": "metricsDir"
  },
  "package": {
    "defaultValue": "pkgs.chivesweave",
    "description": "The Chivesweave expression to use\n",
    "option": "package"
  },
  "peer": {
    "defaultValue": [],
    "description": "List of primary node peers\n",
    "option": "peer"
  },
  "transactionBlacklists": {
    "defaultValue": [],
    "description": "List of paths to textfiles containing blacklisted txids\n",
    "option": "transactionBlacklists"
  },
  "transactionWhitelists": {
    "defaultValue": [],
    "description": "List of paths to textfiles containing whitelisted txids\n",
    "option": "transactionWhitelists"
  },
  "user": {
    "defaultValue": "chivesweave",
    "description": "Run Chivesweave Node under this user.",
    "option": "user"
  }
}
```
