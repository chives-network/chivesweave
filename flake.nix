{
  description = "The Chivesweave server and App Developer Toolkit.";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixpkgs-unstable";
    utils.url = "github:numtide/flake-utils";
    flake-compat = {
      url = "github:edolstra/flake-compat";
      flake = false;
    };
  };

  outputs = { self, nixpkgs, utils, ... }: utils.lib.eachDefaultSystem (system:
    let
      pkgs = import nixpkgs { inherit system; };
      chivesweave = pkgs.callPackage ./nix/chivesweave.nix { inherit pkgs; };
    in {
      packages = utils.lib.flattenTree {
        inherit chivesweave;
      };

      nixosModules.chivesweave = {
        imports = [ ./nix/module.nix ];
        nixpkgs.overlays = [ (prev: final: { inherit chivesweave; }) ];
      };

      defaultPackage = self.packages."${system}".chivesweave;

    });

}
