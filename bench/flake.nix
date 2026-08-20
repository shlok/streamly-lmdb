{
  description = "streamly-lmdb-bench";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixpkgs-unstable";
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs = { self, nixpkgs, flake-utils }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        ghcVersion = "9103";
        packageName = "streamly-lmdb-bench";
        config = {};

        overlays = [
          (final: prev:
            let
              haskellPkgs = final.haskell.packages."ghc${ghcVersion}";
            in {
              myHaskellPkgs = haskellPkgs.override {
                overrides = hfinal: hprev: {
                  ${packageName} =
                    hfinal.callCabal2nix "${packageName}" ./. {
                      # Use local streamly-lmdb in "../".
                      streamly-lmdb =
                        hfinal.callCabal2nix "streamly-lmdb" ../. {
                          lmdb = final.pkgs.lmdb;
                        };
                    };

                  streamly = hfinal.callHackageDirect {
                    pkg = "streamly";
                    ver = "0.11.1";
                    sha256 = "sha256-4h1MwaN7eXMvzXKyjggIjjR3BlsGzl4vfCO7VBGGvrc=";
                  } {};
                  streamly-core = hfinal.callHackageDirect {
                    pkg = "streamly-core";
                    ver = "0.3.1";
                    sha256 = "sha256-k9h+I74GNsluf55hJFDZiLwEO2x9moFvtCarCeCpaa4=";
                  } {};
                };
              };

              ${packageName} = final.myHaskellPkgs.${packageName};

              myDevShell = final.myHaskellPkgs.shellFor {
                packages = p: [p.${packageName}];
                nativeBuildInputs = [
                  final.myHaskellPkgs.cabal-install
                  final.myHaskellPkgs.haskell-language-server
                  final.myHaskellPkgs.ormolu
                  final.pkgs.pcre
                  final.pkgs.clang-tools

                  # Needed so bench can compile the C program.
                  final.pkgs.lmdb
                ];
              };
            })
        ];

        pkgs = import nixpkgs { inherit config overlays system; };
      in {
        packages = {
          default = pkgs.${packageName};
          ${packageName} = pkgs.${packageName};
        };
        
        devShells.default = pkgs.myDevShell;
      });
}
