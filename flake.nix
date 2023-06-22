{
  description = "streamly-lmdb";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixpkgs-unstable";
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs = { self, nixpkgs, flake-utils }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        ghcVersion = "927";
        packageName = "streamly-lmdb";
        config = {};

        overlays = [
          (final: prev:
            let
              haskellPkgs = final.haskell.packages."ghc${ghcVersion}";
            in {
              myHaskellPkgs = haskellPkgs.override {
                overrides = hfinal: hprev: {
                  ${packageName} =
                    hfinal.callCabal2nix packageName ./. {
                      streamly = hprev.streamly_0_9_0.overrideAttrs (old: {
                          buildInputs =
                            if system == "x86_64-darwin" || system == "aarch64-darwin"
                              then [final.pkgs.darwin.apple_sdk.frameworks.Cocoa]
                              else [];
                        });
                      lmdb = pkgs.lmdb;
                    };
                };
              };

              ${packageName} = final.myHaskellPkgs.${packageName};

              myDevShell = final.myHaskellPkgs.shellFor {
                packages = p: [p.${packageName}];
                nativeBuildInputs = [
                  final.myHaskellPkgs.cabal-install
                  final.myHaskellPkgs.haskell-language-server
                  final.pkgs.clang-tools
                ];
              };
            })
        ];

        pkgs = import nixpkgs { inherit config overlays system; };
      in {
        packages = {
          default = pkgs.${packageName};
          ${packageName} = pkgs.${packageName};
          "${packageName}-ci" =
            pkgs.haskell.lib.overrideCabal
              pkgs.${packageName}
              (drv: { testFlags = ["--quickcheck-tests=200"]; });
        };

        devShells.default = pkgs.myDevShell;
      });
}
