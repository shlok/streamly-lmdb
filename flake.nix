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
        config = {};

        overlays = [
          (final: prev:
            let
              haskellPkgs = final.haskell.packages."ghc${ghcVersion}";
            in {
              myHaskellPkgs = haskellPkgs.override {
                overrides = hfinal: hprev: {
                  streamly-lmdb =
                    final.haskell.lib.compose.addBuildDepends
                      [final.pkgs.lmdb]
                      (hfinal.callCabal2nix "streamly-lmdb" ./. {
                          streamly = hfinal.streamly_0_9_0;
                      });
                };
              };

              streamly-lmdb = final.myHaskellPkgs.streamly-lmdb;

              myDevShell = final.myHaskellPkgs.shellFor {
                packages = p: [p.streamly-lmdb];

                buildInputs = [final.pkgs.lmdb];

                nativeBuildInputs = [
                  final.myHaskellPkgs.cabal-install
                  final.myHaskellPkgs.haskell-language-server
                ];
              };
            })
        ];

        pkgs = import nixpkgs { inherit config overlays system; };
      in {
        packages = {
          default = pkgs.streamly-lmdb;
          streamly-lmdb = pkgs.streamly-lmdb;
        };
        
        devShells.default = pkgs.myDevShell;
      });
}
