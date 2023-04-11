{
  description = "streamly-lmdb-bench";

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
                  streamly-lmdb-bench =
                    final.haskell.lib.compose.addBuildDepends
                      [final.pkgs.lmdb]
                      (hfinal.callCabal2nix "streamly-lmdb-bench" ./. {
                        # Use local streamly-lmdb in "../".
                        streamly-lmdb =
                          final.haskell.lib.compose.addBuildDepends
                          [final.pkgs.lmdb]
                          (hfinal.callCabal2nix "streamly-lmdb" ../. {});
                      });
                };
              };

              streamly-lmdb-bench = final.myHaskellPkgs.streamly-lmdb-bench;

              myDevShell = final.myHaskellPkgs.shellFor {
                packages = p: [p.streamly-lmdb-bench];

                buildInputs = [final.pkgs.lmdb];

                nativeBuildInputs = [
                  final.myHaskellPkgs.cabal-install
                  final.myHaskellPkgs.haskell-language-server
                  final.pkgs.pcre
                ];
              };
            })
        ];

        pkgs = import nixpkgs { inherit config overlays system; };
      in {
        packages = {
          default = pkgs.streamly-lmdb-bench;
          streamly-lmdb-bench = pkgs.streamly-lmdb-bench;
        };
        
        devShells.default = pkgs.myDevShell;
      });
}
