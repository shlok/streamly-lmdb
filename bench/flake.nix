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
                    final.haskell.lib.compose.addBuildDepends
                      [final.pkgs.lmdb]
                      (hfinal.callCabal2nix packageName ./. {
                        # Use local streamly-lmdb in "../".
                        streamly-lmdb =
                          final.haskell.lib.compose.addBuildDepends
                          [final.pkgs.lmdb]
                          (hfinal.callCabal2nix "streamly-lmdb" ../. {
                            streamly = hfinal.streamly_0_9_0;
                          });
                      });
                };
              };

              ${packageName} = final.myHaskellPkgs.${packageName};

              myDevShell = final.myHaskellPkgs.shellFor {
                packages = p: [p.${packageName}];

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
          default = pkgs.${packageName};
          ${packageName} = pkgs.${packageName};
        };
        
        devShells.default = pkgs.myDevShell;
      });
}
