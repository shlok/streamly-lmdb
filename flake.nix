{
  description = "streamly-lmdb";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixpkgs-unstable";
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs = { self, nixpkgs, flake-utils }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        ghcVersion = "965";
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
                      lmdb = final.pkgs.lmdb;
                    };

                  # nixpkgs-unstable 4a4ecb0ab415c9fccfb005567a215e6a9564cdf5 (2024-06-03).
                  # We want Ormolu 0.7.4 for better commenting within if-else.
                  ormolu = hfinal.ormolu_0_7_4_0;
                  # This version of Ormolu requires ghc-lib-parser 9.8.x.
                  ghc-lib-parser = hfinal.ghc-lib-parser_9_8_2_20240223;
                  # Since we specify haskell-language-server below, we also need to bring a few more
                  # things in align with the same ghc-lib-parser. (The fourmolu and stylish-haskell
                  # lines should be avoidable by disabling those flags in haskell-language-server,
                  # but currently this seems non-trivial; see
                  # https://github.com/srid/haskell-flake/issues/245; see also
                  # configuration-ghc-9.2.x.nix in nixpkgs.)
                  fourmolu = hfinal.fourmolu_0_15_0_0;
                  ghc-lib-parser-ex = hfinal.ghc-lib-parser-ex_9_8_0_2;
                  hlint = hfinal.hlint_3_8;
                  stylish-haskell = hfinal.stylish-haskell_0_14_6_0;
                };
              };

              ${packageName} = final.myHaskellPkgs.${packageName};

              myDevShell = final.myHaskellPkgs.shellFor {
                packages = p: [p.${packageName}];
                nativeBuildInputs = [
                  final.myHaskellPkgs.cabal-install
                  final.myHaskellPkgs.haskell-language-server
                  final.myHaskellPkgs.ormolu
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
              (drv: { testFlags = ["--quickcheck-tests=500"]; });
        };

        devShells.default = pkgs.myDevShell;
      });
}
