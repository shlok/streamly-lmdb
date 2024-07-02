# streamly-lmdb benchmarks

This directory contains three command-line programs for accessing an LMDB database:

1. `bench-lmdb.c`: A plain C program.
2. `bench-lmdb-plain.hs`: A plain Haskell program (plain `IO`-monadic code) that has the same functionality as the C program.
3. `bench-lmdb-streamly.hs`: A Haskell program that has the same functionality but uses our library (streamly-lmdb).

These programs are currently undocumented, apart from the brief usage descriptions that are shown when the programs are run without arguments.

## Running benchmarks

* Note (July 2024): We have only tested this on NixOS 22.11. It should work fine on other systems (e.g., Debian Linux); feel free to try.
* If you are on NixOS, please make sure you have enabled Nix Flakes.
* If you are on a non-NixOS system, please make sure you have installed [Nix](https://nixos.org) and enabled Nix Flakes.
* Within this directory (`bench`), enter the development shell with `nix develop -L`.
* Run `cabal build -ffusion-plugin && cabal exec -- bench`.
* View `stdout` and the `csv` files. We are currently not explaining the data in detail here, but our main conclusions are outlined in `../README.md`.
