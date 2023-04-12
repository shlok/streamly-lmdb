# streamly-lmdb benchmarks

This directory contains three command-line programs for accessing an LMDB database:

1. `bench-lmdb.c`: A plain C program.
2. `bench-lmdb-plain.hs`: A plain Haskell program (plain `IO`-monadic code) that has the same functionality as the C program.
3. `bench-lmdb-streamly.hs`: A Haskell program that has the same functionality but uses our library (streamly-lmdb).

These programs are currently undocumented, apart from the brief usage descriptions that are shown when the programs are run without arguments.

## Running benchmarks

* Note (April 2023): We have only tested this on Debian 11. Feel free to try on other systems.
* Make sure you have installed [Nix](https://nixos.org) with support for Nix Flakes.
* Within this directory (`bench`), enter the development shell with `nix develop`.
* Run `cabal build` and `cabal exec -- bench`. (You will get results for the GHC version specified in `flake.nix`.)
* View the `csv` files. We are currently not explaining the columns in detail here, but our main conclusions are outlined in `../README.md`.
