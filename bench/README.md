# streamly-lmdb benchmarks

This directory contains three command-line programs for accessing an LMDB database:

1. `bench-lmdb.c`: A plain C program.
2. `bench-lmdb-plain.hs`: A plain Haskell program (plain `IO`-monadic code) that has the same functionality as the C program.
3. `bench-lmdb-streamly.hs`: A Haskell program that has the same functionality but uses our library (streamly-lmdb).

These programs are currently undocumented, apart from the brief usage descriptions that are shown when the programs are run without arguments.

## Running benchmarks

* Install [Stack](https://docs.haskellstack.org/en/stable/README/).
* Install LMDB on your system. Debian Linux: `sudo apt-get install liblmdb-dev`. macOS: `brew install lmdb`.
* Install other necessary programs. Debian Linux: `sudo apt-get install lmdb-utils pcregrep`. macOS: `brew install gnu-time TODO`.
* Run `stack build` and `stack exec -- bench`.
* View the `csv` files. We are currently not explaining the columns in detail here, but our main conclusions are outlined in `../README.md`.
