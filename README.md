# streamly-lmdb

[![Hackage](https://img.shields.io/hackage/v/streamly-lmdb.svg?style=flat)](https://hackage.haskell.org/package/streamly-lmdb)
![CI](https://github.com/shlok/streamly-lmdb/workflows/CI/badge.svg?branch=master)

Stream data to or from LMDB databases using the Haskell [streamly](https://hackage.haskell.org/package/streamly) library.

## Requirements

Install LMDB on your system:

* Debian Linux: `sudo apt-get install liblmdb-dev`.
* macOS: `brew install lmdb`.

## Quick start

```haskell
{-# LANGUAGE OverloadedStrings #-}

module Main where

import Data.Function ((&))
import qualified Streamly.Data.Fold as F
import qualified Streamly.Data.Stream.Prelude as S
import Streamly.External.LMDB
  ( Limits (mapSize),
    WriteOptions (writeTransactionSize),
    defaultLimits,
    defaultReadOptions,
    defaultWriteOptions,
    getDatabase,
    openEnvironment,
    readLMDB,
    tebibyte,
    writeLMDB,
  )

main :: IO ()
main = do
  -- Open an environment. There should already exist a file or
  -- directory at the given path. (Empty for a new environment.)
  env <-
    openEnvironment "/path/to/lmdb-database" $
      defaultLimits {mapSize = tebibyte}

  -- Get the main database.
  -- Note: It is common practice with LMDB to create the database
  -- once and reuse it for the remainder of the program’s execution.
  db <- getDatabase env Nothing

  -- Stream key-value pairs into the database.
  let fold' = writeLMDB db defaultWriteOptions {writeTransactionSize = 1}
  let writeStream = S.fromList [("baz", "a"), ("foo", "b"), ("bar", "c")]
  _ <- S.fold fold' writeStream

  -- Stream key-value pairs out of the
  -- database, printing them along the way.
  -- Output:
  --     ("bar","c")
  --     ("baz","a")
  --     ("foo","b")
  let unfold' = readLMDB db Nothing defaultReadOptions
  let readStream = S.unfold unfold' undefined
  S.mapM print readStream
    & S.fold F.drain
```

## Benchmarks

See `bench/README.md`. Summary (with rough figures from our machine<sup>†</sup>):

* **Reading:**
  - For iterating through a fully cached LMDB database, this library has roughly a 110 ns/pair overhead compared to C. (Plain Haskell `IO` code has roughly a 70 ns/pair overhead compared to C. The two preceding figures being similar fulfills the promise of [streamly](https://hackage.haskell.org/package/streamly) and stream fusion.)
  - By using `unsafeReadLMDB` instead of `readLMDB`, you can get the overhead down to roughly 100 ns/pair.
  - By additionally using the `readUnsafeFFI` option (to use `unsafe` FFI calls under the hood), you can get the overhead down to roughly 40 ns/pair.

* **Writing:**
  - For writing to an LMDB database, this library has roughly a 210 ns/pair overhead compared to C. (Plain Haskell `IO` code has roughly a 100 ns/pair overhead compared to C. The two preceding figures being similar fulfills the promise of [streamly](https://hackage.haskell.org/package/streamly) and stream fusion.)
  - By using the `writeUnsafeFFI` option (to use `unsafe` FFI calls under the hood), you can get the overhead down to roughly 140 ns/pair.

<sup>†</sup> May 2023; [Linode](https://linode.com); Debian 11, Dedicated 32GB: 16 CPU, 640GB Storage, 32GB RAM.
