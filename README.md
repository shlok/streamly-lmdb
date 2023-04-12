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

See `bench/README.md`. Summary (with rough figures from our machine<sup>†</sup> using GHC 9.2.7 [GHC 8.10.7]):

* **Reading.** For reading a fully cached LMDB database, this library (when `unsafeReadLMDB` is used instead of `readLMDB`) has roughly a 5 ns/pair [15 ns/pair] overhead compared to plain Haskell `IO` code, which has roughly another 10 ns/pair overhead compared to C. (The first two being similar fulfills the promise of [streamly](https://hackage.haskell.org/package/streamly) and stream fusion.) We deduce that if your total workload per pair takes longer than around 15 ns [25 ns], your bottleneck will not be your usage of this library as opposed to C.
* **Writing**. Writing with plain Haskell `IO` code and with this library is, respectively, roughly 15% [30%] and 50% slower than writing with C. We have not dug further into these differences because this write performance is currently good enough for our purposes.

(There have apparently been some performance improvements between GHC 8.10.7 and 9.2.7.)

<sup>†</sup> April 2023; [Linode](https://linode.com); Debian 11, Dedicated 32GB: 16 CPU, 640GB Storage, 32GB RAM.
