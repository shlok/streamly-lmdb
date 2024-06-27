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

* Reading (iterating through a fully cached LMDB database):
  - When using the ordinary `readLMDB` (which creates intermediate key/value `ByteString`s managed by the RTS), the overhead compared to C depends on the key/value sizes; for 480-byte keys and 2400-byte values, the overhead is roughly 950 ns/pair.
  - By using `unsafeReadLMDB` instead of `readLMDB` (to avoid the intermediate `ByteString`s), we can get the overhead compared to C down to roughly 90 ns/pair. (Plain Haskell `IO` code has roughly a 50 ns/pair overhead compared to C. The two preceding figures being similar fulfills the promise of [streamly](https://hackage.haskell.org/package/streamly) and stream fusion.)
* Writing:
  - The overhead of this library compared to C depends on the size of the key/value pairs (`ByteString`s managed by the RTS). For 480-byte keys and 2400-byte values, the overhead is around 3.5 μs/pair.
  - For now, we don’t provide “unsafe” write functionality (to avoid the key/value `ByteString`s) because this write performance is currently good enough for our purposes.
* For reference, we note that opening and reading 1 byte [16 KiB] from a file on disk with C takes us over 2.5 μs [20 μs].)

<sup>†</sup> June 2024; NixOS 22.11; Intel i7-12700K (3.6 GHz, 12 cores); Corsair VENGEANCE LPX DDR4 RAM 64GB (2 x 32GB) 3200MHz; Samsung 970 EVO Plus SSD 2TB (M.2 NVMe).
