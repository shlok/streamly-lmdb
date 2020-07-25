# streamly-lmdb

[![Hackage](https://img.shields.io/hackage/v/streamly-lmdb.svg?style=flat)](https://hackage.haskell.org/package/streamly-lmdb)
[![Build Status](https://travis-ci.org/shlok/streamly-lmdb.svg?branch=master)](https://travis-ci.org/shlok/streamly-lmdb)

Stream data to or from LMDB databases using the Haskell [streamly](https://hackage.haskell.org/package/streamly) library.

## Requirements

Install LMDB on your system:

* Debian Linux: `sudo apt-get install liblmdb-dev`.
* macOS: `brew install lmdb`.

## Quick start

```haskell
{-# LANGUAGE OverloadedStrings #-}

module Main where

import Streamly.External.LMDB
    (Limits (mapSize), WriteOptions (chunkSize), defaultLimits,
    defaultWriteOptions, getDatabase, openEnvironment, readLMDB,
    tebibyte, writeLMDB)
import qualified Streamly.Prelude as S

main :: IO ()
main = do
    -- Open an environment. There should already exist a file or
    -- directory at the given path. (Empty for a new environment.)
    env <- openEnvironment "/path/to/lmdb-database" $
            defaultLimits { mapSize = tebibyte }

    -- Get the main database.
    -- Note: It is common practice with LMDB to create the database
    -- once and reuse it for the remainder of the program’s execution.
    db <- getDatabase env Nothing

    -- Stream key-value pairs into the database.
    let fold' = writeLMDB db defaultWriteOptions { chunkSize = 1 }
    let writeStream = S.fromList [("baz", "a"), ("foo", "b"), ("bar", "c")]
    _ <- S.fold fold' writeStream

    -- Stream key-value pairs out of the
    -- database, printing them along the way.
    -- Output:
    --     ("bar","c")
    --     ("baz","a")
    --     ("foo","b")
    let unfold' = readLMDB db
    let readStream = S.unfold unfold' undefined
    S.mapM_ print readStream
```

## Benchmarks

See `bench/README.md`. Summary (with rough figures from our machine<sup>†</sup>):

* **Reading.** For reading a fully cached LMDB database, this library (when `unsafeReadLMDB` is used instead of `readLMDB`) has a 10 ns/pair overhead compared to plain Haskell `IO` code, which has another 10 ns/pair overhead compared to C. (The first two being similar fulfills the promise of [streamly](https://hackage.haskell.org/package/streamly) and stream fusion.) We deduce that if your total workload per pair takes longer than 20 ns, your bottleneck will not be your usage of this library as opposed to C.
* **Writing**. Writing with plain Haskell `IO` code and with this library is, respectively, 10% and 20% slower than writing with C. We have not dug further into these differences because this write performance is currently good enough for our purposes.

<sup>†</sup> [Linode](https://linode.com); Debian 10, Dedicated 32GB: 16 CPU, 640GB Storage, 32GB RAM.
