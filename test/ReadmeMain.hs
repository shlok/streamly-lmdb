{-# LANGUAGE OverloadedStrings #-}

module ReadmeMain where

import Data.Function
import qualified Streamly.Data.Fold as F
import qualified Streamly.Data.Stream.Prelude as S
import Streamly.External.LMDB

main :: IO ()
main = do
  -- Open an environment. There should already exist a file or
  -- directory at the given path. (Empty for a new environment.)
  env <-
    openEnvironment
      "/path/to/lmdb-database"
      defaultLimits {mapSize = tebibyte}

  -- Get the main database.
  -- Note: It is common practice with LMDB to create the database
  -- once and reuse it for the remainder of the program’s execution.
  db <- getDatabase env Nothing

  -- Stream key-value pairs into the database.
  withReadWriteTransaction env $ \txn ->
    [("baz", "a"), ("foo", "b"), ("bar", "c")]
      & S.fromList
      & S.fold (writeLMDB defaultWriteOptions db txn)

  -- Stream key-value pairs out of the
  -- database, printing them along the way.
  -- Output:
  --     ("bar","c")
  --     ("baz","a")
  --     ("foo","b")
  S.unfold readLMDB (defaultReadOptions, db, NoTxn)
    & S.mapM print
    & S.fold F.drain
