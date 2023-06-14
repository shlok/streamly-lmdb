module Main where

import Streamly.External.LMDB
  ( Database,
    Environment,
    ReadWrite,
    defaultLimits,
    getDatabase,
    mapSize,
    openEnvironment,
    tebibyte,
  )
import qualified Streamly.External.LMDB.Tests (tests)
import Streamly.External.LMDB.Channel
import System.Directory (removeDirectoryRecursive)
import System.Environment (setEnv)
import System.IO.Temp (createTempDirectory, getCanonicalTemporaryDirectory)
import Test.Tasty (TestTree, defaultMain, testGroup, withResource)

main :: IO ()
main = do
  setEnv "TASTY_NUM_THREADS" "1" -- Multiple tests use the same LMDB database.
  defaultMain $
    withResource
      ( do
          tmpParent <- getCanonicalTemporaryDirectory
          tmpDir <- createTempDirectory tmpParent "streamly-lmdb-tests"
          env <- openEnvironment tmpDir $ defaultLimits {mapSize = tebibyte}
          chan <- createChannel defaultChannelOptions
          startChannel chan
          db <- getDatabase chan env Nothing
          return (tmpDir, (db, env, chan))
      )
      (\(tmpDir, _) -> removeDirectoryRecursive tmpDir)
      (\io -> tests $ snd <$> io)

tests :: IO (Database ReadWrite, Environment ReadWrite, Channel) -> TestTree
tests res =
  testGroup
    "Tests"
    [ testGroup "Streamly.External.LMDB.Tests" $
        Streamly.External.LMDB.Tests.tests res
    ]
