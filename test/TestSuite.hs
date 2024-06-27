module Main where

import qualified Streamly.External.LMDB.Tests
import Test.Tasty

main :: IO ()
main = defaultMain tests

tests :: TestTree
tests =
  testGroup
    "Tests"
    [ testGroup
        "Streamly.External.LMDB.Tests"
        Streamly.External.LMDB.Tests.tests
    ]
