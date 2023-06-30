{-# LANGUAGE NumericUnderscores #-}

module Main where

import Streamly.External.LMDB.Channel
import qualified Streamly.External.LMDB.Tests
import Test.Tasty

main :: IO ()
main =
  defaultMain $
    withResource
      ( do
          chan <- createChannel $ defaultChannelOptions { channelTimeout = 60_000_000 }
          startChannel chan
          return chan
      )
      endChannel
      tests

tests :: IO Channel -> TestTree
tests res =
  testGroup
    "Tests"
    [ testGroup "Streamly.External.LMDB.Tests" $
        Streamly.External.LMDB.Tests.tests res
    ]
