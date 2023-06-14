{-# LANGUAGE NamedFieldPuns #-}
{-# LANGUAGE NumericUnderscores #-}

module Streamly.External.LMDB.Channel
  ( Channel,
    ChannelOptions (..),
    defaultChannelOptions,
    createChannel,
    startChannel,
    endChannel,
  )
where

import Control.Concurrent (forkOS)
import Control.Monad
import qualified Streamly.External.LMDB.Internal.Channel as I

-- | A channel for use with the write-related functionality in this library. (This is needed to
-- maintain certain low-level LMDB requirements.)
--
-- We suggest simply creating a single channel (e.g., in @main@) and reusing it for the remainder of
-- the program’s execution:
--
-- @
-- chan <- createChannel defaultChannelOptions
-- startChannel chan
-- @
--
-- You are however welcome to create more channels, as long as you heed the documented caveats that
-- may accompany some of the write-related functions.
type Channel = I.Channel

newtype ChannelOptions = ChannelOptions
  { -- | The timeout, in microseconds, for actions that are (internally within the library) run on
    -- the channel.
    channelTimeout :: Int
  }

-- | A default timeout of 10,000,000 microseconds (10 seconds).
defaultChannelOptions :: ChannelOptions
defaultChannelOptions = ChannelOptions {channelTimeout = 10_000_000}

-- | Creates a new channel.
createChannel :: ChannelOptions -> IO Channel
createChannel ChannelOptions {channelTimeout} =
  I.createChannel $ I.ChannelOptions {I.channelTimeout = channelTimeout}

-- | Starts a channel. Each channel can only be started once.
startChannel :: Channel -> IO ()
startChannel =
  -- For this library, we are only interested in channels on bound threads.
  void . forkOS . I.startChannel

-- | Ends a channel. The channel should not be used afterwards; otherwise timeout exceptions will be
-- thrown.
--
-- If you have merely a few dozen channels at most, there is often no need for this; it is
-- considered normal practice to simply create your channel(s) once and reuse them for the remainder
-- of the program’s execution.
endChannel :: Channel -> IO ()
endChannel = I.endChannel
