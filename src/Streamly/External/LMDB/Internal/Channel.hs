{-# LANGUAGE NamedFieldPuns #-}

module Streamly.External.LMDB.Internal.Channel where

import Control.Concurrent
import Control.Exception.Safe
import Control.Monad
import Streamly.External.LMDB.Internal.Error
import System.Timeout

-- | A wrapper around 'Chan' that allows us to repeatedly, using 'runOnChannel', run IO actions on
-- the same thread.
data Channel = Channel
  { cChan :: !(Chan (IO ())),
    cRunLock :: !(MVar ()),
    cStart :: !(MVar ()),
    cStartLock :: !(MVar ()),
    cEnd :: !(MVar ()),
    cTimeout :: !Int
  }

newtype ChannelOptions = ChannelOptions
  { -- | The timeout, in microseconds, for IO actions that are 'runOnChannel'.
    channelTimeout :: Int
  }

-- | Creates a new channel.
createChannel :: ChannelOptions -> IO Channel
createChannel ChannelOptions {channelTimeout} = do
  cChan <- newChan
  cRunLock <- newMVar ()

  cStart <- newEmptyMVar
  cStartLock <- newMVar ()

  cEnd <- newEmptyMVar
  return $ Channel {cChan, cRunLock, cStart, cStartLock, cEnd, cTimeout = channelTimeout}

-- | Starts a channel. Each channel can only be started once.
startChannel :: Channel -> IO ()
startChannel Channel {cChan, cStart, cStartLock, cEnd} = do
  withMVarMasked cStartLock $ \() -> do
    isEmptyMVar cStart >>= flip unless (throwChanErr "channel was started more than once")
    putMVar cStart ()
  go
  where
    go = do
      join $ readChan cChan
      isEmptyMVar cEnd >>= flip when go

-- | Ends a channel. No further 'runOnChannel' should be done on the channel; otherwise timeout
-- exceptions will be thrown.
endChannel :: Channel -> IO ()
endChannel Channel {cChan, cEnd} = do
  putMVar cEnd ()
  writeChan cChan (return ())

-- | Runs an IO action on the given channel.
--
-- Before calling this function, please make sure the channel has first been started with
-- 'startChannel'. (When 'runOnChannel' is called on an unstarted or ended channel, it will throw a
-- timeout error as per 'channelTimeout'.)
--
-- The IO action will be executed on the /same thread/ on which 'startChannel' was run. If the IO
-- action throws a synchronous exception, it is rethrown; the channel keeps running.
runOnChannel :: Channel -> IO a -> IO a
runOnChannel Channel {cChan, cRunLock, cTimeout} io =
  -- TODO: Consider adding a maxBuffer to 'Channel'.
  withMVar cRunLock $ \() -> do
    -- Create a new result MVar for every 'runOnChannel' to avoid a rigid @Channel a@ type (i.e., to
    -- allow a different @a@ for each 'runOnChannel').
    result <- newEmptyMVar

    writeChan
      cChan
      ( -- Catch /synchronous/ exceptions and keep the channel running. (Note that this action
        -- doesn’t execute here, but on the startChannel thread.)
        tryAny io >>= putMVar result
      )

    -- Wait for result. (Despite the mask we could receive an asynchronous exception here.)
    me <- timeout cTimeout $ takeMVar result
    case me of
      Nothing ->
        throwChanErr $
          "runOnChannel timed out; unstarted or ended channel? "
            ++ "too large LMDB write transaction size? "
            ++ "a deadlock (caused by a streamly-lmdb bug)?"
      Just e -> either throwIO return e -- Rethrow synchronous exception from the IO action.

throwChanErr :: String -> m a
throwChanErr = throwError "Channel"
