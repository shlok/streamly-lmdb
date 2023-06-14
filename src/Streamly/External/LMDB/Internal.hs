{-# LANGUAGE GeneralizedNewtypeDeriving #-}

module Streamly.External.LMDB.Internal where

import Control.Concurrent
import Data.Time.Clock
import Foreign
import Streamly.External.LMDB.Internal.Foreign

-- This is in a separate internal module because the tests make use of the Database constructor.

class Mode a where
  isReadOnlyMode :: a -> Bool

data ReadWrite

data ReadOnly

instance Mode ReadWrite where isReadOnlyMode _ = False

instance Mode ReadOnly where isReadOnlyMode _ = True

data Environment mode
  = Environment
      !(Ptr MDB_env)
      -- 'Just' for 'ReadWrite' (for use with the various 'ReadWrite'-based functions); 'Nothing'
      -- for 'ReadOnly'.
      !(Maybe (MVar WriteCounter, MVar WriteOwner, MVar WriteOwnerData))

-- An increasing counter for the various 'ReadWrite'-based functions using the same environment.
newtype WriteCounter = WriteCounter Int deriving (Bounded, Eq, Num, Ord)

-- The counter that currently owns the environment.
newtype WriteOwner = WriteOwner WriteCounter

-- Various data that the current owner keeps track of. (This needs to be separate from 'WriteOwner'
-- because 'modifyMVar' is not atomic when faced with other 'putMVar's.)
data WriteOwnerData = WriteOwnerData
  { wPtxn :: !(Ptr MDB_txn),
    wLastPairTime :: !UTCTime
  }

data Database mode = Database !(Environment mode) !MDB_dbi_t
