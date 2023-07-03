{-# LANGUAGE GeneralizedNewtypeDeriving #-}

module Streamly.External.LMDB.Internal where

import Control.Concurrent
import Control.Concurrent.STM
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
      !(TMVar NumReaders, MVar WriteCounter, MVar WriteOwner, MVar WriteOwnerData, CloseDbLock)

-- The number of current readers. This needs to be kept track of due to MDB_NOLOCK; see comments in
-- writeLMDB.
newtype NumReaders = NumReaders Int deriving (Eq, Num, Ord)

-- An increasing counter for various write-related functions using the same environment.
newtype WriteCounter = WriterCounter Int deriving (Bounded, Eq, Num, Ord)

-- The counter that currently owns the environment when it comes to writing.
newtype WriteOwner = WriteOwner WriteCounter

-- Data that the current 'WriteOwner' keeps track of. (This needs to be separate from 'WriteOwner'
-- because 'modifyMVar' is not atomic when faced with other 'putMVar's.)
newtype WriteOwnerData = WriteOwnerData {wPtxn :: Ptr MDB_txn}

-- For closeDatabase serialization.
newtype CloseDbLock = CloseDbLock (MVar ())

data Database mode = Database !(Environment mode) !MDB_dbi_t
