{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE NamedFieldPuns #-}
{-# LANGUAGE NumericUnderscores #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}

-- | = Acknowledgments
--
-- The functionality for the limits and getting the environment and database, in particular the idea
-- of specifying the read-only or read-write mode at the type level, was mostly obtained from the
-- [lmdb-simple](https://hackage.haskell.org/package/lmdb-simple) library.
module Streamly.External.LMDB
  ( -- * Environment

    -- | With LMDB, one first creates a so-called “environment,” which one can think of as a file or
    -- folder on disk.
    Environment,
    openEnvironment,
    isReadOnlyEnvironment,
    closeEnvironment,

    -- ** Mode
    Mode,
    ReadWrite,
    ReadOnly,

    -- ** Limits
    Limits (..),
    defaultLimits,
    gibibyte,
    tebibyte,

    -- * Database

    -- | After creating an environment, one creates within it one or more databases.
    Database,
    getDatabase,
    clearDatabase,
    closeDatabase,

    -- * Relationship between reading and writing
    -- $readingWritingRelationship

    -- * Reading
    readLMDB,
    unsafeReadLMDB,

    -- ** Read-only transactions and cursors
    ReadOnlyTxn,
    beginReadOnlyTxn,
    abortReadOnlyTxn,
    Cursor,
    openCursor,
    closeCursor,

    -- ** Read options
    ReadOptions (..),
    defaultReadOptions,
    ReadDirection (..),

    -- * Writing
    writeLMDB,
    WriteOptions (..),
    defaultWriteOptions,
    OverwriteOptions (..),
    WriteAppend (..),
    WriteFailureFold,
    writeFailureThrow,
    writeFailureThrowDebug,
    writeFailureStop,
    writeFailureIgnore,

    -- * Error types
    LMDB_Error (..),
    MDB_ErrCode (..),
  )
where

import Control.Concurrent
import Control.Concurrent.STM
import Control.Exception.Safe
import Control.Monad
import Control.Monad.IO.Class (MonadIO, liftIO)
import Data.ByteString (ByteString, packCStringLen)
import Data.ByteString.Unsafe (unsafePackCStringLen, unsafeUseAsCStringLen)
import Data.Maybe
import Data.Void (Void)
import Foreign (Ptr, alloca, free, malloc, nullPtr, peek)
import Foreign.C (Errno (Errno), eNOTDIR)
import Foreign.C.String (CStringLen)
import Foreign.Marshal.Utils (with)
import Foreign.Storable (poke)
import qualified Streamly.Data.Fold as F
import Streamly.External.LMDB.Channel (Channel)
import Streamly.External.LMDB.Internal
import Streamly.External.LMDB.Internal.Channel hiding (Channel)
import Streamly.External.LMDB.Internal.Error
import Streamly.External.LMDB.Internal.Foreign
import Streamly.Internal.Data.Fold (Fold (Fold), Step (..))
import Streamly.Internal.Data.IOFinalizer
import Streamly.Internal.Data.Stream.StreamD.Type (Step (Stop, Yield))
import Streamly.Internal.Data.Unfold (lmap)
import Streamly.Internal.Data.Unfold.Type (Unfold (Unfold))
import Text.Printf

isReadOnlyEnvironment :: forall mode. (Mode mode) => Bool
isReadOnlyEnvironment = isReadOnlyMode @mode (error "isReadOnlyEnvironment: unreachable")

-- | LMDB environments have various limits on the size and number of databases and concurrent
-- readers.
data Limits = Limits
  { -- | Memory map size, in bytes (also the maximum size of all databases).
    mapSize :: !Int,
    -- | Maximum number of named databases.
    maxDatabases :: !Int,
    -- | Maximum number of concurrent 'ReadOnly' transactions
    --   (also the number of slots in the lock table).
    maxReaders :: !Int
  }

-- | The default limits are 1 MiB map size, 0 named databases, and 126 concurrent readers. These can
-- be adjusted freely, and in particular the 'mapSize' may be set very large (limited only by
-- available address space). However, LMDB is not optimized for a large number of named databases so
-- 'maxDatabases' should be kept to a minimum.
--
-- The default 'mapSize' is intentionally small, and should be changed to something appropriate for
-- your application. It ought to be a multiple of the OS page size, and should be chosen as large as
-- possible to accommodate future growth of the database(s). Once set for an environment, this limit
-- cannot be reduced to a value smaller than the space already consumed by the environment, however
-- it can later be increased.
--
-- If you are going to use any named databases then you will need to change 'maxDatabases' to the
-- number of named databases you plan to use. However, you do not need to change this field if you
-- are only going to use the single main (unnamed) database.
defaultLimits :: Limits
defaultLimits =
  Limits
    { mapSize = 1_024 * 1_024, -- 1 MiB.
      maxDatabases = 0,
      maxReaders = 126
    }

-- | A convenience constant for obtaining a 1 GiB map size.
gibibyte :: Int
gibibyte = 1_024 * 1_024 * 1_024

-- | A convenience constant for obtaining a 1 TiB map size.
tebibyte :: Int
tebibyte = 1_024 * 1_024 * 1_024 * 1_024

-- | Open an LMDB environment in either 'ReadWrite' or 'ReadOnly' mode. The 'FilePath' argument may
-- be either a directory or a regular file, but it must already exist. If a regular file, an
-- additional file with "-lock" appended to the name is used for the reader lock table.
--
-- Note that an environment must have been opened in 'ReadWrite' mode at least once before it can be
-- opened in 'ReadOnly' mode.
--
-- An environment opened in 'ReadOnly' mode may still modify the reader lock table (except when the
-- filesystem is read-only, in which case no locks are used).
--
-- To satisfy certain low-level LMDB requirements, please do not have opened the same environment
-- (i.e., the same 'FilePath') more than once in the same process at the same time. Furthermore,
-- please use the environment in the process that opened it (not after forking a new process).
openEnvironment :: forall mode. Mode mode => FilePath -> Limits -> IO (Environment mode)
openEnvironment path limits = do
  -- Low-level requirements:
  -- https://github.com/LMDB/lmdb/blob/8d0cbbc936091eb85972501a9b31a8f86d4c51a7/libraries/liblmdb/lmdb.h#L100,
  -- https://github.com/LMDB/lmdb/blob/8d0cbbc936091eb85972501a9b31a8f86d4c51a7/libraries/liblmdb/lmdb.h#L102

  penv <- mdb_env_create

  mdb_env_set_mapsize penv (mapSize limits)
  let maxDbs = maxDatabases limits in when (maxDbs /= 0) $ mdb_env_set_maxdbs penv maxDbs
  mdb_env_set_maxreaders penv (maxReaders limits)

  mvars <- (,,,) <$> newTMVarIO 0 <*> newMVar (-1) <*> newEmptyMVar <*> newEmptyMVar

  -- Always use MDB_NOTLS; this is crucial for Haskell applications; see
  -- https://github.com/LMDB/lmdb/blob/8d0cbbc936091eb85972501a9b31a8f86d4c51a7/libraries/liblmdb/lmdb.h#L615
  --
  -- Always use MDB_NOLOCK; see comments in writeLMDB.
  let env = Environment penv mvars :: Mode mode => Environment mode
      flags = mdb_notls : mdb_nolock : [mdb_rdonly | isReadOnlyEnvironment @mode]

  let isNotDirectoryError :: LMDB_Error -> Bool
      isNotDirectoryError LMDB_Error {e_code = Left code}
        | Errno (fromIntegral code) == eNOTDIR = True
      isNotDirectoryError _ = False

  r <- tryJust (guard . isNotDirectoryError) $ mdb_env_open penv path (combineOptions flags)
  case r of
    Left _ -> mdb_env_open penv path (combineOptions $ mdb_nosubdir : flags)
    Right _ -> return ()

  return env

-- | Closes the given environment.
--
-- If you have merely a few dozen environments at most, there should be no need for this. (It is a
-- common practice with LMDB to create one’s environments once and reuse them for the remainder of
-- the program’s execution.)
--
-- To satisfy certain low-level LMDB requirements:
--
-- * Please use the same 'Channel' for all calls to this function.
-- * Before calling this function:
--
--     * Call 'closeDatabase' on all databases in the environment.
--     * Close all cursors and commit/abort all transactions on the environment. You can achieve
--       this by passing in a precreated cursor and transaction to 'readLMDB' and 'unsafeReadLMDB',
--       and respectively close and commit/abort them afterwards. (Alternatively, if precreated
--       cursor and transaction are really not desired, it should be possible to manually trigger
--       garbage collection and wait a long enough time (e.g., 100 microseconds) for certain
--       internal finalizers to execute.)
--
-- * After calling this function, do not use the environment or any related databases, transactions,
--   and cursors.
closeEnvironment :: (Mode mode) => Channel -> Environment mode -> IO ()
closeEnvironment chan (Environment penv _) =
  -- Requirements:
  -- https://github.com/LMDB/lmdb/blob/8d0cbbc936091eb85972501a9b31a8f86d4c51a7/libraries/liblmdb/lmdb.h#L787
  runOnChannel chan . mask_ $
    c_mdb_env_close penv

-- | Gets a database with the given name. When creating a database (i.e., getting it for the first
-- time), one must do so in 'ReadWrite' mode.
--
-- If only one database is desired within the environment, the name can be 'Nothing' (known as the
-- “unnamed database”).
--
-- If one or more named databases (a database with a 'Just' name) are desired, the 'maxDatabases' of
-- the environment’s limits should have been adjusted accordingly. The unnamed database will in this
-- case contain the names of the named databases as keys, which one is allowed to read but not
-- write.
--
-- To satisfy certain low-level LMDB requirements, please use the same 'Channel' for all calls to
-- this function.
getDatabase ::
  forall mode.
  (Mode mode) =>
  Environment mode ->
  Maybe String ->
  IO (Database mode)
getDatabase env@(Environment penv mvars) name = mask_ $ do
  -- This function will presumably be called relatively rarely; for simplicity we therefore imagine
  -- for write serialization purposes that we’re always a writer (even though the mode could be
  -- ReadOnly). This by itself also takes care of lower-level concurrency requirements mentioned at
  -- https://github.com/LMDB/lmdb/blob/8d0cbbc936091eb85972501a9b31a8f86d4c51a7/libraries/liblmdb/lmdb.h#L1118
  -- (without the need of a channel).
  --
  -- The MVar/TMVar logic is similar as in writeLMDB.
  let (numReadersT, writeCounterM, writeOwnerM, writeOwnerDataM) = mvars
  writeCounter <- incrementWriteCounter writeCounterM
  putMVar writeOwnerM $ WriteOwner writeCounter
  let disclaimWriteOwnership = void . mask_ $ tryTakeMVar writeOwnerM >> tryTakeMVar writeOwnerDataM

  numReadersReset <-
    onException
      (waitForZeroReaders numReadersT)
      disclaimWriteOwnership

  dbi <-
    finally
      ( do
          ptxn <-
            finally
              ( mdb_txn_begin
                  penv
                  nullPtr
                  (combineOptions $ [mdb_rdonly | isReadOnlyEnvironment @mode])
              )
              -- Re-allow readers.
              numReadersReset

          onException
            ( mdb_dbi_open
                ptxn
                name
                (combineOptions $ [mdb_create | not $ isReadOnlyEnvironment @mode])
                <* mdb_txn_commit ptxn
            )
            (c_mdb_txn_abort ptxn)
      )
      disclaimWriteOwnership

  return $ Database env dbi

-- | Closes the given database.
--
-- If you have merely a few dozen databases at most, there should be no need for this. (It is a
-- common practice with LMDB to create one’s databases once and reuse them for the remainder of the
-- program’s execution.)
--
-- To satisfy certain low-level LMDB requirements:
--
-- * Please use the same 'Channel' for all calls to this function.
-- * Before calling this function, make sure all write transactions that have modified the database
--   have already been committed or aborted. (An unfortunate current state of affairs: If
--   'writeLMDB' encounters an asynchronous exception, an internal finalizer commits/aborts the
--   active transaction upon garbage collection. It should be possible to manually trigger garbage
--   collection and wait a long enough time (e.g., 100 microseconds) for the finalizer to execute.)
-- * After calling this function, do not use the database or any of its cursors again.
closeDatabase :: (Mode mode) => Channel -> Database mode -> IO ()
closeDatabase chan (Database (Environment penv _) dbi) =
  -- Requirements:
  -- https://github.com/LMDB/lmdb/blob/8d0cbbc936091eb85972501a9b31a8f86d4c51a7/libraries/liblmdb/lmdb.h#L1200
  -- TODO: Look more into the aforementioned limitation.
  runOnChannel chan . mask_ $
    c_mdb_dbi_close penv dbi

-- | Creates an unfold with which we can stream key-value pairs from the given database.
--
-- If an existing read-only transaction and cursor are not provided, a read-only transaction and
-- cursor are automatically created and kept open for the duration of the unfold; we suggest doing
-- this as a first option. However, if you find this to be a bottleneck (e.g., if you find upon
-- profiling that a significant time is being spent at @mdb_txn_begin@, or if you find yourself
-- having to increase 'maxReaders' in the environment’s limits because the transactions and cursors
-- are not being garbage collected fast enough), consider precreating a transaction and cursor using
-- 'beginReadOnlyTxn' and 'openCursor'.
--
-- If you don’t want the overhead of intermediate @ByteString@s (on your way to your eventual data
-- structures), use 'unsafeReadLMDB' instead.
{-# INLINE readLMDB #-}
readLMDB ::
  (MonadIO m, Mode mode) =>
  Database mode ->
  Maybe (ReadOnlyTxn mode, Cursor) ->
  ReadOptions ->
  Unfold m Void (ByteString, ByteString)
readLMDB db mtxncurs ropts = unsafeReadLMDB db mtxncurs ropts packCStringLen packCStringLen

-- | Similar to 'readLMDB', except that the keys and values are not automatically converted into
-- Haskell @ByteString@s.
--
-- To ensure safety, make sure that the memory pointed to by the 'CStringLen' for each key/value
-- mapping function call is (a) only read (and not written to); and (b) not used after the mapping
-- function has returned. One way to transform the 'CStringLen's to your desired data structures is
-- to use 'Data.ByteString.Unsafe.unsafePackCStringLen'.
{-# INLINE unsafeReadLMDB #-}
unsafeReadLMDB ::
  (MonadIO m, Mode mode) =>
  Database mode ->
  Maybe (ReadOnlyTxn mode, Cursor) ->
  ReadOptions ->
  (CStringLen -> IO k) ->
  (CStringLen -> IO v) ->
  Unfold m Void (k, v)
unsafeReadLMDB (Database (Environment penv mvars) dbi) mtxncurs ropts kmap vmap =
  let (firstOp, subsequentOp) = case (readDirection ropts, readStart ropts) of
        (Forward, Nothing) -> (mdb_first, mdb_next)
        (Forward, Just _) -> (mdb_set_range, mdb_next)
        (Backward, Nothing) -> (mdb_last, mdb_prev)
        (Backward, Just _) -> (mdb_set_range, mdb_prev)
      (txn_begin, cursor_open, cursor_get, cursor_close, txn_abort) =
        if readUnsafeFFI ropts
          then
            ( mdb_txn_begin_unsafe,
              mdb_cursor_open_unsafe,
              c_mdb_cursor_get_unsafe,
              c_mdb_cursor_close_unsafe,
              c_mdb_txn_abort_unsafe
            )
          else
            ( mdb_txn_begin,
              mdb_cursor_open,
              c_mdb_cursor_get,
              c_mdb_cursor_close,
              c_mdb_txn_abort
            )

      supply = lmap . const
   in supply firstOp $
        Unfold
          ( \(op, pcurs, pk, pv, ref) -> do
              rc <-
                liftIO $
                  if op == mdb_set_range && subsequentOp == mdb_prev
                    then do
                      -- A “reverse MDB_SET_RANGE” (i.e., a “less than or equal to”) is not
                      -- available in LMDB, so we simulate it ourselves.
                      kfst' <- peek pk
                      kfst <- packCStringLen (mv_data kfst', fromIntegral $ mv_size kfst')
                      rc <- cursor_get pcurs pk pv op
                      if rc /= 0 && rc == mdb_notfound
                        then cursor_get pcurs pk pv mdb_last
                        else
                          if rc == 0
                            then do
                              k' <- peek pk
                              k <- unsafePackCStringLen (mv_data k', fromIntegral $ mv_size k')
                              if k /= kfst
                                then cursor_get pcurs pk pv mdb_prev
                                else return rc
                            else return rc
                    else cursor_get pcurs pk pv op

              found <-
                liftIO $
                  if rc /= 0 && rc /= mdb_notfound
                    then do
                      runIOFinalizer ref
                      throwLMDBErrNum "mdb_cursor_get" rc
                    else return $ rc /= mdb_notfound

              if found
                then do
                  !k <- liftIO $ (\x -> kmap (mv_data x, fromIntegral $ mv_size x)) =<< peek pk
                  !v <- liftIO $ (\x -> vmap (mv_data x, fromIntegral $ mv_size x)) =<< peek pv
                  return $ Yield (k, v) (subsequentOp, pcurs, pk, pv, ref)
                else do
                  runIOFinalizer ref
                  return Stop
          )
          ( \op -> do
              let (numReadersT, _, _, _) = mvars

              (pcurs, pk, pv, ref) <- liftIO $ mask_ $ do
                (ptxn, pcurs) <- case mtxncurs of
                  Nothing -> do
                    atomically $ do
                      -- This is interruptible during brief moments just before and during the
                      -- beginning of a write transaction. See comments about MDB_NOLOCK in
                      -- writeLMDB.
                      n <- takeTMVar numReadersT
                      putTMVar numReadersT $ n + 1

                    ptxn <- txn_begin penv nullPtr mdb_rdonly
                    pcurs <- cursor_open ptxn dbi
                    return (ptxn, pcurs)
                  Just (ReadOnlyTxn _ ptxn, Cursor pcurs) ->
                    return (ptxn, pcurs)
                pk <- malloc
                pv <- malloc

                _ <- case readStart ropts of
                  Nothing -> return ()
                  Just k -> unsafeUseAsCStringLen k $ \(kp, kl) ->
                    poke pk (MDB_val (fromIntegral kl) kp)

                ref <- newIOFinalizer . mask_ $ do
                  free pv >> free pk
                  when (isNothing mtxncurs) $ do
                    -- With LMDB, there is ordinarily no need to commit read-only transactions. (The
                    -- exception is when we want to make databases that were opened during the
                    -- transaction available later, but that’s not applicable here.) We can
                    -- therefore abort ptxn, both for failure (exceptions) and success.
                    --
                    -- Note furthermore that this should be sound in the face of async exceptions
                    -- (where this finalizer could get called from a different thread) because LMDB
                    -- with MDB_NOTLS allows for read-only transactions being used from multiple
                    -- threads; see
                    -- https://github.com/LMDB/lmdb/blob/8d0cbbc936091eb85972501a9b31a8f86d4c51a7/libraries/liblmdb/lmdb.h#L984
                    cursor_close pcurs >> txn_abort ptxn

                    atomically $ do
                      -- This should not be interruptible because we are, of course, finishing up a
                      -- reader that is still known to exist.
                      n <- takeTMVar numReadersT
                      putTMVar numReadersT $ n - 1

                return (pcurs, pk, pv, ref)
              return (op, pcurs, pk, pv, ref)
          )

data ReadOnlyTxn mode = ReadOnlyTxn !(Environment mode) !(Ptr MDB_txn)

-- | Begins an LMDB read-only transaction for use with 'readLMDB' or 'unsafeReadLMDB'. It is your
-- responsibility to (a) use the transaction only on databases in the same environment, (b) make
-- sure that those databases were already obtained before the transaction was begun, and (c) dispose
-- of the transaction with 'abortReadOnlyTxn'.
beginReadOnlyTxn :: Environment mode -> IO (ReadOnlyTxn mode)
beginReadOnlyTxn env@(Environment penv mvars) = mask_ $ do
  let (numReadersT, _, _, _) = mvars

  -- Similar comments for NumReaders as in unsafeReadLMDB.
  atomically $ do
    n <- takeTMVar numReadersT
    putTMVar numReadersT $ n + 1

  onException
    (ReadOnlyTxn env <$> mdb_txn_begin penv nullPtr mdb_rdonly)
    ( atomically $ do
        n <- takeTMVar numReadersT
        putTMVar numReadersT $ n - 1
    )

-- | Disposes of a read-only transaction created with 'beginReadOnlyTxn'.
abortReadOnlyTxn :: ReadOnlyTxn mode -> IO ()
abortReadOnlyTxn (ReadOnlyTxn (Environment _ mvars) ptxn) = mask_ $ do
  let (numReadersT, _, _, _) = mvars
  c_mdb_txn_abort ptxn
  -- Similar comments for NumReaders as in unsafeReadLMDB.
  atomically $ do
    n <- takeTMVar numReadersT
    putTMVar numReadersT $ n - 1

newtype Cursor = Cursor (Ptr MDB_cursor)

-- | Opens a cursor for use with 'readLMDB' or 'unsafeReadLMDB'. It is your responsibility to (a)
-- make sure the cursor only gets used by a single 'readLMDB' or 'unsafeReadLMDB' @Unfold@ at the
-- same time (to be safe, one can open a new cursor for every 'readLMDB' or 'unsafeReadLMDB' call),
-- (b) make sure the provided database is within the environment on which the provided transaction
-- was begun, and (c) dispose of the cursor with 'closeCursor' (logically before 'abortReadOnlyTxn',
-- although the order doesn’t really matter for read-only transactions).
openCursor :: ReadOnlyTxn mode -> Database mode -> IO Cursor
openCursor (ReadOnlyTxn _ ptxn) (Database _ dbi) =
  Cursor <$> mdb_cursor_open ptxn dbi

-- | Disposes of a cursor created with 'openCursor'.
closeCursor :: Cursor -> IO ()
closeCursor (Cursor pcurs) =
  c_mdb_cursor_close pcurs

data ReadOptions = ReadOptions
  { readDirection :: !ReadDirection,
    -- | If 'Nothing', a forward [backward] iteration starts at the beginning [end] of the database.
    -- Otherwise, it starts at the first key that is greater [less] than or equal to the 'Just' key.
    readStart :: !(Maybe ByteString),
    -- | Use @unsafe@ FFI calls under the hood. This can increase iteration speed, but one should
    -- bear in mind that @unsafe@ FFI calls, since they block all other threads, can have an adverse
    -- impact on the performance of the rest of the program.
    readUnsafeFFI :: !Bool
  }
  deriving (Show)

-- | By default, we start reading from the beginning of the database (i.e., from the smallest key),
-- and we don’t use unsafe FFI calls.
defaultReadOptions :: ReadOptions
defaultReadOptions =
  ReadOptions
    { readDirection = Forward,
      readStart = Nothing,
      readUnsafeFFI = False
    }

-- | Direction of key iteration.
data ReadDirection = Forward | Backward deriving (Show)

data OverwriteOptions
  = -- | When a key reoccurs, overwrite the value.
    OverwriteAllow
  | -- | When a key reoccurs, hand the offending key-value pair to the 'writeFailureFold'—except
    -- when the value is the same as before.
    OverwriteAllowSameValue
  | -- | When a key reoccurs, hand the offending key-value pair to the 'writeFailureFold'.
    OverwriteDisallow !WriteAppend
  deriving (Eq, Show)

-- | Assume the input data is already ordered. This allows the use of @MDB_APPEND@ under the hood
-- and substantially improves write performance.
newtype WriteAppend = WriteAppend Bool deriving (Eq, Show)

data WriteOptions a = WriteOptions
  { -- | The number of key-value pairs per write transaction. When a write transaction of the
    -- 'writeLMDB' completes, waiting write transactions of other concurrent 'writeLMDB's on the
    -- same environment get a chance to begin.
    writeTransactionSize :: !Int,
    writeOverwriteOptions :: !OverwriteOptions,
    -- | Use @unsafe@ FFI calls under the hood. This can increase iteration speed, but one should
    -- bear in mind that @unsafe@ FFI calls, since they block all other threads, can have an adverse
    -- impact on the performance of the rest of the program.
    writeUnsafeFFI :: !Bool,
    -- | A fold for handling unsuccessful writes.
    writeFailureFold :: !(WriteFailureFold a)
  }

-- | By default, we use a write transaction size of 1 (one write transaction for each key-value
-- pair), allow overwriting, don’t use unsafe FFI calls, and use 'writeFailureThrow' as the failure
-- fold.
defaultWriteOptions :: WriteOptions ()
defaultWriteOptions =
  WriteOptions
    { writeTransactionSize = 1,
      writeOverwriteOptions = OverwriteDisallow (WriteAppend False),
      writeUnsafeFFI = False,
      writeFailureFold = writeFailureThrow
    }

-- | A fold for handling unsuccessful writes.
type WriteFailureFold a = Fold IO (LMDB_Error, ByteString, ByteString) a

-- | Throw an exception upon write failure.
writeFailureThrow :: WriteFailureFold ()
writeFailureThrow =
  Fold (\() (e, _, _) -> throw e) (return $ Partial ()) (const $ return ())

-- | Throw an exception upon write failure; and include the offending key-value pair in the
-- exception’s description.
writeFailureThrowDebug :: WriteFailureFold ()
writeFailureThrowDebug =
  Fold
    ( \() (e, k, v) ->
        let desc = e_description e
         in throw e {e_description = printf "%s; key=%s; value=%s" desc (show k) (show v)}
    )
    (return $ Partial ())
    (const $ return ())

-- | Gracefully stop upon write failure.
writeFailureStop :: WriteFailureFold ()
writeFailureStop = void F.one

-- | Ignore write failures.
writeFailureIgnore :: WriteFailureFold ()
writeFailureIgnore = F.drain

newtype StreamlyLMDBError = StreamlyLMDBError String deriving (Show)

instance Exception StreamlyLMDBError

-- | Creates a fold with which we can stream key-value pairs into the given database.
--
-- The fold currently cannot be used with a scan. (The plan is for this shortcoming to be remedied
-- with or after a future release of streamly that addresses the underlying issue.)
--
-- Please specify a suitable transaction size in the write options; the default of 1 (one write
-- transaction for each key-value pair) could yield suboptimal performance. One could try, e.g., 100
-- KB chunks and benchmark from there.
{-# INLINE writeLMDB #-}
writeLMDB ::
  (MonadIO m) =>
  Database ReadWrite ->
  WriteOptions a ->
  Fold m (ByteString, ByteString) a
writeLMDB (Database (Environment penv mvars) dbi) wopts =
  go (writeFailureFold wopts)
  where
    {-# INLINE go #-}
    go (Fold failStep failInit failExtract) =
      let txnSize = max 1 (writeTransactionSize wopts)
          throwErr = throwError "writeLMDB"
          overwriteOpt = writeOverwriteOptions wopts
          nooverwrite = case overwriteOpt of
            OverwriteAllowSameValue ->
              -- Disallow overwriting from LMDB’s point of view; see (1).
              True
            OverwriteDisallow _ -> True
            OverwriteAllow -> False
          append = case overwriteOpt of
            OverwriteDisallow (WriteAppend True) -> True
            _ -> False
          flags =
            combineOptions $ [mdb_nooverwrite | nooverwrite] ++ [mdb_append | append]

          (txn_begin, txn_commit, put_, get) =
            if writeUnsafeFFI wopts
              then
                ( mdb_txn_begin_unsafe,
                  mdb_txn_commit_unsafe,
                  mdb_put_unsafe_,
                  c_mdb_get_unsafe
                )
              else
                ( mdb_txn_begin,
                  mdb_txn_commit,
                  mdb_put_,
                  c_mdb_get
                )
       in Fold
            ( \(threadId, counter, iter, chunkSz, mIoFinalizer, ownershipLock, fstep) (k, v) -> do
                -- In the first few iterations, ascertain that we are still on the same thread (to
                -- make sure streamly folds are working as expected).
                iter' <-
                  if iter < 3
                    then do
                      threadId' <- liftIO myThreadId
                      when (threadId' /= threadId) $ throwErr "veered off the original thread"
                      return $ iter + 1
                    else return iter

                -- We use the environment’s owner mvar to make sure only one write transaction is
                -- active at a time, even when the same environment is used concurrently for
                -- multiple writeLMDB (and other 'ReadWrite'-based functions such as
                -- 'clearDatabase'). More on LMDB’s low-level requirements about serial writes:
                -- https://github.com/LMDB/lmdb/blob/8d0cbbc936091eb85972501a9b31a8f86d4c51a7/libraries/liblmdb/lmdb.h#L23.
                --
                -- This mvar mechanism allows us to write, into the same environment, incoming
                -- key-value pairs originating from multiple threads. (Thanks to
                -- 'writeTransactionSize', there is no need for one writeLMDB fold to finish before
                -- another writeLMDB can fold into the same environment; we can have multiple
                -- writeLMDB folds active on the same environment at the same time.)
                --
                -- We used MDB_NOLOCK for the environment to allow write transactions to cross
                -- thread boundaries. This is necessary because transaction commits that happen upon
                -- GC following an asynchronous exception (see (3)) happen on a different thread
                -- from the other transaction FFI calls. For more on the implications of this, see
                -- (4) and $readingWritingRelationship. (The only way we see around MDB_NOLOCK is
                -- passing all writeLMDB FFI calls through a channel, which would either be
                -- dead-slow or require chunking that reduces the streaming nature of the library).
                --
                -- (For now, we use mask without restore assuming that the operations are quick
                -- (e.g., because the user chose a reasonably small 'writeTransactionSize'). TODO:
                -- Think about introducing mask restore where applicable.)

                let (numReadersT, _, writeOwnerM, writeOwnerDataM) = mvars
                let disclaimWriteOwnership =
                      void . mask_ $ tryTakeMVar writeOwnerM >> tryTakeMVar writeOwnerDataM

                liftIO . withMVarMasked ownershipLock $ \() -> do
                  -- Check whether this writeLMDB still owns the environment.
                  hasOwnership <- fromMaybe False <$> whenOwned writeOwnerM counter (return True)

                  (chunkSz', mIoFinalizer') <-
                    if hasOwnership
                      then -- The environment is still owned by this writeLMDB. The same IOFinalizer
                      -- is in effect. We continue writing to the same transaction.
                      do
                        return (chunkSz, mIoFinalizer)
                      else do
                        -- If the environment is owned by a different counter or the owner mvar is
                        -- empty (i.e., the environment is not owned by any counter), we wait for
                        -- the owner mvar to become empty. (Note that in the empty mvar case, we
                        -- still wait for the mvar to become empty because by the time we reach
                        -- here, another counter might already have claimed ownership.) (Note that
                        -- an async exception could trigger here despite the mask; this is not a
                        -- problem.)
                        putMVar writeOwnerM $ WriteOwner counter

                        -- (4) Due to MDB_NOLOCK, we have to make sure that no readers are active
                        -- when a write transaction begins; see
                        -- https://github.com/LMDB/lmdb/blob/8d0cbbc936091eb85972501a9b31a8f86d4c51a7/libraries/liblmdb/lmdb.h#L619
                        -- We assume that readers modify numReadersT only with takeTMVar followed by
                        -- putTMVar, which implies that during the IO actions we perform until the
                        -- next numReadersReset (i.e., the IO actions to begin the write
                        -- transaction) there can be no modification of numReadersT.
                        numReadersReset <-
                          onException
                            (waitForZeroReaders numReadersT)
                            disclaimWriteOwnership

                        ptxn <-
                          finally
                            ( onException
                                (txn_begin penv nullPtr 0)
                                disclaimWriteOwnership
                            )
                            -- Re-allow readers.
                            numReadersReset

                        putMVar writeOwnerDataM (WriteOwnerData {wPtxn = ptxn})

                        ioFinalizer' <-
                          newIOFinalizer $ do
                            -- (3) This gets called upon GC, after this writeLMDB is going away upon
                            -- synchronous or asynchronous exception or upon successful fold
                            -- completion. (It’s not possible to “cancel” an mkWeakIORef finalizer,
                            -- so this will inevitably get called.)
                            --
                            -- To avoid an extraneous transaction commit after a commit (a C
                            -- double-free), we make sure we still have ownership.
                            withMVarMasked ownershipLock $ \() -> do
                              ownerData' <- tryReadMVar writeOwnerDataM
                              case ownerData' of
                                Nothing -> return ()
                                Just (WriteOwnerData {wPtxn}) ->
                                  void . whenOwned writeOwnerM counter $
                                    -- We also disclaim ownership if the commit fails (and allow
                                    -- future transaction operations to presumably fail). TODO: Can
                                    -- we report the failure directly?
                                    finally
                                      (txn_commit wPtxn)
                                      disclaimWriteOwnership



                        return (0, Just ioFinalizer')

                  -- Note: We commit and disclaim ownership upon synchronous exceptions and success
                  -- because this should be done as early as possible. (Although the GC finalizer
                  -- would eventually get to it, this might be unexpectedly late from
                  -- the client’s point of view.)

                  -- Insert the incoming key-value pair. If the insert succeeds from our point of
                  -- view (although it could have failed from LMDB’s point of view; see (1)), we get
                  -- a new chunk size. Otherwise, we get the LMDB exception.
                  ptxn <- do
                    mvarContents' <- tryReadMVar writeOwnerDataM
                    case mvarContents' of
                      Nothing -> throwErr "mvar expected"
                      Just (WriteOwnerData {wPtxn}) -> return wPtxn
                  eChunkSz'' <-
                    unsafeUseAsCStringLen k $ \(kp, kl) -> unsafeUseAsCStringLen v $ \(vp, vl) ->
                      catch
                        ( do
                            put_ ptxn dbi kp (fromIntegral kl) vp (fromIntegral vl) flags
                            return . Right $ chunkSz' + 1
                        )
                        ( \(e :: LMDB_Error) ->
                            -- (1) Discard LMDB error if OverwriteAllowSameValue was specified and
                            -- the error from LMDB was due to the exact same key-value pair already
                            -- existing in the database.
                            with (MDB_val (fromIntegral kl) kp) $ \pk -> alloca $ \pv -> do
                              rc <- get ptxn dbi pk pv
                              if rc == 0
                                then do
                                  v' <- peek pv
                                  vbs <-
                                    unsafePackCStringLen (mv_data v', fromIntegral $ mv_size v')
                                  let ok =
                                        overwriteOpt == OverwriteAllowSameValue
                                          && e_code e == Right MDB_KEYEXIST
                                          && vbs == v
                                  return $
                                    if ok
                                      then Right chunkSz'
                                      else Left e
                                else do
                                  return $ Left e
                        )

                  case eChunkSz'' of
                    Right chunkSz'' -> do
                      -- If writeTransactionSize has been reached, commit and disclaim ownership.
                      when (chunkSz'' >= txnSize) $
                        finally
                          (txn_commit ptxn)
                          disclaimWriteOwnership

                      return $
                        Partial
                          ( threadId,
                            counter,
                            iter',
                            chunkSz'',
                            mIoFinalizer',
                            ownershipLock,
                            fstep
                          )
                    Left e -> do
                      case fstep of
                        Done _ -> do
                          -- When the failure fold completes with Done, the outer writeLMDB fold
                          -- should already have completed with Done as well; see (2). We should
                          -- therefore never arrive here.
                          throwErr "unexpected Done (step)"
                        Partial s -> do
                          -- Feed the offending key-value pair into the failure fold. (This could
                          -- throw a synchronous exception.)
                          fstep' <-
                            onException
                              (failStep s (e, k, v))
                              ( finally
                                  (txn_commit ptxn)
                                  disclaimWriteOwnership
                              )

                          case fstep' of
                            Done a -> do
                              -- (2) The failure fold completed with Done; complete the outer
                              -- writeLMDB fold with Done as well.
                              finally
                                (txn_commit ptxn)
                                disclaimWriteOwnership
                              return $ Done a
                            Partial _ -> do
                              -- The failure fold did not request completion; continue the outer
                              -- writeLMDB fold as if nothing happened.
                              return $
                                Partial
                                  ( threadId,
                                    counter,
                                    iter',
                                    chunkSz',
                                    mIoFinalizer',
                                    ownershipLock,
                                    fstep'
                                  )
            )
            ( do
                threadId <- liftIO myThreadId

                let (_, writeCounterM, _, _) = mvars
                counter <- liftIO $ incrementWriteCounter writeCounterM

                -- Avoids conflicts between the main writeLMDB thread and the GC thread (see (3)).
                ownershipLock <- liftIO $ newMVar ()

                fstep <- liftIO failInit
                case fstep of
                  Done _ -> throwErr "writeFailureFold should begin with Partial"
                  Partial _ -> return ()

                return $
                  Partial
                    ( threadId,
                      counter,
                      -- Iteration.
                      0 :: Int,
                      -- Chunk size.
                      0 :: Int,
                      Nothing :: Maybe IOFinalizer,
                      ownershipLock,
                      -- Step of failure fold.
                      fstep
                    )
            )
            -- This final part is incompatible with scans.
            ( \(threadId, counter, _, _, _, ownershipLock, fstep) -> liftIO $ do
                threadId' <- myThreadId
                when (threadId' /= threadId) $
                  throwErr "veered off the original thread at the end"

                let (_, _, writeOwnerM, writeOwnerDataM) = mvars
                let disclaimWriteOwnership =
                      void . mask_ $ tryTakeMVar writeOwnerM >> tryTakeMVar writeOwnerDataM

                withMVarMasked ownershipLock $ \() -> do
                  void $ whenOwned writeOwnerM counter $ do
                    ownerData' <- tryReadMVar writeOwnerDataM
                    case ownerData' of
                      Nothing -> throwErr "ownerData expected (extract)"
                      Just (WriteOwnerData {wPtxn}) ->
                        finally
                          (txn_commit wPtxn)
                          disclaimWriteOwnership

                  case fstep of
                    Done _ ->
                      -- As the failure fold completed with Done, the outer writeLMDB fold should
                      -- have completed as well; see (2).
                      throwErr "Unexpected Done (extract)"
                    Partial s -> do
                      failExtract s
            )

-- | Clears, i.e., removes all key-value pairs from, the given database. Please note that other
-- write transactions block until this operation completes.
clearDatabase :: Database ReadWrite -> IO ()
clearDatabase (Database (Environment penv mvars) dbi) = mask $ \restore -> do
  -- The MVar/TMVar logic is similar as in writeLMDB.
  let (numReadersT, writeCounterM, writeOwnerM, writeOwnerDataM) = mvars
  writeCounter <- incrementWriteCounter writeCounterM
  putMVar writeOwnerM $ WriteOwner writeCounter
  let disclaimWriteOwnership = void . mask_ $ tryTakeMVar writeOwnerM >> tryTakeMVar writeOwnerDataM

  numReadersReset <-
    onException
      (waitForZeroReaders numReadersT)
      disclaimWriteOwnership

  finally
    ( do
        ptxn <-
          finally
            (mdb_txn_begin penv nullPtr 0)
            -- Re-allow readers.
            numReadersReset

        onException
          -- Unmask a potentially long-running operation.
          ( restore $ do
              mdb_clear ptxn dbi
              mdb_txn_commit ptxn
          )
          (c_mdb_txn_abort ptxn) -- TODO: Check if abort could be long-running.
    )
    disclaimWriteOwnership

-- | Executes the given IO action when the given counter matches with the WriteOwner.
whenOwned :: MVar WriteOwner -> WriteCounter -> IO a -> IO (Maybe a)
whenOwned owner counter io = do
  mvarContents <- tryReadMVar owner
  case mvarContents of
    Just (WriteOwner ownerCounter) | ownerCounter == counter -> Just <$> io
    _ -> return Nothing

incrementWriteCounter :: MVar WriteCounter -> IO WriteCounter
incrementWriteCounter counter = do
  modifyMVarMasked
    counter
    ( \c -> do
        unless (c < maxBound) $ throwError "incrementWriteCounter" "maxBound exceeded"
        let c' = c + 1
        return (c', c')
    )

-- Due to MDB_NOLOCK, this is needed before beginning read-write transactions. The caller should
-- wrap this with onException to handle async exception that could happen during STM retry
-- (regardless of masking).
waitForZeroReaders :: TMVar NumReaders -> IO (IO ())
waitForZeroReaders numReadersT = do
  let throwErr = throwError "waitForZeroReaders"
  let numReadersReset = atomically $ putTMVar numReadersT 0
  numReaders <-
    atomically $ do
      numReaders <- takeTMVar numReadersT
      check $ numReaders <= 0 -- Sanity check: use <=0 to catch unexpected negative readers.
      return numReaders
  when (numReaders /= 0) $ throwErr "zero numReaders expected"
  return numReadersReset

-- $readingWritingRelationship
--
-- Please note that read-write transactions in this library (e.g., due to 'writeLMDB' and
-- 'clearDatabase') wait until existing read-only transactions (e.g., due to 'readLMDB' and
-- 'beginReadOnlyTxn') on the same environment complete. (On the other hand, read-only transactions
-- can begin while a read-write transaction is active on the same environment.)
--
-- (This is required because we use @MDB_NOLOCK@ under the hood for certain low-level reasons. We do
-- not consider this is a serious limitation because long-lived transactions are [discouraged by
-- LMDB](https://github.com/LMDB/lmdb/blob/8d0cbbc936091eb85972501a9b31a8f86d4c51a7/libraries/liblmdb/lmdb.h#L107)
-- in any case.)
