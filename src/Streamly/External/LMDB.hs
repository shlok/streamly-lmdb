{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE ScopedTypeVariables #-}

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
    writeFailureThrow,
    writeFailureThrowDebug,
    writeFailureStop,
    writeFailureIgnore,

    -- * Error types
    LMDB_Error (..),
    MDB_ErrCode (..),
  )
where

import Control.Concurrent (isCurrentThreadBound, myThreadId)
import Control.Concurrent.Async (asyncBound, wait)
import Control.Exception
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
import Streamly.External.LMDB.Internal (Database (..), Mode (..), ReadOnly, ReadWrite)
import Streamly.External.LMDB.Internal.Foreign
import Streamly.Internal.Data.Fold (Fold (Fold), Step (..))
import Streamly.Internal.Data.IOFinalizer (newIOFinalizer, runIOFinalizer)
import Streamly.Internal.Data.Stream.StreamD.Type (Step (Stop, Yield))
import Streamly.Internal.Data.Unfold (lmap)
import Streamly.Internal.Data.Unfold.Type (Unfold (Unfold))
import Text.Printf

newtype Environment mode = Environment (Ptr MDB_env)

isReadOnlyEnvironment :: Mode mode => Environment mode -> Bool
isReadOnlyEnvironment = isReadOnlyMode . mode
  where
    mode :: Environment mode -> mode
    mode = undefined

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
    { mapSize = 1024 * 1024, -- 1 MiB.
      maxDatabases = 0,
      maxReaders = 126
    }

-- | A convenience constant for obtaining a 1 GiB map size.
gibibyte :: Int
gibibyte = 1024 * 1024 * 1024

-- | A convenience constant for obtaining a 1 TiB map size.
tebibyte :: Int
tebibyte = 1024 * 1024 * 1024 * 1024

-- | Open an LMDB environment in either 'ReadWrite' or 'ReadOnly' mode. The 'FilePath' argument may
-- be either a directory or a regular file, but it must already exist. If a regular file, an
-- additional file with "-lock" appended to the name is used for the reader lock table.
--
-- Note that an environment must have been opened in 'ReadWrite' mode at least once before it can be
-- opened in 'ReadOnly' mode.
--
-- An environment opened in 'ReadOnly' mode may still modify the reader lock table (except when the
-- filesystem is read-only, in which case no locks are used).
openEnvironment :: Mode mode => FilePath -> Limits -> IO (Environment mode)
openEnvironment path limits = do
  penv <- mdb_env_create

  mdb_env_set_mapsize penv (mapSize limits)
  let maxDbs = maxDatabases limits in when (maxDbs /= 0) $ mdb_env_set_maxdbs penv maxDbs
  mdb_env_set_maxreaders penv (maxReaders limits)

  -- Always use MDB_NOTLS; this is crucial for Haskell applications. (See
  -- https://github.com/LMDB/lmdb/blob/8d0cbbc936091eb85972501a9b31a8f86d4c51a7/libraries/liblmdb/lmdb.h#L615)
  let env = Environment penv :: Mode mode => Environment mode
      flags = mdb_notls : [mdb_rdonly | isReadOnlyEnvironment env]

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
-- the program’s execution.) If you find yourself needing this, it is your responsibility to heed
-- the [documented
-- caveats](https://github.com/LMDB/lmdb/blob/8d0cbbc936091eb85972501a9b31a8f86d4c51a7/libraries/liblmdb/lmdb.h#L787).
--
-- In particular, you will probably, before calling this function, want to (a) use 'closeDatabase',
-- and (b) pass in precreated transactions and cursors to 'readLMDB' and 'unsafeReadLMDB' to make
-- sure there are no transactions or cursors still left to be cleaned up by the garbage collector.
-- (As an alternative to (b), one could try manually triggering the garbage collector.)
closeEnvironment :: (Mode mode) => Environment mode -> IO ()
closeEnvironment (Environment penv) =
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
getDatabase :: (Mode mode) => Environment mode -> Maybe String -> IO (Database mode)
getDatabase env@(Environment penv) name = do
  ptxn <- mdb_txn_begin penv nullPtr (combineOptions $ [mdb_rdonly | isReadOnlyEnvironment env])
  dbi <- mdb_dbi_open ptxn name (combineOptions $ [mdb_create | not $ isReadOnlyEnvironment env])
  mdb_txn_commit ptxn
  return $ Database penv dbi

-- | Clears, i.e., removes all key-value pairs from, the given database.
clearDatabase :: (Mode mode) => Database mode -> IO ()
clearDatabase (Database penv dbi) =
  asyncBound
    ( do
        ptxn <- mdb_txn_begin penv nullPtr 0
        mdb_clear ptxn dbi
        mdb_txn_commit ptxn
    )
    >>= wait

-- | Closes the given database.
--
-- If you have merely a few dozen databases at most, there should be no need for this. (It is a
-- common practice with LMDB to create one’s databases once and reuse them for the remainder of the
-- program’s execution.) If you find yourself needing this, it is your responsibility to heed the
-- [documented
-- caveats](https://github.com/LMDB/lmdb/blob/8d0cbbc936091eb85972501a9b31a8f86d4c51a7/libraries/liblmdb/lmdb.h#L1200).
closeDatabase :: (Mode mode) => Database mode -> IO ()
closeDatabase (Database penv dbi) =
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
-- In any case, bear in mind at all times LMDB’s [caveats regarding long-lived
-- transactions](https://github.com/LMDB/lmdb/blob/8d0cbbc936091eb85972501a9b31a8f86d4c51a7/libraries/liblmdb/lmdb.h#L107).
--
-- If you don’t want the overhead of intermediate @ByteString@s (on your way to your eventual data
-- structures), use 'unsafeReadLMDB' instead.
{-# INLINE readLMDB #-}
readLMDB ::
  (MonadIO m, Mode mode) =>
  Database mode ->
  Maybe (ReadOnlyTxn, Cursor) ->
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
  Maybe (ReadOnlyTxn, Cursor) ->
  ReadOptions ->
  (CStringLen -> IO k) ->
  (CStringLen -> IO v) ->
  Unfold m Void (k, v)
unsafeReadLMDB (Database penv dbi) mtxncurs ropts kmap vmap =
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
              (pcurs, pk, pv, ref) <- liftIO $
                mask_ $ do
                  (ptxn, pcurs) <- case mtxncurs of
                    Nothing -> do
                      ptxn <- txn_begin penv nullPtr mdb_rdonly
                      pcurs <- cursor_open ptxn dbi
                      return (ptxn, pcurs)
                    Just (ReadOnlyTxn ptxn, Cursor pcurs) ->
                      return (ptxn, pcurs)
                  pk <- malloc
                  pv <- malloc

                  _ <- case readStart ropts of
                    Nothing -> return ()
                    Just k -> unsafeUseAsCStringLen k $ \(kp, kl) ->
                      poke pk (MDB_val (fromIntegral kl) kp)

                  ref <- newIOFinalizer $ do
                    free pv >> free pk
                    when (isNothing mtxncurs) $
                      -- With LMDB, there is ordinarily no need to commit read-only transactions.
                      -- (The exception is when we want to make databases that were opened during
                      -- the transaction available later, but that’s not applicable here.) We can
                      -- therefore abort ptxn, both for failure (exceptions) and success.
                      cursor_close pcurs >> txn_abort ptxn
                  return (pcurs, pk, pv, ref)
              return (op, pcurs, pk, pv, ref)
          )

newtype ReadOnlyTxn = ReadOnlyTxn (Ptr MDB_txn)

-- | Begins an LMDB read-only transaction for use with 'readLMDB' or 'unsafeReadLMDB'. It is your
-- responsibility to (a) use the transaction only on databases in the same environment, (b) make
-- sure that those databases were already obtained before the transaction was begun, and (c) dispose
-- of the transaction with 'abortReadOnlyTxn'.
beginReadOnlyTxn :: Environment mode -> IO ReadOnlyTxn
beginReadOnlyTxn (Environment penv) = ReadOnlyTxn <$> mdb_txn_begin penv nullPtr mdb_rdonly

-- | Disposes of a read-only transaction created with 'beginReadOnlyTxn'.
abortReadOnlyTxn :: ReadOnlyTxn -> IO ()
abortReadOnlyTxn (ReadOnlyTxn ptxn) = c_mdb_txn_abort ptxn

newtype Cursor = Cursor (Ptr MDB_cursor)

-- | Opens a cursor for use with 'readLMDB' or 'unsafeReadLMDB'. It is your responsibility to (a)
-- make sure the cursor only gets used by a single 'readLMDB' or 'unsafeReadLMDB' @Unfold@ at the
-- same time (to be safe, one can open a new cursor for every 'readLMDB' or 'unsafeReadLMDB' call),
-- (b) make sure the provided database is within the environment on which the provided transaction
-- was begun, and (c) dispose of the cursor with 'closeCursor' (logically before 'abortReadOnlyTxn',
-- although the order doesn’t really matter for read-only transactions).
openCursor :: ReadOnlyTxn -> Database mode -> IO Cursor
openCursor (ReadOnlyTxn ptxn) (Database _ dbi) =
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
    -- bear in mind that @unsafe@ FFI calls can have an adverse impact on the performance of the
    -- rest of the program (e.g., its ability to effectively spawn green threads).
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
  { -- | The number of key-value pairs per write transaction.
    writeTransactionSize :: !Int,
    writeOverwriteOptions :: !OverwriteOptions,
    -- | Use @unsafe@ FFI calls under the hood. This can increase write performance, but one should
    -- bear in mind that @unsafe@ FFI calls can have an adverse impact on the performance of the
    -- rest of the program (e.g., its ability to effectively spawn green threads).
    writeUnsafeFFI :: !Bool,
    -- | A fold for handling unsuccessful writes.
    writeFailureFold :: !(Fold IO (LMDB_Error, ByteString, ByteString) a)
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

-- | Throw an exception upon write failure.
writeFailureThrow :: Fold IO (LMDB_Error, ByteString, ByteString) ()
writeFailureThrow =
  Fold (\() (e, _, _) -> throw e) (return $ Partial ()) (const $ return ())

-- | Throw an exception upon write failure; and include the offending key-value pair in the
-- exception’s description.
writeFailureThrowDebug :: Fold IO (LMDB_Error, ByteString, ByteString) ()
writeFailureThrowDebug =
  Fold
    ( \() (e, k, v) ->
        let desc = e_description e
         in throw e {e_description = printf "%s; key=%s; value=%s" desc (show k) (show v)}
    )
    (return $ Partial ())
    (const $ return ())

-- | Gracefully stop upon write failure.
writeFailureStop :: Fold IO (LMDB_Error, ByteString, ByteString) ()
writeFailureStop = void F.one

-- | Ignore write failures.
writeFailureIgnore :: Fold IO (LMDB_Error, ByteString, ByteString) ()
writeFailureIgnore = F.drain

newtype StreamlyLMDBError = StreamlyLMDBError String deriving (Show)

instance Exception StreamlyLMDBError

-- | Creates a fold with which we can stream key-value pairs into the given database.
--
-- It is the responsibility of the user to execute the fold on a bound thread.
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
writeLMDB (Database penv dbi) wopts =
  go (writeFailureFold wopts)
  where
    {-# INLINE go #-}
    go (Fold failStep failInit failExtract) =
      let txnSize = max 1 (writeTransactionSize wopts)

          overwriteOpt = writeOverwriteOptions wopts
          nooverwrite = case overwriteOpt of
            OverwriteAllowSameValue ->
              -- Disallow overwriting from LMDB’s point of view; see (*).
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
              then (mdb_txn_begin_unsafe, mdb_txn_commit_unsafe, mdb_put_unsafe_, c_mdb_get_unsafe)
              else (mdb_txn_begin, mdb_txn_commit, mdb_put_, c_mdb_get)
       in Fold
            ( \(threadId, iter, mtxn, fstep) (k, v) -> do
                -- In the first few iterations, ascertain that we are still on the same (bound)
                -- thread.
                iter' <-
                  if iter < 3
                    then do
                      threadId' <- liftIO myThreadId
                      when (threadId' /= threadId) $
                        throwErr "writeLMDB veered off the original bound thread"
                      return $ iter + 1
                    else return iter

                let beginTxn = liftIO . mask_ $ do
                      ptxn <- txn_begin penv nullPtr 0
                      ref <- newIOFinalizer $ txn_commit ptxn
                      return (ptxn, ref, 0)

                (ptxn, ref, chunkSz) <- case mtxn of
                  Nothing -> beginTxn -- The first transaction.
                  Just txn@(_, ref, chunkSz) ->
                    if chunkSz >= txnSize
                      then do
                        runIOFinalizer ref
                        beginTxn
                      else -- create txn again.
                        return txn

                -- If the insert succeeds from our point of view (although it could have failed from
                -- LMDB’s point of view; see (*)), we get a new chunk size. Otherwise, we get the
                -- LMDB exception.
                eChunkSz <- liftIO $
                  unsafeUseAsCStringLen k $ \(kp, kl) -> unsafeUseAsCStringLen v $ \(vp, vl) ->
                    catch
                      ( do
                          put_ ptxn dbi kp (fromIntegral kl) vp (fromIntegral vl) flags
                          return . Right $ chunkSz + 1
                      )
                      ( \(e :: LMDB_Error) ->
                          -- (*) Discard LMDB error if OverwriteAllowSameValue was specified and the
                          -- error from LMDB was due to the exact same key-value pair already
                          -- existing in the database.
                          with (MDB_val (fromIntegral kl) kp) $ \pk -> alloca $ \pv -> do
                            rc <- get ptxn dbi pk pv
                            if rc == 0
                              then do
                                v' <- peek pv
                                vbs <- unsafePackCStringLen (mv_data v', fromIntegral $ mv_size v')
                                let ok =
                                      overwriteOpt == OverwriteAllowSameValue
                                        && e_code e == Right MDB_KEYEXIST
                                        && vbs == v
                                return $
                                  if ok
                                    then Right chunkSz
                                    else Left e
                              else do
                                return $ Left e
                      )

                case eChunkSz of
                  Right chunkSz' ->
                    return $ Partial (threadId, iter', Just (ptxn, ref, chunkSz'), fstep)
                  Left e -> do
                    case fstep of
                      Done _ ->
                        -- When the failure fold completes with Done, the outer writeLMDB fold
                        -- should already have completed with Done as well; see below. We should
                        -- therefore never arrive here.
                        throwErr "writeLMDB: Unexpected Done."
                      Partial s -> do
                        -- Feed the offending key-value pair into the failure fold.
                        fstep' <- liftIO $ onException (failStep s (e, k, v)) (runIOFinalizer ref)
                        case fstep' of
                          Done a -> do
                            -- (**) The failure fold completed with Done; complete the outer
                            -- writeLMDB fold with Done as well.
                            runIOFinalizer ref
                            return $ Done a
                          Partial _ ->
                            -- The failure fold did not request completion; continue the outer
                            -- writeLMDB fold as if nothing happened.
                            return $
                              Partial (threadId, iter', Just (ptxn, ref, chunkSz), fstep')
            )
            ( do
                threadId <- liftIO myThreadId
                liftIO isCurrentThreadBound
                  >>= flip unless (throwErr "writeLMDB should be executed on a bound thread")

                fstep <- liftIO failInit
                case fstep of
                  Done _ -> throwErr "writeLMDB: writeFailureFold should begin with Partial"
                  Partial _ -> return ()

                return $ Partial (threadId, 0 :: Int, Nothing, fstep)
            )
            -- This final part is incompatible with scans.
            ( \(threadId, _, mtxn, fstep) -> liftIO $ do
                threadId' <- myThreadId
                when (threadId' /= threadId) $
                  throwErr "writeLMDB veered off the original bound thread at the end"

                case mtxn of
                  Nothing -> return ()
                  Just (_, ref, _) -> runIOFinalizer ref

                case fstep of
                  Done _ ->
                    -- As the failure fold completed with Done, the outer writeLMDB fold should have
                    -- completed as well; see (**).
                    throwErr "writeLMDB: Unexpected Done (extract)."
                  Partial s ->
                    failExtract s
            )

throwErr :: String -> m a
throwErr = throw . StreamlyLMDBError
