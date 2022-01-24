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
    --
    -- Note: We currently have no functionality here for closing and disposing of databases or
    -- environments because we have had no need for it yet. In any case, it is a common practice
    -- with LMDB to create one’s environments and databases once and reuse them for the remainder of
    -- the program’s execution.
    Database,
    getDatabase,
    clearDatabase,

    -- * Reading
    readLMDB,
    unsafeReadLMDB,
    ReadOptions (..),
    defaultReadOptions,
    ReadDirection (..),

    -- * Writing
    writeLMDB,
    WriteOptions (..),
    defaultWriteOptions,
    OverwriteOptions (..),

    -- * Error types
    LMDB_Error (..),
    MDB_ErrCode (..),
  )
where

import Control.Concurrent (isCurrentThreadBound, myThreadId)
import Control.Concurrent.Async (asyncBound, wait)
import Control.Exception (Exception, catch, mask_, throw, tryJust)
import Control.Monad (guard, unless, when)
import Control.Monad.IO.Class (MonadIO, liftIO)
import Data.ByteString (ByteString, packCStringLen)
import Data.ByteString.Unsafe (unsafePackCStringLen, unsafeUseAsCStringLen)
import Data.Maybe (fromJust)
import Data.Void (Void)
import Foreign (Ptr, alloca, free, malloc, nullPtr, peek)
import Foreign.C (Errno (Errno), eNOTDIR)
import Foreign.C.String (CStringLen)
import Foreign.Marshal.Utils (with)
import Foreign.Storable (poke)
import Streamly.External.LMDB.Internal (Database (..), Mode (..), ReadOnly, ReadWrite)
import Streamly.External.LMDB.Internal.Foreign
  ( LMDB_Error (..),
    MDB_ErrCode (..),
    MDB_env,
    MDB_val (MDB_val, mv_data, mv_size),
    c_mdb_cursor_close,
    c_mdb_cursor_get,
    c_mdb_get,
    c_mdb_txn_abort,
    combineOptions,
    mdb_append,
    mdb_clear,
    mdb_create,
    mdb_cursor_open,
    mdb_dbi_open,
    mdb_env_create,
    mdb_env_open,
    mdb_env_set_mapsize,
    mdb_env_set_maxdbs,
    mdb_env_set_maxreaders,
    mdb_first,
    mdb_last,
    mdb_next,
    mdb_nooverwrite,
    mdb_nosubdir,
    mdb_notfound,
    mdb_notls,
    mdb_prev,
    mdb_put_,
    mdb_rdonly,
    mdb_set_range,
    mdb_txn_begin,
    mdb_txn_commit,
    throwLMDBErrNum,
  )
import Streamly.Internal.Data.Fold (Fold (Fold), Step (Partial))
import Streamly.Internal.Data.IOFinalizer (newIOFinalizer, runIOFinalizer)
import Streamly.Internal.Data.Stream.StreamD.Type (Step (Stop, Yield))
import Streamly.Internal.Data.Unfold (supply)
import Streamly.Internal.Data.Unfold.Type (Unfold (Unfold))

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

-- | Direction of key iteration.
data ReadDirection = Forward | Backward deriving (Show)

data ReadOptions = ReadOptions
  { readDirection :: !ReadDirection,
    -- | If 'Nothing', a forward [backward] iteration starts at the beginning [end] of the database.
    -- Otherwise, it starts at the first key that is greater [less] than or equal to the 'Just' key.
    readStart :: !(Maybe ByteString)
  }
  deriving (Show)

-- | By default, we start reading from the beginning of the database (i.e., from the smallest key).
defaultReadOptions :: ReadOptions
defaultReadOptions =
  ReadOptions
    { readDirection = Forward,
      readStart = Nothing
    }

-- | Creates an unfold with which we can stream key-value pairs from the given database.
--
-- A read transaction is kept open for the duration of the unfold; one should therefore bear in mind
-- LMDB's [caveats regarding long-lived transactions](https://git.io/JJZE6).
--
-- If you don’t want the overhead of intermediate 'ByteString's (on your way to your eventual data
-- structures), use 'unsafeReadLMDB' instead.
{-# INLINE readLMDB #-}
readLMDB ::
  (MonadIO m, Mode mode) =>
  Database mode ->
  ReadOptions ->
  Unfold m Void (ByteString, ByteString)
readLMDB db ropts = unsafeReadLMDB db ropts packCStringLen packCStringLen

-- | Creates an unfold with which we can stream key-value pairs from the given database.
--
-- A read transaction is kept open for the duration of the unfold; one should therefore bear in mind
-- LMDB's [caveats regarding long-lived transactions](https://git.io/JJZE6).
--
-- To ensure safety, make sure that the memory pointed to by the 'CStringLen' for each key/value
-- mapping function call is (a) only read (and not written to); and (b) not used after the mapping
-- function has returned. One way to transform the 'CStringLen's to your desired data structures is
-- to use 'Data.ByteString.Unsafe.unsafePackCStringLen'.
{-# INLINE unsafeReadLMDB #-}
unsafeReadLMDB ::
  (MonadIO m, Mode mode) =>
  Database mode ->
  ReadOptions ->
  (CStringLen -> IO k) ->
  (CStringLen -> IO v) ->
  Unfold m Void (k, v)
unsafeReadLMDB (Database penv dbi) ropts kmap vmap =
  let (firstOp, subsequentOp) = case (readDirection ropts, readStart ropts) of
        (Forward, Nothing) -> (mdb_first, mdb_next)
        (Forward, Just _) -> (mdb_set_range, mdb_next)
        (Backward, Nothing) -> (mdb_last, mdb_prev)
        (Backward, Just _) -> (mdb_set_range, mdb_prev)
   in supply firstOp $
        Unfold
          ( \(op, pcurs, pk, pv, ref) -> do
              rc <-
                liftIO $
                  if op == mdb_set_range && subsequentOp == mdb_prev
                    then do
                      -- Reverse MDB_SET_RANGE.
                      kfst' <- peek pk
                      kfst <- packCStringLen (mv_data kfst', fromIntegral $ mv_size kfst')
                      rc <- c_mdb_cursor_get pcurs pk pv op
                      if rc /= 0 && rc == mdb_notfound
                        then c_mdb_cursor_get pcurs pk pv mdb_last
                        else
                          if rc == 0
                            then do
                              k' <- peek pk
                              k <- unsafePackCStringLen (mv_data k', fromIntegral $ mv_size k')
                              if k /= kfst
                                then c_mdb_cursor_get pcurs pk pv mdb_prev
                                else return rc
                            else return rc
                    else c_mdb_cursor_get pcurs pk pv op

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
                  ptxn <- liftIO $ mdb_txn_begin penv nullPtr mdb_rdonly
                  pcurs <- liftIO $ mdb_cursor_open ptxn dbi
                  pk <- liftIO malloc
                  pv <- liftIO malloc

                  _ <- case readStart ropts of
                    Nothing -> return ()
                    Just k -> unsafeUseAsCStringLen k $ \(kp, kl) ->
                      poke pk (MDB_val (fromIntegral kl) kp)

                  ref <- liftIO . newIOFinalizer $ do
                    free pv >> free pk
                    c_mdb_cursor_close pcurs
                    -- No need to commit this read-only transaction.
                    c_mdb_txn_abort ptxn
                  return (pcurs, pk, pv, ref)
              return (op, pcurs, pk, pv, ref)
          )

data OverwriteOptions = OverwriteAllow | OverwriteAllowSame | OverwriteDisallow deriving (Eq)

data WriteOptions = WriteOptions
  { writeTransactionSize :: !Int,
    overwriteOptions :: !OverwriteOptions,
    writeAppend :: !Bool
  }

defaultWriteOptions :: WriteOptions
defaultWriteOptions =
  WriteOptions
    { writeTransactionSize = 1,
      overwriteOptions = OverwriteAllow,
      writeAppend = False
    }

newtype ExceptionString = ExceptionString String deriving (Show)

instance Exception ExceptionString

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
writeLMDB :: (MonadIO m) => Database ReadWrite -> WriteOptions -> Fold m (ByteString, ByteString) ()
writeLMDB (Database penv dbi) options =
  let txnSize = max 1 (writeTransactionSize options)
      overwriteOpt = overwriteOptions options
      flags =
        combineOptions $
          [mdb_nooverwrite | overwriteOpt `elem` [OverwriteAllowSame, OverwriteDisallow]]
            ++ [mdb_append | writeAppend options]
   in Fold
        ( \(threadId, iter, currChunkSz, mtxn) (k, v) -> do
            -- In the first few iterations, ascertain that we are still on the same (bound) thread.
            iter' <-
              liftIO $
                if iter < 3
                  then do
                    threadId' <- myThreadId
                    when (threadId' /= threadId) $
                      throw
                        (ExceptionString "Error: writeLMDB veered off the original bound thread")
                    return $ iter + 1
                  else return iter

            currChunkSz' <-
              liftIO $
                if currChunkSz >= txnSize
                  then do
                    let (_, ref) = fromJust mtxn
                    runIOFinalizer ref
                    return 0
                  else return currChunkSz

            (ptxn, ref) <-
              if currChunkSz' == 0
                then liftIO $
                  mask_ $ do
                    ptxn <- mdb_txn_begin penv nullPtr 0
                    ref <- newIOFinalizer $ mdb_txn_commit ptxn
                    return (ptxn, ref)
                else return $ fromJust mtxn

            liftIO $
              unsafeUseAsCStringLen k $ \(kp, kl) -> unsafeUseAsCStringLen v $ \(vp, vl) ->
                catch
                  (mdb_put_ ptxn dbi kp (fromIntegral kl) vp (fromIntegral vl) flags)
                  ( \(e :: LMDB_Error) -> do
                      -- Discard error if OverwriteAllowSame was specified and the error from LMDB
                      -- was due to the exact same key-value pair already existing in the database.
                      ok <- with (MDB_val (fromIntegral kl) kp) $ \pk ->
                        alloca $ \pv -> do
                          rc <- c_mdb_get ptxn dbi pk pv
                          if rc == 0
                            then do
                              v' <- peek pv
                              vbs <- unsafePackCStringLen (mv_data v', fromIntegral $ mv_size v')
                              return $
                                overwriteOpt == OverwriteAllowSame
                                  && e_code e == Right MDB_KEYEXIST
                                  && vbs == v
                            else return False
                      unless ok $ runIOFinalizer ref >> throw e
                  )
            return $ Partial (threadId, iter', currChunkSz' + 1, Just (ptxn, ref))
        )
        ( do
            isBound <- liftIO isCurrentThreadBound
            threadId <- liftIO myThreadId
            if isBound
              then return $ Partial (threadId, 0 :: Int, 0, Nothing)
              else throw $ ExceptionString "Error: writeLMDB should be executed on a bound thread"
        )
        -- This final part is incompatible with scans.
        ( \(threadId, _, _, mtxn) -> liftIO $ do
            threadId' <- myThreadId
            when (threadId' /= threadId) $
              throw
                (ExceptionString "Error: writeLMDB veered off the original bound thread at the end")
            case mtxn of
              Nothing -> return ()
              Just (_, ref) -> runIOFinalizer ref
        )
