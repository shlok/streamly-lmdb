{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NamedFieldPuns #-}
{-# LANGUAGE NumericUnderscores #-}
{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE PolyKinds #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE StandaloneKindSignatures #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE TypeFamilies #-}

module Streamly.External.LMDB.Internal where

import Control.Concurrent
import qualified Control.Concurrent.Lifted as LI
import Control.Concurrent.STM
import qualified Control.Exception as E
import qualified Control.Exception.Lifted as LI
import Control.Exception.Safe
import Control.Monad
import Control.Monad.IO.Class
import Control.Monad.Trans.Control
import Data.Bifunctor
import Data.ByteString (ByteString)
import qualified Data.ByteString as B
import qualified Data.ByteString.Unsafe as B
import Data.Foldable
import Data.Kind
import Data.Sequence (Seq)
import qualified Data.Sequence as Seq
import Foreign hiding (void)
import Foreign.C
import GHC.TypeLits
import Streamly.Data.Fold (Fold)
import qualified Streamly.Data.Fold as F
import qualified Streamly.Data.Stream.Prelude as S
import Streamly.External.LMDB.Internal.Error
import Streamly.External.LMDB.Internal.Foreign
import qualified Streamly.Internal.Data.Fold as F
import Streamly.Internal.Data.IOFinalizer
import Streamly.Internal.Data.Stream (Stream)
import Streamly.Internal.Data.Unfold (Unfold)
import qualified Streamly.Internal.Data.Unfold as U
import System.Directory
import System.Mem
import Text.Printf

isReadOnlyEnvironment :: forall emode. (Mode emode) => Bool
isReadOnlyEnvironment =
  isReadOnlyMode @emode (error "isReadOnlyEnvironment: unreachable")

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

-- | The default limits are 1 MiB map size, 0 named databases (see [Databases](#g:databases)), and
-- 126 concurrent readers. These can be adjusted freely, and in particular the 'mapSize' may be set
-- very large (limited only by available address space). However, LMDB is not optimized for a large
-- number of named databases so 'maxDatabases' should be kept to a minimum.
--
-- The default 'mapSize' is intentionally small, and should be changed to something appropriate for
-- your application. It ought to be a multiple of the OS page size, and should be chosen as large as
-- possible to accommodate future growth of the database(s). Once set for an environment, this limit
-- cannot be reduced to a value smaller than the space already consumed by the environment; however,
-- it can later be increased.
--
-- If you are going to use any named databases then you will need to change 'maxDatabases' to the
-- number of named databases you plan to use. However, you do not need to change this field if you
-- are only going to use the single main (unnamed) database.
defaultLimits :: Limits
defaultLimits =
  Limits
    { mapSize = mebibyte,
      maxDatabases = 0,
      maxReaders = 126
    }

-- | Open an LMDB environment in either 'ReadWrite' or 'ReadOnly' mode. The 'FilePath' argument may
-- be either a directory or a regular file, but it must already exist; when creating a new
-- environment, one should create an empty file or directory beforehand. If a regular file, an
-- additional file with "-lock" appended to the name is automatically created for the reader lock
-- table.
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
openEnvironment :: forall emode. (Mode emode) => FilePath -> Limits -> IO (Environment emode)
openEnvironment path limits = mask_ $ do
  -- Low-level requirements:
  -- https://github.com/LMDB/lmdb/blob/8d0cbbc936091eb85972501a9b31a8f86d4c51a7/libraries/liblmdb/lmdb.h#L100,
  -- https://github.com/LMDB/lmdb/blob/8d0cbbc936091eb85972501a9b31a8f86d4c51a7/libraries/liblmdb/lmdb.h#L102

  penv <- mdb_env_create
  onException
    ( do
        mdb_env_set_mapsize penv limits.mapSize
        let maxDbs = limits.maxDatabases in when (maxDbs /= 0) $ mdb_env_set_maxdbs penv maxDbs
        mdb_env_set_maxreaders penv limits.maxReaders

        exists <- doesPathExist path
        unless exists $
          throwError
            "openEnvironment"
            ( "no file or directory found at the specified path; "
                ++ "please create an empty file or directory beforehand"
            )

        isDir <- doesDirectoryExist path

        -- Always use MDB_NOTLS, which is crucial for Haskell applications; see
        -- https://github.com/LMDB/lmdb/blob/8d0cbbc936091eb85972501a9b31a8f86d4c51a7/libraries/liblmdb/lmdb.h#L615
        let isRo = isReadOnlyEnvironment @emode
            flags = mdb_notls : ([mdb_rdonly | isRo] ++ [mdb_nosubdir | not isDir])

        catchJust
          ( \case
              LMDB_Error {e_code = Left code}
                | Errno (fromIntegral code) == eNOENT && isRo -> Just ()
              _ -> Nothing
          )
          (mdb_env_open penv path (combineOptions flags))
          ( \() ->
              -- Provide a friendlier error for a presumably common user mistake.
              throwError
                "openEnvironment"
                ( "mdb_env_open returned 2 (ENOENT); "
                    ++ "one possibility is that a new empty environment "
                    ++ "wasn't first opened in ReadWrite mode"
                )
          )
    )
    -- In particular if mdb_env_open fails, the environment must be closed; see
    -- https://github.com/LMDB/lmdb/blob/8d0cbbc936091eb85972501a9b31a8f86d4c51a7/libraries/liblmdb/lmdb.h#L546
    (c_mdb_env_close penv)

  mvars <-
    (,,,)
      <$> newTMVarIO 0
      <*> (WriteLock <$> newEmptyMVar)
      <*> (WriteThread <$> newEmptyMVar)
      <*> (CloseDbLock <$> newMVar ())

  return $ Environment penv mvars

-- | Closes the given environment.
--
-- If you have merely a few dozen environments at most, there should be no need for this. (It is a
-- common practice with LMDB to create one’s environments once and reuse them for the remainder of
-- the program’s execution.)
--
-- To satisfy certain low-level LMDB requirements:
--
-- * Before calling this function, please call 'closeDatabase' on all databases in the environment.
-- * Before calling this function, close all cursors and commit\/abort all transactions on the
--   environment. To make sure this requirement is satisified for read-only transactions, either (a)
--   call 'waitReaders' or (b) pass precreated cursors/transactions to 'readLMDB' and
--   'unsafeReadLMDB'.
-- * After calling this function, do not use the environment or any related databases, transactions,
--   and cursors.
closeEnvironment :: forall emode. (Mode emode) => Environment emode -> IO ()
closeEnvironment (Environment penv _) = do
  -- An environment should only be closed once, so the low-level concurrency requirements should be
  -- fulfilled:
  -- https://github.com/LMDB/lmdb/blob/8d0cbbc936091eb85972501a9b31a8f86d4c51a7/libraries/liblmdb/lmdb.h#L787
  c_mdb_env_close penv

-- | Gets a database with the given name.
--
-- If only one database is desired within the environment, the name can be 'Nothing' (known as the
-- “unnamed database”).
--
-- If one or more named databases (a database with a 'Just' name) are desired, the 'maxDatabases' of
-- the environment’s limits should have been adjusted accordingly. The unnamed database will in this
-- case contain the names of the named databases as keys, which one is allowed to read but not
-- write.
--
-- /Warning/: When getting a named database for the first time (i.e., creating it), one must do so
-- in the 'ReadWrite' environment mode. (This restriction does not apply for the unnamed database.)
-- In this case, this function spawns a bound thread and creates a temporary read-write transaction
-- under the hood; see [Transactions](#g:transactions).
getDatabase ::
  forall emode.
  (Mode emode) =>
  Environment emode ->
  Maybe String ->
  IO (Database emode)
getDatabase env@(Environment penv mvars) mName = mask_ $ do
  -- To satisfy the lower-level concurrency requirements mentioned at
  -- https://github.com/LMDB/lmdb/blob/8d0cbbc936091eb85972501a9b31a8f86d4c51a7/libraries/liblmdb/lmdb.h#L1118
  -- we imagine, for simplicity, that everything below is a read-write transaction. Thusly, we also
  -- satisfy the MDB_NOTLS read-write transaction serialization requirement (for the case where a
  -- read-write transaction actually occur below). This simplification shouldn’t cause any problems,
  -- esp. since this function is presumably called relatively rarely in practice.

  let (_, WriteLock lock, _, _) = mvars
  putMVar lock () -- Interruptible when waiting for other read-write transactions.
  let disclaimWriteOwnership = takeMVar lock

  dbi <-
    finally
      ( case mName of
          Nothing -> do
            -- Use a read-only transaction to get the unnamed database. (MDB_CREATE is never needed
            -- for the unnamed database.)
            ptxn <- mdb_txn_begin penv nullPtr mdb_rdonly
            onException
              (mdb_dbi_open ptxn Nothing 0 <* mdb_txn_commit ptxn)
              (c_mdb_txn_abort ptxn)
          Just name -> do
            mdbi <- getNamedDb env name
            case mdbi of
              Just dbi ->
                return dbi
              Nothing ->
                -- The named database was not found.
                if isReadOnlyEnvironment @emode
                  then
                    throwError
                      "getDatabase"
                      ( "please use the ReadWrite environment mode for getting a named database "
                          ++ "for the first time (i.e., for creating a named database)"
                      )
                  else
                    -- Use a read-write transaction to create the named database.
                    --
                    -- We run this in a bound thread to make sure the read-write transaction doesn’t
                    -- cross OS threads. (We do this ourselves instead of putting this burden on the
                    -- user because this function is presumably called relatively rarely in
                    -- practice.)
                    runInBoundThread $ do
                      ptxn <- mdb_txn_begin penv nullPtr 0
                      onException
                        (mdb_dbi_open ptxn (Just name) mdb_create <* mdb_txn_commit ptxn)
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
-- * Before calling this function, please make sure all read-write transactions that have modified
--   the database have already been committed or aborted.
-- * After calling this function, do not use the database or any of its cursors again. To make sure
--   this requirement is satisfied for cursors on read-only transactions, either (a) call
--   'waitReaders' or (b) pass precreated cursors/transactions to 'readLMDB' and 'unsafeReadLMDB'.
closeDatabase :: forall emode. (Mode emode) => Database emode -> IO ()
closeDatabase (Database (Environment penv mvars) dbi) = do
  -- We need to serialize the closing; see
  -- https://github.com/LMDB/lmdb/blob/8d0cbbc936091eb85972501a9b31a8f86d4c51a7/libraries/liblmdb/lmdb.h#L1200
  let (_, _, _, CloseDbLock lock) = mvars
  withMVar lock $ \() ->
    c_mdb_dbi_close penv dbi

-- | A type for an optional thing where we want to fix the transaction mode to @ReadOnly@ in the
-- nothing case. (@Maybe@ isn’t powerful enough for this.)
data MaybeTxn tmode a where
  NoTxn :: MaybeTxn ReadOnly a
  JustTxn :: a -> MaybeTxn tmode a

-- | A type for an @Either@-like choice where we want to fix the transaction mode to @ReadOnly@ in
-- the @Left@ case. (@Either@ isn’t powerful enough for this.)
data EitherTxn tmode a b where
  LeftTxn :: a -> EitherTxn ReadOnly a b
  RightTxn :: b -> EitherTxn tmode a b

-- | Use @unsafe@ FFI calls under the hood. This can increase iteration speed, but one should
-- bear in mind that @unsafe@ FFI calls, since they block all other threads, can have an adverse
-- impact on the performance of the rest of the program.
--
-- /Internal/.
newtype UseUnsafeFFI = UseUnsafeFFI Bool deriving (Show)

-- | Creates an unfold with which we can stream key-value pairs from the given database.
--
-- If an existing transaction and cursor are not provided, there are two possibilities: (a) If a
-- chunk size is not provided, a read-only transaction and cursor are automatically created for the
-- entire duration of the unfold. (b) Otherwise, new transactions and cursors are automatically
-- created according to the desired chunk size. In this case, each transaction (apart from the first
-- one) starts as expected at the key next to (i.e., the largest\/smallest key less\/greater than)
-- the previously encountered key.
--
-- If you want to iterate through a large database while avoiding a long-lived transaction (see
-- [Transactions](#g:transactions)), it is your responsibility to either chunk up your usage of
-- 'readLMDB' (with which 'readStart' can help) or specify a chunk size as described above.
--
-- Runtime consideration: If you call 'readLMDB' very frequently without a precreated transaction
-- and cursor, you might find upon profiling that a significant time is being spent at
-- @mdb_txn_begin@, or find yourself having to increase 'maxReaders' in the environment’s limits
-- because the transactions and cursors are not being garbage collected fast enough. In this case,
-- please consider precreating a transaction and cursor.
--
-- If you don’t want the overhead of intermediate @ByteString@s (on your way to your eventual data
-- structures), use 'unsafeReadLMDB' instead.
{-# INLINE readLMDB #-}
readLMDB ::
  forall m emode tmode.
  (MonadIO m, Mode emode, Mode tmode, SubMode emode tmode) =>
  Unfold
    m
    ( ReadOptions,
      Database emode,
      EitherTxn tmode (Maybe ChunkSize) (Transaction tmode emode, Cursor)
    )
    (ByteString, ByteString)
readLMDB =
  U.lmap
    (\(ropts, db, etxncurs) -> (ropts, UseUnsafeFFI False, db, etxncurs))
    readLMDB'

-- | Similar to 'readLMDB', except that it has an extra 'UseUnsafeFFI' parameter.
--
-- /Internal/.
{-# INLINE readLMDB' #-}
readLMDB' ::
  forall m emode tmode.
  (MonadIO m, Mode emode, Mode tmode, SubMode emode tmode) =>
  Unfold
    m
    ( ReadOptions,
      UseUnsafeFFI,
      Database emode,
      EitherTxn tmode (Maybe ChunkSize) (Transaction tmode emode, Cursor)
    )
    (ByteString, ByteString)
readLMDB' =
  U.lmap
    (\(ropts, us, db, etxncurs) -> (ropts, us, db, etxncurs, B.packCStringLen, B.packCStringLen))
    unsafeReadLMDB'

-- | Similar to 'readLMDB', except that the keys and values are not automatically converted into
-- Haskell @ByteString@s.
--
-- To ensure safety, please make sure that the memory pointed to by the 'CStringLen' for each
-- key/value mapping function call is (a) only read (and not written to); and (b) not used after the
-- mapping function has returned. One way to transform the 'CStringLen's to your desired data
-- structures is to use 'Data.ByteString.Unsafe.unsafePackCStringLen'.
{-# INLINE unsafeReadLMDB #-}
unsafeReadLMDB ::
  forall m k v emode tmode.
  (MonadIO m, Mode emode, Mode tmode, SubMode emode tmode) =>
  Unfold
    m
    ( ReadOptions,
      Database emode,
      EitherTxn tmode (Maybe ChunkSize) (Transaction tmode emode, Cursor),
      CStringLen -> IO k,
      CStringLen -> IO v
    )
    (k, v)
unsafeReadLMDB =
  U.lmap
    (\(ropts, db, etxncurs, kmap, vmap) -> (ropts, UseUnsafeFFI False, db, etxncurs, kmap, vmap))
    unsafeReadLMDB'

-- | Similar to 'unsafeReadLMDB', except that it has an extra 'UseUnsafeFFI' parameter.
--
-- /Internal/.
{-# INLINE unsafeReadLMDB' #-}
unsafeReadLMDB' ::
  forall m k v emode tmode.
  (MonadIO m, Mode emode, Mode tmode, SubMode emode tmode) =>
  Unfold
    m
    ( ReadOptions,
      UseUnsafeFFI,
      Database emode,
      EitherTxn tmode (Maybe ChunkSize) (Transaction tmode emode, Cursor),
      CStringLen -> IO k,
      CStringLen -> IO v
    )
    (k, v)
unsafeReadLMDB' =
  -- Performance notes:
  -- + Unfortunately, introducing ChunkSize support increased overhead compared to C from around 50
  --   ns/pair to around 110 ns/s (for the safe FFI case).
  -- + We mention below what things helped with performance.
  -- + We tried various other things (e.g., using [0] or [4] phase control for the inlined
  --   subfunctions, wrapping changing state in IORefs, etc.), but to no avail.
  -- + For now, we presume that there is simply not much more gain left to achieve, given the extra
  --   workload needed for the chunking support. (TODO: We noticed that removing the unsafe FFI
  --   support altogether seems to give around 5 ns/pair speedup, but for now we don’t bother.)
  let {-# INLINE newTxnCurs #-}
      newTxnCurs ::
        (Ptr MDB_env -> Ptr MDB_txn -> CUInt -> IO (Ptr MDB_txn)) ->
        (Ptr MDB_txn -> IO ()) ->
        (Ptr MDB_txn -> MDB_dbi_t -> IO (Ptr MDB_cursor)) ->
        (Ptr MDB_cursor -> IO ()) ->
        Ptr MDB_env ->
        MDB_dbi_t ->
        TMVar NumReaders ->
        IO (Ptr MDB_cursor, IOFinalizer)
      newTxnCurs
        txn_begin
        txn_abort
        cursor_open
        cursor_close
        penv
        dbi
        numReadersT =
          mask_ $ do
            atomically $ do
              n <- takeTMVar numReadersT
              putTMVar numReadersT $ n + 1

            let decrReaders = atomically $ do
                  -- This should, when this is called, never be interruptible because we are, of
                  -- course, finishing up a reader that is still known to exist.
                  n <- takeTMVar numReadersT
                  putTMVar numReadersT $ n - 1

            ptxn <-
              onException
                (txn_begin penv nullPtr mdb_rdonly)
                decrReaders

            pcurs <-
              onException
                (cursor_open ptxn dbi)
                (txn_abort ptxn >> decrReaders)

            txnCursRef <-
              onException
                ( do
                    newIOFinalizer . mask_ $ do
                      -- With LMDB, there is ordinarily no need to commit read-only transactions.
                      -- (The exception is when we want to make databases that were opened during
                      -- the transaction available later, but that’s not applicable here.) We can
                      -- therefore abort ptxn, both for failure (exceptions) and success.
                      --
                      -- Note furthermore that this should be sound in the face of asynchronous
                      -- exceptions (where this finalizer could get called from a different thread)
                      -- because LMDB with MDB_NOTLS allows for read-only transactions being used
                      -- from multiple threads; see
                      -- https://github.com/LMDB/lmdb/blob/8d0cbbc936091eb85972501a9b31a8f86d4c51a7/libraries/liblmdb/lmdb.h#L984
                      cursor_close pcurs >> txn_abort ptxn >> decrReaders
                )
                (cursor_close pcurs >> txn_abort ptxn >> decrReaders)

            return (pcurs, txnCursRef)

      {-# INLINE positionCurs #-}
      positionCurs ::
        (Ptr MDB_cursor -> Ptr MDB_val -> Ptr MDB_val -> MDB_cursor_op_t -> IO CInt) ->
        ReadStart ->
        Ptr MDB_cursor ->
        Ptr MDB_val ->
        Ptr MDB_val ->
        -- Three possibilities (cursor_get return value): 0 (found), mdb_notfound (not found), other
        -- non-zero (error).
        IO CInt
      positionCurs cursor_get readStart pcurs pk pv =
        case readStart of
          ReadBeg -> cursor_get pcurs pk pv mdb_first
          ReadEnd -> cursor_get pcurs pk pv mdb_last
          ReadGE k -> B.unsafeUseAsCStringLen k $ \(kp, kl) -> do
            poke pk (MDB_val (fromIntegral kl) kp)
            cursor_get pcurs pk pv mdb_set_range
          -- For the other cases, LMDB has no built-in operators; so we simulate them ourselves.
          ReadGT k -> B.unsafeUseAsCStringLen k $ \(kp, kl) -> do
            poke pk (MDB_val (fromIntegral kl) kp)
            rc <- cursor_get pcurs pk pv mdb_set_range
            if rc == 0
              then do
                k' <- peek pk >>= \x -> B.unsafePackCStringLen (x.mv_data, fromIntegral x.mv_size)
                if k' == k
                  then cursor_get pcurs pk pv mdb_next
                  else return rc
              else
                -- Error; or not found (if GE is not found, GT doesn’t exist either).
                return rc
          ReadLE k -> B.unsafeUseAsCStringLen k $ \(kp, kl) -> do
            poke pk (MDB_val (fromIntegral kl) kp)
            rc <- cursor_get pcurs pk pv mdb_set_range
            if rc == 0
              then do
                k' <- peek pk >>= \x -> B.unsafePackCStringLen (x.mv_data, fromIntegral x.mv_size)
                if k' == k
                  then return rc
                  else cursor_get pcurs pk pv mdb_prev
              else do
                if rc == mdb_notfound
                  then cursor_get pcurs pk pv mdb_last
                  else return rc
          ReadLT k -> B.unsafeUseAsCStringLen k $ \(kp, kl) -> do
            poke pk (MDB_val (fromIntegral kl) kp)
            rc <- cursor_get pcurs pk pv mdb_set_range
            if rc == 0
              then
                -- In both GE and GT cases, find the previous one (to reach LT).
                cursor_get pcurs pk pv mdb_prev
              else do
                if rc == mdb_notfound
                  then cursor_get pcurs pk pv mdb_last
                  else return rc
   in U.Unfold
        ( \( (rc, chunkSz, pcurs, txnCursRef),
             rf@ReadLMDBFixed_ {r_db = Database (Environment !penv !mvars) !dbi}
             ) ->
              liftIO $ do
                let (numReadersT, _, _, _) = mvars

                if rc == 0
                  then do
                    -- + pk and pv now contain the data we want to yield; prepare p and v for
                    --   yielding.
                    -- + Note: pk will remain important below in the case where the desired maximum
                    --   ChunkSize is exceeded.
                    -- + (Avoiding the extra byte size things in the non-ChunkBytes cases seemed to
                    --   improve performance by around 10 ns/pair.)
                    -- + (These bang patterns and/or the below bang pattern seemed to improve
                    --   performance by around 20 ns/pair.)
                    (!k, !v, !chunkSz') <-
                      if rf.r_chunkSzInc > 1
                        then do
                          (kSz, k) <-
                            peek rf.r_pk >>= \x ->
                              let sz = fromIntegral x.mv_size in (sz,) <$> rf.r_kmap (x.mv_data, sz)
                          (vSz, v) <-
                            peek rf.r_pv >>= \x ->
                              let sz = fromIntegral x.mv_size in (sz,) <$> rf.r_vmap (x.mv_data, sz)
                          return (k, v, chunkSz + kSz + vSz)
                        else do
                          k <- peek rf.r_pk >>= \x -> rf.r_kmap (x.mv_data, fromIntegral x.mv_size)
                          v <- peek rf.r_pv >>= \x -> rf.r_vmap (x.mv_data, fromIntegral x.mv_size)
                          return (k, v, chunkSz + rf.r_chunkSzInc)

                    -- If the chunk size has exceeded a desired limit, dispose of the existing
                    -- read-only transaction and cursor and create new ones.
                    !x <-
                      if chunkSz' < rf.r_chunkSzMax
                        then do
                          -- Staying on the same chunk.
                          rc' <- rf.r_cursor_get pcurs rf.r_pk rf.r_pv rf.r_nextPrevOp
                          return ((rc', chunkSz', pcurs, txnCursRef), rf)
                        else do
                          -- We make a copy of pk before aborting the current read-only transaction
                          -- (which makes the data in pk unavailable).
                          prevk <-
                            peek rf.r_pk
                              >>= \x -> B.packCStringLen (x.mv_data, fromIntegral x.mv_size)
                          runIOFinalizer txnCursRef
                          (pcurs', txnCursRef') <- rf.r_newtxncurs penv dbi numReadersT
                          rc' <-
                            positionCurs
                              rf.r_cursor_get
                              (rf.r_nextChunkOp prevk)
                              pcurs'
                              rf.r_pk
                              rf.r_pv
                          return ((rc', 0, pcurs', txnCursRef'), rf)

                    return $ U.Yield (k, v) x
                  else
                    if rc == mdb_notfound
                      then do
                        runIOFinalizer txnCursRef >> runIOFinalizer rf.r_pref
                        return U.Stop
                      else do
                        runIOFinalizer txnCursRef >> runIOFinalizer rf.r_pref
                        throwLMDBErrNum "mdb_cursor_get" rc
        )
        ( \(ropts, us, db@(Database (Environment penv mvars) dbi), etxncurs, kmap, vmap) ->
            liftIO $ do
              let useInternalTxnCurs = case etxncurs of
                    LeftTxn _ -> True
                    RightTxn _ -> False

              -- Type-level guarantee.
              when (useInternalTxnCurs && not (isReadOnlyEnvironment @tmode)) $ error "unreachable"

              case etxncurs of
                LeftTxn Nothing -> return ()
                RightTxn _ -> return ()
                LeftTxn (Just (ChunkNumPairs maxPairs)) ->
                  unless (maxPairs > 0) $
                    throwError "readLMDB" "please specify positive ChunkNumPairs"
                LeftTxn (Just (ChunkBytes maxBytes)) ->
                  unless (maxBytes > 0) $
                    throwError "readLMDB" "please specify positive ChunkBytes"

              (pk, pv, pref) <- mask_ $ do
                pk <- malloc
                pv <- onException malloc (free pk)
                pref <- onException (newIOFinalizer $ free pv >> free pk) (free pv >> free pk)
                return (pk, pv, pref)

              let (numReadersT, _, _, _) = mvars
                  chunkSz :: Int = 0

                  -- + Avoid case lookups in each iteration.
                  -- + (This seemed to improve performance by over 200 ns/pair.)
                  (nextPrevOp, nextChunkOp) = case ropts.readDirection of
                    Forward -> (mdb_next, ReadGT)
                    Backward -> (mdb_prev, ReadLT)
                  (chunkSzInc, chunkSzMax) :: (Int, Int) = case etxncurs of
                    -- chunkSz stays zero.
                    LeftTxn Nothing -> (0, maxBound)
                    RightTxn _ -> (0, maxBound)
                    -- chunkSz increments by 1.
                    LeftTxn (Just (ChunkNumPairs maxPairs)) -> (1, maxPairs)
                    -- “chunkSzInc > 1” means we should increment by bytes. (2 is meaningless.)
                    LeftTxn (Just (ChunkBytes maxBytes)) -> (2, maxBytes)

                  (txn_begin, cursor_open, cursor_get, cursor_close, txn_abort) =
                    case us of
                      UseUnsafeFFI True ->
                        ( mdb_txn_begin_unsafe,
                          mdb_cursor_open_unsafe,
                          c_mdb_cursor_get_unsafe,
                          c_mdb_cursor_close_unsafe,
                          c_mdb_txn_abort_unsafe
                        )
                      UseUnsafeFFI False ->
                        ( mdb_txn_begin,
                          mdb_cursor_open,
                          c_mdb_cursor_get,
                          c_mdb_cursor_close,
                          c_mdb_txn_abort
                        )

                  newtxncurs = newTxnCurs txn_begin txn_abort cursor_open cursor_close

              (pcurs, txnCursRef) <- case etxncurs of
                LeftTxn _ -> do
                  -- Create first transaction and cursor.
                  newtxncurs penv dbi numReadersT
                RightTxn (_, Cursor pcurs) -> do
                  -- Transaction and cursor are provided by the user.
                  f <- newIOFinalizer $ return ()
                  return (pcurs, f)

              rc <- positionCurs cursor_get ropts.readStart pcurs pk pv

              return
                ( -- State that can change in iterations.
                  (rc, chunkSz, pcurs, txnCursRef),
                  ReadLMDBFixed_
                    { r_db = db,
                      r_kmap = kmap,
                      r_vmap = vmap,
                      r_newtxncurs = newtxncurs,
                      r_cursor_get = cursor_get,
                      r_nextPrevOp = nextPrevOp,
                      r_nextChunkOp = nextChunkOp,
                      r_chunkSzInc = chunkSzInc,
                      r_chunkSzMax = chunkSzMax,
                      r_pk = pk,
                      r_pv = pv,
                      r_pref = pref
                    }
                )
        )

-- | State that stays fixed in 'readLMDB' iterations.
--
-- /Internal/.
data ReadLMDBFixed_ emode k v = ReadLMDBFixed_
  { -- (Keeping the records lazy seemed to improve performance by around 5-10 ns/pair.)
    r_db :: Database emode,
    r_kmap :: CStringLen -> IO k,
    r_vmap :: CStringLen -> IO v,
    r_newtxncurs ::
      Ptr MDB_env ->
      MDB_dbi_t ->
      TMVar NumReaders ->
      IO (Ptr MDB_cursor, IOFinalizer),
    r_cursor_get ::
      Ptr MDB_cursor ->
      Ptr MDB_val ->
      Ptr MDB_val ->
      MDB_cursor_op_t ->
      IO CInt,
    r_nextPrevOp :: MDB_cursor_op_t,
    r_nextChunkOp :: ByteString -> ReadStart,
    r_chunkSzInc :: Int,
    r_chunkSzMax :: Int,
    r_pk :: Ptr MDB_val,
    r_pv :: Ptr MDB_val,
    r_pref :: IOFinalizer
  }

-- | Looks up the value for the given key in the given database.
--
-- If an existing transaction is not provided, a read-only transaction is automatically created
-- internally.
--
-- Runtime consideration: If you call 'getLMDB' very frequently without a precreated transaction,
-- you might find upon profiling that a significant time is being spent at @mdb_txn_begin@, or find
-- yourself having to increase 'maxReaders' in the environment’s limits because the transactions are
-- not being garbage collected fast enough. In this case, please consider precreating a transaction.
{-# INLINE getLMDB #-}
getLMDB ::
  forall emode tmode.
  (Mode emode, Mode tmode, SubMode emode tmode) =>
  Database emode ->
  MaybeTxn tmode (Transaction tmode emode) ->
  ByteString ->
  IO (Maybe ByteString)
getLMDB (Database env@(Environment _ _) dbi) mtxn k =
  let {-# INLINE brack #-}
      brack io = case mtxn of
        NoTxn -> withReadOnlyTransaction env $ \(Transaction _ ptxn) -> io ptxn
        JustTxn (Transaction _ ptxn) -> io ptxn
   in B.unsafeUseAsCStringLen k $ \(kp, kl) ->
        with (MDB_val (fromIntegral kl) kp) $ \pk -> alloca $ \pv ->
          brack $ \ptxn -> do
            rc <- c_mdb_get ptxn dbi pk pv
            if rc == 0
              then do
                v' <- peek pv
                Just <$> B.packCStringLen (v'.mv_data, fromIntegral v'.mv_size)
              else
                if rc == mdb_notfound
                  then return Nothing
                  else throwLMDBErrNum "mdb_get" rc

-- | A read-only (@tmode@: 'ReadOnly') or read-write (@tmode@: 'ReadWrite') transaction.
--
-- @emode@: the environment’s mode. Note: 'ReadOnly' environments can only have 'ReadOnly'
-- transactions; we enforce this at the type level.
data Transaction tmode emode = Transaction !(Environment emode) !(Ptr MDB_txn)

-- | Begins an LMDB read-only transaction on the given environment.
--
-- For read-only transactions returned from this function, it is your responsibility to (a) make
-- sure the transaction only gets used by a single 'readLMDB', 'unsafeReadLMDB', or 'getLMDB' at the
-- same time, (b) use the transaction only on databases in the environment on which the transaction
-- was begun, (c) make sure that those databases were already obtained before the transaction was
-- begun, (d) dispose of the transaction with 'abortReadOnlyTransaction', and (e) be aware of the
-- caveats regarding long-lived transactions; see [Transactions](#g:transactions).
--
-- To easily manage a read-only transaction’s lifecycle, we suggest using 'withReadOnlyTransaction'.
{-# INLINE beginReadOnlyTransaction #-}
beginReadOnlyTransaction ::
  forall emode.
  (Mode emode) =>
  Environment emode ->
  IO (Transaction ReadOnly emode)
beginReadOnlyTransaction env@(Environment penv mvars) = mask_ $ do
  -- The non-concurrency requirement:
  -- https://github.com/LMDB/lmdb/blob/mdb.master/libraries/liblmdb/lmdb.h#L614
  let (numReadersT, _, _, _) = mvars

  -- Similar comments for NumReaders as in unsafeReadLMDB.
  atomically $ do
    n <- takeTMVar numReadersT
    putTMVar numReadersT $ n + 1

  onException
    (Transaction @ReadOnly env <$> mdb_txn_begin penv nullPtr mdb_rdonly)
    ( atomically $ do
        n <- takeTMVar numReadersT
        putTMVar numReadersT $ n - 1
    )

-- | Disposes of a read-only transaction created with 'beginReadOnlyTransaction'.
--
-- It is your responsibility to not use the transaction or any of its cursors afterwards.
{-# INLINE abortReadOnlyTransaction #-}
abortReadOnlyTransaction :: forall emode. (Mode emode) => Transaction ReadOnly emode -> IO ()
abortReadOnlyTransaction (Transaction (Environment _ mvars) ptxn) = mask_ $ do
  let (numReadersT, _, _, _) = mvars
  c_mdb_txn_abort ptxn
  -- Similar comments for NumReaders as in unsafeReadLMDB.
  atomically $ do
    n <- takeTMVar numReadersT
    putTMVar numReadersT $ n - 1

-- | Creates a temporary read-only transaction on which the provided action is performed, after
-- which the transaction gets aborted. The transaction also gets aborted upon exceptions.
--
-- You have the same responsibilities as documented for 'beginReadOnlyTransaction' (apart from the
-- transaction disposal).
{-# INLINE withReadOnlyTransaction #-}
withReadOnlyTransaction ::
  forall m a emode.
  (Mode emode, MonadBaseControl IO m, MonadIO m) =>
  Environment emode ->
  (Transaction ReadOnly emode -> m a) ->
  m a
withReadOnlyTransaction env =
  LI.bracket
    (liftIO $ beginReadOnlyTransaction env)
    -- Aborting a transaction should never fail (as it merely frees a pointer), so any potential
    -- issues solved by safe-exceptions (in particular “swallowing asynchronous exceptions via
    -- failing cleanup handlers”) shouldn’t apply here.
    (liftIO . abortReadOnlyTransaction)

-- | A cursor.
newtype Cursor = Cursor (Ptr MDB_cursor)

-- | Opens a cursor for use with 'readLMDB' or 'unsafeReadLMDB'. It is your responsibility to (a)
-- make sure the cursor only gets used by a single 'readLMDB' or 'unsafeReadLMDB' at the same time,
-- (b) make sure the provided database is within the environment on which the provided transaction
-- was begun, and (c) dispose of the cursor with 'closeCursor'.
--
-- To easily manage a cursor’s lifecycle, we suggest using 'withCursor'.
{-# INLINE openCursor #-}
openCursor ::
  forall emode tmode.
  (Mode emode, Mode tmode, SubMode emode tmode) =>
  Transaction tmode emode ->
  Database emode ->
  IO Cursor
openCursor (Transaction _ ptxn) (Database _ dbi) =
  Cursor <$> mdb_cursor_open ptxn dbi

-- | Disposes of a cursor created with 'openCursor'.
{-# INLINE closeCursor #-}
closeCursor :: Cursor -> IO ()
closeCursor (Cursor pcurs) =
  -- (Sidenote: Although a cursor will, at least for users who use brackets, usually be called
  -- before the transaction gets aborted (read-only/read-write) or committed (read-write), the order
  -- doesn’t really matter for read-only transactions.)
  c_mdb_cursor_close pcurs

-- | Creates a temporary cursor on which the provided action is performed, after which the cursor
-- gets closed. The cursor also gets closed upon exceptions.
--
-- You have the same responsibilities as documented for 'openCursor' (apart from the cursor
-- disposal).
{-# INLINE withCursor #-}
withCursor ::
  forall m a emode tmode.
  (MonadBaseControl IO m, MonadIO m, Mode emode, Mode tmode, SubMode emode tmode) =>
  Transaction tmode emode ->
  Database emode ->
  (Cursor -> m a) ->
  m a
withCursor txn db =
  LI.bracket
    (liftIO $ openCursor txn db)
    -- Closing a cursor should never fail (as it merely frees a pointer), so any potential issues
    -- solved by safe-exceptions (in particular “swallowing asynchronous exceptions via failing
    -- cleanup handlers”) shouldn’t apply here.
    (liftIO . closeCursor)

data ReadOptions = ReadOptions
  -- It might seem strange to allow, e.g., ReadBeg and Backward together. However, this simplifies
  -- things in the sense that it separates the initial position concept from the iteration
  -- (next/prev) concept.
  { readStart :: !ReadStart,
    readDirection :: !ReadDirection
  }
  deriving (Show)

-- | By default, we start reading from the beginning of the database (i.e., from the smallest key)
-- and iterate in forward direction.
defaultReadOptions :: ReadOptions
defaultReadOptions =
  ReadOptions
    { readStart = ReadBeg,
      readDirection = Forward
    }

-- | Direction of key iteration.
data ReadDirection = Forward | Backward deriving (Show)

-- | The key from which an iteration should start.
data ReadStart
  = -- | Start from the smallest key.
    ReadBeg
  | -- | Start from the largest key.
    ReadEnd
  | -- | Start from the smallest key that is greater than or equal to the given key.
    ReadGE !ByteString
  | -- | Start from the smallest key that is greater than the given key.
    ReadGT !ByteString
  | -- | Start from the largest key that is less than or equal to the given key.
    ReadLE !ByteString
  | -- | Start from the largest key that is less than the given key.
    ReadLT !ByteString
  deriving (Show)

-- | Begins an LMDB read-write transaction on the given environment.
--
-- Unlike read-only transactions, a given read-write transaction is not allowed to stray from the OS
-- thread on which it was begun, and it is your responsibility to make sure of this. You can achieve
-- this with, e.g., 'Control.Concurrent.runInBoundThread'.
--
-- Additionally, for read-write transactions returned from this function, it is your responsibility
-- to (a) use the transaction only on databases in the environment on which the transaction was
-- begun, (b) make sure that those databases were already obtained before the transaction was begun,
-- (c) commit\/abort the transaction with 'commitReadWriteTransaction'\/'abortReadWriteTransaction',
-- and (d) be aware of the caveats regarding long-lived transactions; see
-- [Transactions](#g:transactions).
--
-- To easily manage a read-write transaction’s lifecycle, we suggest using
-- 'withReadWriteTransaction'.
{-# INLINE beginReadWriteTransaction #-}
beginReadWriteTransaction :: Environment ReadWrite -> IO (Transaction ReadWrite ReadWrite)
beginReadWriteTransaction env@(Environment penv mvars) = mask_ $ do
  isCurrentThreadBound
    >>= flip
      unless
      (throwError "beginReadWriteTransaction" "please call on a bound thread")
  threadId <- myThreadId

  let (_, WriteLock lock, WriteThread writeThread, _) = mvars
  putMVar lock () -- Interruptible when waiting for other read-write transactions.
  tryPutMVar writeThread threadId >>= flip unless (error "unreachable")
  let disclaimWriteOwnership = mask_ $ tryTakeMVar writeThread >> tryTakeMVar lock

  onException
    (Transaction @ReadWrite env <$> mdb_txn_begin penv nullPtr 0)
    disclaimWriteOwnership

-- | Aborts a read-write transaction created with 'beginReadWriteTransaction'.
--
-- It is your responsibility to not use the transaction afterwards.
{-# INLINE abortReadWriteTransaction #-}
abortReadWriteTransaction :: Transaction ReadWrite ReadWrite -> IO ()
abortReadWriteTransaction (Transaction (Environment _ mvars) ptxn) = mask_ $ do
  let throwErr = throwError "abortReadWriteTransaction"
  let (_, WriteLock lock, WriteThread writeThread, _) = mvars
  detectUserErrors True writeThread throwErr
  c_mdb_txn_abort ptxn
  void $ tryTakeMVar lock

-- | Commits a read-write transaction created with 'beginReadWriteTransaction'.
--
-- It is your responsibility to not use the transaction afterwards.
{-# INLINE commitReadWriteTransaction #-}
commitReadWriteTransaction :: Transaction ReadWrite ReadWrite -> IO ()
commitReadWriteTransaction (Transaction (Environment _ mvars) ptxn) = mask_ $ do
  let throwErr = throwError "commitReadWriteTransaction"
  let (_, WriteLock lock, WriteThread writeThread, _) = mvars
  detectUserErrors True writeThread throwErr
  onException
    (mdb_txn_commit ptxn)
    (c_mdb_txn_abort ptxn >> tryTakeMVar lock)
  void $ tryTakeMVar lock

-- | Spawns a new bound thread and creates a temporary read-write transaction on which the provided
-- action is performed, after which the transaction gets committed. The transaction gets aborted
-- upon exceptions.
--
-- You have the same responsibilities as documented for 'beginReadWriteTransaction' (apart from
-- running it on a bound thread and committing/aborting it).
{-# INLINE withReadWriteTransaction #-}
withReadWriteTransaction ::
  forall m a.
  (MonadBaseControl IO m, MonadIO m) =>
  Environment ReadWrite ->
  (Transaction ReadWrite ReadWrite -> m a) ->
  m a
withReadWriteTransaction env io =
  LI.runInBoundThread $
    -- We need an enhanced bracket. Using the normal bracket and simply committing after running io
    -- in the “in-between” computation is incorrect because this entire “in-between” computation
    -- falls under “restore” in bracket’s implementation, so an asynchronous exception can cause a
    -- commit to be followed by an abort. (Our 'testAsyncExceptionsConcurrent' test exposed this.)
    liftedBracket2
      (liftIO $ beginReadWriteTransaction env)
      -- + Aborting a transaction should never fail (as it merely frees a pointer), so any potential
      --   issues solved by safe-exceptions (in particular “swallowing asynchronous exceptions via
      --   failing cleanup handlers”) shouldn’t apply here.
      -- + Note: We have convinced ourselves that both the abort and commit are uninterruptible. (In
      --   particular, we presume 'myThreadId' (in 'detectUserErrors') is uninterruptible.)
      (liftIO . abortReadWriteTransaction)
      (liftIO . commitReadWriteTransaction)
      io

-- |
-- * @OverwriteAllow@: When a key reoccurs, overwrite the value.
-- * @OverwriteDisallow@: When a key reoccurs, don’t overwrite and hand the maladaptive key-value
--   pair to the accumulator.
-- * @OverwriteAppend@: Assume the input data is already increasing, which allows the use of
--   @MDB_APPEND@ under the hood and substantially improves write performance. Hand arriving
--   key-value pairs in a maladaptive order to the accumulator.
data OverwriteOptions m a where
  OverwriteAllow :: OverwriteOptions m ()
  OverwriteDisallow :: Either (WriteAccum m a) (WriteAccumWithOld m a) -> OverwriteOptions m a
  OverwriteAppend :: WriteAccum m a -> OverwriteOptions m a

-- | A fold for @(key, new value)@.
type WriteAccum m a = Fold m (ByteString, ByteString) a

-- | A fold for @(key, new value, old value)@. This has the overhead of getting the old value.
type WriteAccumWithOld m a = Fold m (ByteString, ByteString, ByteString) a

newtype WriteOptions m a = WriteOptions
  { writeOverwriteOptions :: OverwriteOptions m a
  }

-- | A function that shows a database key.
type ShowKey = ByteString -> String

-- | A function that shows a database value.
type ShowValue = ByteString -> String

-- | Throws upon the first maladaptive key. If desired, shows the maladaptive key-value pair in the
-- exception.
{-# INLINE writeAccumThrow #-}
writeAccumThrow :: (Monad m) => Maybe (ShowKey, ShowValue) -> WriteAccum m ()
writeAccumThrow mshow =
  F.foldlM'
    ( \() (k, v) ->
        throwError "writeLMDB" $
          "Maladaptive key encountered"
            ++ maybe
              ""
              (\(showk, showv) -> printf "; (key,value)=(%s,%s)" (showk k) (showv v))
              mshow
    )
    (return ())

-- | Throws upon the first maladaptive key where the old value differs from the new value. If
-- desired, shows the maladaptive key-value pair with the old value in the exception.
{-# INLINE writeAccumThrowAllowSameValue #-}
writeAccumThrowAllowSameValue :: (Monad m) => Maybe (ShowKey, ShowValue) -> WriteAccumWithOld m ()
writeAccumThrowAllowSameValue mshow =
  F.foldlM'
    ( \() (k, v, oldv) ->
        when (v /= oldv) $
          throwError "writeLMDB" $
            "Maladaptive key encountered"
              ++ maybe
                ""
                ( \(showk, showv) ->
                    printf "; (key,value,oldValue)=(%s,%s,%s)" (showk k) (showv v) (showv oldv)
                )
                mshow
    )
    (return ())

-- | Silently ignores maladaptive keys.
{-# INLINE writeAccumIgnore #-}
writeAccumIgnore :: (Monad m) => WriteAccum m ()
writeAccumIgnore = F.drain

-- | Gracefully stops upon the first maladaptive key.
{-# INLINE writeAccumStop #-}
writeAccumStop :: (Monad m) => WriteAccum m ()
writeAccumStop = void F.one

-- | By default, we allow overwriting.
defaultWriteOptions :: WriteOptions m ()
defaultWriteOptions =
  WriteOptions
    { writeOverwriteOptions = OverwriteAllow
    }

-- | A chunk size.
data ChunkSize
  = -- | Chunk up key-value pairs by number of pairs. The final chunk can have a fewer number of
    -- pairs.
    ChunkNumPairs !Int
  | -- | Chunk up key-value pairs by number of bytes. As soon as the byte count for the keys and
    -- values is reached, a new chunk is created (such that each chunk has at least one key-value
    -- pair and can end up with more than the desired number of bytes). The final chunk can have
    -- less than the desired number of bytes.
    ChunkBytes !Int
  deriving (Show)

-- | Chunks up the incoming stream of key-value pairs using the desired chunk size. One can try,
-- e.g., @ChunkBytes mebibyte@ (1 MiB chunks) and benchmark from there.
--
-- The chunks are processed using the desired fold.
{-# INLINE chunkPairsFold #-}
chunkPairsFold ::
  forall m a.
  (Monad m) =>
  ChunkSize ->
  Fold m (Seq (ByteString, ByteString)) a ->
  Fold m (ByteString, ByteString) a
chunkPairsFold chunkSz (F.Fold astep ainit aextr afinal) =
  let {-# INLINE final #-}
      final sequ as =
        case sequ of
          Seq.Empty -> afinal as
          _ -> do
            astep' <- astep as sequ
            case astep' of
              F.Done b -> return b
              F.Partial as' -> afinal as'
   in case chunkSz of
        ChunkNumPairs numPairs ->
          F.Fold
            ( \(!sequ, !as) (k, v) ->
                let sequ' = sequ Seq.|> (k, v)
                 in if Seq.length sequ' == numPairs
                      then
                        -- The (user-supplied) astep could already be Done here.
                        first (Seq.empty,) <$> astep as sequ'
                      else return . F.Partial $ (sequ', as)
            )
            -- The (user-supplied) ainit could already be Done here.
            (first (Seq.empty,) <$> ainit)
            -- If driven with a scan, the collection fold is assumed to also be compatible with
            -- scans and will result in the same output repeatedly for a chunk being built. (This
            -- should already be clear to the user.)
            (\(_, as) -> aextr as)
            -- This is the only direct exit point of this outer fold (since elsewhere it yields a
            -- partial). This is therefore the only place where afinal needs to be called.
            (uncurry final)
        ChunkBytes bytes ->
          -- All the comments for the above case hold here too.
          F.Fold
            ( \(!sequ, !byt, !as) (k, v) ->
                let sequ' = sequ Seq.|> (k, v)
                    byt' = byt + B.length k + B.length v
                 in if byt' >= bytes
                      then
                        first (Seq.empty,0,) <$> astep as sequ'
                      else
                        -- For long streams of empty keys and values, sequ' can also get long; but
                        -- this should be expected behavior (and is an irrelevant edge case for most
                        -- users anyway).
                        return . F.Partial $ (sequ', byt', as)
            )
            (first (Seq.empty,0,) <$> ainit)
            (\(_, _, as) -> aextr as)
            (\(sequ, _, as) -> final sequ as)

-- | Chunks up the incoming stream of key-value pairs using the desired chunk size. One can try,
-- e.g., @ChunkBytes mebibyte@ (1 MiB chunks) and benchmark from there.
{-# INLINE chunkPairs #-}
chunkPairs ::
  (Monad m) =>
  ChunkSize ->
  Stream m (ByteString, ByteString) ->
  Stream m (Seq (ByteString, ByteString))
chunkPairs chunkSz =
  S.foldMany $
    case chunkSz of
      ChunkNumPairs numPairs ->
        F.Fold
          ( \(!sequ) (k, v) ->
              let sequ' = sequ Seq.|> (k, v)
               in return $
                    if Seq.length sequ' == numPairs
                      then F.Done sequ'
                      else F.Partial sequ'
          )
          (return $ F.Partial Seq.empty)
          (error "unreachable")
          return
      ChunkBytes bytes ->
        F.Fold
          ( \(!sequ, !byt) (k, v) ->
              let sequ' = sequ Seq.|> (k, v)
                  byt' = byt + B.length k + B.length v
               in return $
                    if byt' >= bytes
                      then F.Done sequ'
                      else F.Partial (sequ', byt')
          )
          (return $ F.Partial (Seq.empty, 0))
          (error "unreachable")
          (\(sequ, _) -> return sequ)

-- | Writes a chunk of key-value pairs to the given database. Under the hood, it uses 'writeLMDB'
-- surrounded with a 'withReadWriteTransaction'.
{-# INLINE writeLMDBChunk #-}
writeLMDBChunk ::
  forall m a.
  (MonadBaseControl IO m, MonadIO m, MonadCatch m) =>
  WriteOptions m a ->
  Database ReadWrite ->
  Seq (ByteString, ByteString) ->
  m a
writeLMDBChunk =
  writeLMDBChunk' (UseUnsafeFFI False)

-- | Similar to 'writeLMDBChunk', except that it has an extra 'UseUnsafeFFI' parameter.
--
-- /Internal/.
{-# INLINE writeLMDBChunk' #-}
writeLMDBChunk' ::
  forall m a.
  (MonadBaseControl IO m, MonadIO m, MonadCatch m) =>
  UseUnsafeFFI ->
  WriteOptions m a ->
  Database ReadWrite ->
  Seq (ByteString, ByteString) ->
  m a
writeLMDBChunk' useUnsafeFFI wopts db@(Database env _) sequ =
  withReadWriteTransaction env $ \txn ->
    S.fold (writeLMDB' useUnsafeFFI wopts db txn) . S.fromList . toList $ sequ

-- | Creates a fold that writes a stream of key-value pairs to the provided database using the
-- provided transaction.
--
-- If you have a long stream of key-value pairs that you want to write to an LMDB database while
-- avoiding a long-lived transaction (see [Transactions](#g:transactions)), you can use the
-- functions for [chunked writing](#g:chunkedwriting).
{-# INLINE writeLMDB #-}
writeLMDB ::
  forall m a.
  (MonadIO m, MonadCatch m, MonadThrow m) =>
  WriteOptions m a ->
  Database ReadWrite ->
  Transaction ReadWrite ReadWrite ->
  Fold m (ByteString, ByteString) a
writeLMDB =
  writeLMDB' (UseUnsafeFFI False)

-- | Similar to 'writeLMDB', except that it has an extra 'UseUnsafeFFI' parameter.
--
-- /Internal/.
{-# INLINE writeLMDB' #-}
writeLMDB' ::
  forall m a.
  (MonadIO m, MonadCatch m, MonadThrow m) =>
  UseUnsafeFFI ->
  WriteOptions m a ->
  Database ReadWrite ->
  Transaction ReadWrite ReadWrite ->
  Fold m (ByteString, ByteString) a
writeLMDB'
  (UseUnsafeFFI us)
  wopts
  (Database env@(Environment _ mvars) dbi)
  txn@(Transaction _ ptxn) =
    -- Notes on why writeLMDB relies on the user creating read-write transactions up front, as
    -- opposed to writeLMDB itself maintaining the write transactions internally (as was the case in
    -- versions <=0.7.0):
    --   * The old way was not safe because LMDB read-write transactions are (unless MDB_NOLOCK is
    --     used) not allowed to cross OS threads; but upon asynchronous exceptions, the read-write
    --     transaction aborting would happen upon garbage collection (GC), which can occur on a
    --     different OS thread (even if the user ran the original writeLMDB on a bound thread).
    --   * We see no way around this but to wrap every read-write transaction in a bona fide bracket
    --     managed by the user (not a streamly-type bracket, which, again, relies on GC).
    --   * Two things we investigated: (a) Channels allow us to pass all writing to a specific OS
    --     thread, but doing this one-by-one for every mdb_put is way too slow; for channels to
    --     become performant, they need chunking. (b) We can use MDB_NOLOCK to avoid the
    --     same-OS-thread requirement, but this means other processes can no longer safely interact
    --     with the LMDB environment.
    --   * Two benefits of the new way: (a) A stream can be demuxed into writeLMDB folds on the same
    --     environment. (b) The writeLMDB fold works with scans.
    let put_ =
          if us
            then mdb_put_unsafe_
            else mdb_put_

        throwErr = throwError "writeLMDB"

        {-# INLINE validate #-}
        validate = do
          let (_, _, WriteThread writeThread, _) = mvars
          liftIO $ detectUserErrors False writeThread throwErr

        {-# INLINE putCatchKeyExists #-}
        putCatchKeyExists ::
          ByteString ->
          ByteString ->
          s -> -- State of the Accum fold (for the failures).
          CUInt ->
          (() -> m (F.Step s d)) ->
          m (F.Step s d)
        putCatchKeyExists k v s op =
          catchJust
            ( \case
                LMDB_Error {e_code = Right MDB_KEYEXIST} -> Just ()
                _ -> Nothing
            )
            ( do
                liftIO $
                  B.unsafeUseAsCStringLen k $ \(kp, kl) ->
                    B.unsafeUseAsCStringLen v $ \(vp, vl) ->
                      put_ ptxn dbi kp (fromIntegral kl) vp (fromIntegral vl) op
                return $ F.Partial s
            )

        {-# INLINE commonFold #-}
        commonFold (F.Fold fstep finit fextr ffinal) op =
          F.Fold @m
            (\s (k, v) -> putCatchKeyExists k v s op (\() -> fstep s (k, v)))
            (validate >> finit)
            fextr
            ffinal
     in case writeOverwriteOptions wopts of
          OverwriteAllow ->
            F.foldlM' @m @()
              ( \() (k, v) -> liftIO $
                  B.unsafeUseAsCStringLen k $ \(kp, kl) -> B.unsafeUseAsCStringLen v $ \(vp, vl) ->
                    put_ ptxn dbi kp (fromIntegral kl) vp (fromIntegral vl) 0
              )
              validate
          OverwriteDisallow (Left f) ->
            commonFold f mdb_nooverwrite
          OverwriteDisallow (Right (F.Fold fstep finit fextr ffinal)) ->
            F.Fold @m
              ( \s (k, v) ->
                  putCatchKeyExists k v s mdb_nooverwrite $ \_ -> do
                    mVold <- liftIO $ getLMDB (Database env dbi) (JustTxn txn) k
                    vold <- case mVold of
                      Nothing -> throwErr "getLMDB; old value not found; this should never happen"
                      Just vold -> return vold
                    fstep s (k, v, vold)
              )
              (validate >> finit)
              fextr
              ffinal
          OverwriteAppend f ->
            commonFold f mdb_append

-- | Waits for active read-only transactions on the given environment to finish. Note: This triggers
-- garbage collection.
waitReaders :: (Mode emode) => Environment emode -> IO ()
waitReaders (Environment _ mvars) = do
  let (numReadersT, _, _, _) = mvars
  performGC -- Complete active readers as soon as possible.
  numReaders <-
    atomically $ do
      numReaders <- takeTMVar numReadersT
      check $ numReaders <= 0 -- Sanity check: use <=0 to catch unexpected negative readers.
      return numReaders
  when (numReaders /= 0) $ throwError "waitReaders" "zero numReaders expected"

-- | Clears, i.e., removes all key-value pairs from, the given database.
--
-- /Warning/: Under the hood, this function spawns a bound thread and creates a potentially
-- long-lived read-write transaction; see [Transactions](#g:transactions).
clearDatabase :: Database ReadWrite -> IO ()
clearDatabase (Database (Environment penv mvars) dbi) = mask $ \restore -> do
  let (_, WriteLock lock, _, _) = mvars
  putMVar lock () -- Interruptible when waiting for other read-write transactions.
  let disclaimWriteOwnership = takeMVar lock

  finally
    ( runInBoundThread $ do
        ptxn <- mdb_txn_begin penv nullPtr 0
        onException
          -- Unmask a potentially long-running operation.
          ( restore $ do
              mdb_clear ptxn dbi
              mdb_txn_commit ptxn
          )
          (c_mdb_txn_abort ptxn) -- TODO: Could abort be long-running?
    )
    disclaimWriteOwnership

-- | A convenience constant for obtaining 1 KiB.
kibibyte :: (Num a) => a
kibibyte = 1_024

-- | A convenience constant for obtaining 1 MiB.
mebibyte :: (Num a) => a
mebibyte = 1_024 * 1_024

-- | A convenience constant for obtaining 1 GiB.
gibibyte :: (Num a) => a
gibibyte = 1_024 * 1_024 * 1_024

-- | A convenience constant for obtaining 1 TiB.
tebibyte :: (Num a) => a
tebibyte = 1_024 * 1_024 * 1_024 * 1_024

-- | A type class for 'ReadOnly' and 'ReadWrite' environments and transactions.
class Mode a where
  isReadOnlyMode :: a -> Bool

data ReadWrite

data ReadOnly

instance Mode ReadWrite where isReadOnlyMode _ = False

instance Mode ReadOnly where isReadOnlyMode _ = True

-- | Enforces at the type level that @ReadWrite@ environments support both @ReadWrite@ and
-- @ReadOnly@ transactions, but @ReadOnly@ environments support only @ReadOnly@ transactions.
type SubMode :: k -> k -> Constraint
type family SubMode emode tmode where
  SubMode ReadWrite _ = ()
  SubMode ReadOnly ReadOnly = ()
  SubMode ReadOnly ReadWrite =
    TypeError ('Text "ReadOnly environments only support ReadOnly transactions")

data Environment emode
  = Environment
      !(Ptr MDB_env)
      !(TMVar NumReaders, WriteLock, WriteThread, CloseDbLock)

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

-- | Keeps track of the 'ThreadId' of the current read-write transaction.
newtype WriteThread = WriteThread (MVar ThreadId)

-- For read-write transaction serialization.
newtype WriteLock = WriteLock (MVar ())

-- For closeDatabase serialization.
newtype CloseDbLock = CloseDbLock (MVar ())

data Database emode = Database !(Environment emode) !MDB_dbi_t

-- | Utility function for getting a named database with a read-only transaction, returning 'Nothing'
-- if it was not found.
--
-- /Internal/.
getNamedDb ::
  forall emode.
  (Mode emode) =>
  Environment emode ->
  String ->
  IO (Maybe MDB_dbi_t)
getNamedDb (Environment penv _) name = mask_ $ do
  -- Use a read-only transaction to try to get the named database.
  ptxn <- mdb_txn_begin penv nullPtr mdb_rdonly
  onException
    ( catchJust
        ( \case
            -- Assumption: mdb_txn_commit never returns MDB_NOTFOUND.
            LMDB_Error {e_code} | e_code == Right MDB_NOTFOUND -> Just ()
            _ -> Nothing
        )
        (Just <$> mdb_dbi_open ptxn (Just name) 0 <* mdb_txn_commit ptxn)
        ( \() -> do
            -- The named database was not found.
            c_mdb_txn_abort ptxn
            return Nothing
        )
    )
    (c_mdb_txn_abort ptxn)

-- | A utility function for detecting a few user errors.
--
-- /Internal/.
detectUserErrors :: Bool -> MVar ThreadId -> (String -> IO ()) -> IO ()
detectUserErrors shouldTake writeThread throwErr = do
  let info = "LMDB transactions might now be in a mangled state"
      inappr ctx = printf "inappropriately called (%s); %s" ctx info
      unexpThread = "called on unexpected thread; " ++ info
      caseShouldTake = "before aborting/committing read-write transaction"
      caseNotShouldTake = "before starting writeLMDB"
  threadId <- myThreadId
  if shouldTake
    then
      -- Before aborting/committing read-write transactions.
      tryTakeMVar writeThread >>= \case
        Nothing -> throwErr $ inappr caseShouldTake
        Just tid
          | tid /= threadId -> throwErr unexpThread
          | otherwise -> return ()
    else do
      -- Before starting a writeLMDB.
      isEmptyMVar writeThread >>= flip when (throwErr $ inappr caseNotShouldTake)
      void $ withMVarMasked writeThread $ \tid ->
        if tid /= threadId
          then throwErr unexpThread
          else void $ return tid

-- |
-- @liftedBracket2 acquire failure success thing@
--
-- Same as @Control.Exception.Lifted.bracket@ (from @lifted-base@) except it distinguishes between
-- failure and success.
--
-- Notes:
--
-- * When @acquire@, @success@, or @failure@ throw exceptions, any monadic side effects in @m@ will
--   be discarded.
-- * When @thing@ throws an exception, any monadic side effects in @m@ produced by @thing@ will be
--   discarded, but the side effects of @acquire@ and (non-excepting) @failure@ will be retained.
-- * When (following a @thing@ success) @success@ throws an exception, any monadic side effects in
--   @m@ produced by @success@ will be discarded, but the side effects of @acquire@, @thing@, and
--   (non-excepting) @failure@ will be retained.
--
-- /Internal/.
{-# INLINE liftedBracket2 #-}
liftedBracket2 ::
  (MonadBaseControl IO m) =>
  m a ->
  (a -> m b) ->
  (a -> m b) ->
  (a -> m c) ->
  m c
liftedBracket2 acquire failure success thing = control $ \runInIO ->
  bracket2
    (runInIO acquire)
    (\st -> runInIO $ restoreM st >>= failure)
    (\st -> runInIO $ restoreM st >>= success)
    (\st -> runInIO $ restoreM st >>= thing)

-- | Same as @Control.Exception.bracket@ except it distinguishes between failure and success. (If
-- the success action throws an exception, the failure action gets called.)
--
-- /Internal/.
{-# INLINE bracket2 #-}
bracket2 ::
  IO a ->
  (a -> IO b) ->
  (a -> IO b) ->
  (a -> IO c) ->
  IO c
bracket2 acquire failure success thing = mask $ \restore -> do
  a <- acquire
  r <- restore (thing a) `E.onException` failure a
  _ <- success a `E.onException` failure a
  return r
