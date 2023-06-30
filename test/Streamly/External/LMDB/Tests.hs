{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE InstanceSigs #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE TypeApplications #-}

module Streamly.External.LMDB.Tests where

import Control.Concurrent
import Control.Concurrent.Async
import Control.Exception hiding (assert)
import Control.Monad
import Data.ByteString (ByteString)
import qualified Data.ByteString as B
import Data.ByteString.Unsafe
import Data.Either
import Data.Function
import Data.List
import qualified Data.Map.Strict as M
import Data.Maybe
import Data.Serialize.Put
import qualified Data.Set as Set
import qualified Data.Vector as V
import Data.Word
import Foreign
import qualified Streamly.Data.Fold as F
import Streamly.Data.Stream.Prelude (fromList, toList, unfold)
import qualified Streamly.Data.Stream.Prelude as S
import Streamly.External.LMDB
import Streamly.External.LMDB.Channel
import Streamly.External.LMDB.Internal
import Streamly.External.LMDB.Internal.Foreign
import qualified Streamly.Internal.Data.Fold as F
import qualified Streamly.Internal.Data.Fold.Concurrent as F
import System.Directory
import System.IO.Temp
import Test.QuickCheck hiding (mapSize)
import Test.QuickCheck.Monadic
import Test.Tasty (TestTree)
import Test.Tasty.QuickCheck hiding (mapSize)
import Text.Printf

tests :: IO Channel -> [TestTree]
tests res =
  [ testReadLMDB res,
    testUnsafeReadLMDB res
  ]
    ++ ( do
           overwriteOpts <-
             [ OverwriteAllow,
               OverwriteAllowSameValue,
               OverwriteDisallow $ WriteAppend False,
               OverwriteDisallow $ WriteAppend True
               ]
           failureFold <- [FailureThrow, FailureStop, FailureIgnore]
           return $ testWriteLMDB overwriteOpts failureFold res
       )
    ++ ( do
           overwriteOpts <-
             [ OverwriteAllow,
               OverwriteAllowSameValue,
               OverwriteDisallow $ WriteAppend False,
               OverwriteDisallow $ WriteAppend True
               ]
           return $ testWriteLMDBToList overwriteOpts res
       )
    ++ [ testWriteLMDBConcurrent res,
         testWriteLMDBDemux res,
         testWriteLMDBDemuxSameDb res,
         testAsyncExceptionsConcurrent res,
         testClearDatabaseConcurrent res,
         testBetween
       ]

-- | Sometimes we don’t want to close the database afterwards (because we happen to know that doing
-- so might not be safe).
newtype ShouldCloseDb = ShouldCloseDb Bool

withEnvDb ::
  ShouldCloseDb ->
  IO Channel ->
  (Environment ReadWrite -> Database ReadWrite -> PropertyM IO a) ->
  PropertyM IO a
withEnvDb shouldClose res f =
  withEnvDbN
    1
    shouldClose
    res
    ( \v -> case V.toList v of
        [(env, db)] -> f env db
        _ -> error "withEnvDb: unreachable"
    )

withEnvDbN ::
  Int ->
  ShouldCloseDb ->
  IO Channel ->
  (V.Vector (Environment ReadWrite, Database ReadWrite) -> PropertyM IO a) ->
  PropertyM IO a
withEnvDbN n (ShouldCloseDb shouldClose) res f = do
  chan <- run res
  -- TODO: bracket for PropertyM?
  dirEnvsDbs <-
    V.fromList
      <$> run
        ( replicateM n $ do
            tmpParent <- getCanonicalTemporaryDirectory
            tmpDir <- createTempDirectory tmpParent "streamly-lmdb-tests"
            env <- openEnvironment tmpDir $ defaultLimits {mapSize = tebibyte}
            db <- getDatabase chan env Nothing
            return (tmpDir, env, db)
        )
  a <- f $ V.map (\(_, x, y) -> (x, y)) dirEnvsDbs
  run $ forM_ dirEnvsDbs $ \(dir, env, db) -> do
    waitReaders env

    when shouldClose $ do
      closeDatabase chan db

    -- If we don’t close the database, we still need this to avoid certain “too many open files”
    -- errors. (Since we will not use any transactions/databases/cursors after this call, this
    -- should be safe; see
    -- https://github.com/LMDB/lmdb/blob/8d0cbbc936091eb85972501a9b31a8f86d4c51a7/libraries/liblmdb/lmdb.h#L788)
    closeEnvironment chan env

    removeDirectoryRecursive dir
  return a

withReadOnlyTxnAndCurs ::
  (Mode mode) =>
  Environment mode ->
  Database mode ->
  ((ReadOnlyTxn mode, Cursor) -> IO r) ->
  IO r
withReadOnlyTxnAndCurs env db =
  bracket
    (beginReadOnlyTxn env >>= \txn -> openCursor txn db >>= \curs -> return (txn, curs))
    (\(txn, curs) -> closeCursor curs >> abortReadOnlyTxn txn)

-- | Write key-value pairs to a database in a normal manner, read them back using our library, and
-- make sure the result is what we wrote.
testReadLMDB :: IO Channel -> TestTree
testReadLMDB res =
  testProperty "readLMDB" . monadicIO . withEnvDb (ShouldCloseDb True) res $ \env db -> do
    keyValuePairs <- toByteStrings <$> arbitraryKeyValuePairs'' 500

    run $ writeChunk db False keyValuePairs
    let keyValuePairsInDb = sort . removeDuplicateKeysRetainLast $ keyValuePairs

    (readOpts, expectedResults) <- pick $ readOptionsAndResults keyValuePairsInDb
    let unf txn = toList $ unfold (readLMDB db txn readOpts) undefined
    results <- run $ unf Nothing
    resultsTxn <- run $ withReadOnlyTxnAndCurs env db (unf . Just)

    return $ results == expectedResults && resultsTxn == expectedResults

-- | Similar to 'testReadLMDB', except that it tests the unsafe function in a different manner.
testUnsafeReadLMDB :: IO Channel -> TestTree
testUnsafeReadLMDB res =
  testProperty "unsafeReadLMDB" . monadicIO . withEnvDb (ShouldCloseDb True) res $ \env db -> do
    keyValuePairs <- toByteStrings <$> arbitraryKeyValuePairs'' 500

    run $ writeChunk db False keyValuePairs
    let keyValuePairsInDb = sort . removeDuplicateKeysRetainLast $ keyValuePairs

    (readOpts, expectedResults) <- pick $ readOptionsAndResults keyValuePairsInDb
    let expectedLengths = map (\(k, v) -> (B.length k, B.length v)) expectedResults
    let unf txn =
          toList $
            unfold (unsafeReadLMDB db txn readOpts (return . snd) (return . snd)) undefined
    lengths <- run $ unf Nothing
    lengthsTxn <- run $ withReadOnlyTxnAndCurs env db (unf . Just)

    return $ lengths == expectedLengths && lengthsTxn == expectedLengths

data FailureFold = FailureThrow | FailureStop | FailureIgnore deriving (Show)

-- | Write key-value pairs to a database using our library with various options, read all key-value
-- pairs back from the database using our library (already covered by 'testReadLMDB'), and make sure
-- they are as expected.
testWriteLMDB :: OverwriteOptions -> FailureFold -> IO Channel -> TestTree
testWriteLMDB overwriteOpts failureFold res =
  testProperty (printf "writeLMDB (%s, %s)" (show overwriteOpts) (show failureFold))
    . monadicIO
    . withEnvDb (ShouldCloseDb True) res
    $ \env db -> do
      -- These options should have no effect on the end-result. Note: Low chunk sizes (e.g., 1)
      -- normally result in bad performance. We therefore base the number of pairs on the chunk
      -- size.
      chunkSz <- pick $ chooseInt (1, 100)
      let maxPairs = chunkSz * 5
      unsafeFFI <- pick arbitrary

      let failureFold' = case failureFold of
            FailureThrow -> writeFailureThrow
            FailureStop -> writeFailureStop
            FailureIgnore -> writeFailureIgnore

      let fol' =
            writeLMDB db $
              defaultWriteOptions
                { writeTransactionSize = chunkSz,
                  writeOverwriteOptions = overwriteOpts,
                  writeUnsafeFFI = unsafeFFI,
                  writeFailureFold = failureFold'
                }

      -- Write key-value pairs to the database.
      keyValuePairs <- toByteStrings <$> arbitraryKeyValuePairsDupsMoreLikely maxPairs
      let hasDuplicates = hasDuplicateKeys keyValuePairs
          hasDuplicatesWithDiffVals = hasDuplicateKeysWithDiffVals keyValuePairs
          isSorted = sort keyValuePairs == keyValuePairs
          isStrictlySorted = isSorted && not hasDuplicates
      e <- run . try @SomeException $ S.fold fol' $ fromList keyValuePairs

      -- If we comment out the cleanup code in writeLMDB at synchronous exceptions or normal
      -- completion (which we shouldn’t normally do), we need this for the tests to pass.
      -- run $ waitWriters env
      let removeUnusedWarning = env

      -- Make sure exceptions occurred as expected from the written key-value pairs.
      let assert1 s = assertMsg $ "assert (first checks) failure " ++ const s removeUnusedWarning
      case failureFold of
        FailureStop -> assert1 "(1)" (isRight e)
        FailureIgnore -> assert1 "(2)" (isRight e)
        FailureThrow -> case overwriteOpts of
          OverwriteAllow -> assert1 "(3)" (isRight e)
          OverwriteAllowSameValue -> case e of
            Left _ -> assert1 "(4)" hasDuplicatesWithDiffVals
            Right _ -> assert1 "(5)" $ not hasDuplicatesWithDiffVals
          OverwriteDisallow (WriteAppend False) -> case e of
            Left _ -> assert1 "(6)" hasDuplicates
            Right _ -> assert1 "(7)" $ not hasDuplicates
          OverwriteDisallow (WriteAppend True) -> case e of
            Left _ -> assert1 "(8)" $ not isStrictlySorted
            Right _ -> assert1 "(9)" isStrictlySorted

      -- Regardless of whether an exception occurred, we now read all key-value pairs back from the
      -- database and make sure they are as expected.
      readPairsAll <- run . toList $ unfold (readLMDB db Nothing defaultReadOptions) undefined
      let assert2 s = assertMsg $ "assert (second checks) failure " ++ s
      case failureFold of
        FailureStop ->
          case overwriteOpts of
            OverwriteAllow ->
              assert2 "(1)" $ readPairsAll == sort (removeDuplicateKeysRetainLast keyValuePairs)
            OverwriteAllowSameValue ->
              assert2 "(2)" $
                readPairsAll
                  == sort
                    ( removeDuplicateKeysRetainLast $
                        prefixBeforeDuplicateWithDiffVal keyValuePairs
                    )
            OverwriteDisallow (WriteAppend False) ->
              assert2 "(3)" $ readPairsAll == sort (prefixBeforeDuplicate keyValuePairs)
            OverwriteDisallow (WriteAppend True) ->
              assert2 "(4)" $ readPairsAll == sort (prefixBeforeStrictlySortedKeysEnd keyValuePairs)
        FailureIgnore ->
          case overwriteOpts of
            OverwriteAllow ->
              assert2 "(5)" $ readPairsAll == sort (removeDuplicateKeysRetainLast keyValuePairs)
            OverwriteAllowSameValue ->
              assert2 "(6)" $ readPairsAll == sort (filterOutReoccurringKeys keyValuePairs)
            OverwriteDisallow (WriteAppend False) ->
              assert2 "(7)" $ readPairsAll == sort (filterOutReoccurringKeys keyValuePairs)
            OverwriteDisallow (WriteAppend True) ->
              assert2 "(8)" $ readPairsAll == fst (filterGreaterThan keyValuePairs)
        FailureThrow ->
          -- Same as FailureStop.
          case overwriteOpts of
            OverwriteAllow ->
              assert2 "(9)" $ readPairsAll == sort (removeDuplicateKeysRetainLast keyValuePairs)
            OverwriteAllowSameValue ->
              assert2 "(10)" $
                readPairsAll
                  == sort
                    ( removeDuplicateKeysRetainLast $
                        prefixBeforeDuplicateWithDiffVal keyValuePairs
                    )
            OverwriteDisallow (WriteAppend False) ->
              assert2 "(11)" $ readPairsAll == sort (prefixBeforeDuplicate keyValuePairs)
            OverwriteDisallow (WriteAppend True) ->
              assert2 "(12)" $
                readPairsAll == sort (prefixBeforeStrictlySortedKeysEnd keyValuePairs)

-- | Write key-value pairs to a database using our library with various options while collecting
-- failures into a list, read all key-value pairs back from the database using our library (already
-- covered by 'testReadLMDB'), and make sure they and the list of failures are as expected.
testWriteLMDBToList :: OverwriteOptions -> IO Channel -> TestTree
testWriteLMDBToList overwriteOpts res =
  testProperty (printf "writeLMDBToList (%s)" (show overwriteOpts))
    . monadicIO
    . withEnvDb (ShouldCloseDb True) res
    $ \env db -> do
      do
        -- These options should have no effect on the end-result. Note: Low chunk sizes (e.g., 1)
        -- normally result in bad performance. We therefore base the number of pairs on the chunk
        -- size.
        chunkSz <- pick $ chooseInt (1, 100)
        let maxPairs = chunkSz * 5
        unsafeFFI <- pick arbitrary

        let fol' =
              writeLMDB db $
                defaultWriteOptions
                  { writeTransactionSize = chunkSz,
                    writeOverwriteOptions = overwriteOpts,
                    writeUnsafeFFI = unsafeFFI,
                    writeFailureFold = F.toList
                  }

        -- Write key-value pairs to the database.
        keyValuePairs <- toByteStrings <$> arbitraryKeyValuePairsDupsMoreLikely maxPairs
        e <- run . try @SomeException $ S.fold fol' $ fromList keyValuePairs

        -- If we comment out the cleanup code in writeLMDB at synchronous exceptions or normal
        -- completion (which we shouldn’t normally do), we need this for the tests to pass.
        -- run $ waitWriters env
        let removeUnusedWarning = env

        case const e removeUnusedWarning of
          Left _ ->
            -- Make sure no exceptions occurred (since the failures are merely collected to a list).
            assertMsg "unexpected exception" (isRight e)
          Right errors -> do
            --  Read all key-value pairs back from the database and make sure that they, as well as
            --  the collected offending key-value pairs, are as expected.
            readPairsAll <- run . toList $ unfold (readLMDB db Nothing defaultReadOptions) undefined
            let offendingPairs = map (\(_, k, v) -> (k, v)) errors
            let theAssert s = assertMsg $ "assert failure " ++ s
            case overwriteOpts of
              -- The readPairsAll parts are the same as in the FailureIgnore case in testWriteLMDB.
              OverwriteAllow ->
                theAssert "(1)" $
                  null offendingPairs
                    && readPairsAll == sort (removeDuplicateKeysRetainLast keyValuePairs)
              OverwriteAllowSameValue ->
                theAssert "(2)" $
                  offendingPairs == getRepeatingKeys True keyValuePairs
                    && readPairsAll == sort (filterOutReoccurringKeys keyValuePairs)
              OverwriteDisallow (WriteAppend False) ->
                theAssert "(3)" $
                  offendingPairs == getRepeatingKeys False keyValuePairs
                    && readPairsAll == sort (filterOutReoccurringKeys keyValuePairs)
              OverwriteDisallow (WriteAppend True) ->
                theAssert "(4)" $
                  offendingPairs == snd (filterGreaterThan keyValuePairs)
                    && readPairsAll == fst (filterGreaterThan keyValuePairs)

-- | Write key-value pairs to a database concurrently, read all key-value pairs back from the
-- database using our library (already covered by 'testReadLMDB'), and make sure they are as
-- expected.
testWriteLMDBConcurrent :: IO Channel -> TestTree
testWriteLMDBConcurrent res =
  testProperty "writeLMDBConcurrent" . monadicIO . withEnvDb (ShouldCloseDb True) res $ \env db -> do
    -- The idea: Pick a chunk size. Sometimes the number of key-value pairs will be greater than the
    -- chunk size on some threads but less than the chunk size on other threads. Our test being
    -- successful (esp. combined with the (*) below) means that writeLMDB’s concurrency mechanism
    -- has to be working.
    numThreads <- pick $ chooseInt (1, 10)
    chunkSz <- pick $ chooseInt (1, 5)

    -- (*) More predictability for manual sanity checks; see also the other (*) below. TODO: Bring
    -- more of these manual sanity checks into the test itself.
    -- let numThreads = 3
    -- let chunkSz = 5

    pairss <- generateConcurrentPairs numThreads 0 (4 * chunkSz)
    run $
      forConcurrently_ pairss $
        \(ThreadIdx threadIdx, pairs, failureFold, unsafeFFI) -> do
          let fol =
                writeLMDB @IO db $
                  defaultWriteOptions
                    { writeTransactionSize = chunkSz,
                      writeOverwriteOptions = OverwriteAllow,
                      writeUnsafeFFI = unsafeFFI,
                      writeFailureFold = failureFold
                    }

          S.fromList @IO pairs
            & S.indexed
            & S.mapM
              ( \(pairIdx, pair) -> do
                  let removeUnusedWarning = pairIdx + threadIdx
                  -- (*) Check whether our writeLMDB folds get interleaved (as opposed to the folds
                  -- executing one after another).
                  -- putStrLn $ printf "(threadIdx, pairIdx) = (%d, %d)" threadIdx pairIdx
                  return $ const pair removeUnusedWarning
              )
            & S.fold fol

          -- If we comment out the commit/disclaimOwnership code in writeLMDB at synchronous
          -- exceptions or normal completion (which we shouldn’t normally do), we need this for the
          -- tests to pass.
          -- waitWriters env
          let removeUnusedWarning = env
          return $ const () removeUnusedWarning

    -- (Instead of putting waitWriters above, we can also do it here; but then the test takes much
    -- longer to execute, presumably because the last transaction of each of the above threads
    -- cannot begin until GC and finalization has committed a last transaction of another thread.)
    -- run $ waitWriters env

    let expectedPairs = concatMap (\(_, pairs, _, _) -> pairs) pairss
    readPairsAll <- run . toList $ unfold (readLMDB db Nothing defaultReadOptions) undefined
    assertMsg "assert failure" $ expectedPairs == readPairsAll

-- | Write key-value pairs using multiple demuxed writeLMDBs with separate environments, read all
-- key-value pairs back using our library (already covered by 'testReadLMDB'), and make sure they
-- are as expected.
testWriteLMDBDemux :: IO Channel -> TestTree
testWriteLMDBDemux res =
  testProperty "writeLMDBDemux" . monadicIO $ do
    numDemux <- pick $ chooseInt (1, 5)
    withEnvDbN numDemux (ShouldCloseDb True) res $ \envDbs -> do
      chunkSzs <- V.generateM numDemux $ \_ -> pick $ chooseInt (1, 5)

      -- We use unique keys because we (at least for now) don’t want to worry about the order in
      -- which the pairs get demuxed.
      pairs <-
        fromMaybe (error "pairs expected")
          <$> (generatePairsWithUniqueKeys 0 (4 * sum chunkSzs) & S.fold F.one)

      folds <- V.iforM envDbs $ \idx (_, db) -> do
        let chunkSz = (V.!) chunkSzs idx

        -- These settings should make no difference to the result.
        x <- pick $ elements [0 :: Int, 1, 2]
        let failureFold
              | x == 0 = writeFailureThrow
              | x == 1 = writeFailureStop
              | x == 2 = writeFailureIgnore
              | otherwise = error "unreachable"
        unsafeFFI :: Bool <- pick arbitrary
        runPar :: Bool <- pick arbitrary
        return
          . (if runPar then F.parEval id else id)
          . writeLMDB @IO db
          $ defaultWriteOptions
            { writeTransactionSize = chunkSz,
              writeOverwriteOptions = OverwriteAllow,
              writeUnsafeFFI = unsafeFFI,
              writeFailureFold = failureFold
            }

      (demuxIO, _) <-
        run $
          S.fromList pairs
            & S.indexed
            & S.fold
              ( F.demuxIO
                  (\(idx, _) -> idx `rem` numDemux)
                  (\(idx, _) -> return $ F.lmap snd ((V.!) folds (idx `rem` numDemux)))
              )
      _ <- run demuxIO

      let expectedPairs = pairs
      readPairs <-
        sort . concat . V.toList
          <$> forM
            envDbs
            (\(_, db) -> run . toList $ unfold (readLMDB db Nothing defaultReadOptions) undefined)

      assertMsg "assert failure" $ expectedPairs == readPairs

-- | Write key-value pairs using multiple demuxed writeLMDBs into the same database with a write
-- transaction size of 1, read all key-value pairs back using our library (already covered by
-- 'testReadLMDB'), and make sure they are as expected.
testWriteLMDBDemuxSameDb :: IO Channel -> TestTree
testWriteLMDBDemuxSameDb res =
  testProperty "writeLMDBDemuxSameDb"
    . monadicIO
    . withEnvDb
      -- We could not safely close the database when a deadlock leaves an active write transaction.
      -- (We leave this here in case we want to revisit our attempts to cover deadlocks.)
      (ShouldCloseDb False)
      res
    $ \_ db -> do
      numDemux <- pick $ chooseInt (2, 5) -- Use at least 2 writeLMDBs for the demux.

      -- Set chunkSz >=2 and minPairs >=2 to observe a deadlock. (Using timeout to cover both cases
      -- was not reliable enough: If the timeout was high enough to reliably allow chunkSz=1 success
      -- (around 0.1 seconds), it was already taking too long to complete the deadlock cases.
      -- Additionally our CI machines might be less powerful.)
      let chunkSz = 1
          minPairs = 0
          maxPairs = max minPairs (4 * numDemux * chunkSz)

      -- We use unique keys because we (at least for now) don’t want to worry about the order in
      -- which the pairs get demuxed.
      pairs <-
        fromMaybe (error "pairs expected")
          <$> (generatePairsWithUniqueKeys minPairs maxPairs & S.fold F.one)

      folds <- V.replicateM numDemux $ do
        -- These settings should make no difference to the result.
        x <- pick $ elements [0 :: Int, 1, 2]
        let failureFold
              | x == 0 = writeFailureThrow
              | x == 1 = writeFailureStop
              | x == 2 = writeFailureIgnore
              | otherwise = error "unreachable"
        unsafeFFI :: Bool <- pick arbitrary
        runPar :: Bool <- pick arbitrary
        return
          . (if runPar then F.parEval id else id)
          . writeLMDB @IO db
          $ defaultWriteOptions
            { writeTransactionSize = chunkSz,
              writeOverwriteOptions = OverwriteAllow,
              writeUnsafeFFI = unsafeFFI,
              writeFailureFold = failureFold
            }

      (demuxIO, _) <-
        run $
          -- In the deadlock case, we get stuck here in an infinite loop. (The deadlock was not
          -- getting thrown as BlockedIndefinitelyOnMVar or BlockedIndefinitelyOnSTM.)
          S.fromList pairs
            & S.indexed
            & S.fold
              ( F.demuxIO
                  (\(idx, _) -> idx `rem` numDemux)
                  (\(idx, _) -> return $ F.lmap snd ((V.!) folds (idx `rem` numDemux)))
              )
      _ <- run demuxIO

      let expectedPairs = pairs
      readPairs <- toList $ unfold (readLMDB db Nothing defaultReadOptions) undefined
      assertMsg "assert failure" $ expectedPairs == readPairs

-- | Perform reads and writes on a database concurrently, throwTo threads at random, and read all
-- key-value pairs back from the database using our library (already covered by 'testReadLMDB') and
-- make sure they are as expected.
testAsyncExceptionsConcurrent :: IO Channel -> TestTree
testAsyncExceptionsConcurrent res =
  testProperty "asyncExceptionsConcurrent" . monadicIO . withEnvDb (ShouldCloseDb True) res $
    \env db -> do
      numThreads <- pick $ chooseInt (1, 10)
      chunkSz <- pick $ chooseInt (1, 5)

      pairss <- generateConcurrentPairs numThreads 0 (4 * chunkSz)

      -- Whether we will kill the thread.
      shouldKills :: [Bool] <- replicateM numThreads $ pick arbitrary

      -- Whether we will perform read on the thread instead of writing. (This read/write
      -- interleaving is to ascertain that the locking across readers and writers is working as
      -- expected.)
      shouldReads :: [Bool] <- replicateM numThreads $ pick arbitrary

      threads <- run $
        forConcurrently (zip shouldReads pairss) $
          \(shouldRead, (threadIdx, pairs, failureFold, unsafeFFI)) ->
            (threadIdx,)
              <$> asyncBound
                ( if shouldRead
                    then do
                      _ <- toList $ unfold (readLMDB db Nothing defaultReadOptions) undefined
                      return ()
                    else do
                      onException
                        ( S.fromList @IO pairs
                            & S.fold
                              ( writeLMDB @IO db $
                                  defaultWriteOptions
                                    { writeTransactionSize = chunkSz,
                                      writeOverwriteOptions = OverwriteAllow,
                                      writeUnsafeFFI = unsafeFFI,
                                      writeFailureFold = failureFold
                                    }
                              )
                        )
                        ( -- Handle asynchronous exception. Without this, improper closing of the
                          -- environment and/or database can crash the test (as expected).
                          waitWriters env
                        )
                )

      delay <- pick $ chooseInt (0, 10)
      run $ threadDelay delay

      run $ forConcurrently_ (zip shouldKills threads) $ \(shouldKill, (_, as)) ->
        if shouldKill
          then cancel as
          else wait as

      let expectedSubset =
            Set.fromList
              . concatMap (\(_, _, (_, pairs, _, _)) -> pairs)
              . filter (\(shouldRead, shouldKill, _) -> not shouldRead && not shouldKill)
              $ zip3 shouldReads shouldKills pairss

      readPairsAll <-
        Set.fromList <$> (run . toList $ unfold (readLMDB db Nothing defaultReadOptions) undefined)

      assertMsg "assert failure" $ expectedSubset `Set.isSubsetOf` readPairsAll

data TestAction = ActionRead | ActionWrite | ActionClear deriving (Show)

instance Arbitrary TestAction where
  arbitrary = oneof [return ActionRead, return ActionWrite, return ActionClear]

-- | Perform reads, writes, and clearing on a database concurrently, read all key-value pairs back
-- from the database using our library (already covered by 'testReadLMDB') and make sure they are as
-- expected.
testClearDatabaseConcurrent :: IO Channel -> TestTree
testClearDatabaseConcurrent res =
  testProperty "clearDatabaseConcurrent" . monadicIO . withEnvDb (ShouldCloseDb True) res $
    \_ db -> do
      numThreads <- pick $ chooseInt (1, 10)
      chunkSz <- pick $ chooseInt (1, 5)
      pairss <- generateConcurrentPairs numThreads 0 (4 * chunkSz)

      actions :: [TestAction] <- replicateM numThreads $ pick arbitrary
      lastActionM <- run $ newMVar ActionRead

      -- Fixes rare issues with lastAction inconsistency.
      writeLockM <- run $ newMVar ()

      _ <- run $
        forConcurrently (zip actions pairss) $
          \(action, (_, pairs, failureFold, unsafeFFI)) ->
            case action of
              ActionRead -> do
                _ <- toList $ unfold (readLMDB db Nothing defaultReadOptions) undefined
                return () -- Only the last write action should matter.
              ActionWrite ->
                withMVar writeLockM $ \() -> do
                  S.fromList @IO pairs
                    & S.fold
                      ( writeLMDB @IO db $
                          defaultWriteOptions
                            { writeTransactionSize = chunkSz,
                              writeOverwriteOptions = OverwriteAllow,
                              writeUnsafeFFI = unsafeFFI,
                              writeFailureFold = failureFold
                            }
                      )
                  modifyMVar_ lastActionM (\_ -> return ActionWrite)
              ActionClear ->
                withMVar writeLockM $ \() -> do
                  clearDatabase db
                  modifyMVar_ lastActionM (\_ -> return ActionClear)

      readPairsAll <-
        Set.fromList <$> (run . toList $ unfold (readLMDB db Nothing defaultReadOptions) undefined)

      lastAction' <- run $ readMVar lastActionM
      assertMsg "assert failure" $
        case lastAction' of
          ActionClear -> null readPairsAll
          _ -> True

-- | A bytestring with a limited random length. (We don’t see much value in testing with longer
-- bytestrings.)
newtype ShortBS = ShortBS ByteString deriving (Show)

instance Arbitrary ShortBS where
  arbitrary :: Gen ShortBS
  arbitrary = do
    len <- chooseInt (0, 50)
    ShortBS . B.pack <$> vector len

toByteStrings :: [(ShortBS, ShortBS)] -> [(ByteString, ByteString)]
toByteStrings = map (\(ShortBS k, ShortBS v) -> (k, v))

arbitraryKeyValuePairs :: Int -> PropertyM IO [(ShortBS, ShortBS)]
arbitraryKeyValuePairs maxLen = do
  len <- pick $ chooseInt (0, maxLen)
  filter (\(ShortBS k, _) -> not $ B.null k) -- LMDB does not allow empty keys.
    <$> pick (vector len)

-- | A variation that makes duplicate keys more likely. (The idea is to generate more relevant data
-- for testing writeLMDB with the various writeOverwriteOptions and writeFailureFold possibilities.)
arbitraryKeyValuePairsDupsMoreLikely :: Int -> PropertyM IO [(ShortBS, ShortBS)]
arbitraryKeyValuePairsDupsMoreLikely maxLen = do
  arb <- arbitraryKeyValuePairs maxLen
  b <- pick arbitrary
  if not (null arb) && b
    then do
      let (k, v) = head arb
      b' <- pick arbitrary
      v' <- if b' then return v else pick arbitrary
      i <- pick $ chooseInt (negate $ length arb, 2 * length arb)
      let (arb1, arb2) = splitAt i arb
      let arb' = arb1 ++ [(k, v')] ++ arb2
      return arb'
    else return arb

-- | A variation that makes more likely keys with the same prefix and a difference of trailing zero
-- bytes. (The idea is to generate more relevant data for testing readLMDB with the various
-- readDirection and readStart possibilities.)
arbitraryKeyValuePairs'' :: Int -> PropertyM IO [(ShortBS, ShortBS)]
arbitraryKeyValuePairs'' maxLen = do
  arb <- arbitraryKeyValuePairs maxLen
  if null arb
    then return arb
    else
      pick $
        frequency
          [ (1, return arb),
            ( 3,
              do
                let (ShortBS k, v) = head arb
                b' <- arbitrary
                v' <- if b' then return v else arbitrary
                i <- chooseInt (0, length arb - 1)
                let (arb1, arb2) = splitAt i arb
                j <- chooseInt (0, 100) -- Remains within the 512 default maximum LMDB key size.
                let arb3 = map (\j' -> (ShortBS $ k `B.append` B.replicate j' 0, v')) [1 .. j]
                let arb' = arb1 ++ arb3 ++ arb2
                return arb'
            )
          ]

-- | This function retains the last encountered value for each key.
removeDuplicateKeysRetainLast :: (Eq a) => [(a, b)] -> [(a, b)]
removeDuplicateKeysRetainLast =
  foldl' (\acc (a, b) -> if any ((== a) . fst) acc then acc else (a, b) : acc) [] . reverse

hasDuplicateKeys :: (Eq a) => [(a, b)] -> Bool
hasDuplicateKeys l =
  let l2 = nubBy (\(a1, _) (a2, _) -> a1 == a2) l
   in length l /= length l2

hasDuplicateKeysWithDiffVals :: (Eq a, Eq b) => [(a, b)] -> Bool
hasDuplicateKeysWithDiffVals l =
  let l2 = nubBy (\(a1, b1) (a2, b2) -> a1 == a2 && b1 /= b2) l
   in length l /= length l2

prefixBeforeDuplicate :: (Eq a) => [(a, b)] -> [(a, b)]
prefixBeforeDuplicate xs =
  let fstDup = snd <$> find (\((a, _), i) -> a `elem` map fst (take i xs)) (zip xs [0 ..])
   in case fstDup of
        Nothing -> xs
        Just i -> take i xs

prefixBeforeDuplicateWithDiffVal :: (Eq a, Eq b) => [(a, b)] -> [(a, b)]
prefixBeforeDuplicateWithDiffVal xs =
  let fstDup =
        snd
          <$> find
            ( \((a, b), i) ->
                any (\(a', b') -> a == a' && b /= b') (take i xs)
            )
            (zip xs [0 ..])
   in case fstDup of
        Nothing -> xs
        Just i -> take i xs

-- |
-- @> filterOutReoccurringKeys [(1,"a"),(2,"b"),(2,"c"),(10,"d")] = [(1,"a"),(2,"b"),(10,"d")]@
filterOutReoccurringKeys :: (Eq a) => [(a, b)] -> [(a, b)]
filterOutReoccurringKeys = nubBy (\(a1, _) (a2, _) -> a1 == a2)

-- |
-- @> prefixBeforeStrictlySortedKeysEnd [(1,"a"),(2,"b"),(3,"c"),(2, "d")] = [(1,"a"),(2,"b"),(3,"c")]@
-- @> prefixBeforeStrictlySortedKeysEnd [(1,"a"),(2,"b"),(3,"c"),(3, "d")] = [(1,"a"),(2,"b"),(3,"c")]@
prefixBeforeStrictlySortedKeysEnd :: (Ord a) => [(a, b)] -> [(a, b)]
prefixBeforeStrictlySortedKeysEnd xs =
  map fst
    . takeWhile
      ( \((a, _), mxs) ->
          case mxs of
            Nothing -> True
            Just (aPrev, _) -> a > aPrev
      )
    $ zip xs (Nothing : map Just xs)

-- |
-- @filterGreaterThan [(1,"a"),(2,"b"),(1,"c"),(3, "d")] = ([(1,"a"),(2,"b"),(3,"d")],[(1,"c")])@
-- @filterGreaterThan [(1,"a"),(2,"b"),(2,"c"),(3, "d")] = [(1,"a"),(2,"b"),(3,"d"),[(2,"c")]]@
filterGreaterThan :: (Ord a) => [(a, b)] -> ([(a, b)], [(a, b)])
filterGreaterThan =
  (\(xs, ys, _) -> (reverse xs, reverse ys))
    . foldl'
      ( \(accList, accOffending, mLastIncluded) (a, b) -> case mLastIncluded of
          Nothing -> ((a, b) : accList, accOffending, Just a)
          Just lastIncluded ->
            if a > lastIncluded
              then ((a, b) : accList, accOffending, Just a)
              else (accList, (a, b) : accOffending, Just lastIncluded)
      )
      ([], [], Nothing)

-- | If diffValsOnly is False, collects repeating keys (and corresponding values). Otherwise,
-- collects them only if the value differs from the original value.
--
-- @getRepeatingKeys False [(1,"a"),(2,"b"),(1,"a")] = [(1,"a")]@
-- @getRepeatingKeys True [(1,"a"),(2,"b"),(1,"a")] = []@
-- @getRepeatingKeys False [(1,"a"),(2,"b"),(1,"c")] = [(1,"c")]@
-- @getRepeatingKeys True [(1,"a"),(2,"b"),(1,"c")] = [(1,"c")]@
getRepeatingKeys :: (Ord a, Eq b) => Bool -> [(a, b)] -> [(a, b)]
getRepeatingKeys diffValsOnly =
  reverse
    . fst
    . foldl'
      ( \(accList, keyMap) (a, b) ->
          case M.lookup a keyMap of
            Nothing -> (accList, M.insert a b keyMap)
            Just bPrev ->
              if diffValsOnly
                then
                  if b /= bPrev
                    then ((a, b) : accList, keyMap)
                    else (accList, keyMap)
                else ((a, b) : accList, keyMap)
      )
      ([], M.empty)

-- Assumes first < second.
between :: [Word8] -> [Word8] -> [Word8] -> Maybe [Word8]
between [] [] _ = error "first = second"
between _ [] _ = error "first > second"
between [] (w : ws) commonPrefixRev
  | w == 0 && null ws = Nothing
  | w == 0 = between [] ws (w : commonPrefixRev)
  | otherwise = Just $ reverse (0 : commonPrefixRev)
between (w1 : ws1) (w2 : ws2) commonPrefixRev
  | w1 == w2 = between ws1 ws2 (w1 : commonPrefixRev)
  | w1 > w2 = error "first > second"
  | otherwise = Just $ reverse commonPrefixRev ++ [w1] ++ ws1 ++ [0]

testBetween :: TestTree
testBetween = testProperty "testBetween" $ \ws1 ws2 ->
  (ws1 == ws2)
    || let (smaller, bigger) = if ws1 < ws2 then (ws1, ws2) else (ws2, ws1)
        in case between smaller bigger [] of
             Nothing -> drop (length ws1) ws2 == replicate (length ws2 - length ws1) 0
             Just betw -> smaller < betw && betw < bigger

betweenBs :: ByteString -> ByteString -> Maybe ByteString
betweenBs bs1 bs2 = between (B.unpack bs1) (B.unpack bs2) [] >>= (return . B.pack)

type PairsInDatabase = [(ByteString, ByteString)]

type ExpectedReadResult = [(ByteString, ByteString)]

-- | Given database pairs, randomly generates read options and corresponding expected results.
readOptionsAndResults :: PairsInDatabase -> Gen (ReadOptions, ExpectedReadResult)
readOptionsAndResults pairsInDb = do
  forw <- arbitrary
  let dir = if forw then Forward else Backward
  unsafeFFI <- arbitrary
  let len = length pairsInDb
  readAll <- frequency [(1, return True), (3, return False)]
  let ropts = defaultReadOptions {readDirection = dir, readUnsafeFFI = unsafeFFI}
  if readAll
    then return (ropts {readStart = Nothing}, (if forw then id else reverse) pairsInDb)
    else
      if len == 0
        then do
          bs <- arbitrary >>= \(NonEmpty ws) -> return $ B.pack ws
          return (ropts {readStart = Just bs}, [])
        else do
          idx <-
            if len < 3
              then chooseInt (0, len - 1)
              else frequency [(1, chooseInt (1, len - 2)), (3, elements [0, len - 1])]
          let keyAt i = fst $ pairsInDb !! i
          let nextKey
                | idx + 1 <= len - 1 = betweenBs (keyAt idx) (keyAt $ idx + 1)
                | otherwise = Just $ keyAt (len - 1) `B.append` B.singleton 0
          let prevKey
                -- Keys are known to be non-empty.
                | idx == 0 && keyAt idx /= B.singleton 0 = Just $ B.singleton 0
                | idx == 0 = Nothing
                | otherwise = betweenBs (keyAt $ idx - 1) (keyAt idx)
          let forwEq = (ropts {readStart = Just $ keyAt idx}, drop idx pairsInDb)
          let backwEq = (ropts {readStart = Just $ keyAt idx}, reverse $ take (idx + 1) pairsInDb)
          ord <- arbitrary @Ordering -- Proximity to the key at idx (if possible).
          return $ case (ord, dir) of
            (EQ, Forward) -> forwEq
            (EQ, Backward) -> backwEq
            (GT, Forward) -> case nextKey of
              Nothing -> forwEq
              Just nextKey' -> (ropts {readStart = Just nextKey'}, drop (idx + 1) pairsInDb)
            (GT, Backward) -> case nextKey of
              Nothing -> backwEq
              Just nextKey' ->
                (ropts {readStart = Just nextKey'}, reverse $ take (idx + 1) pairsInDb)
            (LT, Forward) -> case prevKey of
              Nothing -> forwEq
              Just prevKey' -> (ropts {readStart = Just prevKey'}, drop idx pairsInDb)
            (LT, Backward) -> case prevKey of
              Nothing -> backwEq
              Just prevKey' -> (ropts {readStart = Just prevKey'}, reverse $ take idx pairsInDb)

newtype ThreadIdx = ThreadIdx Int deriving (Show)

generatePairsWithUniqueKeys ::
  Monad m => Int -> Int -> S.Stream (PropertyM m) [(ByteString, ByteString)]
generatePairsWithUniqueKeys minPairs maxPairs =
  S.fromList [0 :: Word64 ..] -- Increasing Word64s for uniqueness.
    & S.foldMany
      ( F.Fold
          ( \(totalNumPairs, pairsSoFar, !accPairs) w ->
              if pairsSoFar >= totalNumPairs
                then -- Reverse to make the keys increasing.
                  return . F.Done $ reverse accPairs
                else do
                  let k = runPut $ putWord64be w
                  ShortBS v <- pick arbitrary
                  return $ F.Partial (totalNumPairs, pairsSoFar + 1, (k, v) : accPairs)
          )
          ( do
              totalNumPairs <- pick $ chooseInt (minPairs, maxPairs)
              return $ F.Partial (totalNumPairs, 0, [])
          )
          (\_ -> error "unreachable")
      )

-- | Generates pairs meant to be originating from multiple threads.
--
-- We make the keys unique; otherwise the result would be unpredictable (since we wouldn’t know
-- which values for duplicate keys would end up in the final database).
generateConcurrentPairs ::
  Monad m =>
  Int ->
  Int ->
  Int ->
  PropertyM m [(ThreadIdx, [(ByteString, ByteString)], WriteFailureFold (), Bool)]
generateConcurrentPairs numThreads minPairs maxPairs =
  generatePairsWithUniqueKeys minPairs maxPairs
    & S.take numThreads
    & S.indexed -- threadIdx.
    & S.mapM
      ( \(threadIdx, pairs) -> do
          -- These settings should make no difference to the result.
          x <- pick $ elements [0 :: Int, 1, 2]
          let failureFold
                | x == 0 = writeFailureThrow
                | x == 1 = writeFailureStop
                | x == 2 = writeFailureIgnore
                | otherwise = error "unreachable"
          unsafeFFI :: Bool <- pick arbitrary
          return (ThreadIdx threadIdx, pairs, failureFold, unsafeFFI)
      )
    & S.fold F.toList

-- Writes the given key-value pairs to the given database.
writeChunk ::
  (Foldable t, Mode mode) =>
  Database mode ->
  Bool ->
  t (ByteString, ByteString) ->
  IO ()
writeChunk (Database (Environment penv _) dbi) noOverwrite' keyValuePairs =
  let flags = combineOptions $ [mdb_nooverwrite | noOverwrite']
   in asyncBound
        ( do
            ptxn <- mdb_txn_begin penv nullPtr 0
            onException
              ( forM_ keyValuePairs $ \(k, v) ->
                  marshalOut k $ \k' -> marshalOut v $ \v' -> with k' $ \k'' -> with v' $ \v'' ->
                    mdb_put ptxn dbi k'' v'' flags
              )
              (mdb_txn_commit ptxn) -- Make sure the key-value pairs we have so far are committed.
            mdb_txn_commit ptxn
        )
        >>= wait

{-# INLINE marshalOut #-}
marshalOut :: ByteString -> (MDB_val -> IO ()) -> IO ()
marshalOut bs f =
  unsafeUseAsCStringLen bs $ \(ptr, len) -> f $ MDB_val (fromIntegral len) (castPtr ptr)

assertMsg :: Monad m => String -> Bool -> PropertyM m ()
assertMsg _ True = return ()
assertMsg msg False = fail $ "Assert failed: " ++ msg
