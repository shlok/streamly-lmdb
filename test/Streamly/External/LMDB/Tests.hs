{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE InstanceSigs #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE TypeApplications #-}
{-# OPTIONS_GHC -Wno-unrecognised-pragmas #-}

{-# HLINT ignore "Use <$>" #-}

module Streamly.External.LMDB.Tests where

import Control.Concurrent
import Control.Concurrent.Async
import Control.Exception hiding (assert)
import Control.Monad
import Data.Bifunctor
import Data.ByteString (ByteString)
import qualified Data.ByteString as B
import Data.ByteString.Unsafe
import Data.Either
import Data.Foldable
import Data.Function
import Data.Functor
import Data.List
import qualified Data.Map.Strict as M
import Data.Maybe
import qualified Data.Sequence as Seq
import Data.Serialize.Put
import qualified Data.Set as Set
import qualified Data.Vector as V
import Data.Word
import Foreign
import GHC.Conc
import qualified Streamly.Data.Fold as F
import qualified Streamly.Data.Stream.Prelude as S
import qualified Streamly.Data.Unfold as U
import Streamly.External.LMDB
import Streamly.External.LMDB.Internal
import Streamly.External.LMDB.Internal.Error
import Streamly.External.LMDB.Internal.Foreign
import qualified Streamly.Internal.Data.Fold as F
import System.Directory
import System.IO.Temp
import System.Random
import Test.QuickCheck hiding (mapSize)
import Test.QuickCheck.Monadic
import Test.Tasty (TestTree)
import Test.Tasty.HUnit
import Test.Tasty.QuickCheck hiding (mapSize)
import Text.Printf

tests :: [TestTree]
tests =
  [ testGetDatabase,
    testReadLMDB,
    testUnsafeReadLMDB,
    testWriteLMDBChunked ModeSerial,
    testWriteLMDBChunked (ModeParallel (ClearDatabase False)),
    testWriteLMDBChunked (ModeParallel (ClearDatabase True)),
    testWriteLMDBOneChunk OverwrDisallowThrow,
    testWriteLMDBOneChunk OverwrDisallowThrowAllowSameVal,
    testWriteLMDBOneChunk OverwrDisallowIgnore,
    testWriteLMDBOneChunk OverwrDisallowStop,
    testWriteLMDBOneChunk OverwrAppendThrow,
    testWriteLMDBOneChunk OverwrAppendIgnore,
    testWriteLMDBOneChunk OverwrAppendStop,
    testAsyncExceptionsConcurrent,
    testBetween
  ]

testGetDatabase :: TestTree
testGetDatabase =
  testCase "getDatabase" $ do
    -- Make sure things work concurrently on separate environments.
    let nenvs = 20
    replicateConcurrently_ nenvs $ do
      tmpParent <- getCanonicalTemporaryDirectory
      tmpDir <- createTempDirectory tmpParent "streamly-lmdb-tests"

      -- Make sure things work concurrently in the same environment.
      let ndbs = 20

      let limits = defaultLimits {mapSize = tebibyte, maxDatabases = ndbs}
          openRoEnv = openEnvironment @ReadOnly tmpDir limits
          openRwEnv = openEnvironment @ReadWrite tmpDir limits

      -- Prepare environment for the first time using read-write mode.
      openRwEnv >>= closeEnvironment

      -- Open environment in ReadOnly mode.
      env1 <- openRoEnv

      -- Can get unnamed database in ReadOnly mode.
      replicateConcurrently_ ndbs $ do
        getDatabase @ReadOnly env1 Nothing >>= closeDatabase

      -- Cannot create named databases in ReadOnly mode.
      forConcurrently_ [1 .. ndbs] $ \i -> do
        e <- try (getDatabase @ReadOnly env1 (Just $ "name" ++ show i))
        case e of
          Left (_ :: SomeException) -> return ()
          Right _ -> error "getDatabase: should not succeed"

      -- Close read-only environment.
      closeEnvironment env1

      -- Can create named databases in ReadWrite mode.
      env2 <- openRwEnv
      forConcurrently_ [1 .. ndbs] $ \i ->
        getDatabase @ReadWrite env2 (Just $ "name" ++ show i) >>= closeDatabase
      closeEnvironment env2

      -- Can get existing named databases in ReadOnly mode.
      env3 <- openRoEnv
      forConcurrently_ [1 .. ndbs] $ \i ->
        getDatabase @ReadOnly env3 (Just $ "name" ++ show i) >>= closeDatabase
      closeEnvironment env3

      removeDirectoryRecursive tmpDir

-- | Creates a single environment with one unnamed database ('Nothing') or >=1 ('Just') named
-- databases for which the provided action is run.
withEnvDbs ::
  Maybe Int ->
  ((Environment ReadWrite, V.Vector (Database ReadWrite)) -> PropertyM IO a) ->
  PropertyM IO a
withEnvDbs mNumDbs f = do
  -- TODO: bracket for PropertyM?
  tmpParent <- run getCanonicalTemporaryDirectory
  tmpDir <- run $ createTempDirectory tmpParent "streamly-lmdb-tests"
  env <-
    run $
      openEnvironment tmpDir $
        defaultLimits
          { mapSize = tebibyte,
            maxDatabases = fromMaybe 0 mNumDbs
          }
  dbs <- run $ case mNumDbs of
    Nothing -> V.singleton <$> getDatabase env Nothing
    Just numDbs
      | numDbs >= 1 -> forM (V.fromList [1 .. numDbs]) $ \i ->
          getDatabase env (Just $ "name" ++ show i)
      | otherwise -> error "invalid mNumDbs"

  a <- f (env, dbs)

  run $ do
    waitReaders env
    forM_ dbs $ \db -> closeDatabase db
    closeEnvironment env
    removeDirectoryRecursive tmpDir
  return a

-- | Write key-value pairs to a database in a normal manner, read them back using our library, and
-- make sure the result is what we wrote. Concurrent read transactions are covered.
testReadLMDB :: TestTree
testReadLMDB =
  testProperty "readLMDB" . monadicIO . withEnvDbs Nothing $ \(env, dbs) -> do
    let db = case V.toList dbs of [x] -> x; _ -> error "unreachable"
    keyValuePairs <- toByteStrings <$> arbitraryKeyValuePairs 200
    run $ writeChunk db False keyValuePairs
    let keyValuePairsInDb = sort . removeDuplicateKeysRetainLast $ keyValuePairs

    let nread = 20
    vec <- V.replicateM nread . pick $ readOptionsAndResults keyValuePairsInDb
    bs <- run $ forConcurrently vec $ \(us, readOpts, expectedResults) -> do
      let unf txn = S.toList $ S.unfold readLMDB' (readOpts, us, db, txn)
      results <- unf NoTxn
      resultsTxn <- withReadOnlyTransaction env $ \t -> withCursor t db $ \c -> unf $ JustTxn (t, c)
      return $ results == expectedResults && resultsTxn == expectedResults

    return $ and bs

-- | Similar to 'testReadLMDB', except that it tests the unsafe function in a different manner.
testUnsafeReadLMDB :: TestTree
testUnsafeReadLMDB =
  testProperty "unsafeReadLMDB" . monadicIO . withEnvDbs Nothing $ \(env, dbs) -> do
    let db = case V.toList dbs of [x] -> x; _ -> error "unreachable"
    keyValuePairs <- toByteStrings <$> arbitraryKeyValuePairs 200
    run $ writeChunk db False keyValuePairs
    let keyValuePairsInDb = sort . removeDuplicateKeysRetainLast $ keyValuePairs

    let nread = 20
    vec <- V.replicateM nread . pick $ readOptionsAndResults keyValuePairsInDb
    bs <- run $ forConcurrently vec $ \(us, readOpts, expectedResults) -> do
      let expectedLengths = map (bimap B.length B.length) expectedResults
      let unf txn =
            S.toList $
              S.unfold unsafeReadLMDB' (readOpts, us, db, txn, return . snd, return . snd)
      lengths <- unf NoTxn
      lengthsTxn <- withReadOnlyTransaction env $ \t -> withCursor t db $ \c -> unf $ JustTxn (t, c)
      return $ lengths == expectedLengths && lengthsTxn == expectedLengths

    return $ and bs

newtype ClearDatabase = ClearDatabase Bool

data ChunkedMode = ModeSerial | ModeParallel !ClearDatabase

instance Show ChunkedMode where
  show ModeSerial = "serial"
  show (ModeParallel (ClearDatabase clear)) =
    printf
      "parallel (%s)"
      (if clear then "with clearDatabase" else "without clearDatabase")

-- | Write key-value pairs to a database using our library in a chunked manner with overwriting
-- allowed, read all key-value pairs back from the database using our library (already covered by
-- 'testReadLMDB'), and make sure they are as expected.
--
-- When parallelization is enabled: No duplicate keys are written to the database (as we can’t
-- predict the value order). This test makes sure the read-write transaction serialization mechanism
-- is working properly. While we’re at it, we also sprinkle in read-only transactions alongside the
-- read-write ones using 'readLMDB' and 'getLMDB'; and more read-write transactions from
-- 'clearDatabase'.
--
-- In both cases, we use the opportunity to test writing the same data to a single unnamed database
-- and one or more named databases.
testWriteLMDBChunked :: ChunkedMode -> TestTree
testWriteLMDBChunked mode =
  testProperty (printf "writeLMDBChunked (%s)" (show mode)) . monadicIO $ do
    numDbs <-
      (\x -> if x == 0 then Nothing else Just x)
        <$> pick (chooseInt (0, 3))
    withEnvDbs numDbs $ \(_, dbs) -> do
      -- These options should have no effect on the end-result.
      us <- UseUnsafeFFI <$> pick arbitrary
      let wopts = defaultWriteOptions

      -- The chunk size should also have no effect on the end-result. Note: Low chunk sizes (e.g., 1
      -- pair or 1 byte) normally result in bad performance. We therefore base the number of pairs
      -- on the chunk size.
      chunkByNumPairs <- pick arbitrary
      (chunkSize, maxPairsBase) <- -- “base”: Not yet multiplied by numThreads (for parallel case).
        if chunkByNumPairs
          then do
            chunkNumPairs <- pick $ chooseInt (1, 50)

            -- Assures both less and more pairs than just a single chunk.
            let maxPairs = chunkNumPairs * 5

            return (ChunkNumPairs chunkNumPairs, maxPairs)
          else do
            -- Compare with the ShortBs max length of 50. (For the longest possible key-value pairs
            -- (100 bytes), we still have the possibility of up to two pairs per chunk.)
            chunkBytes <- pick $ chooseInt (1, 200)

            -- Average ShortBs length: 50/2=25; average key-value pair length: 2*25=50.
            let avgNumKeyValuePairsPerChunk = max 1 (chunkBytes `quot` 50)
            let maxPairs = avgNumKeyValuePairsPerChunk * 5

            return (ChunkBytes chunkBytes, maxPairs)

      case mode of
        ModeSerial -> do
          -- Write key-value pairs to the database. No parallelization is desired, so duplicate keys
          -- are allowed.
          keyValuePairs <- toByteStrings <$> arbitraryKeyValuePairs maxPairsBase
          run $
            S.fromList @IO keyValuePairs
              & chunkPairs chunkSize
              -- Assign each chunk to each database and flatten.
              & fmap (\sequ -> V.toList $ V.map (sequ,) dbs)
              & S.unfoldMany U.fromList
              & S.mapM (\(sequ, db) -> writeLMDBChunk' us wopts db sequ)
              & S.fold F.drain

          -- Read all key-value pairs back from the databases.
          readPairss <- forM dbs $ \db ->
            run . S.toList $ S.unfold readLMDB (defaultReadOptions, db, NoTxn)

          -- And make sure they are as expected.
          let expectedPairsInEachDb = sort $ removeDuplicateKeysRetainLast keyValuePairs
          return $ V.all (== expectedPairsInEachDb) readPairss
        ModeParallel (ClearDatabase clearDb) -> do
          -- Write key-value pairs to the database. Parallelization is desired, so duplicate keys
          -- are eliminated up-front.
          assertEnoughCapabilities
          numThreads <- pick $ chooseInt (2, 2 * numCapabilities)
          keyValuePairs <-
            toByteStrings . removeDuplicateKeysRetainLast
              <$> arbitraryKeyValuePairs (maxPairsBase * numThreads)

          chunks <-
            S.fromList keyValuePairs
              & chunkPairs chunkSize
              & S.fold F.toList

          -- The chunk we don’t write out but at which we instead clear the database.
          mClearDbChunkIdx <-
            if clearDb && not (null chunks)
              then Just <$> pick (chooseInt (0, length chunks - 1))
              else return Nothing

          readPairsAlts <- -- Alternative way of reading pairs back from the databases.
            run $
              S.fromList @IO chunks
                & S.indexed
                -- Assign each chunk to each database and flatten.
                & fmap (\(chunkIdx, sequ) -> map (chunkIdx,sequ,) [0 .. V.length dbs - 1])
                & S.unfoldMany U.fromList
                & S.parMapM
                  (S.maxThreads numThreads . S.maxBuffer numThreads)
                  ( \(chunkIdx, sequ, dbIdx) -> do
                      let db = dbs V.! dbIdx

                      -- Clear or write.
                      case mClearDbChunkIdx of
                        Just clearDbChunkIdx | clearDbChunkIdx == chunkIdx -> clearDatabase db
                        _ -> writeLMDBChunk' us wopts db sequ

                      -- Sprinkle in read-only transactions to make sure those can coexist with
                      -- read-write transactions. This is also an alternative (inefficient) way of
                      -- reading back from the database.
                      b <- randomIO
                      (chunkIdx,dbIdx,)
                        <$> if b
                          then
                            -- Read entire database using 'readLMDB'. While the database grows as
                            -- the chunks get written, these read-only transactions get longer
                            -- lived.
                            S.toList $ S.unfold readLMDB (defaultReadOptions, db, NoTxn)
                          else
                            -- Read using 'getLMDB' (not the entire database but only this chunk).
                            toList sequ
                              & S.fromList @IO
                              & S.mapM
                                ( \(k, _) ->
                                    -- The database could have been cleared above, in which case the
                                    -- key might not exist in the database.
                                    getLMDB db NoTxn k >>= \mv -> return $ (k,) <$> mv
                                )
                              & S.catMaybes
                              & S.fold F.toList
                  )
                & S.fold F.toList

          -- Read all key-value pairs back from the databases.
          readPairss <- forM dbs $ \db ->
            run . S.toList $ S.unfold readLMDB (defaultReadOptions, db, NoTxn)

          -- Make sure they are as expected. (Recall that for readPairsAlts, we sometimes read the
          -- whole database and sometimes only one chunk.)
          return $ case mClearDbChunkIdx of
            Nothing ->
              -- No clearing took place.
              let readPairssExpected =
                    V.fromList
                      . M.elems
                      -- Make sure there is at least empty data for each database. (This is for the
                      -- special case of no key-value pairs.)
                      . M.unionWith (++) (M.fromList . map (,[]) $ [0 .. V.length dbs - 1])
                      -- Needed because of the parallel reading.
                      . M.map (sort . removeDuplicateKeysRetainLast)
                      . M.fromListWith (++)
                      $ map (\(_, dbIdx, pairs) -> (dbIdx, pairs)) readPairsAlts
               in readPairss == readPairssExpected
                    && all (== V.head readPairss) readPairss
            Just clearDbChunkIdx ->
              -- For one of the chunks (clearDbChunkIdx), clearing took place. We know far less
              -- about the expected data in the database. Data that was written beyond a certain
              -- chunk should exist in there at least.
              let readPairssExpectedSubsets =
                    V.fromList
                      . M.elems
                      . M.map Set.fromList
                      -- Make sure there is at least empty data for each database.
                      . M.unionWith (++) (M.fromList . map (,[]) $ [0 .. V.length dbs - 1])
                      -- (No sorting or duplication removal needed because we convert to sets.)
                      . M.fromListWith (++)
                      . map (\(_, dbIdx, pairs) -> (dbIdx, pairs))
                      . filter
                        ( \(chunkIdx, dbIdx, _) ->
                            -- For this database (dbIdx), find the chunkIdx beyond which data is
                            -- known to exist.
                            let (endChunkIdxTmp, endDbIdx) =
                                  (clearDbChunkIdx * V.length dbs + dbIdx + numThreads - 1)
                                    `quotRem` V.length dbs
                                endChunkIdx =
                                  if endDbIdx < dbIdx
                                    then endChunkIdxTmp - 1
                                    else endChunkIdxTmp
                             in chunkIdx > endChunkIdx
                        )
                      $ readPairsAlts
               in flip all (V.indexed readPairss) $ \(dbIdx, readPairs) ->
                    (readPairssExpectedSubsets V.! dbIdx)
                      `Set.isSubsetOf` Set.fromList readPairs

-- | Choices for the standard writing accumulators.
data OverwriteOpts
  = OverwrDisallowThrow
  | OverwrDisallowThrowAllowSameVal
  | OverwrDisallowIgnore
  | OverwrDisallowStop
  | OverwrAppendThrow
  | OverwrAppendIgnore
  | OverwrAppendStop
  deriving (Show)

-- | Write key-value pairs to a database using our library non-concurrently in one chunk (chunking
-- and concurrency is already tested with 'testWriteLMDBChunked') with the standard overwriting
-- options other than “allow,” and make sure the exceptions and written key-value pairs are as
-- expected. (We see no reason to intermingle this with chunking and concurrency.)
testWriteLMDBOneChunk :: OverwriteOpts -> TestTree
testWriteLMDBOneChunk owOpts =
  testProperty (printf "writeLMDBOneChunk (%s)" (show owOpts)) . monadicIO . withEnvDbs Nothing $
    \(_, dbs) -> do
      let db = case V.toList dbs of [x] -> x; _ -> error "unreachable"

          wopts =
            defaultWriteOptions
              { writeOverwriteOptions = case owOpts of
                  OverwrDisallowThrow -> OverwriteDisallow . Left $ writeAccumThrow @IO Nothing
                  OverwrDisallowThrowAllowSameVal ->
                    OverwriteDisallow . Right $ writeAccumThrowAllowSameValue Nothing
                  OverwrDisallowIgnore -> OverwriteDisallow $ Left writeAccumIgnore
                  OverwrDisallowStop -> OverwriteDisallow $ Left writeAccumStop
                  OverwrAppendThrow -> OverwriteAppend $ writeAccumThrow Nothing
                  OverwrAppendIgnore -> OverwriteAppend writeAccumIgnore
                  OverwrAppendStop -> OverwriteAppend writeAccumStop
              }
          maxPairs = 200

      keyValuePairs <- toByteStrings <$> arbitraryKeyValuePairsDupsMoreLikely maxPairs
      let hasDuplicateKeys' = hasDuplicateKeys keyValuePairs
          hasDuplicateKeysWithDiffVals' = hasDuplicateKeysWithDiffVals keyValuePairs
          isSorted = sort keyValuePairs == keyValuePairs
          isStrictlySorted = isSorted && not hasDuplicateKeys'

      e <- run . try @SomeException $ writeLMDBChunk wopts db (Seq.fromList keyValuePairs)

      -- Make sure exceptions occurred as expected from the written key-value pairs.
      let assert1 s = assertMsg $ "assert (first checks) failure " ++ s
      case owOpts of
        OverwrDisallowThrow -> case e of
          Left _ -> assert1 "(1)" hasDuplicateKeys'
          Right _ -> assert1 "(2)" $ not hasDuplicateKeys'
        OverwrDisallowThrowAllowSameVal -> case e of
          Left _ -> assert1 "(3)" hasDuplicateKeysWithDiffVals'
          Right _ -> assert1 "(4)" $ not hasDuplicateKeysWithDiffVals'
        OverwrDisallowIgnore -> assert1 "(5)" (isRight e)
        OverwrDisallowStop -> assert1 "(6)" (isRight e)
        OverwrAppendThrow -> case e of
          Left _ -> assert1 "(7)" $ not isStrictlySorted
          Right _ -> assert1 "(8)" isStrictlySorted
        OverwrAppendIgnore -> assert1 "(9)" (isRight e)
        OverwrAppendStop -> assert1 "(10)" (isRight e)

      -- Regardless of whether an exception occurred, read all key-value pairs back from the
      -- database and make sure they are as expected.
      readPairs <- run . S.toList $ S.unfold readLMDB (defaultReadOptions, db, NoTxn)
      assertMsg "assert (second checks) failure" $
        readPairs == case owOpts of
          OverwrDisallowThrow ->
            if hasDuplicateKeys' then [] else sort keyValuePairs
          OverwrDisallowThrowAllowSameVal ->
            if hasDuplicateKeysWithDiffVals'
              then []
              else sort $ removeDuplicateKeysRetainLast keyValuePairs
          OverwrDisallowIgnore -> sort $ filterOutReoccurringKeys keyValuePairs
          OverwrDisallowStop -> sort $ prefixBeforeDuplicate keyValuePairs
          OverwrAppendThrow ->
            if keysAreStrictlyIncreasing keyValuePairs
              then keyValuePairs
              else []
          OverwrAppendIgnore -> fst $ filterGreaterThan keyValuePairs
          OverwrAppendStop -> prefixBeforeStrictlySortedKeysEnd keyValuePairs

-- | Perform reads and writes on a database concurrently, throwTo threads at random, and read all
-- key-value pairs back from the database using our library (already covered by 'testReadLMDB') and
-- make sure they are as expected.
testAsyncExceptionsConcurrent :: TestTree
testAsyncExceptionsConcurrent =
  testProperty "asyncExceptionsConcurrent" . monadicIO . withEnvDbs Nothing $ \(env, dbs) -> do
    let db = case V.toList dbs of [x] -> x; _ -> error "unreachable"
    assertEnoughCapabilities
    numThreads <- pick $ chooseInt (1, 2 * numCapabilities)

    pairss <- generateConcurrentPairs numThreads 0 100

    -- Whether to kill the thread.
    shouldKills :: [Bool] <- replicateM numThreads $ pick arbitrary

    -- Delay transactions to increase possibility they will get killed while active.
    delayss :: [Int] <- replicateM numThreads . pick $ chooseInt (0, 10)

    threads <- run $
      forConcurrently (zip pairss delayss) $
        \((threadIdx, pairs), delay) ->
          (threadIdx,)
            <$> asyncBound
              -- Add a surrounding read-only transaction to test it too gets cleaned. (We could be
              -- adding more possibilities, randomness, etc. here; but the idea is those things were
              -- already tested elsewhere, esp. by 'testWriteLMDBChunked'.)
              ( withReadOnlyTransaction env $ \_ -> withReadWriteTransaction env $ \txn -> do
                  S.fromList @IO pairs
                    & S.indexed
                    & S.mapM
                      ( \(idx, pair) -> do
                          when (idx == 0) $ threadDelay delay
                          return pair
                      )
                    & S.fold (writeLMDB @IO defaultWriteOptions db txn)
              )

    delay <- pick $ chooseInt (0, 20)
    run $ threadDelay delay

    run $ forConcurrently_ (zip shouldKills threads) $ \(shouldKill, (_, as)) ->
      if shouldKill
        then cancel as
        else wait as

    let expectedSubset =
          Set.fromList
            . concatMap (\(_, (_, pairs)) -> pairs)
            . filter (\(shouldKill, _) -> not shouldKill)
            $ zip shouldKills pairss

    readPairs <-
      Set.fromList <$> (run . S.toList $ S.unfold readLMDB (defaultReadOptions, db, NoTxn))

    assertMsg "assert failure" $ expectedSubset `Set.isSubsetOf` readPairs

-- | A bytestring with a limited random length. (We don’t see much value in testing with longer
-- bytestrings.)
newtype ShortBS = ShortBS ByteString deriving (Eq, Ord, Show)

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
-- for testing writeLMDB with the various writeOverwriteOptions.)
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

keysAreStrictlyIncreasing :: (Ord a) => [(a, b)] -> Bool
keysAreStrictlyIncreasing xs =
  all
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

-- |
-- * @between first second []@ (should be called with the third parameter at @[]@).
-- * Assumes @first < second@.
-- * Returns a list in between those two.
between :: [Word8] -> [Word8] -> [Word8] -> Maybe [Word8]
-- Idea: Compare the elements from left to right, and collect the equal ones into commonPrefixRev
-- (in reverse order), until we hit inequality.
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
             Nothing -> drop (length smaller) bigger == replicate (length bigger - length smaller) 0
             Just betw -> smaller < betw && betw < bigger

betweenBs :: ByteString -> ByteString -> Maybe ByteString
betweenBs bs1 bs2 = between (B.unpack bs1) (B.unpack bs2) [] <&> B.pack

type PairsInDatabase = [(ByteString, ByteString)]

type ExpectedReadResult = [(ByteString, ByteString)]

-- | Given database pairs, randomly generates read options and corresponding expected results.
readOptionsAndResults :: PairsInDatabase -> Gen (UseUnsafeFFI, ReadOptions, ExpectedReadResult)
readOptionsAndResults pairsInDb = do
  forw <- arbitrary
  let dir = if forw then Forward else Backward
  us <- UseUnsafeFFI <$> arbitrary
  let len = length pairsInDb
  readAll <- frequency [(1, return True), (3, return False)]
  let ropts = defaultReadOptions {readDirection = dir}
  if readAll
    then return (us, ropts {readStart = Nothing}, (if forw then id else reverse) pairsInDb)
    else
      if len == 0
        then do
          bs <- arbitrary >>= \(NonEmpty ws) -> return $ B.pack ws
          return (us, ropts {readStart = Just bs}, [])
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
          let forwEq =
                (us, ropts {readStart = Just $ keyAt idx}, drop idx pairsInDb)
          let backwEq =
                (us, ropts {readStart = Just $ keyAt idx}, reverse $ take (idx + 1) pairsInDb)

          -- Test with three possibilities: The chosen readStart is equal to, less than, or greater
          -- than one of the keys in the database.
          ord <- arbitrary @Ordering

          return $ case (ord, dir) of
            (EQ, Forward) -> forwEq
            (EQ, Backward) -> backwEq
            (GT, Forward) -> case nextKey of
              Nothing -> forwEq
              Just nextKey' -> (us, ropts {readStart = Just nextKey'}, drop (idx + 1) pairsInDb)
            (GT, Backward) -> case nextKey of
              Nothing -> backwEq
              Just nextKey' ->
                (us, ropts {readStart = Just nextKey'}, reverse $ take (idx + 1) pairsInDb)
            (LT, Forward) -> case prevKey of
              Nothing -> forwEq
              Just prevKey' -> (us, ropts {readStart = Just prevKey'}, drop idx pairsInDb)
            (LT, Backward) -> case prevKey of
              Nothing -> backwEq
              Just prevKey' -> (us, ropts {readStart = Just prevKey'}, reverse $ take idx pairsInDb)

generatePairsWithUniqueKeys ::
  (Monad m) => Int -> Int -> S.Stream (PropertyM m) [(ByteString, ByteString)]
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
          (\_ -> error "unreachable")
      )

newtype ThreadIdx = ThreadIdx Int deriving (Show)

-- | Generates pairs meant to be originating from multiple threads.
--
-- We make the keys unique; otherwise the result would be unpredictable (since we wouldn’t know
-- which values for duplicate keys would end up in the final database).
generateConcurrentPairs ::
  (Monad m) =>
  Int ->
  Int ->
  Int ->
  PropertyM m [(ThreadIdx, [(ByteString, ByteString)])]
generateConcurrentPairs numThreads minPairs maxPairs =
  generatePairsWithUniqueKeys minPairs maxPairs
    & S.take numThreads
    & S.indexed -- threadIdx.
    & fmap (first ThreadIdx)
    & S.fold F.toList

assertEnoughCapabilities :: (Monad m) => PropertyM m ()
assertEnoughCapabilities =
  when
    (numCapabilities <= 1)
    ( run $
        throwError
          "enoughCapabilities"
          "available threads <= 1; machine / Haskell RTS settings not useful for testing"
    )

-- Writes the given key-value pairs to the given database in an ordinary way.
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

assertMsg :: (Monad m) => String -> Bool -> PropertyM m ()
assertMsg _ True = return ()
assertMsg msg False = fail $ "Assert failed: " ++ msg
