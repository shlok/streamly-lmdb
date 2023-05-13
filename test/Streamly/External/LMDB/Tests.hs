{-# LANGUAGE InstanceSigs #-}
{-# LANGUAGE TypeApplications #-}

module Streamly.External.LMDB.Tests (tests) where

import Control.Concurrent.Async (asyncBound, wait)
import Control.Exception hiding (assert)
import Control.Monad (forM_)
import Data.ByteString (ByteString)
import qualified Data.ByteString as B
import Data.ByteString.Unsafe
import Data.Either
import Data.List
import Data.Word
import Foreign
import Streamly.Data.Stream.Prelude (fromList, toList, unfold)
import qualified Streamly.Data.Stream.Prelude as S
import Streamly.External.LMDB
import Streamly.External.LMDB.Internal (Database (..))
import Streamly.External.LMDB.Internal.Foreign
import Test.QuickCheck
import Test.QuickCheck.Monadic
import Test.Tasty (TestTree)
import Test.Tasty.QuickCheck
import Text.Printf

tests :: IO (Database ReadWrite, Environment ReadWrite) -> [TestTree]
tests dbenv =
  [ testReadLMDB dbenv,
    testUnsafeReadLMDB dbenv
  ]
    ++ ( do
           overwriteOpts <-
             [ OverwriteAllow,
               OverwriteAllowSame,
               OverwriteDisallow $ WriteAppend False,
               OverwriteDisallow $ WriteAppend True
               ]
           failureFold <- [FailureThrow, FailureStop, FailureIgnore]
           return $ testWriteLMDB overwriteOpts failureFold dbenv
       )
    ++ [ testBetween
       ]

withReadOnlyTxnAndCurs ::
  (Mode mode) =>
  Environment mode ->
  Database mode ->
  ((ReadOnlyTxn, Cursor) -> IO r) ->
  IO r
withReadOnlyTxnAndCurs env db =
  bracket
    (beginReadOnlyTxn env >>= \txn -> openCursor txn db >>= \curs -> return (txn, curs))
    (\(txn, curs) -> closeCursor curs >> abortReadOnlyTxn txn)

-- | Clear the database, write key-value pairs to it in a normal manner, read
-- them back using our library, and make sure the result is what we wrote.
testReadLMDB :: (Mode mode) => IO (Database mode, Environment mode) -> TestTree
testReadLMDB res = testProperty "readLMDB" . monadicIO $ do
  (db, env) <- run res
  keyValuePairs <- toByteStrings <$> arbitraryKeyValuePairs'' 500
  run $ clearDatabase db

  run $ writeChunk db False keyValuePairs
  let keyValuePairsInDb = sort . removeDuplicateKeys $ keyValuePairs

  (readOpts, expectedResults) <- pick $ readOptionsAndResults keyValuePairsInDb
  let unf txn = toList $ unfold (readLMDB db txn readOpts) undefined
  results <- run $ unf Nothing
  resultsTxn <- run $ withReadOnlyTxnAndCurs env db (unf . Just)

  return $ results == expectedResults && resultsTxn == expectedResults

-- | Similar to 'testReadLMDB', except that it tests the unsafe function in a different manner.
testUnsafeReadLMDB :: (Mode mode) => IO (Database mode, Environment mode) -> TestTree
testUnsafeReadLMDB res = testProperty "unsafeReadLMDB" . monadicIO $ do
  (db, env) <- run res
  keyValuePairs <- toByteStrings <$> arbitraryKeyValuePairs'' 500
  run $ clearDatabase db

  run $ writeChunk db False keyValuePairs
  let keyValuePairsInDb = sort . removeDuplicateKeys $ keyValuePairs

  (readOpts, expectedResults) <- pick $ readOptionsAndResults keyValuePairsInDb
  let expectedLengths = map (\(k, v) -> (B.length k, B.length v)) expectedResults
  let unf txn =
        toList $
          unfold (unsafeReadLMDB db txn readOpts (return . snd) (return . snd)) undefined
  lengths <- run $ unf Nothing
  lengthsTxn <- run $ withReadOnlyTxnAndCurs env db (unf . Just)

  return $ lengths == expectedLengths && lengthsTxn == expectedLengths

data FailureFold = FailureThrow | FailureStop | FailureIgnore deriving (Show)

-- | Clear the database, write key-value pairs to it using our library with various options, read
-- all key-value pairs back from the database using our library (already covered by 'testReadLMDB'),
-- and make sure they are as expected.
testWriteLMDB ::
  OverwriteOptions ->
  FailureFold ->
  IO (Database ReadWrite, Environment ReadWrite) ->
  TestTree
testWriteLMDB overwriteOpts failureFold res =
  testProperty (printf "writeLMDB (%s, %s)" (show overwriteOpts) (show failureFold)) . monadicIO $
    do
      (db, _) <- run res
      run $ clearDatabase db

      -- These options should have no effect on the end-result. Note: Low chunk sizes (e.g., 1)
      -- normally result in bad performance. We therefore base the number of pairs on the chunk
      -- size.
      chunkSz <- pick $ chooseInt (1, 100)
      let maxPairs = chunkSz * 5
      unsafeFFI <- pick arbitrary

      let failureFold' = case failureFold of
            FailureThrow -> throwOnWriteFailure
            FailureStop -> stopOnWriteFailure
            FailureIgnore -> ignoreWriteFailures

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
      e <- run $ try @SomeException (asyncBound (S.fold fol' $ fromList keyValuePairs) >>= wait)

      --  overwriteOpts <- [OverwriteAllow, OverwriteAllowSame, OverwriteDisallow]
      --  writeAppend' <- [False, True]
      --  failureFold <- [FailureThrow, FailureStop, FailureIgnore]

      -- Make sure exceptions occurred as expected.
      let exceptionAssert s = assertMsg $ "unexpected exception " ++ s
      case failureFold of
        FailureStop -> exceptionAssert "(1)" (isRight e)
        FailureIgnore -> exceptionAssert "(2)" (isRight e)
        FailureThrow -> case overwriteOpts of
          OverwriteAllow -> exceptionAssert "(3)" (isRight e)
          OverwriteAllowSame -> case e of
            Left _ -> exceptionAssert "(4)" hasDuplicatesWithDiffVals
            Right _ -> exceptionAssert "(5)" $ not hasDuplicatesWithDiffVals
          OverwriteDisallow (WriteAppend False) -> case e of
            Left _ -> exceptionAssert "(6)" hasDuplicates
            Right _ -> exceptionAssert "(7)" $ not hasDuplicates
          OverwriteDisallow (WriteAppend True) -> case e of
            Left _ -> exceptionAssert "(8)" $ not isStrictlySorted
            Right _ -> exceptionAssert "(9)" isStrictlySorted

      -- if writeAppend'
      --   then

      --   else -- Only in this case could an exception possibly have occurred.
      --   -- Four:
      --   -- overwriteOpts: [OverwriteAllow, OverwriteAllowSame, OverwriteDisallow $ WriteAppend False/True]

      --     undefined

      -- Regardless of whether an exception occurred, we now read all key-value pairs back from the
      -- database and make sure they are as expected.

      -- let keyValuePairsInDb = sort . removeDuplicateKeys $ keyValuePairs

      -- readPairsAll <- run . toList $ unfold (readLMDB db Nothing defaultReadOptions) undefined

      return () -- \$ keyValuePairsInDb == readPairsAll

-- -- | Clear the database, write key-value pairs to it using our library with key overwriting
-- -- disallowed, and make sure an exception occurs iff we had a duplicate key in our pairs.
-- -- Furthermore make sure that key-value pairs prior to a duplicate key are actually in the database.
-- testWriteLMDB_2 :: IO (Database ReadWrite, Environment ReadWrite) -> TestTree
-- testWriteLMDB_2 res = testProperty "writeLMDB_2" . monadicIO $ do
--   (db, _) <- run res
--   keyValuePairs <- arbitraryKeyValuePairs'
--   run $ clearDatabase db

--   chunkSz <- pick arbitrary
--   unsafeFFI <- pick arbitrary

--   let fol' =
--         writeLMDB db $
--           defaultWriteOptions
--             { writeTransactionSize = chunkSz,
--               writeOverwriteOptions = OverwriteDisallow,
--               writeUnsafeFFI = unsafeFFI
--             }
--   e <- run $ try @SomeException $ (asyncBound (S.fold fol' (fromList keyValuePairs)) >>= wait)
--   exceptionAsExpected <-
--     case e of
--       Left _ -> return $ hasDuplicateKeys keyValuePairs
--       Right _ -> return . not $ hasDuplicateKeys keyValuePairs

--   let keyValuePairsInDb = sort . prefixBeforeDuplicate $ keyValuePairs
--   readPairsAll <- run . toList $ unfold (readLMDB db Nothing defaultReadOptions) undefined
--   let pairsAsExpected = keyValuePairsInDb == readPairsAll

--   return $ exceptionAsExpected && pairsAsExpected

-- -- | Clear the database, write key-value pairs to it using our library with key overwriting
-- -- disallowed except when attempting to replace an existing key-value pair, and make sure an
-- -- exception occurs iff we had a duplicate key with different values in our pairs. Furthermore make
-- -- sure that key-value pairs prior to a such a duplicate key are actually in the database.
-- testWriteLMDB_3 :: IO (Database ReadWrite, Environment ReadWrite) -> TestTree
-- testWriteLMDB_3 res = testProperty "writeLMDB_3" . monadicIO $ do
--   (db, _) <- run res
--   keyValuePairs <- arbitraryKeyValuePairs'
--   run $ clearDatabase db

--   chunkSz <- pick arbitrary
--   unsafeFFI <- pick arbitrary

--   let fol' =
--         writeLMDB db $
--           defaultWriteOptions
--             { writeTransactionSize = chunkSz,
--               writeOverwriteOptions = OverwriteAllowSame,
--               writeUnsafeFFI = unsafeFFI
--             }
--   e <- run $ try @SomeException $ (asyncBound (S.fold fol' (fromList keyValuePairs)) >>= wait)
--   exceptionAsExpected <-
--     case e of
--       Left _ -> return $ hasDuplicateKeysWithDiffVals keyValuePairs
--       Right _ -> return . not $ hasDuplicateKeysWithDiffVals keyValuePairs

--   let keyValuePairsInDb =
--         sort . removeDuplicateKeys . prefixBeforeDuplicateWithDiffVal $
--           keyValuePairs
--   readPairsAll <- run . toList $ unfold (readLMDB db Nothing defaultReadOptions) undefined
--   let pairsAsExpected = keyValuePairsInDb == readPairsAll

--   return $ exceptionAsExpected && pairsAsExpected

-- | A bytestring with a limited random length. (We don’t see much value in testing with longer
-- bytestring.)
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

-- | Note that this function retains the last value for each key.
removeDuplicateKeys :: (Eq a) => [(a, b)] -> [(a, b)]
removeDuplicateKeys =
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

-- Writes the given key-value pairs to the given database.
writeChunk ::
  (Foldable t, Mode mode) =>
  Database mode ->
  Bool ->
  t (ByteString, ByteString) ->
  IO ()
writeChunk (Database penv dbi) noOverwrite' keyValuePairs =
  let flags = combineOptions $ [mdb_nooverwrite | noOverwrite']
   in asyncBound
        ( do
            ptxn <- mdb_txn_begin penv nullPtr 0
            onException
              ( forM_ keyValuePairs $ \(k, v) ->
                  marshalOut k $ \k' -> marshalOut v $ \v' -> with k' $ \k'' -> with v' $ \v'' ->
                    mdb_put ptxn dbi k'' v'' flags
              )
              ( mdb_txn_commit ptxn -- Make sure the key-value pairs we have so far are committed.
              )
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
