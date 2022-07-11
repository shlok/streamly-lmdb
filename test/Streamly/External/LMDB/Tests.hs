{-# LANGUAGE TypeApplications #-}

module Streamly.External.LMDB.Tests (tests) where

import Control.Concurrent.Async (asyncBound, wait)
import Control.Exception (SomeException, bracket, onException, try)
import Control.Monad (forM_)
import Data.ByteString (ByteString, pack, unpack)
import qualified Data.ByteString as B
import Data.ByteString.Unsafe (unsafeUseAsCStringLen)
import Data.List (find, foldl', nubBy, sort)
import Data.Word (Word8)
import Foreign (castPtr, nullPtr, with)
import Streamly.External.LMDB
  ( Cursor,
    Environment,
    Mode,
    OverwriteOptions (..),
    ReadDirection (..),
    ReadOnlyTxn,
    ReadOptions (..),
    ReadWrite,
    WriteOptions (..),
    abortReadOnlyTxn,
    beginReadOnlyTxn,
    clearDatabase,
    closeCursor,
    defaultReadOptions,
    defaultWriteOptions,
    openCursor,
    readLMDB,
    unsafeReadLMDB,
    writeLMDB,
  )
import Streamly.External.LMDB.Internal (Database (..))
import Streamly.External.LMDB.Internal.Foreign
  ( MDB_val (..),
    combineOptions,
    mdb_nooverwrite,
    mdb_put,
    mdb_txn_begin,
    mdb_txn_commit,
  )
import Streamly.Prelude (fromList, toList, unfold)
import qualified Streamly.Prelude as S
import Test.QuickCheck (Gen, NonEmptyList (..), choose, elements, frequency)
import Test.QuickCheck.Monadic (PropertyM, monadicIO, pick, run)
import Test.Tasty (TestTree)
import Test.Tasty.QuickCheck (arbitrary, testProperty)

tests :: IO (Database ReadWrite, Environment ReadWrite) -> [TestTree]
tests dbenv =
  [ testReadLMDB dbenv,
    testUnsafeReadLMDB dbenv,
    testWriteLMDB dbenv,
    testWriteLMDB_2 dbenv,
    testWriteLMDB_3 dbenv,
    testBetween
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
  keyValuePairs <- arbitraryKeyValuePairs''
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
  keyValuePairs <- arbitraryKeyValuePairs''
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

-- | Clear the database, write key-value pairs to it using our library with key overwriting allowed,
-- read them back using our library (already covered by 'testReadLMDB'), and make sure the result is
-- what we wrote.
testWriteLMDB :: IO (Database ReadWrite, Environment ReadWrite) -> TestTree
testWriteLMDB res = testProperty "writeLMDB" . monadicIO $ do
  (db, _) <- run res
  keyValuePairs <- arbitraryKeyValuePairs
  run $ clearDatabase db

  chunkSz <- pick arbitrary

  let fol' =
        writeLMDB db $
          defaultWriteOptions
            { writeTransactionSize = chunkSz,
              overwriteOptions = OverwriteAllow
            }

  -- TODO: Run with new "bound" functionality in streamly.
  run $ asyncBound (S.fold fol' (fromList keyValuePairs)) >>= wait
  let keyValuePairsInDb = sort . removeDuplicateKeys $ keyValuePairs

  readPairsAll <- run . toList $ unfold (readLMDB db Nothing defaultReadOptions) undefined

  return $ keyValuePairsInDb == readPairsAll

-- | Clear the database, write key-value pairs to it using our library with key overwriting
-- disallowed, and make sure an exception occurs iff we had a duplicate key in our pairs.
-- Furthermore make sure that key-value pairs prior to a duplicate key are actually in the database.
testWriteLMDB_2 :: IO (Database ReadWrite, Environment ReadWrite) -> TestTree
testWriteLMDB_2 res = testProperty "writeLMDB_2" . monadicIO $ do
  (db, _) <- run res
  keyValuePairs <- arbitraryKeyValuePairs'
  run $ clearDatabase db

  chunkSz <- pick arbitrary

  -- TODO: Run with new "bound" functionality in streamly.
  let fol' =
        writeLMDB db $
          defaultWriteOptions
            { writeTransactionSize = chunkSz,
              overwriteOptions = OverwriteDisallow
            }
  e <- run $ try @SomeException $ (asyncBound (S.fold fol' (fromList keyValuePairs)) >>= wait)
  exceptionAsExpected <-
    case e of
      Left _ -> return $ hasDuplicateKeys keyValuePairs
      Right _ -> return . not $ hasDuplicateKeys keyValuePairs

  let keyValuePairsInDb = sort . prefixBeforeDuplicate $ keyValuePairs
  readPairsAll <- run . toList $ unfold (readLMDB db Nothing defaultReadOptions) undefined
  let pairsAsExpected = keyValuePairsInDb == readPairsAll

  return $ exceptionAsExpected && pairsAsExpected

-- | Clear the database, write key-value pairs to it using our library with key overwriting
-- disallowed except when attempting to replace an existing key-value pair, and make sure an
-- exception occurs iff we had a duplicate key with different values in our pairs. Furthermore make
-- sure that key-value pairs prior to a such a duplicate key are actually in the database.
testWriteLMDB_3 :: IO (Database ReadWrite, Environment ReadWrite) -> TestTree
testWriteLMDB_3 res = testProperty "writeLMDB_3" . monadicIO $ do
  (db, _) <- run res
  keyValuePairs <- arbitraryKeyValuePairs'
  run $ clearDatabase db

  chunkSz <- pick arbitrary

  -- TODO: Run with new "bound" functionality in streamly.
  let fol' =
        writeLMDB db $
          defaultWriteOptions
            { writeTransactionSize = chunkSz,
              overwriteOptions = OverwriteAllowSame
            }
  e <- run $ try @SomeException $ (asyncBound (S.fold fol' (fromList keyValuePairs)) >>= wait)
  exceptionAsExpected <-
    case e of
      Left _ -> return $ hasDuplicateKeysWithDiffVals keyValuePairs
      Right _ -> return . not $ hasDuplicateKeysWithDiffVals keyValuePairs

  let keyValuePairsInDb =
        sort . removeDuplicateKeys . prefixBeforeDuplicateWithDiffVal $
          keyValuePairs
  readPairsAll <- run . toList $ unfold (readLMDB db Nothing defaultReadOptions) undefined
  let pairsAsExpected = keyValuePairsInDb == readPairsAll

  return $ exceptionAsExpected && pairsAsExpected

arbitraryKeyValuePairs :: PropertyM IO [(ByteString, ByteString)]
arbitraryKeyValuePairs =
  map (\(ws1, ws2) -> (pack ws1, pack ws2))
    . filter (\(ws1, _) -> not (null ws1)) -- LMDB does not allow empty keys.
    <$> pick arbitrary

-- A variation that makes duplicate keys more likely.
arbitraryKeyValuePairs' :: PropertyM IO [(ByteString, ByteString)]
arbitraryKeyValuePairs' = do
  arb <- arbitraryKeyValuePairs
  b <- pick arbitrary
  if not (null arb) && b
    then do
      let (k, v) = head arb
      b' <- pick arbitrary
      v' <- if b' then return v else pack <$> pick arbitrary
      i <- pick $ choose (negate $ length arb, 2 * length arb)
      let (arb1, arb2) = splitAt i arb
      let arb' = arb1 ++ [(k, v')] ++ arb2
      return arb'
    else return arb

-- A variation that makes more likely keys with same the prefix and a difference of trailing zero
-- bytes.
arbitraryKeyValuePairs'' :: PropertyM IO [(ByteString, ByteString)]
arbitraryKeyValuePairs'' = do
  arb <- arbitraryKeyValuePairs
  if null arb
    then return arb
    else
      pick $
        frequency
          [ (1, return arb),
            ( 3,
              do
                let (k, v) = head arb
                b' <- arbitrary
                v' <- if b' then return v else pack <$> arbitrary
                i <- choose (0, length arb - 1)
                let (arb1, arb2) = splitAt i arb
                let arb3 = map (\i' -> (k `B.append` B.replicate i' 0, v')) [1 .. (i + 1)]
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
betweenBs bs1 bs2 = between (unpack bs1) (unpack bs2) [] >>= (return . pack)

type PairsInDatabase = [(ByteString, ByteString)]

type ExpectedReadResult = [(ByteString, ByteString)]

-- | Given database pairs, randomly generates read options and corresponding expected results.
readOptionsAndResults :: PairsInDatabase -> Gen (ReadOptions, ExpectedReadResult)
readOptionsAndResults pairsInDb = do
  forw <- arbitrary
  let dir = if forw then Forward else Backward
  let len = length pairsInDb
  readAll <- frequency [(1, return True), (3, return False)]
  let ropts = defaultReadOptions {readDirection = dir}
  if readAll
    then return (ropts {readStart = Nothing}, (if forw then id else reverse) pairsInDb)
    else
      if len == 0
        then do
          bs <- arbitrary >>= \(NonEmpty ws) -> return $ pack ws
          return (ropts {readStart = Just bs}, [])
        else do
          idx <-
            if len < 3
              then choose (0, len - 1)
              else frequency [(1, choose (1, len - 2)), (3, elements [0, len - 1])]
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
              (mdb_txn_commit ptxn) -- Make sure the key-value pairs we have so far are committed.
            mdb_txn_commit ptxn
        )
        >>= wait

{-# INLINE marshalOut #-}
marshalOut :: ByteString -> (MDB_val -> IO ()) -> IO ()
marshalOut bs f =
  unsafeUseAsCStringLen bs $ \(ptr, len) -> f $ MDB_val (fromIntegral len) (castPtr ptr)
