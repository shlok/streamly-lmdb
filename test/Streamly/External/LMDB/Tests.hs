{-# LANGUAGE TypeApplications #-}

module Streamly.External.LMDB.Tests (tests) where

import Control.Concurrent.Async (asyncBound, wait)
import Control.Exception (SomeException, onException, try)
import Control.Monad (forM_)
import Data.ByteString (ByteString, pack)
import Data.ByteString.Unsafe (unsafeUseAsCStringLen)
import Data.List (find, foldl', nubBy, sort)
import Foreign (castPtr, nullPtr, with)
import Streamly.Prelude (fromList, toList, unfold)
import Test.QuickCheck (choose)
import Test.QuickCheck.Monadic (PropertyM, monadicIO, pick, run)
import Test.Tasty (TestTree)
import Test.Tasty.QuickCheck (arbitrary, testProperty)

import qualified Data.ByteString as B
import qualified Streamly.Prelude as S

import Streamly.External.LMDB (Mode, ReadWrite, OverwriteOptions(..), WriteOptions(..),
    clearDatabase, defaultWriteOptions, readLMDB, unsafeReadLMDB, writeLMDB)
import Streamly.External.LMDB.Internal (Database (..))
import Streamly.External.LMDB.Internal.Foreign (MDB_val (..), combineOptions,
    mdb_nooverwrite, mdb_put, mdb_txn_begin, mdb_txn_commit)

tests :: IO (Database ReadWrite) -> [TestTree]
tests db =
    [ testReadLMDB db
    , testUnsafeReadLMDB db
    , testWriteLMDB db
    , testWriteLMDB_2 db
    , testWriteLMDB_3 db ]

-- | Clear the database, write key-value pairs to it in a normal manner, read
-- them back using our library, and make sure the result is what we wrote.
testReadLMDB :: (Mode mode) => IO (Database mode) -> TestTree
testReadLMDB res = testProperty "readLMDB" . monadicIO $ do
    db <- run res
    keyValuePairs <- arbitraryKeyValuePairs
    run $ clearDatabase db

    run $ writeChunk db False keyValuePairs
    let keyValuePairsInDb = sort . removeDuplicateKeys $ keyValuePairs

    let stream = unfold (readLMDB db) undefined
    readPairsAll <- run . toList $ stream
    let allAsExpected = readPairsAll == keyValuePairsInDb

    -- Also make sure the stream’s 'take' functionality works.
    readPairsFirstFew <- run . toList $ S.take 3 stream
    let firstFewAsExpected = readPairsFirstFew == take 3 keyValuePairsInDb

    return $ allAsExpected && firstFewAsExpected

-- | Similar to 'testReadLMDB', except that it tests the unsafe function in a different manner.
testUnsafeReadLMDB :: (Mode mode) => IO (Database mode) -> TestTree
testUnsafeReadLMDB res = testProperty "unsafeReadLMDB" . monadicIO $ do
    db <- run res
    keyValuePairs <- arbitraryKeyValuePairs
    run $ clearDatabase db

    run $ writeChunk db False keyValuePairs
    let lengthsInDb = map (\(k, v) -> (B.length k, B.length v)) . sort . removeDuplicateKeys $ keyValuePairs

    let stream = unfold (unsafeReadLMDB db (return . snd) (return . snd)) undefined
    readLengthsAll <- run . toList $ stream

    return $ readLengthsAll == lengthsInDb

-- | Clear the database, write key-value pairs to it using our library with key overwriting allowed, read
-- them back using our library (already covered by 'testReadLMDB'), and make sure the result is what we wrote.
testWriteLMDB :: IO (Database ReadWrite) -> TestTree
testWriteLMDB res = testProperty "writeLMDB" . monadicIO $ do
    db <- run res
    keyValuePairs <- arbitraryKeyValuePairs
    run $ clearDatabase db

    chunkSz <- pick arbitrary

    let fol' = writeLMDB db $ defaultWriteOptions { writeTransactionSize = chunkSz
                                                  , overwriteOptions = OverwriteAllow }

    -- TODO: Run with new "bound" functionality in streamly.
    run $ asyncBound (S.fold fol' (fromList keyValuePairs)) >>= wait
    let keyValuePairsInDb = sort . removeDuplicateKeys $ keyValuePairs

    readPairsAll <- run . toList $ unfold (readLMDB db) undefined

    return $ keyValuePairsInDb == readPairsAll

-- | Clear the database, write key-value pairs to it using our library with key overwriting
-- disallowed, and make sure an exception occurs iff we had a duplicate key in our pairs.
-- Furthermore make sure that key-value pairs prior to a duplicate key are actually in the database.
testWriteLMDB_2 :: IO (Database ReadWrite) -> TestTree
testWriteLMDB_2 res = testProperty "writeLMDB_2" . monadicIO $ do
    db <- run res
    keyValuePairs <- arbitraryKeyValuePairs'
    run $ clearDatabase db

    chunkSz <- pick arbitrary

    -- TODO: Run with new "bound" functionality in streamly.
    let fol' = writeLMDB db $ defaultWriteOptions { writeTransactionSize = chunkSz
                                                  , overwriteOptions = OverwriteDisallow }
    e <- run $ try @SomeException $ (asyncBound (S.fold fol' (fromList keyValuePairs)) >>= wait)
    exceptionAsExpected <-
        case e of
            Left _ -> return $ hasDuplicateKeys keyValuePairs
            Right _ -> return . not $ hasDuplicateKeys keyValuePairs

    let keyValuePairsInDb = sort . prefixBeforeDuplicate  $ keyValuePairs
    readPairsAll <- run . toList $ unfold (readLMDB db) undefined
    let pairsAsExpected = keyValuePairsInDb == readPairsAll

    return $ exceptionAsExpected && pairsAsExpected

-- | Clear the database, write key-value pairs to it using our library with key overwriting
-- disallowed except when attempting to replace an existing key-value pair, and make sure an
-- exception occurs iff we had a duplicate key with different values in our pairs. Furthermore
-- make sure that key-value pairs prior to a such a duplicate key are actually in the database.
testWriteLMDB_3 :: IO (Database ReadWrite) -> TestTree
testWriteLMDB_3 res = testProperty "writeLMDB_3" . monadicIO $ do
    db <- run res
    keyValuePairs <- arbitraryKeyValuePairs'
    run $ clearDatabase db

    chunkSz <- pick arbitrary

    -- TODO: Run with new "bound" functionality in streamly.
    let fol' = writeLMDB db $ defaultWriteOptions { writeTransactionSize = chunkSz
                                                  , overwriteOptions = OverwriteAllowSame }
    e <- run $ try @SomeException $ (asyncBound (S.fold fol' (fromList keyValuePairs)) >>= wait)
    exceptionAsExpected <-
        case e of
            Left _ -> return $ hasDuplicateKeysWithDiffVals keyValuePairs
            Right _ -> return . not $ hasDuplicateKeysWithDiffVals keyValuePairs

    let keyValuePairsInDb = sort . removeDuplicateKeys . prefixBeforeDuplicateWithDiffVal $ keyValuePairs
    readPairsAll <- run . toList $ unfold (readLMDB db) undefined
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
    if length arb > 0 && b then do
        let (k, v) = arb !! 0
        b' <- pick arbitrary
        v' <- if b' then return v else pack <$> pick arbitrary
        i <- pick $ choose (negate $ length arb, 2 * length arb)
        let (arb1, arb2) = splitAt i arb
        let arb' = arb1 ++ [(k, v')] ++ arb2
        return arb'
    else
        return arb

-- | Note that this function retains the last value for each key.
removeDuplicateKeys :: (Eq a) => [(a, b)] -> [(a, b)]
removeDuplicateKeys = foldl' (\acc (a, b) -> if any ((== a) . fst) acc then acc else (a, b) : acc) [] . reverse

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
    let fstDup = snd <$> find (\((a, _), i) -> a `elem` map fst (take i xs)) (zip xs [0..])
    in case fstDup of
        Nothing -> xs
        Just i -> take i xs

prefixBeforeDuplicateWithDiffVal :: (Eq a, Eq b) => [(a, b)] -> [(a, b)]
prefixBeforeDuplicateWithDiffVal xs =
    let fstDup = snd <$> find (\((a, b), i) -> any (\(a', b') -> a == a' && b /= b') (take i xs)) (zip xs [0..])
    in case fstDup of
        Nothing -> xs
        Just i -> take i xs

-- Writes the given key-value pairs to the given database.
writeChunk :: (Foldable t, Mode mode) => Database mode -> Bool -> t (ByteString, ByteString) -> IO ()
writeChunk (Database penv dbi) noOverwrite' keyValuePairs =
    let flags = combineOptions $ [mdb_nooverwrite | noOverwrite']
    in asyncBound (do
        ptxn <- mdb_txn_begin penv nullPtr 0
        onException (forM_ keyValuePairs $ \(k, v) ->
            marshalOut k $ \k' -> marshalOut v $ \v' -> with k' $ \k'' -> with v' $ \v'' ->
                mdb_put ptxn dbi k'' v'' flags)
            (mdb_txn_commit ptxn) -- Make sure the key-value pairs we have so far are committed.
        mdb_txn_commit ptxn) >>= wait

{-# INLINE marshalOut #-}
marshalOut :: ByteString -> (MDB_val -> IO ()) -> IO ()
marshalOut bs f = unsafeUseAsCStringLen bs $ \(ptr, len) -> f $ MDB_val (fromIntegral len) (castPtr ptr)
