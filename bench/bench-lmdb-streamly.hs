{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}

import Control.Monad (forM_)
import qualified Data.ByteString as B
import Data.ByteString.Unsafe (unsafePackCStringLen)
import Foreign (Ptr, callocBytes)
import Foreign.C.Types (CChar)
import Foreign.Marshal.Array (advancePtr, pokeArray)
import Streamly.External.LMDB
import Streamly.External.LMDB.Channel
import Streamly.Internal.Data.Fold.Type (Fold (Fold), Step (Partial))
import Streamly.Internal.Data.Stream.StreamD.Type (Step (Stop, Yield))
import qualified Streamly.Internal.Data.Unfold as U
import Streamly.Internal.Data.Unfold.Type (Unfold (Unfold))
import System.Directory (createDirectory, doesPathExist)
import System.Environment (getArgs)
import System.Exit

main :: IO ()
main = getArgs >>= dispatch

dispatch :: [String] -> IO ()
dispatch ["write-unsafeffi", path, keyFactor', valueFactor', numPairs', chunkSize'] =
  write (UnsafeFFI True) path keyFactor' valueFactor' numPairs' chunkSize'
dispatch ["write-safeffi", path, keyFactor', valueFactor', numPairs', chunkSize'] =
  write (UnsafeFFI False) path keyFactor' valueFactor' numPairs' chunkSize'
-- For reading, these two should be the fastest. (A precreated read-only transaction should make
-- almost no difference for large enough databases, and we only check this for this first
-- unsafe-unsafeffi combination.)
dispatch ["read-cursor-unsafe-unsafeffi-notxn", path] = do
  readCursorUnsafe path (UnsafeFFI True) (PrecreatedTxn False)
dispatch ["read-cursor-unsafe-unsafeffi-txn", path] = do
  readCursorUnsafe path (UnsafeFFI True) (PrecreatedTxn True)

-- Using safe FFI should be slower.
dispatch ["read-cursor-unsafe-safeffi-notxn", path] = do
  readCursorUnsafe path (UnsafeFFI False) (PrecreatedTxn False)

-- Additionally, using safeLMDB should be even slower.
dispatch ["read-cursor-safe-safeffi-notxn", path] = do
  readCursorSafe path (UnsafeFFI False) (PrecreatedTxn False)
dispatch _ = do
  printUsage
  exitFailure

newtype PrecreatedTxn = PrecreatedTxn Bool

newtype UnsafeFFI = UnsafeFFI Bool

write :: UnsafeFFI -> String -> String -> String -> String -> String -> IO ()
write (UnsafeFFI unsafeFFI) path keyFactor' valueFactor' numPairs' chunkSize' = do
  let keyFactor = read keyFactor'
  let valFactor = read valueFactor'
  let numPairs :: Int = read numPairs'
  let chunkSz = read chunkSize'
  exists <- doesPathExist path
  if not $
    keyFactor > 0
      && valFactor > 0
      && numPairs > 0
      && chunkSz > 0
      && chunkSz <= numPairs
    then do
      putStrLn "Invalid write arguments."
      exitFailure
    else
      if exists
        then do
          putStrLn $ "File already exists at " ++ show path
          exitFailure
        else do
          createDirectory path
          env <- openEnvironment @ReadWrite path (defaultLimits {mapSize = tebibyte})
          chan <- createChannel defaultChannelOptions
          startChannel chan
          db <- getDatabase chan env Nothing

          let keySize = 8 * keyFactor
          let valSize = 8 * valFactor
          keyData :: Ptr CChar <- callocBytes $ fromIntegral keySize
          valData :: Ptr CChar <- callocBytes $ fromIntegral valSize

          let keyData0 = map (\i -> advancePtr keyData (8 * i + 4)) [0 .. keyFactor - 1]
          let keyData1 = map (\i -> advancePtr keyData (8 * i + 5)) [0 .. keyFactor - 1]
          let keyData2 = map (\i -> advancePtr keyData (8 * i + 6)) [0 .. keyFactor - 1]
          let keyData3 = map (\i -> advancePtr keyData (8 * i + 7)) [0 .. keyFactor - 1]

          let valData0 = map (\i -> advancePtr valData (8 * i + 4)) [0 .. valFactor - 1]
          let valData1 = map (\i -> advancePtr valData (8 * i + 5)) [0 .. valFactor - 1]
          let valData2 = map (\i -> advancePtr valData (8 * i + 6)) [0 .. valFactor - 1]
          let valData3 = map (\i -> advancePtr valData (8 * i + 7)) [0 .. valFactor - 1]

          let unf0 =
                Unfold
                  ( \x0 ->
                      if x0 == 256
                        then error "Overflow."
                        else do
                          copyBytes keyData0 x0 >> copyBytes valData0 x0
                          return $ Yield () (x0 + 1)
                  )
                  return
          let unf1 =
                Unfold
                  ( \x1 ->
                      if x1 == 256
                        then return Stop
                        else do
                          copyBytes keyData1 x1 >> copyBytes valData1 x1
                          return $ Yield () (x1 + 1)
                  )
                  return
          let unf2 =
                Unfold
                  ( \x2 ->
                      if x2 == 256
                        then return Stop
                        else do
                          copyBytes keyData2 x2 >> copyBytes valData2 x2
                          return $ Yield () (x2 + 1)
                  )
                  return
          let unf3 =
                Unfold
                  ( \x3 ->
                      if x3 == 256
                        then return Stop
                        else do
                          copyBytes keyData3 x3 >> copyBytes valData3 x3
                          return $ Yield () (x3 + 1)
                  )
                  return

          let unf =
                (U.lmap . const) (((0, 0), 0), 0) $
                  ((unf0 `crossProduct` unf1) `crossProduct` unf2)
                    `crossProduct` unf3

          let unf' =
                U.take numPairs $
                  U.mapM
                    ( \_ -> do
                        k <- unsafePackCStringLen (keyData, 8 * fromIntegral keyFactor)
                        v <- unsafePackCStringLen (valData, 8 * fromIntegral valFactor)
                        return (k, v)
                    )
                    unf

          let writeFold =
                writeLMDB
                  chan
                  db
                  ( defaultWriteOptions
                      { writeTransactionSize = chunkSz,
                        writeUnsafeFFI = unsafeFFI
                      }
                  )
          U.fold writeFold unf' undefined

readCursorUnsafe :: String -> UnsafeFFI -> PrecreatedTxn -> IO ()
readCursorUnsafe path (UnsafeFFI unsafeFFI) (PrecreatedTxn precreatedTxn) = do
  env <- openEnvironment @ReadOnly path (defaultLimits {mapSize = tebibyte})
  chan <- createChannel defaultChannelOptions
  startChannel chan
  db <- getDatabase chan env Nothing
  mtxncurs <-
    if precreatedTxn
      then do
        tx <- beginReadOnlyTxn env
        curs <- openCursor tx db
        return . Just $ (tx, curs)
      else return Nothing
  let fold' =
        Fold
          ( \(!key, !value, !total, !pair) (kl, vl) ->
              return $ Partial (key + kl, value + vl, total + kl + vl, pair + 1)
          )
          (return $ Partial (0, 0, 0, 0 :: Int))
          return
      ropts = defaultReadOptions {readUnsafeFFI = unsafeFFI}
  (k, v, t, p) <-
    U.fold
      fold'
      (unsafeReadLMDB db mtxncurs ropts (return . snd) (return . snd))
      undefined
  putStrLn $ "Key byte count:   " ++ show k
  putStrLn $ "Value byte count: " ++ show v
  putStrLn $ "Total byte count: " ++ show t
  putStrLn $ "Pair count:       " ++ show p

readCursorSafe :: String -> UnsafeFFI -> PrecreatedTxn -> IO ()
readCursorSafe path (UnsafeFFI unsafeFFI) (PrecreatedTxn precreatedTxn) = do
  env <- openEnvironment @ReadOnly path (defaultLimits {mapSize = tebibyte})
  chan <- createChannel defaultChannelOptions
  startChannel chan
  db <- getDatabase chan env Nothing
  mtxncurs <-
    if precreatedTxn
      then do
        tx <- beginReadOnlyTxn env
        curs <- openCursor tx db
        return . Just $ (tx, curs)
      else return Nothing
  let fold' =
        Fold
          ( \(!key, !value, !total, !pair) (b1, b2) ->
              return $
                Partial
                  ( key + B.length b1,
                    value + B.length b2,
                    total + B.length b1 + B.length b2,
                    pair + 1
                  )
          )
          (return $ Partial (0, 0, 0, 0 :: Int))
          return
      ropts = defaultReadOptions {readUnsafeFFI = unsafeFFI}
  (k, v, t, p) <- U.fold fold' (readLMDB db mtxncurs ropts) undefined
  putStrLn $ "Key byte count:   " ++ show k
  putStrLn $ "Value byte count: " ++ show v
  putStrLn $ "Total byte count: " ++ show t
  putStrLn $ "Pair count:       " ++ show p

printUsage :: IO ()
printUsage = do
  putStrLn $
    "bench-lmdb write [path] [key factor] [value factor] "
      ++ "[# key-value pairs] [# pairs in each transaction]"
  putStrLn "bench-lmdb read-cursor-unsafe-unsafeffi-notxn [path]"
  putStrLn "bench-lmdb read-cursor-unsafe-unsafeffi-txn [path]"
  putStrLn "bench-lmdb read-cursor-unsafe-safeffi-notxn [path]"
  putStrLn "bench-lmdb read-cursor-safe-safeffi-notxn [path]"

{-# INLINE copyBytes #-}
copyBytes :: [Ptr CChar] -> Int -> IO ()
copyBytes ptr x =
  forM_ ptr $ \p -> do
    let x_ = fromIntegral x
    pokeArray p [x_]

{-# INLINE crossProduct #-}
crossProduct :: Monad m => Unfold m a b -> Unfold m c d -> Unfold m (a, c) (b, d)
crossProduct u1 u2 = U.cross (U.lmap fst u1) (U.lmap snd u2)
