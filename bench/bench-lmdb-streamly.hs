{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}

module Main where

import Control.Monad
import qualified Data.ByteString as B
import Data.Function
import Data.Maybe
import Foreign (Ptr, callocBytes)
import Foreign.C.Types (CChar)
import Foreign.Marshal.Array (advancePtr, pokeArray)
import qualified Streamly.Data.Fold as F
import qualified Streamly.Data.Stream as S
import Streamly.Data.Unfold (Unfold)
import Streamly.External.LMDB.Internal
import qualified Streamly.Internal.Data.Unfold as U
import System.Directory
import System.Environment (getArgs)
import System.Exit

main :: IO ()
main = getArgs >>= dispatch

dispatch :: [String] -> IO ()
dispatch ["write-chunked-unsafeffi", path, keyFactor', valueFactor', numPairs', chunkSz'] =
  write (UseUnsafeFFI True) Chunked path keyFactor' valueFactor' numPairs' chunkSz'
dispatch ["write-chunked-safeffi", path, keyFactor', valueFactor', numPairs', chunkSz'] =
  write (UseUnsafeFFI False) Chunked path keyFactor' valueFactor' numPairs' chunkSz'
dispatch ["write-splitupstream-unsafeffi", path, keyFactor', valueFactor', numPairs', chunkSz'] =
  write (UseUnsafeFFI True) SplitUpstream path keyFactor' valueFactor' numPairs' chunkSz'
dispatch ["write-splitupstream-safeffi", path, keyFactor', valueFactor', numPairs', chunkSz'] =
  write (UseUnsafeFFI False) SplitUpstream path keyFactor' valueFactor' numPairs' chunkSz'
-- For reading, these two should be the fastest. (A precreated read-only transaction should make
-- almost no difference for large enough databases, and we only check this for this first
-- unsafe-unsafeffi combination.)
dispatch ["read-cursor-unsafe-unsafeffi-notxn", path] = do
  readCursorUnsafe path (UseUnsafeFFI True) (PrecreatedTxn False)
dispatch ["read-cursor-unsafe-unsafeffi-txn", path] = do
  readCursorUnsafe path (UseUnsafeFFI True) (PrecreatedTxn True)

-- Using safe FFI should be slower.
dispatch ["read-cursor-unsafe-safeffi-notxn", path] = do
  readCursorUnsafe path (UseUnsafeFFI False) (PrecreatedTxn False)

-- Additionally, using safe readLMDB should be even slower.
dispatch ["read-cursor-safe-safeffi-notxn", path] = do
  readCursorSafe path (UseUnsafeFFI False) (PrecreatedTxn False)
dispatch _ = do
  printUsage
  exitFailure

newtype PrecreatedTxn = PrecreatedTxn Bool

data SplitMode
  = -- | More intelligent splitting of the upstream workload, such that each stream is no longer
    -- “long.” This should be a fairly good apples-to-apples comparison to the C and plain Haskell
    -- IO programs since no chunking into sequences is required.
    --
    -- Note: We do not provide a “long-lived transaction” (everything in one transaction) mode
    -- because that brings in unwanted oranges: the effect of bigger/smaller transactions (e.g., 100
    -- KiB vs. 1 MiB) is an entirely separate matter that users should benchmark separately.
    SplitUpstream
  | Chunked

{-# INLINE write #-}
write :: UseUnsafeFFI -> SplitMode -> String -> String -> String -> String -> String -> IO ()
write us splitMode path keyFactorS valueFactorS numPairsS chunkSzS = do
  let keyFactor = read keyFactorS
  let valFactor = read valueFactorS
  let numPairs :: Int = read numPairsS
  let chunkSz = read chunkSzS -- Number of key-value pairs (regardless of splitMode).
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
          db <- getDatabase env Nothing

          let keySize = 8 * keyFactor
          let valSize = 8 * valFactor
          keyData :: Ptr CChar <- callocBytes $ fromIntegral keySize
          valData :: Ptr CChar <- callocBytes $ fromIntegral valSize

          let toKeyValuePair = do
                -- + For Chunked, we cannot use unsafePackCStringLen.
                -- + For SplitUpstream, we could use unsafePackCStringLen but we don’t because most
                --   users will be using the fold with normal bytestrings.
                k <- B.packCStringLen (keyData, 8 * fromIntegral keyFactor)
                v <- B.packCStringLen (valData, 8 * fromIntegral valFactor)
                return (k, v)

          let wopts = defaultWriteOptions

          -- + Starts off at a given x (i.e., “seeks” to the appropriate upstream place); meant to
          --   be called with (0,0,0,0).
          -- + numPairs': the number of remaining pairs; meant to be called with numPairs.
          -- + Handles chunkSz key-value pairs before recursing into the next chunk (the final chunk
          --   may have fewer pairs).
          let {-# INLINE goSplitUpstream #-}
              goSplitUpstream x numPairs' = do
                let chunkSz' = min numPairs' chunkSz
                (mx', ()) <- withReadWriteTransaction env $ \txn ->
                  S.unfold
                    -- One extra to find recursion starting point.
                    (U.take (chunkSz' + 1) $ unfoldFourWords keyFactor valFactor keyData valData)
                    x
                    & S.fold
                      ( F.tee
                          F.latest
                          ( F.take chunkSz' $ -- Ignore the one extra element.
                              F.lmapM (const toKeyValuePair) $
                                writeLMDB' us wopts db txn
                          )
                      )
                let x' = fromMaybe (error "unreachable (overflow occurs earlier)") mx'
                    numPairs'' = numPairs' - chunkSz'
                if numPairs'' < 0
                  then
                    error "unreachable"
                  else
                    if numPairs'' == 0
                      then return ()
                      else
                        goSplitUpstream x' numPairs''

          let goChunked =
                S.unfold
                  ( unfoldFourWords keyFactor valFactor keyData valData
                      & U.take numPairs
                      & U.mapM (const toKeyValuePair)
                  )
                  (0, 0, 0, 0)
                  & chunkPairs (ChunkNumPairs chunkSz)
                  & S.mapM (writeLMDBChunk' us wopts db)
                  & S.fold F.drain

          case splitMode of
            SplitUpstream -> goSplitUpstream (0, 0, 0, 0) numPairs
            Chunked -> goChunked

readCursorUnsafe :: String -> UseUnsafeFFI -> PrecreatedTxn -> IO ()
readCursorUnsafe path us (PrecreatedTxn precreatedTxn) = do
  env <- openEnvironment @ReadOnly path (defaultLimits {mapSize = tebibyte})
  db <- getDatabase env Nothing
  let fold' =
        F.foldlM'
          ( \(!key, !value, !total, !pair) (kl, vl) ->
              return (key + kl, value + vl, total + kl + vl, pair + 1)
          )
          (return (0, 0, 0, 0 :: Int))

      ropts = defaultReadOptions
  (k, v, t, p) <-
    if precreatedTxn
      then withReadOnlyTransaction env $ \txn -> withCursor txn db $ \curs ->
        U.fold
          fold'
          unsafeReadLMDB'
          (ropts, us, db, JustTxn (txn, curs), return . snd, return . snd)
      else
        U.fold
          fold'
          unsafeReadLMDB'
          (ropts, us, db, NoTxn, return . snd, return . snd)

  putStrLn $ "Key byte count:   " ++ show k
  putStrLn $ "Value byte count: " ++ show v
  putStrLn $ "Total byte count: " ++ show t
  putStrLn $ "Pair count:       " ++ show p

readCursorSafe :: String -> UseUnsafeFFI -> PrecreatedTxn -> IO ()
readCursorSafe path us (PrecreatedTxn precreatedTxn) = do
  env <- openEnvironment @ReadOnly path (defaultLimits {mapSize = tebibyte})
  db <- getDatabase env Nothing
  let fold' =
        F.foldlM'
          ( \(!key, !value, !total, !pair) (b1, b2) ->
              return
                ( key + B.length b1,
                  value + B.length b2,
                  total + B.length b1 + B.length b2,
                  pair + 1
                )
          )
          (return (0, 0, 0, 0 :: Int))

      ropts = defaultReadOptions
  (k, v, t, p) <-
    if precreatedTxn
      then withReadOnlyTransaction env $ \txn -> withCursor txn db $ \curs ->
        U.fold fold' readLMDB' (ropts, us, db, JustTxn (txn, curs))
      else
        U.fold fold' readLMDB' (ropts, us, db, NoTxn)
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

data UnfMod = ModNone | Mod3 | Mod32 | Mod321 | Mod3210

-- | A customized unfold for four integers, each from 0 to 255. (A cross-product doesn’t work for us
-- because it’s not seekable: with a custom starting point, the “next” iteration of each unfold
-- doesn’t start at 0 like we want it to.)
{-# INLINE unfoldFourWords #-}
unfoldFourWords ::
  Int ->
  Int ->
  Ptr CChar ->
  Ptr CChar ->
  Unfold IO (Int, Int, Int, Int) (Int, Int, Int, Int)
unfoldFourWords keyFactor valFactor keyData valData =
  let keyData0 = map (\i -> advancePtr keyData (8 * i + 4)) [0 .. keyFactor - 1]
      keyData1 = map (\i -> advancePtr keyData (8 * i + 5)) [0 .. keyFactor - 1]
      keyData2 = map (\i -> advancePtr keyData (8 * i + 6)) [0 .. keyFactor - 1]
      keyData3 = map (\i -> advancePtr keyData (8 * i + 7)) [0 .. keyFactor - 1]

      valData0 = map (\i -> advancePtr valData (8 * i + 4)) [0 .. valFactor - 1]
      valData1 = map (\i -> advancePtr valData (8 * i + 5)) [0 .. valFactor - 1]
      valData2 = map (\i -> advancePtr valData (8 * i + 6)) [0 .. valFactor - 1]
      valData3 = map (\i -> advancePtr valData (8 * i + 7)) [0 .. valFactor - 1]

      copy0 x0 = copyBytes keyData0 x0 >> copyBytes valData0 x0
      copy1 x1 = copyBytes keyData1 x1 >> copyBytes valData1 x1
      copy2 x2 = copyBytes keyData2 x2 >> copyBytes valData2 x2
      copy3 x3 = copyBytes keyData3 x3 >> copyBytes valData3 x3
   in U.mkUnfoldM
        ( \(unfMod, x@(x0, x1, x2, x3)) -> do
            (unfMod', x') <-
              if x3 >= 255
                then
                  if x2 >= 255
                    then
                      if x1 >= 255
                        then
                          if x0 >= 255
                            then
                              error "overflow"
                            else do
                              return (Mod3210, (x0 + 1, 0, 0, 0))
                        else do
                          return (Mod321, (x0, x1 + 1, 0, 0))
                    else
                      return (Mod32, (x0, x1, x2 + 1, 0))
                else
                  return (Mod3, (x0, x1, x2, x3 + 1))

            case unfMod of
              ModNone -> return ()
              Mod3 -> copy3 x3
              Mod32 -> copy3 x3 >> copy2 x2
              Mod321 -> copy3 x3 >> copy2 x2 >> copy1 x1
              Mod3210 -> copy3 x3 >> copy2 x2 >> copy1 x1 >> copy0 x0

            return $ U.Yield x (unfMod', x')
        )
        (\x -> return (ModNone, x))
