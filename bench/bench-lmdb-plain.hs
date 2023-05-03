{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE ScopedTypeVariables #-}

import Control.Monad (forM_, when)
import Foreign (Ptr, alloca, callocBytes, free, malloc, nullPtr, peek)
import Foreign.C.Types
import Foreign.Marshal.Array (advancePtr, pokeArray)
import Streamly.External.LMDB.Internal.Foreign
import System.Directory (createDirectory, doesPathExist)
import System.Environment (getArgs)
import System.Exit

main :: IO ()
main = do
  penv <- alloca $ \ppenv ->
    c_mdb_env_create ppenv >>= \rc ->
      if rc /= 0 then throwLMDBErrNum "mdb_env_create" rc else peek ppenv

  c_mdb_env_set_mapsize penv (1024 * 1024 * 1024 * 1024) >>= \rc ->
    when (rc /= 0) $ throwLMDBErrNum "mdb_env_set_mapsize" rc

  getArgs >>= dispatch penv

dispatch :: Ptr MDB_env -> [String] -> IO ()
dispatch penv ["write-unsafeffi", path, keyFactor', valueFactor', numPairs', chunkSize'] =
  write penv (UnsafeFFI True) path keyFactor' valueFactor' numPairs' chunkSize'
dispatch penv ["write-safeffi", path, keyFactor', valueFactor', numPairs', chunkSize'] =
  write penv (UnsafeFFI False) path keyFactor' valueFactor' numPairs' chunkSize'
dispatch penv ["read-cursor-unsafeffi", path] = readCursor penv (UnsafeFFI True) path
dispatch penv ["read-cursor-safeffi", path] = readCursor penv (UnsafeFFI False) path
dispatch _ _ = do
  printUsage
  exitFailure

newtype UnsafeFFI = UnsafeFFI Bool

write :: Ptr MDB_env -> UnsafeFFI -> FilePath -> String -> String -> String -> String -> IO ()
write penv (UnsafeFFI unsafeFFI) path keyFactor' valueFactor' numPairs' chunkSize' = do
  let keyFactor = read keyFactor'
  let valFactor = read valueFactor'
  let numPairs = read numPairs'
  let chunkSz = read chunkSize'

  let (txn_begin, txn_commit, put_) =
        if unsafeFFI
          then (mdb_txn_begin_unsafe, mdb_txn_commit_unsafe, mdb_put_unsafe_)
          else (mdb_txn_begin, mdb_txn_commit, mdb_put_)

  exists <- doesPathExist path
  if not $
    keyFactor > 0
      && valFactor > 0
      && numPairs > 0
      && 0 < chunkSz
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
          mdb_env_open penv path mdb_notls
          ptxn' <- txn_begin penv nullPtr mdb_rdonly
          dbi <- mdb_dbi_open ptxn' Nothing 0
          txn_commit ptxn'

          let keySize = 8 * fromIntegral keyFactor
          let valSize = 8 * fromIntegral valFactor
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

          let go0 :: Ptr MDB_txn -> Int -> Int -> Int -> IO ()
              {-# INLINE go0 #-}
              go0 !ptxn !i !currChunkSz !x0 = do
                copyBytes keyData0 x0 >> copyBytes valData0 x0
                (ptxn_, i_, ccs_) <- go1 ptxn i currChunkSz 0
                if x0 < 255 then go0 ptxn_ i_ ccs_ (x0 + 1) else error "Overflow."
                where
                  {-# INLINE go1 #-}
                  go1 :: Ptr MDB_txn -> Int -> Int -> Int -> IO (Ptr MDB_txn, Int, Int)
                  go1 !ptxn1 !i1 !currChunkSz1 !x1 = do
                    copyBytes keyData1 x1 >> copyBytes valData1 x1
                    (ptxn1_, i1_, ccs_) <- go2 ptxn1 i1 currChunkSz1 0
                    if x1 < 255 then go1 ptxn1_ i1_ ccs_ (x1 + 1) else return (ptxn1_, i1_, ccs_)
                    where
                      {-# INLINE go2 #-}
                      go2 :: Ptr MDB_txn -> Int -> Int -> Int -> IO (Ptr MDB_txn, Int, Int)
                      go2 !ptxn2 !i2 !currChunkSz2 !x2 = do
                        copyBytes keyData2 x2 >> copyBytes valData2 x2
                        (ptxn2_, i2_, ccs_) <- go3 ptxn2 i2 currChunkSz2 0
                        if x2 < 255
                          then go2 ptxn2_ i2_ ccs_ (x2 + 1)
                          else return (ptxn2_, i2_, ccs_)
                        where
                          {-# INLINE go3 #-}
                          go3 :: Ptr MDB_txn -> Int -> Int -> Int -> IO (Ptr MDB_txn, Int, Int)
                          go3 !ptxn3 !i3 !currChunkSz3 !x3 = do
                            copyBytes keyData3 x3 >> copyBytes valData3 x3
                            when (i3 >= numPairs) $
                              txn_commit ptxn3 >> exitSuccess -- Complete.
                            currChunkSz3_ <-
                              if currChunkSz3 >= chunkSz
                                then txn_commit ptxn3 >> return 0
                                else return currChunkSz3
                            ptxn3_ <-
                              if currChunkSz3_ == 0
                                then txn_begin penv nullPtr 0
                                else return ptxn3

                            put_ ptxn3_ dbi keyData keySize valData valSize 0

                            if x3 < 255
                              then go3 ptxn3_ (i3 + 1) (currChunkSz3_ + 1) (x3 + 1)
                              else return (ptxn3_, i3 + 1, currChunkSz3_ + 1)

          go0 nullPtr 0 0 0

readCursor :: Ptr MDB_env -> UnsafeFFI -> FilePath -> IO ()
readCursor penv (UnsafeFFI unsafeFFI) path = do
  let (txn_begin, txn_commit, cursor_open, cursor_get, cursor_close) =
        if unsafeFFI
          then
            ( mdb_txn_begin_unsafe,
              mdb_txn_commit_unsafe,
              mdb_cursor_open_unsafe,
              c_mdb_cursor_get_unsafe,
              c_mdb_cursor_close_unsafe
            )
          else
            ( mdb_txn_begin,
              mdb_txn_commit,
              mdb_cursor_open,
              c_mdb_cursor_get,
              c_mdb_cursor_close
            )

  mdb_env_open penv path $ combineOptions [mdb_notls, mdb_rdonly]
  ptxn' <- txn_begin penv nullPtr mdb_rdonly
  dbi <- mdb_dbi_open ptxn' Nothing 0
  txn_commit ptxn'

  ptxn <- txn_begin penv nullPtr mdb_rdonly
  pcurs <- cursor_open ptxn dbi

  pk <- malloc
  pv <- malloc

  let go :: Int -> Int -> Int -> Int -> MDB_cursor_op_t -> IO (Int, Int, Int, Int)
      go !keyByteCount !valueByteCount !totalByteCount !pairCount !op = do
        rc <- cursor_get pcurs pk pv op
        if rc == mdb_notfound
          then return (keyByteCount, valueByteCount, totalByteCount, pairCount)
          else
            if rc /= 0
              then throwLMDBErrNum "mdb_cursor_get" rc
              else do
                !keySize <- fromIntegral . mv_size <$> peek pk
                !valueSize <- fromIntegral . mv_size <$> peek pv

                go
                  (keyByteCount + keySize)
                  (valueByteCount + valueSize)
                  (totalByteCount + keySize + valueSize)
                  (pairCount + 1)
                  mdb_next

  (keyByteCount, valueByteCount, totalByteCount, pairCount) <- go 0 0 0 0 mdb_first

  free pk >> free pv
  cursor_close pcurs
  txn_commit ptxn

  putStrLn $ "Key byte count:   " ++ show keyByteCount
  putStrLn $ "Value byte count: " ++ show valueByteCount
  putStrLn $ "Total byte count: " ++ show totalByteCount
  putStrLn $ "Pair count:       " ++ show pairCount

printUsage :: IO ()
printUsage = do
  putStrLn $
    "bench-lmdb write-unsafeffi [path] [key factor] [value factor] "
      ++ "[# key-value pairs] [# pairs in each transaction]"
  putStrLn $
    "bench-lmdb write-safeffi [path] [key factor] [value factor] "
      ++ "[# key-value pairs] [# pairs in each transaction]"
  putStrLn "bench-lmdb read-cursor-unsafeffi [path]"
  putStrLn "bench-lmdb read-cursor-safeffi [path]"

{-# INLINE copyBytes #-}
copyBytes :: [Ptr CChar] -> Int -> IO ()
copyBytes ptr x =
  forM_ ptr $ \p -> do
    let x_ = fromIntegral x
    pokeArray p [x_]
