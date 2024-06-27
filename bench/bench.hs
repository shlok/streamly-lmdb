{-# LANGUAGE NamedFieldPuns #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}

module Main where

import Control.Monad
import qualified Data.ByteString as B
import Data.List
import Data.Maybe
import qualified Data.Text as T
import Data.Time.Clock
import qualified Data.Vector as V
import Data.Word
import Statistics.Sample
import qualified Streamly.Data.Fold as F
import qualified Streamly.Data.Stream as S
import qualified Text.Printf as T
import Turtle

data Platform = PlatformLinux | PlatformMacOS deriving (Eq)

main :: IO ()
main = do
  uname <- strict $ inshell "uname" empty
  platform <-
    if "Linux" `T.isInfixOf` uname
      then return PlatformLinux
      else
        if "Darwin" `T.isInfixOf` uname
          then return PlatformMacOS
          else failWithMsg "Currently only Linux and macOS are supported."

  -- Required for pcregrep on Linux.
  when (platform == PlatformLinux) $ export "LC_ALL" "en_US.UTF-8"

  let logFile :: (IsString a) => a
      logFile = "./tmp/bench.sh.log"

  export "logfile" (fromString logFile)
  testfile logFile >>= flip when (rm logFile)

  procs "echo" ["Compiling C program... "] empty
  shells "mkdir -p ./tmp" empty
  shells "gcc -O2 -Wall bench-lmdb.c -llmdb -o bench-lmdb >> $logfile 2>&1" empty

  procs "echo" ["Compiling Haskell programs... "] empty
  shells "cabal build -ffusion-plugin >> $logfile 2>&1" empty

  let c_executable = "./bench-lmdb"

  -- Verbosity 0 helped avoid the beginning "Resolving dependencies..." output.
  hs_plain_executable <-
    T.strip
      <$> strict (inshell "cabal exec --verbose=0 -- which bench-lmdb-plain 2> /dev/null" empty)
  hs_streamly_executable <-
    T.strip
      <$> strict (inshell "cabal exec --verbose=0 -- which bench-lmdb-streamly 2> /dev/null" empty)

  autoYes <- answerIsYes False "Run all benchmarks without asking?"

  echoHrule
  answerIsYes autoYes "Perform read benchmarks?"
    >>= flip
      when
      ( do
          t1 <- getCurrentTime
          echo "Creating databases using C program (unless they already exist)..."
          let tmpDir :: (IsString a) => a
              tmpDir = "./tmp/read"
          mktree tmpDir

          let kib = 1024.0 :: Double
          let pairCounts = [2500000 :: Int]
          let kFactors = [20 :: Int, 40, 60]
          let vFactors = [100 :: Int, 200, 300]

          forM_ pairCounts $ \pairCount -> forM_ kFactors $ \kFactor ->
            forM_ vFactors $ \vFactor -> do
              let dbPath =
                    format
                      ("" % fp % "/test_" % d % "_" % d % "_" % d % "")
                      tmpDir
                      pairCount
                      kFactor
                      vFactor
              (_, existingDbPairCount) <-
                shellStrict
                  ( format
                      ( "mdb_stat -e "
                          % s
                          % " 2> /dev/null | pcregrep -o1 'Entries: (.+)$'"
                      )
                      dbPath
                  )
                  empty
              if T.strip existingDbPairCount == format ("" % d % "") pairCount
                then procs "echo" [format ("    Database " % s % " already exists.") dbPath] empty
                else do
                  procs "echo" ["-n", format ("    Creating database " % s % "... ") dbPath] empty
                  testdir (T.unpack dbPath)
                    >>= flip when (shells (format ("rm -r " % s % " 2> /dev/null") dbPath) empty)
                  let chunkSize :: Int =
                        round $ 100 * kib / (8 * fromIntegral kFactor + 8 * fromIntegral vFactor)
                  shells
                    ( format
                        ("" % s % " write " % s % " " % d % " " % d % " " % d % " " % d % "")
                        c_executable
                        dbPath
                        kFactor
                        vFactor
                        pairCount
                        chunkSize
                    )
                    empty
                  echo "success."

          echo "Measuring read-cursor..."

          let csvFile :: (IsString a) => a
              csvFile = "./tmp/read/read-cursor.csv"
          shells (format ("rm -f " % fp % "") csvFile) empty

          output csvFile . return . unsafeTextToLine . T.pack $
            "pair-count,k-factor,v-factor,"
              ++ "c_mean,std,"
              ++ "hs-plain-unsafeffi_mean,std,"
              ++ "hs-plain-safeffi_mean,std,"
              ++ "hs-streamly-unsafe-unsafeffi-notxn_mean,std,"
              ++ "hs-streamly-unsafe-unsafeffi-txn_mean,std,"
              ++ "hs-streamly-unsafe-safeffi-notxn_mean,std,"
              ++ "hs-streamly-safe-safeffi-notxn_mean,std"

          forM_ pairCounts $ \pairCount -> forM_ kFactors $ \kFactor ->
            forM_ vFactors $ \vFactor -> do
              let dbPath =
                    format
                      ("" % fp % "/test_" % d % "_" % d % "_" % d % "")
                      tmpDir
                      pairCount
                      kFactor
                      vFactor
              let expectedInOutput = format ("Pair count:       " % d % "") pairCount
              let warmCount = 3
              let timeCount = 10

              let runProcs description exec cmd = do
                    procs
                      "echo"
                      [ format
                          ("    Timing database " % s % " with " % s % "...")
                          dbPath
                          description
                      ]
                      empty
                    toStats pairCount
                      <$> timeCommand
                        Nothing
                        (format ("" % s % " " % s % " " % s % "") exec cmd dbPath)
                        [expectedInOutput]
                        warmCount
                        timeCount

              meansAndStds <-
                sequence
                  [ runProcs
                      "C"
                      c_executable
                      "read-cursor",
                    runProcs
                      "Haskell (plain, unsafeffi)"
                      hs_plain_executable
                      "read-cursor-unsafeffi",
                    runProcs
                      "Haskell (plain, safeffi)"
                      hs_plain_executable
                      "read-cursor-safeffi",
                    runProcs
                      "Haskell (streamly, unsafe-unsafeffi-notxn)"
                      hs_streamly_executable
                      "read-cursor-unsafe-unsafeffi-notxn",
                    runProcs
                      "Haskell (streamly, unsafe-unsafeffi-txn)"
                      hs_streamly_executable
                      "read-cursor-unsafe-unsafeffi-txn",
                    runProcs
                      "Haskell (streamly, unsafe-safeffi-notxn)"
                      hs_streamly_executable
                      "read-cursor-unsafe-safeffi-notxn",
                    runProcs
                      "Haskell (streamly, safe-safeffi-notxn)"
                      hs_streamly_executable
                      "read-cursor-safe-safeffi-notxn"
                  ]

              let line =
                    intercalate "," $
                      [show pairCount, show kFactor, show vFactor]
                        ++ concatMap (\(mean', std) -> [show mean', show std]) meansAndStds

              append csvFile . return . unsafeTextToLine . T.pack $ line
              procs "echo" [format ("    Appended results to " % s % "") csvFile] empty

          t2 <- getCurrentTime
          printf
            ("Time taken for read benchmarks: " % f % " hours.\n")
            (realToFrac (diffUTCTime t2 t1) / (60 * 60))
      )

  let writeBenchmarkSettings =
        WriteBenchmarkSettings
          { c_executable,
            hs_plain_executable,
            hs_streamly_executable,
            wMb = 1024.0 * 1024.0 :: Double,
            wPairCounts = [300000 :: Int],
            wKFactors = [20 :: Int, 40, 60],
            wVFactors = [100 :: Int, 200, 300]
          }

  echoHrule
  answerIsYes
    autoYes
    ( "Test write functionality? (I.e., that all "
        ++ "LMDB databases created in write benchmarks are the same?)"
    )
    >>= flip
      when
      (writeBenchmark WriteBenchmarkTest writeBenchmarkSettings)

  echoHrule
  answerIsYes autoYes "Perform write benchmarks?"
    >>= flip
      when
      (writeBenchmark WriteBenchmarkPerform writeBenchmarkSettings)

  echoHrule
  answerIsYes autoYes "Test file read speeds with C?"
    >>= flip
      when
      ( do
          t1 <- getCurrentTime

          bufSizT <-
            lineToText <$> single (inshell (format ("" % s % " show-bufsiz") c_executable) empty)
          let bufSiz :: Int = read . T.unpack $ bufSizT
          putStrLn $ "BUFSIZ: " ++ show bufSiz

          let tmpDir :: (IsString a) => a
              tmpDir = "./tmp/c-file-read"
          shells (format ("rm -rf " % fp % "") tmpDir) empty
          mktree tmpDir
          let csvFile :: (IsString a) => a
              csvFile = "./tmp/c-file-read/c-file-read.csv"
          output csvFile . return . unsafeTextToLine . T.pack $
            "bufsiz,bytes per file,microseconds per file"

          forM_ [1, bufSiz `quot` 2, bufSiz, 2 * bufSiz] $ \bytesPerFile -> do
            microsecondsPerFile <- testFileReadSpeedsInC bufSiz bytesPerFile c_executable
            append csvFile . return . unsafeTextToLine . T.pack $
              show bufSiz ++ "," ++ show bytesPerFile ++ "," ++ show microsecondsPerFile
          printf ("Output results to " % s % "\n") csvFile

          t2 <- getCurrentTime
          printf
            ("Time taken for testing file read speeds: " % f % " hours.\n")
            (realToFrac (diffUTCTime t2 t1) / (60 * 60))
      )

  echoHrule
  answerIsYes autoYes "Test memcpy speed with C?"
    >>= flip
      when
      ( do
          t1 <- getCurrentTime
          let tmpDir :: (IsString a) => a
              tmpDir = "./tmp/memcpy-speed"
          shells (format ("rm -rf " % fp % "") tmpDir) empty
          mktree tmpDir
          let csvFile :: (IsString a) => a
              csvFile = "./tmp/memcpy-speed/memcpy-speed.csv"
          output csvFile . return . unsafeTextToLine . T.pack $
            "iterations,factor,memcpy bytes per iteration,ns per iteration,ns per byte"

          let iterationss = [10000000 :: Int, 50000000, 100000000]
              factors = [10 :: Int, 20, 30, 40]
          forM_ iterationss $ \iterations -> forM_ factors $ \factor -> do
            out <-
              strict $
                inshell
                  ( format
                      ("" % s % " test-memcpy " % d % " " % d % "")
                      c_executable
                      iterations
                      factor
                  )
                  empty
            let line = T.pack . head . lines $ T.unpack out -- Discard final newline.
            append csvFile . return . unsafeTextToLine $ line
            procs "echo" [format ("    Appended results to " % s % "") csvFile] empty
          t2 <- getCurrentTime
          printf
            ("Time taken for testing memcpy speed: " % f % " hours.\n")
            (realToFrac (diffUTCTime t2 t1) / (60 * 60))
      )

data WriteBenchmarkSettings = WriteBenchmarkSettings
  { c_executable :: !Text,
    hs_plain_executable :: !Text,
    hs_streamly_executable :: !Text,
    wPairCounts :: ![Int],
    wKFactors :: ![Int],
    wVFactors :: ![Int],
    wMb :: !Double
  }

data WriteBenchmarkMode
  = -- | Perform the actual benchmark.
    WriteBenchmarkPerform
  | -- | For each pairCount/kFactor/vFactor combination, make sure all executables output the same
    -- database.
    WriteBenchmarkTest

writeBenchmark :: WriteBenchmarkMode -> WriteBenchmarkSettings -> IO ()
writeBenchmark
  mode
  ( WriteBenchmarkSettings
      { c_executable,
        hs_plain_executable,
        hs_streamly_executable,
        wPairCounts,
        wKFactors,
        wVFactors,
        wMb
      }
    ) = do
    t1 <- getCurrentTime
    let tmpDir :: (IsString a) => a
        tmpDir = case mode of
          WriteBenchmarkPerform -> "./tmp/write"
          WriteBenchmarkTest -> "./tmp/write-test"
    shells (format ("rm -rf " % fp % "") tmpDir) empty
    mktree tmpDir

    let csvFile :: (IsString a) => a
        csvFile = "./tmp/write/write.csv"

    case mode of
      WriteBenchmarkPerform ->
        output csvFile . return . unsafeTextToLine . T.pack $
          "pair-count,k-factor,v-factor,"
            ++ "c_mean,std,"
            ++ "hs-plain-unsafeffi_mean,std,"
            ++ "hs-plain-safeffi_mean,std,"
            ++ "hs-streamly-chunked-unsafeffi_mean,std,"
            ++ "hs-streamly-chunked-safeffi_mean,std,"
            ++ "hs-streamly-splitupstream-unsafeffi_mean,std,"
            ++ "hs-streamly-splitupstream-safeffi_mean,std"
      WriteBenchmarkTest -> return ()

    forM_ wPairCounts $ \pairCount -> forM_ wKFactors $ \kFactor ->
      forM_ wVFactors $ \vFactor -> do
        let descriptionsExecsCmds =
              [ ( "C",
                  c_executable,
                  "write"
                ),
                ( "Haskell (plain, unsafeffi)",
                  hs_plain_executable,
                  "write-unsafeffi"
                ),
                ( "Haskell (plain, safeffi)",
                  hs_plain_executable,
                  "write-safeffi"
                ),
                ( "Haskell (streamly, chunked, unsafeffi)",
                  hs_streamly_executable,
                  "write-chunked-unsafeffi"
                ),
                ( "Haskell (streamly, chunked, safeffi)",
                  hs_streamly_executable,
                  "write-chunked-safeffi"
                ),
                ( "Haskell (streamly, splitupstream, unsafeffi)",
                  hs_streamly_executable,
                  "write-splitupstream-unsafeffi"
                ),
                ( "Haskell (streamly, splitupstream safeffi)",
                  hs_streamly_executable,
                  "write-splitupstream-safeffi"
                )
              ]

        let dbPath =
              format
                ("" % fp % "/test_" % d % "_" % d % "_" % d % "")
                tmpDir
                pairCount
                kFactor
                vFactor

        let chunkSize :: Int =
              round $ wMb / (8 * fromIntegral kFactor + 8 * fromIntegral vFactor)

        let args cmd =
              format
                ("" % s % " " % s % " " % d % " " % d % " " % d % " " % d % "")
                cmd
                dbPath
                kFactor
                vFactor
                pairCount
                chunkSize

        case mode of
          WriteBenchmarkPerform -> do
            let warmCount = 3
            let timeCount = 10

            let runProcs (desc, exec, cmd) = do
                  procs
                    "echo"
                    [ format
                        ("    Writing database " % s % " with " % s % "...")
                        dbPath
                        desc
                    ]
                    empty
                  toStats pairCount
                    <$> timeCommand
                      (Just dbPath)
                      (format ("" % s % " " % s % "") exec (args cmd))
                      []
                      warmCount
                      timeCount

            meansAndStds <- forM descriptionsExecsCmds $ \(desc, exec, cmd) ->
              runProcs (desc, exec, cmd)

            let line =
                  intercalate "," $
                    [show pairCount, show kFactor, show vFactor]
                      ++ concatMap (\(mean', std) -> [show mean', show std]) meansAndStds

            append csvFile . return . unsafeTextToLine . T.pack $ line
            procs "echo" [format ("    Appended results to " % s % "") csvFile] empty
          WriteBenchmarkTest -> do
            printf ("  (pairCount,kFactor,vFactor)=" % w % "\n") (pairCount, kFactor, vFactor)
            sha1Hashes <-
              S.fromList @IO descriptionsExecsCmds
                & S.mapM
                  ( \(desc, exec, cmd) -> do
                      printf ("    Writing database " % s % " with " % s % "...\n") dbPath desc

                      let cmdAndArgs = format ("" % s % " " % s % "") exec (args cmd)
                      sh $ inshell cmdAndArgs empty

                      printf "        Dumping it...\n"
                      let dumpPath = format ("" % s % ".dump") dbPath
                      sh $
                        inshell
                          (format ("mdb_dump " % s % " > " % s % "") dbPath dumpPath)
                          empty

                      printf "        Computing dump SHA-1 hash... "
                      hash <-
                        single $
                          inshell (format ("sha1sum " % s % " | cut -f 1 -d ' '") dumpPath) empty
                      echo hash

                      rmtree $ T.unpack dbPath
                      rm . T.unpack $ dumpPath

                      return $ lineToText hash
                  )
                & S.fold F.toList
            if all (== head sha1Hashes) sha1Hashes
              then
                echo "  Hashes MATCH"
              else
                error "  Hashes MISMATCH"
    t2 <- getCurrentTime
    printf
      ("Time taken for " % s % "write benchmarks: " % f % " hours.\n")
      (case mode of WriteBenchmarkPerform -> ""; WriteBenchmarkTest -> "testing ")
      (realToFrac (diffUTCTime t2 t1) / (60 * 60))

-- | Returns the first available “microseconds per file” number (the one where reading was surely
-- done directly from disk).
testFileReadSpeedsInC :: Int -> Int -> Text -> IO Double
testFileReadSpeedsInC bufSiz bytesPerFile c_executable' = do
  let tmpDir :: (IsString a) => a
      tmpDir = "./tmp/c-file-read-tmp-files"

  procs "echo" ["-n", format ("    Removing " % s % " (if it exists)... ") tmpDir] empty
  shells (format ("rm -rf " % fp % "") tmpDir) empty
  echo "done."
  mktree tmpDir

  let fileCount = 150000 `quot` max 1 (bytesPerFile `quot` bufSiz)
      numBytes = fileCount * bytesPerFile

  procs
    "echo"
    [ "-n",
      format
        ("    Writing " % d % " " % d % "-byte files (with Haskell)... ")
        fileCount
        bytesPerFile
    ]
    empty
  lastFileIdx <-
    S.fromList [0 .. numBytes - 1]
      -- The bytes “wrap around” and keep repeating from 0 to 255.
      & fmap (fromIntegral @_ @Word8)
      & S.groupsOf bytesPerFile F.toList
      & S.indexed
      & S.mapM
        ( \(fileIdx, bytes) -> do
            let filePath :: String = T.printf "%s/file%d" (tmpDir :: String) fileIdx
            B.writeFile filePath (B.pack bytes)
            return fileIdx
        )
      & S.fold F.latest
  unless
    (fromMaybe (error "unreachable") lastFileIdx + 1 == fileCount)
    (error $ "unreachable;" ++ show lastFileIdx ++ ";" ++ show fileCount)
  echo "done."

  let descriptions =
        [ "1st time (directly from disk)",
          "2nd time (possibly directly from memory)",
          "3rd time (possibly directly from memory)",
          "4th time (possibly directly from memory)",
          "5th time (possibly directly from memory)"
        ]
  microsecondsPerFiless <- forM descriptions $ \desc -> do
    procs
      "echo"
      [ "-n",
        format
          ("    Reading " % d % " " % d % "-byte files (with C program; " % s % ")...\n")
          fileCount
          bytesPerFile
          desc
      ]
      empty

    let sumOneToN n = n * (n + 1) `quot` 2
        (quot', rem') = numBytes `quotRem` 256
        expectedSum = quot' * sumOneToN 255 + sumOneToN (rem' - 1)
        expectedInOutput =
          format ("File count: " % d % "\nSum: " % d % "") fileCount expectedSum
    seconds <-
      timeCommand'
        (format ("" % s % " read-files " % fp % " " % d % "") c_executable' tmpDir bytesPerFile)
        [expectedInOutput]
    -- putStrLn $ "        Sum match (sanity check): " ++ show expectedSum

    let microsecondsPerFile = (realToFrac seconds :: Double) / fromIntegral fileCount * 1e6
    putStrLn $ "        Microseconds / file: " ++ show microsecondsPerFile
    return microsecondsPerFile

  procs "echo" ["-n", format ("    Removing " % s % "... ") tmpDir] empty
  shells (format ("rm -rf " % fp % "") tmpDir) empty
  echo "done."
  return $ head microsecondsPerFiless

failWithMsg :: Line -> IO a
failWithMsg msg = echo msg >> exit (ExitFailure 1)

echoHrule :: IO ()
echoHrule = echo "----------------------------------------"

answerIsYes :: Bool -> String -> IO Bool
answerIsYes autoYes question = do
  procs "echo" ["-n", T.pack $ question ++ " (y/N) "] empty
  if autoYes
    then do
      procs "echo" ["(automatic yes)"] empty
      return True
    else do
      line <- readline
      case line of
        Just answer | answer == "y" -> return True
        _ -> return False

timeCommand' :: Text -> [Text] -> IO NominalDiffTime
timeCommand' cmdAndArgs expectedInOut = do
  (out, t) <- time . strict $ inshell cmdAndArgs empty
  if all (`T.isInfixOf` out) expectedInOut
    then return t
    else
      error $
        "all of "
          ++ show expectedInOut
          ++ " were not found in output "
          ++ show out
          ++ " of command "
          ++ show cmdAndArgs

timeCommand :: Maybe T.Text -> Text -> [Text] -> Int -> Int -> IO [NominalDiffTime]
timeCommand forceRemovalPath cmdAndArgs expectedInOut warmCount timeCount = do
  let removePath =
        maybe (return ()) (\fp' -> shells (format ("rm -rf " % s % "") fp') empty) forceRemovalPath
  let echoCount c = procs "echo" ["-n", format ("" % d % " ") c] empty
  when (warmCount > 0) $ do
    procs "echo" ["-n", "        Warming up... "] empty
    forM_ [1 .. warmCount] $ \c ->
      echoCount c >> removePath >> timeCommand' cmdAndArgs expectedInOut
    echo "done."
  procs "echo" ["-n", "        Timing... "] empty
  times <- forM [1 .. timeCount] $ \c ->
    echoCount c >> removePath >> timeCommand' cmdAndArgs expectedInOut
  removePath
  echo "done."
  return times

-- Mean and standard deviation of nanoseconds per pair.
toStats :: Int -> [NominalDiffTime] -> (Double, Double)
toStats pairCount secs =
  let nsPerPair =
        V.fromList $ map (realToFrac . (/ fromIntegral pairCount) . (* 1e9)) secs
   in (mean nsPerPair, stdDev nsPerPair)
