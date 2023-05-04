{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}

module Main where

import Control.Monad (forM, forM_)
import qualified Data.ByteString as B
import Data.List (intercalate)
import qualified Data.Text as T
import qualified Data.Vector as V
import Data.Word
import Statistics.Sample (mean, stdDev)
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
  shells "cabal build >> $logfile 2>&1" empty

  let c_executable = "./bench-lmdb"
  hs_plain_executable <-
    T.strip <$> strict (inshell "cabal exec -- which bench-lmdb-plain 2> /dev/null" empty)
  hs_streamly_executable <-
    T.strip <$> strict (inshell "cabal exec -- which bench-lmdb-streamly 2> /dev/null" empty)

  echoHrule
  answerIsYes "Perform read benchmarks?"
    >>= flip
      when
      ( do
          echo "Creating databases using C program (unless they already exist)..."
          let tmpDir :: (IsString a) => a
              tmpDir = "./tmp/read"
          mktree tmpDir

          let mb = 1000000.0 :: Double
          let pairCounts = [125000000 :: Int]
          let kvFactors = [1 :: Int, 2, 4]
          forM_ pairCounts $ \pairCount -> forM_ kvFactors $ \kvFactor -> do
            let dbPath = format ("" % fp % "/test_" % d % "_" % d % "") tmpDir pairCount kvFactor
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
                testdir (fromText dbPath)
                  >>= flip when (shells (format ("rm -r " % s % " 2> /dev/null") dbPath) empty)
                let chunkSize :: Int =
                      round $ mb / (8 * fromIntegral kvFactor + 8 * fromIntegral kvFactor)
                shells
                  ( format
                      ("" % s % " write " % s % " " % d % " " % d % " " % d % " " % d % "")
                      c_executable
                      dbPath
                      kvFactor
                      kvFactor
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
            "pair-count,kv-factor,"
              ++ "c_mean,std,"
              ++ "hs-plain-unsafeffi_mean,std,"
              ++ "hs-plain-safeffi_mean,std,"
              ++ "hs-streamly-unsafe-unsafeffi-notxn_mean,std,"
              ++ "hs-streamly-unsafe-unsafeffi-txn_mean,std,"
              ++ "hs-streamly-unsafe-safeffi-notxn_mean,std,"
              ++ "hs-streamly-safe-safeffi-notxn_mean,std"

          forM_ pairCounts $ \pairCount -> forM_ kvFactors $ \kvFactor -> do
            let dbPath = format ("" % fp % "/test_" % d % "_" % d % "") tmpDir pairCount kvFactor
            let expectedInOutput = format ("Pair count:       " % d % "") pairCount
            let warmCount = 3
            let timeCount = 10

            let runProcs description exec cmd = do
                  procs
                    "echo"
                    [format ("    Timing database " % s % " with " % s % "...") dbPath description]
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
                    [show pairCount, show kvFactor]
                      ++ concatMap (\(mean', std) -> [show mean', show std]) meansAndStds

            append csvFile . return . unsafeTextToLine . T.pack $ line
            procs "echo" [format ("    Appended results to " % s % "") csvFile] empty
      )

  echoHrule
  answerIsYes "Perform write benchmarks?"
    >>= flip
      when
      ( do
          let tmpDir :: (IsString a) => a
              tmpDir = "./tmp/write"
          shells (format ("rm -rf " % fp % "") tmpDir) empty
          mktree tmpDir
          let csvFile :: (IsString a) => a
              csvFile = "./tmp/write/write.csv"
          output csvFile . return . unsafeTextToLine . T.pack $
            "pair-count,kv-factor,"
              ++ "c_mean,std,"
              ++ "hs-plain-unsafeffi_mean,std,"
              ++ "hs-plain-safeffi_mean,std,"
              ++ "hs-streamly-unsafeffi_mean,std,"
              ++ "hs-streamly-safeffi_mean,std"

          let mb = 1000000.0 :: Double
          let pairCounts = [10000000 :: Int]
          let kvFactors = [1 :: Int, 2]

          forM_ pairCounts $ \pairCount -> forM_ kvFactors $ \kvFactor -> do
            let dbPath = format ("" % fp % "/test_" % d % "_" % d % "") tmpDir pairCount kvFactor
            let chunkSize :: Int =
                  round $ mb / (8 * fromIntegral kvFactor + 8 * fromIntegral kvFactor)
            let warmCount = 3
            let timeCount = 10

            let runProcs description exec cmd = do
                  let args =
                        format
                          ("" % s % " " % s % " " % d % " " % d % " " % d % " " % d % "")
                          cmd
                          dbPath
                          kvFactor
                          kvFactor
                          pairCount
                          chunkSize
                  procs
                    "echo"
                    [format ("    Writing database " % s % " with " % s % "...") dbPath description]
                    empty
                  toStats pairCount
                    <$> timeCommand
                      (Just dbPath)
                      (format ("" % s % " " % s % "") exec args)
                      []
                      warmCount
                      timeCount

            meansAndStds <-
              sequence
                [ runProcs
                    "C"
                    c_executable
                    "write",
                  runProcs
                    "Haskell (plain, unsafeffi)"
                    hs_plain_executable
                    "write-unsafeffi",
                  runProcs
                    "Haskell (plain, safeffi)"
                    hs_plain_executable
                    "write-safeffi",
                  runProcs
                    "Haskell (streamly, unsafeffi)"
                    hs_streamly_executable
                    "write-unsafeffi",
                  runProcs
                    "Haskell (streamly, safeffi)"
                    hs_streamly_executable
                    "write-safeffi"
                ]

            let line =
                  intercalate "," $
                    [show pairCount, show kvFactor]
                      ++ concatMap (\(mean', std) -> [show mean', show std]) meansAndStds

            append csvFile . return . unsafeTextToLine . T.pack $ line
            procs "echo" [format ("    Appended results to " % s % "") csvFile] empty
      )

  echoHrule
  answerIsYes "Perform file reading with C?"
    >>= flip
      when
      ( do
          let tmpDir :: (IsString a) => a
              tmpDir = "./tmp/c-file-read"

          procs "echo" ["-n", format ("    Removing " % s % " (if it exists)... ") tmpDir] empty
          shells (format ("rm -rf " % fp % "") tmpDir) empty
          echo "done."
          mktree tmpDir

          let multiplier = 5000
              add = 200
              fileCount :: Word64 = 256 * multiplier + add

          procs
            "echo"
            ["-n", format ("    Writing " % d % " 1-byte files (with Haskell)... ") fileCount]
            empty
          forM_ [0 .. fileCount - 1] $ \idx -> do
            let byte :: Word8 = fromIntegral idx
                filePath :: String = T.printf "%s/file%d" (tmpDir :: String) idx
            B.writeFile filePath (B.singleton byte)
          echo "done."

          let descriptions =
                [ "1st time (directly from disk)",
                  "2nd time (possibly directly from memory)",
                  "3rd time (possibly directly from memory)",
                  "4th time (possibly directly from memory)",
                  "5th time (possibly directly from memory)"
                ]
          forM_ descriptions $ \desc -> do
            procs
              "echo"
              [ format
                  ("    Reading " % d % " 1-byte files (with C program; " % s % ")... ")
                  fileCount
                  desc
              ]
              empty

            let expectedSum :: Word64 = (255 * 256 `div` 2) * multiplier + (add - 1) * add `div` 2
                expectedInOutput =
                  format ("File count: " % d % "\nSum: " % d % "") fileCount expectedSum
            seconds <-
              timeCommand'
                (format ("" % s % " read-files " % fp % "") c_executable tmpDir)
                [expectedInOutput]

            putStrLn $
              "        Microseconds / file: "
                ++ show ((realToFrac seconds :: Double) / fromIntegral fileCount * 1e6)
          
          procs "echo" ["-n", format ("    Removing " % s % "... ") tmpDir] empty
          shells (format ("rm -rf " % fp % "") tmpDir) empty
          echo "done."
      )

failWithMsg :: Line -> IO a
failWithMsg msg = echo msg >> exit (ExitFailure 1)

echoHrule :: IO ()
echoHrule = echo "----------------------------------------"

answerIsYes :: String -> IO Bool
answerIsYes question = do
  procs "echo" ["-n", T.pack $ question ++ " (y/N) "] empty
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
