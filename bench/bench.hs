{-# LANGUAGE OverloadedStrings, ScopedTypeVariables #-}

module Main where

import Control.Monad (forM, forM_, replicateM, replicateM_, unless, when)
import Data.List (intercalate)
import Data.Maybe (fromJust, maybe)
import Statistics.Sample (mean, stdDev)
import Turtle
import qualified Data.Text as T
import qualified Data.Vector as V
import qualified System.IO as IO

data Platform = PlatformLinux | PlatformMacOS deriving (Eq)

main :: IO ()
main = do
    uname <- strict $ inshell "uname" empty
    platform <-
        if "Linux" `T.isInfixOf` uname then return PlatformLinux
        else if "Darwin" `T.isInfixOf` uname then return PlatformMacOS
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
    shells "stack build >> $logfile 2>&1" empty

    let c_executable = "./bench-lmdb"
    hs_plain_executable <- T.strip <$> strict (inshell "stack exec -- which bench-lmdb-plain 2> /dev/null" empty)
    hs_streamly_executable <- T.strip <$> strict (inshell "stack exec -- which bench-lmdb-streamly 2> /dev/null" empty)

    echoHrule
    answerIsYes "Perform read benchmarks?" >>= flip when (do
        echo "Creating databases using C program (unless they already exist)..."
        let tmpDir :: (IsString a) => a
            tmpDir = "./tmp/read"
        mktree tmpDir

        let mb = 1000000.0 :: Double
        let pairCounts = [31250000 :: Int, 62500000, 125000000]
        let kvFactors = [1 :: Int, 2, 4]
        forM_ pairCounts $ \pairCount -> forM_ kvFactors $ \kvFactor -> do
            let dbPath = format (""%fp%"/test_"%d%"_"%d%"") tmpDir pairCount kvFactor
            (_, existingDbPairCount) <-
                shellStrict (format ("mdb_stat -e "%s%" 2> /dev/null | pcregrep -o1 'Entries: (.+)$'") dbPath) empty
            if T.strip existingDbPairCount == format (""%d%"") pairCount then
                procs "echo" [format ("    Database "%s%" already exists.") dbPath] empty
            else do
                procs "echo" ["-n", format ("    Creating database "%s%"... ") dbPath] empty
                testdir (fromText dbPath) >>= flip when (shells (format ("rm -r "%s%" 2> /dev/null") dbPath) empty)
                let chunkSize :: Int = round $ mb / (8 * fromIntegral kvFactor + 8 * fromIntegral kvFactor)
                shells (format (""%s%" write "%s%" "%d%" "%d%" "%d%" "%d%"")
                    c_executable dbPath kvFactor kvFactor pairCount chunkSize) empty
                echo "success."

        echo "Measuring read-cursor..."

        let csvFile :: (IsString a) => a
            csvFile = "./tmp/read/read-cursor.csv"
        shells (format ("rm -f "%fp%"") csvFile) empty

        output csvFile . return . unsafeTextToLine . T.pack $
            "pairCount,kvFactor,"
            ++ "c_mean,c_std,"
            ++ "hsPlain_mean,hsPlain_std,"
            ++ "hsStreamly_mean,hsStreamly_std,"
            ++ "hsStreamlySafe_mean,hsStreamlySafe_std,"
            ++ "hsPlain_diff_c_mean,hsPlain_diff_c_std,"
            ++ "hsStreamly_diff_hsPlain_mean,hsStreamly_diff_hsPlain_std,"
            ++ "hsStreamlySafe_diff_hsStreamly_mean,hsStreamlySafe_diff_hsStreamly_std"

        forM_ pairCounts $ \pairCount -> forM_ kvFactors $ \kvFactor -> do
            let dbPath = format (""%fp%"/test_"%d%"_"%d%"") tmpDir pairCount kvFactor
            let expectedInOutput = format ("Pair count:       "%d%"") pairCount
            let args = format ("read-cursor "%s%"") dbPath
            let warmCount = 2
            let timeCount = 5

            -- Mean and standard deviation of nanoseconds per pair.
            let toStats :: [NominalDiffTime] -> (Double, Double)
                toStats secs =
                    let  nsPerPair = V.fromList $ map (realToFrac . (/fromIntegral pairCount) . (*1e9)) secs
                    in (mean nsPerPair, stdDev nsPerPair)

            procs "echo" [format ("    Timing database "%s%" with C...") dbPath] empty
            (cMean, cStd) <-
                toStats <$>
                timeCommand Nothing (format (""%s%" "%s%"") c_executable args)
                    [expectedInOutput] warmCount timeCount

            procs "echo" [format ("    Timing database "%s%" with Haskell (plain)...") dbPath] empty
            (hsPlainMean, hsPlainStd) <-
                toStats <$>
                timeCommand Nothing (format (""%s%" "%s%"") hs_plain_executable args)
                    [expectedInOutput] warmCount timeCount

            procs "echo" [format ("    Timing database "%s%" with Haskell (streamly)...") dbPath] empty
            (hsStreamlyMean, hsStreamlyStd) <-
                toStats <$>
                timeCommand Nothing (format (""%s%" "%s%"") hs_streamly_executable args)
                    [expectedInOutput] warmCount timeCount

            procs "echo" [format ("    Timing database "%s%" with Haskell (streamly, safe)...") dbPath] empty
            (hsStreamlySafeMean, hsStreamlySafeStd) <-
                toStats <$>
                timeCommand Nothing (format (""%s%" read-cursor-safe "%s%"") hs_streamly_executable dbPath)
                    [expectedInOutput] warmCount timeCount

            let diffStd :: Double -> Double -> Double
                diffStd std1 std2 =
                    let two = 2 :: Int
                    in sqrt $ std1^two + std2^two

            let line = intercalate "," $ [show pairCount, show kvFactor] ++ map show [cMean, cStd,
                    hsPlainMean, hsPlainStd, hsStreamlyMean, hsStreamlyStd, hsStreamlySafeMean, hsStreamlySafeStd,
                    hsPlainMean - cMean, diffStd hsPlainStd cStd,
                    hsStreamlyMean - hsPlainMean, diffStd hsStreamlyStd hsPlainStd,
                    hsStreamlySafeMean - hsStreamlyMean, diffStd hsStreamlySafeStd hsStreamlyStd]
            append csvFile . return . unsafeTextToLine . T.pack $ line
            procs "echo" [format ("    Appended results to "%s%"") csvFile] empty
        )

    echoHrule
    answerIsYes "Perform write benchmarks?" >>= flip when (do
        let tmpDir :: (IsString a) => a
            tmpDir = "./tmp/write"
        shells (format ("rm -rf "%fp%"") tmpDir) empty
        mktree tmpDir
        let csvFile :: (IsString a) => a
            csvFile = "./tmp/write/write.csv"
        output csvFile . return . unsafeTextToLine . T.pack $
            "pairCount,kvFactor,"
            ++ "c_mean,c_std,"
            ++ "hsPlain_mean,hsPlain_std,"
            ++ "hsStreamly_mean,hsStreamly_std,"
            ++ "hsPlain_div_c_mean,hsPlain_div_c_std,"
            ++ "hsStreamly_div_c_mean,hsStreamly_div_c_std,"
            ++ "hsStreamly_div_hsPlain_mean,hsStreamly_div_hsPlain_std"

        let mb = 1000000.0 :: Double
        let pairCounts = [1000000 :: Int, 10000000]
        let kvFactors = [1 :: Int, 2]
        forM_ pairCounts $ \pairCount -> forM_ kvFactors $ \kvFactor -> do
            let dbPath = format (""%fp%"/test_"%d%"_"%d%"") tmpDir pairCount kvFactor
            let chunkSize :: Int = round $ mb / (8 * fromIntegral kvFactor + 8 * fromIntegral kvFactor)
            let args  = format ("write "%s%" "%d%" "%d%" "%d%" "%d%"") dbPath kvFactor kvFactor pairCount chunkSize
            let warmCount = 1
            let timeCount = 5
            procs "echo" [format ("    Writing database "%s%" with C...") dbPath] empty
            cTimes <- timeCommand (Just dbPath) (format (""%s%" "%s%"") c_executable args)
                        [] warmCount timeCount
            procs "echo" [format ("    Writing database "%s%" with Haskell (plain)...") dbPath] empty
            hsPlainTimes <- timeCommand (Just dbPath) (format (""%s%" "%s%"") hs_plain_executable args)
                        [] warmCount timeCount
            procs "echo" [format ("    Writing database "%s%" with Haskell (streamly)...") dbPath] empty
            hsStreamlyTimes <- timeCommand (Just dbPath) (format (""%s%" "%s%"") hs_streamly_executable args)
                        [] warmCount timeCount

            let hsPlainDivCTimes = [a / b | a <- hsPlainTimes, b <- cTimes]
            let hsStreamlyDivCTimes = [a / b | a <- hsStreamlyTimes, b <- cTimes]
            let hsStreamlyDivHsPlainTimes = [a / b | a <- hsStreamlyTimes, b <- hsPlainTimes]

            let mean' :: [NominalDiffTime] -> Double
                mean' xs = mean . V.fromList $ map realToFrac xs

            let std' :: [NominalDiffTime] -> Double
                std' xs = stdDev . V.fromList $ map realToFrac xs

            let line = intercalate "," $ [show pairCount, show kvFactor] ++ map show [
                    mean' cTimes, std' cTimes,
                    mean' hsPlainTimes, std' hsPlainTimes,
                    mean' hsStreamlyTimes, std' hsStreamlyTimes,
                    mean' hsPlainDivCTimes, std' hsPlainDivCTimes,
                    mean' hsStreamlyDivCTimes, std' hsStreamlyDivCTimes,
                    mean' hsStreamlyDivHsPlainTimes, std' hsStreamlyDivHsPlainTimes ]
            append csvFile . return . unsafeTextToLine . T.pack $ line
            procs "echo" [format ("    Appended results to "%s%"") csvFile] empty
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
    if all (`T.isInfixOf` out) expectedInOut then
        return t
    else
        error $ "all of " ++ show expectedInOut ++ " were not found in output "
            ++ show out ++ " of command " ++ show cmdAndArgs

timeCommand :: Maybe T.Text -> Text -> [Text] -> Int -> Int -> IO [NominalDiffTime]
timeCommand forceRemovalPath cmdAndArgs expectedInOut warmCount timeCount = do
    let removePath = maybe (return ()) (\fp' -> shells (format ("rm -rf "%s%"") fp') empty) forceRemovalPath
    let echoCount c = procs "echo" ["-n", format (""%d%" ") c] empty
    when (warmCount > 0) $ do
        procs "echo" ["-n", "        Warming up... "] empty
        forM_ [1..warmCount] $ \c ->
            echoCount c >> removePath >> timeCommand' cmdAndArgs expectedInOut
        echo "done."
    procs "echo" ["-n", "        Timing... "] empty
    times <- forM [1..timeCount] $ \c ->
        echoCount c >> removePath >> timeCommand' cmdAndArgs expectedInOut
    removePath
    echo "done."
    return times
