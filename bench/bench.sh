#!/usr/bin/env bash

#-------------------------------------------------------------------------------
# Utility functions.

cmd_fail() {
    if [[ $? != 0 ]]; then
        echo "failed."
        exit 1
    fi
}

cmd_result() {
    cmd_fail
    echo "success."
}

remove_if_exists() {
    local path="$1"

    local type="item"
    if [[ -f $path ]]; then
        type="file"
    elif [[ -d $path ]]; then
        type="directory"
    fi

    if [[ -e $path ]]; then
        echo -n "Removing existing $type ${path##*/}... "
        rm -r "$path"
        cmd_result
    fi
}

remove_test_dir() {
    local removal_dir="$1"
    if [[ -d $removal_dir ]]; then
        if [[ $removal_dir == "./tmp"* ]]; then
            rm -rf $removal_dir
        else
            echo "Directory outside of ./tmp not removed. $removal_dir"
            exit 1
        fi
    fi
}

echo_hrule() {
    echo "----------------------------------------"
}

# Appends "{minimum time},{maximum time},{average time}" to the given output file.
time_executable() {
    local warmup_count="$1"
    local avg_count="$2"
    local expected_in_output="$3"
    local removal_dir="$4"
    local output_file="$5"
    local executable="$6"
    local arguments="${@:7}"
    if [[ $warmup_count != 0 ]]; then
        echo -n "        Warming up... "
        for j in $(seq 1 $warmup_count); do
            echo -n "$j "
            remove_test_dir $removal_dir
            $executable $arguments > /dev/null
            if [[ $? != 0 ]]; then echo "failed."; exit 1; fi
        done
        echo "done."
    fi
    echo -n "        Timing... "
    times="["
    for j in $(seq 1 $avg_count); do
        echo -n "$j "
        remove_test_dir $removal_dir
        gtime_executable=""
        if [[ $(uname) = *Linux* ]]; then
            gtime_executable="/usr/bin/time"
        elif [[ $(uname) = *Darwin* ]]; then
            gtime_executable="gtime"
        fi
        output=$($gtime_executable -f '_time%e' $executable $arguments 2>&1)
        if [[ $output != *"$expected_in_output"* ]]; then
            echo "Error: $expected_in_output not found in output: $output"; exit 1
        fi
        seconds=$(echo "$output" | pcregrep -o1 '_time(.+)$')
        times="$times$seconds,"
    done
    echo "done."
    times=${times%,}"]"
    min_time=$(echo "print float(min($times))" | python)
    max_time=$(echo "print float(max($times))" | python)
    avg_time=$(echo "print float(sum($times))/len($times)" | python)
    echo -n "$min_time,$max_time,$avg_time" >> $output_file
}

#-------------------------------------------------------------------------------

if [[ ! -f ./bench-lmdb.c ]]; then
    echo "Please run script from its own directory."
    exit 1
fi

if [[ $(uname) = *Linux* ]]; then
    export LC_ALL="en_US.UTF-8"
fi

logfile="./tmp/bench.sh.log"
remove_if_exists $logfile

echo -n "Compiling C program... "
mkdir -p ./tmp
gcc -O2 -Wall bench-lmdb.c -llmdb -o bench-lmdb >> $logfile 2>&1
cmd_result

echo -n "Compiling Haskell programs... "
stack build >> $logfile 2>&1
cmd_result

c_executable="./bench-lmdb"
hs_plain_executable=$(stack exec -- which bench-lmdb-plain 2> /dev/null)
hs_streamly_executable=$(stack exec -- which bench-lmdb-streamly 2> /dev/null)

mb=1000000.0

echo_hrule

#-------------------------------------------------------------------------------

read -p "Perform read benchmarks? (y/n) " answer
if [[ $answer == y ]]; then
    echo "Creating databases using C program (unless they already exist)..."
    tmp_dir="./tmp/read"
    mkdir -p $tmp_dir
    for pair_count in 31250000 62500000 125000000; do
        for kv_factor in 1 2; do
            db_path="$tmp_dir/test_${pair_count}_$kv_factor"
            existing_db_pair_count=$(mdb_stat -e $db_path 2> /dev/null | pcregrep -o1 'Entries: (.+)$')
            if [[ $existing_db_pair_count = $pair_count ]]; then
                echo "    Database $db_path already exists."
            else
                echo -n "    Creating database $db_path... "
                rm -r $db_path 2> /dev/null
                chunk_size=$(echo "print int($mb / (8*$kv_factor + 8*$kv_factor))" | python)
                $c_executable write $db_path $kv_factor $kv_factor $pair_count $chunk_size
                cmd_result
            fi
        done
    done

    echo "Measuring read-cursor..."

    csv_file=$tmp_dir/read-cursor.csv
    rm -f $csv_file
    echo -n "pairCount,kvFactor," >> $csv_file
    echo -n "c_mean,c_std," >> $csv_file
    echo -n "hsPlain_mean,hsPlain_std," >> $csv_file
    echo -n "hsStreamly_mean,hsStreamly_std," >> $csv_file
    echo -n "hsStreamlySafe_mean,hsStreamlySafe_std," >> $csv_file
    echo -n "hsPlain_diff_c_mean,hsPlain_diff_c_std," >> $csv_file
    echo -n "hsStreamly_diff_hsPlain_mean,hsStreamly_diff_hsPlain_std," >> $csv_file
    echo -n "hsStreamlySafe_diff_hsStreamly_mean,hsStreamlySafe_diff_hsStreamly_std" >> $csv_file
    echo "" >> $csv_file
    for pair_count in 31250000 62500000 125000000; do
        for kv_factor in 1 2; do
            db_path="$tmp_dir/test_${pair_count}_$kv_factor"
            echo -n "$pair_count,$kv_factor," >> $csv_file
            echo "    Timing database $db_path with C..."
            time_executable 2 5 "Pair count:       $pair_count" "" $csv_file $c_executable read-cursor $db_path
            echo -n "," >> $csv_file
            echo "    Timing database $db_path with Haskell (plain)..."
            time_executable 2 5 "Pair count:       $pair_count" "" $csv_file $hs_plain_executable read-cursor $db_path
            echo -n "," >> $csv_file
            echo "    Timing database $db_path with Haskell (streamly)..."
            time_executable 2 5 "Pair count:       $pair_count" "" $csv_file $hs_streamly_executable read-cursor $db_path
            echo -n "," >> $csv_file
            echo "    Timing database $db_path with Haskell (streamly, safe)..."
            time_executable 2 5 "Pair count:       $pair_count" "" $csv_file $hs_streamly_executable read-cursor-safe $db_path
            echo "" >> $csv_file
            nlines=$(wc -l < $csv_file)
            awk_prog="function f(x){return x/$pair_count*1e9;} " # Turns seconds into nanoseconds per pair.
            awk_prog=$awk_prog"(NR!=$nlines)"'{print $0} '
            awk_prog=$awk_prog"(NR==$nlines)"'{print $1 "," $2 "," f($3) "," f($4) "," f($5) "," f($6) "," f($7) "," f($8) "," f($9) "," f($10) "," f($11) "," f($12) "," f($13) "," f($14)}'
            awk -F , "$awk_prog" $csv_file > tmp_file; mv tmp_file $csv_file
            awk_prog="(NR!=$nlines)"'{print $0} '
            awk_prog=$awk_prog"(NR==$nlines)"'{print $0 "," ($6-$4) "," ($7-$3) "," ($8-$5) "," ($9-$7) "," ($10-$6) "," ($11-$8) "," ($12-$10) "," ($13-$9) "," ($14-$11)}'
            awk -F , "$awk_prog" $csv_file > tmp_file; mv tmp_file $csv_file
            echo "    Appended results to $csv_file"
        done
    done
else
    echo "Skipping read benchmarks."
fi

echo_hrule

#-------------------------------------------------------------------------------

read -p "Perform write benchmarks? (y/n) " answer
if [[ $answer == y ]]; then
    echo "Measuring write..."

    tmp_dir="./tmp/write"
    rm -rf $tmp_dir
    mkdir -p $tmp_dir
    csv_file=$tmp_dir/write.csv
    echo -n "pair_count,kv_factor,c_min,c_max,c_avg," >> $csv_file
    echo -n "hs_plain_min,hs_plain_max,hs_plain_avg," >> $csv_file
    echo -n "hs_streamly_min,hs_streamly_max,hs_streamly_avg," >> $csv_file
    echo -n "hs_plain_avg/c_avg,hs_streamly_avg/c_avg,hs_streamly_avg/hs_plain_avg" >> $csv_file
    echo "" >> $csv_file
    for pair_count in 1000000 10000000; do
        for kv_factor in 1 2; do
            db_path="$tmp_dir/test_${pair_count}_$kv_factor"
            chunk_size=$(echo "print int($mb / (8*$kv_factor + 8*$kv_factor))" | python)
            echo -n "$pair_count,$kv_factor," >> $csv_file
            echo "    Writing database $db_path with C..."
            time_executable 1 5 "" $db_path $csv_file $c_executable write $db_path $kv_factor $kv_factor $pair_count $chunk_size
            echo -n "," >> $csv_file
            echo "    Writing database $db_path with Haskell (plain)..."
            time_executable 1 5 "" $db_path $csv_file $hs_plain_executable write $db_path $kv_factor $kv_factor $pair_count $chunk_size
            echo -n "," >> $csv_file
            echo "    Writing database $db_path with Haskell (streamly)..."
            time_executable 1 5 "" $db_path $csv_file $hs_streamly_executable write $db_path $kv_factor $kv_factor $pair_count $chunk_size
            echo "" >> $csv_file
            nlines=$(wc -l < $csv_file)
            awk_prog="(NR!=$nlines)"'{print $0} '
            awk_prog=$awk_prog"(NR==$nlines)"'{print $0 "," ($8/$5) "," ($11/$5) "," ($11/$8)}'
            awk -F , "$awk_prog" $csv_file > tmp_file; mv tmp_file $csv_file
            echo "    Appended results to $csv_file"
        done
    done
else
    echo "Skipping write benchmarks."
fi
