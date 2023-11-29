#!/bin/bash -e

input_dir=$1

if [[ ! -d "$input_dir" ]]; then
    echo "input directory does not exist, quitting"
    exit 1
fi

today=$(date '+%Y-%m-%d')
yesterday=$(date -d "1 day ago" '+%Y-%m-%d')

echo "It's now $(date -u), today is $today, yesterday was $yesterday, reading input file from $input_dir..."

# 5 Percent
target_rate="0.05"

./feed-traces.sh "$input_dir/$yesterday.cids.gz" $target_rate
