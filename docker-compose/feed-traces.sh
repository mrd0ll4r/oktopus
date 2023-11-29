#!/bin/bash -e

# Script to feed a sample of CIDs from a given file into the indexer infrastructure.
# Needs to be executed from the root directory of the indexer (containing an .env file)
# Calls ./out/post-cids to post the CIDs to RabbitMQ.

input_file=$1
target_rate=$2

echo "Will feed $target_rate of $input_file..."

if [ ! -e "$input_file" ]; then
	echo "$input_file does not exist, quitting"
	exit 1
else
	echo "reading from $input_file..."
fi

num_lines_total=$(zcat "$input_file" | wc -l)
num_lines_sampled=$(echo "a=$num_lines_total*$target_rate; scale=0; a/1" | bc)

echo "Input file has $num_lines_total CIDs, will select and post $num_lines_sampled out of those"

zcat $input_file | shuf | head -n $num_lines_sampled | ./out/post-cids
