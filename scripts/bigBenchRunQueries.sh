#!/usr/bin/env bash

echo "==============================================="
echo "Running queries."
echo "logging run to: $BIG_BENCH_HOME/logs/allQueries.log"
echo "==============================================="

for (( i=1; i <=30; i++ ))
do

	$BIG_BENCH_BASH_SCRIPT_DIR/bigBenchRunQuery.sh ${i}

done

echo "==============================================="
echo "All queries finished"
echo "A log of this run is in: $BIG_BENCH_HOME/logs/allQueries.log"
echo "==============================================="
$BIG_BENCH_BASH_SCRIPT_DIR/showQueryErrors.sh 


