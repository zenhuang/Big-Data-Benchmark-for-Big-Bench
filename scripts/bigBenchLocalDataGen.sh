#!/usr/bin/env bash
#source "${BIG_BENCH_BASH_SCRIPT_DIR}/bigBenchEnvironment.sh"

java -jar ${BIG_BENCH_DATA_GENERATOR_DIR}/pdgf.jar -c -s "$@"
