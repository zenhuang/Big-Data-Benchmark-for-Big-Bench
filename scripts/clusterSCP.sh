#!/usr/bin/env bash
#source "${BIG_BENCH_BASH_SCRIPT_DIR}/bigBenchEnvironment.sh"

IFS=$'\n' IPs=($(cat "${BIG_BENCH_NODES}"))
NODE_COUNT=${#IPs[@]}
SELF=`hostname -s`

for (( i=0; i<${NODE_COUNT}; i++ ));
do
  if [ $SELF != ${IPs[$i]} ]
  then
    echo "Copy to ${IPs[$i]}:"
    scp ${BIG_BENCH_SSH_OPTIONS} $1 ${IPs[$i]}:$2
  fi
done
