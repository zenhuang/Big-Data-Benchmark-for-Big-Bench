#!/usr/bin/env bash
#source "${BIG_BENCH_BASH_SCRIPT_DIR}/bigBenchEnvironment.sh"

IFS=$'\n' IPs=($(cat "${BIG_BENCH_NODES}"))
NODE_COUNT=${#IPs[@]}
SELF=`hostname -s`

for (( i=0; i<${NODE_COUNT}; i++ ));
do
#  if [ $SELF != ${IPs[$i]} ]
#  then
  echo "NODE: ${IPs[$i]}"
  ssh ${BIG_BENCH_SSH_OPTIONS} -t ${IPs[$i]} $@ 
#  fi
done
