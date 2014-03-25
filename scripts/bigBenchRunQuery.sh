#!/usr/bin/env bash

if [ $# -lt 1 ]
then
	echo "parameter missing"
	echo "usage: <queryNumber e.g.: 1  or 22 >  "
	exit 1
fi



if [ $1 -lt 10 ]
then
	
	QUERY_NAME=q0$1	
else
	QUERY_NAME=q$1	
fi	


	
LOG_DIR_NAME="$BIG_BENCH_HOME/logs"
LOG_FILE_NAME="$LOG_DIR_NAME/$QUERY_NAME.log"

if [ ! -d $LOG_DIR_NAME ]; then
	mkdir -d "$LOG_DIR_NAME"
fi

echo "==============================================="
echo "Running query: $QUERY_NAME"
echo "log: $LOG_FILE_NAME"
echo "==============================================="	

time ("$BIG_BENCH_QUERIES_DIR/$QUERY_NAME/run.sh" ; echo  "======= $QUERY_NAME  time =========") > >(tee -a $LOG_FILE_NAME) 2>&1 
echo "==========================="

cat $LOG_FILE_NAME >> $BIG_BENCH_HOME/logs/allQueries.log





