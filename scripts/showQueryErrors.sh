#!/usr/bin/env bash

if [ $# -lt 1 ]
then
	
	ERRLOG_FILE_NAME="$BIG_BENCH_HOME/logs/queryErrors.log"

	grep -n -i "FAIL"       $BIG_BENCH_HOME/logs/q??.log > $ERRLOG_FILE_NAME
	grep -n -i "ERROR:" $BIG_BENCH_HOME/logs/q??.log >> $ERRLOG_FILE_NAME 
	grep -n -i "Could not"  $BIG_BENCH_HOME/logs/q??.log >> $ERRLOG_FILE_NAME 
	grep -n -i "Exception"  $BIG_BENCH_HOME/logs/q??.log >> $ERRLOG_FILE_NAME 
	grep -n -i "unexpected" $BIG_BENCH_HOME/logs/q??.log >> $ERRLOG_FILE_NAME 

	if [ -s "$ERRLOG_FILE_NAME" ]
	then
		echo "==============================================="
		echo "Errors in queries"
		echo "==============================================="
		cat $ERRLOG_FILE_NAME
	else
		echo "All queries ran successfully"
	fi

else
	if [ $1 -lt 10 ]
	then
	
		QUERY_NAME=q0$1	
	else
		QUERY_NAME=q$1	
	fi
	echo "==============================================="
	echo "Errors in query $QUERY_NAME"
	echo "grep from file:  $BIG_BENCH_HOME/logs/$QUERY_NAME.log "
	echo "==============================================="
	grep -n -i "FAIL"       $BIG_BENCH_HOME/logs/$QUERY_NAME.log 
	grep -n -i "ERROR:" $BIG_BENCH_HOME/logs/$QUERY_NAME.log  
	grep -n -i "Could not"  $BIG_BENCH_HOME/logs/$QUERY_NAME.log  
	grep -n -i "Exception"  $BIG_BENCH_HOME/logs/$QUERY_NAME.log 
	grep -n -i "unexpected" $BIG_BENCH_HOME/logs/$QUERY_NAME.log  
fi
