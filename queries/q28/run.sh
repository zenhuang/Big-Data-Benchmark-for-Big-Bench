#!/usr/bin/env bash

QUERY_NUM="q28"
DIRNAME="${BIG_BENCH_QUERIES_DIR}/${QUERY_NUM}"
MH_DIR=$BIG_BENCH_HDFS_ABSOLUTE_TEMP_DIR/$QUERY_NUM

echo "========================="
echo "$QUERY_NUM step 1/8: prepare/initialize"
echo "========================="
hadoop fs -rm -r "$MH_DIR"
hadoop fs -mkdir "$MH_DIR"

echo "========================="
echo "$QUERY_NUM step 2/8: Executing hive queries"
echo "========================="

# Write input for k-means into ctable
hive -hiveconf MH_DIR=${MH_DIR}/ctable -f ${DIRNAME}/q28.sql


echo "========================="
echo "$QUERY_NUM step 3/8: Generating sequence files"
echo "Used Command: "mahout seqdirectory -i $MH_DIR/ctable -o $MH_DIR/Vec1 -seq -ow
echo "========================="

#Java heap space error
#mahout seqdirectory -i $MH_DIR/ctable -o $MH_DIR/Seq1 -ow
#mahout seqdirectory -i $MH_DIR/ctable2 -o $MH_DIR/Seq2 -ow

echo "========================="
echo "$QUERY_NUM step 4/8: Generating vectors"
echo "Used Command: "mahout seq2sparse -i $MH_DIR/Seq -o $MH_DIR/Vec -seq -ow
echo "========================="

#Java heap space error
##mahout seq2sparse -i $MH_DIR/Seq1 -o $MH_DIR/Vec1 -seq -ow 
##mahout seq2sparse -i $MH_DIR/Seq2 -o $MH_DIR/Vec2 -seq -ow 


echo "========================="
echo "$QUERY_NUM step 5/8: Training Classifier"
echo "Used Command: "mahout trainnb -i $MH_DIR/Vec1/tfidf-vectors -o $MH_DIR/model -l key,rating,item,content -li $MH_DIR/label -ow
echo "========================="

#mahout trainnb -i $MH_DIR/Vec1/tfidf-vectors -o $MH_DIR/model -l key,rating,item,content -ow

echo "========================="
echo "$QUERY_NUM step 6/8: Testing Classifier"
echo "Used Command: "mahout testnb -i $MH_DIR/Vec2/tfidf-vectors -­m $MH_DIR/model -l $MH_DIR/label -o $MH_DIR/result -ow
echo "========================="

#mahout testnb -i $MH_DIR/Vec2/tfidf-vectors -­m $MH_DIR/model -l $MH_DIR/label -o $MH_DIR/result -ow
echo "========================="
echo "$QUERY_NUM step 7/8: write result to hdfs"
echo "=========================
#TODO copy result to ${BIG_BENCH_HDFS_ABSOLUTE_QUERY_RESULT_DIR}/${QUERY_NUM}result"


echo "========================="
echo "$QUERY_NUM step 8/8: Clean up"
echo "========================="
hive -f ${DIRNAME}/cleanup.sql
hadoop fs -rm -r "$MH_DIR"

echo "======= $QUERY_NUM  result ======="
echo "results in: ${BIG_BENCH_HDFS_ABSOLUTE_QUERY_RESULT_DIR}/${QUERY_NUM}result"
echo "to display : hadoop fs -cat ${BIG_BENCH_HDFS_ABSOLUTE_QUERY_RESULT_DIR}/${QUERY_NUM}result/*"
echo "========================="

