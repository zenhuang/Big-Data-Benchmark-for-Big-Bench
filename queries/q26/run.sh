#!/usr/bin/env bash

QUERY_NUM="q26"
DIRNAME="${BIG_BENCH_QUERIES_DIR}/${QUERY_NUM}"
MH_DIR=$BIG_BENCH_HDFS_ABSOLUTE_TEMP_DIR/$QUERY_NUM

resultTableName=${QUERY_NUM}result;
resultDir=${BIG_BENCH_HDFS_ABSOLUTE_QUERY_RESULT_DIR}/${resultTableName};

echo "========================="
echo "$QUERY_NUM step 1/6: Prepare temp dir"
echo "========================="
hadoop fs -rm -r "$MH_DIR" &
hadoop fs -rm -r "$resultDir" &
wait
hadoop fs -mkdir "$MH_DIR" &
hadoop fs -mkdir "$resultDir" &
wait

echo "========================="
echo "$QUERY_NUM step 2/6: Executing hive queries"
echo "========================="

# Write input for k-means into ctable
hive -hiveconf MH_DIR=${MH_DIR}/ctable -f ${DIRNAME}/q26.sql

#hadoop fs -cp $BIG_BENCH_HDFS_ABSOLUTE_TEMP_DIR/ctable/000000_0 $MH_DIR

echo "========================="
echo "$QUERY_NUM step 3/6: Generating sparse vectors"
echo "Command "mahout org.apache.mahout.clustering.conversion.InputDriver -i ${MH_DIR}/ctable -o $MH_DIR/Vec  -v org.apache.mahout.math.RandomAccessSparseVector #-c UTF-8 
echo "========================="

mahout org.apache.mahout.clustering.conversion.InputDriver -i ${MH_DIR}/ctable -o $MH_DIR/Vec -v org.apache.mahout.math.RandomAccessSparseVector #-c UTF-8 

#echo "========================="
#echo "Generating vectors"
#echo "Command "mahout seq2sparse -i $MH_DIR/Seq -o $MH_DIR/Vec -seq -ow
#echo "========================="

#mahout seq2sparse -i $MH_DIR/Seq -o $MH_DIR/Vec -seq -ow # --maxDFPercent 85 --namedVector

echo "========================="
echo "$QUERY_NUM step 4/6: Calculating k-means"
echo "Command "mahout kmeans -i $MH_DIR/Vec/ -c $MH_DIR/kmeans-clusters -o $MH_DIR/results -dm org.apache.mahout.common.distance.CosineDistanceMeasure -x 10 -k 4 -ow -cl
echo "========================="

mahout kmeans -i $MH_DIR/Vec -c $MH_DIR/kmeans-clusters -o $MH_DIR/results -dm org.apache.mahout.common.distance.CosineDistanceMeasure -x 10 -k 8 -ow -cl

echo "========================="
echo "$QUERY_NUM step 5/6: Converting result"
echo "Command "mahout seqdump -i $MH_DIR/Vec/ -c $MH_DIR/kmeans-clusters -o $MH_DIR/results -dm org.apache.mahout.common.distance.CosineDistanceMeasure -x 10 -k 8 -ow -cl
echo "========================="



echo "========================="
echo "$QUERY_NUM step 6/6: Copy result to hdfs:"
echo "========================="
#TODO copy result to ${BIG_BENCH_HDFS_ABSOLUTE_QUERY_RESULT_DIR}/${QUERY_NUM}result"




echo "========================="
echo "$QUERY_NUM step 1: Clean up"
echo "========================="

hive -f ${DIRNAME}/cleanup.sql
hadoop fs -rm -r $MH_DIR


echo "======= $QUERY_NUM  result ======="
echo "results in: ${BIG_BENCH_HDFS_ABSOLUTE_QUERY_RESULT_DIR}/${QUERY_NUM}result"
echo "to display : hadoop fs -cat ${BIG_BENCH_HDFS_ABSOLUTE_QUERY_RESULT_DIR}/${QUERY_NUM}result/*"
echo "========================="

#TODO
