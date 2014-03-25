#!/usr/bin/env bash

QUERY_NUM="q25"
DIRNAME="${BIG_BENCH_QUERIES_DIR}/${QUERY_NUM}"
MH_DIR=$BIG_BENCH_HDFS_ABSOLUTE_TEMP_DIR/$QUERY_NUM

resultTableName=${QUERY_NUM}result;
resultDir=${BIG_BENCH_HDFS_ABSOLUTE_QUERY_RESULT_DIR}/${resultTableName};

echo "========================="
echo "Prepare temp dir"
echo "========================="
hadoop fs -rm -r "$MH_DIR" &
hadoop fs -rm -r "$resultDir" &
wait
hadoop fs -mkdir "$MH_DIR" &
hadoop fs -mkdir "$resultDir" &
wait

echo "========================="
echo "Executing hive queries"
echo "========================="

# Write input for k-means into ctable
hive -hiveconf MH_DIR=${MH_DIR}/ctable -f ${DIRNAME}/q25.sql

#hadoop fs -cp $BIG_BENCH_HDFS_ABSOLUTE_TEMP_DIR/ctable/000000_0 $MH_DIR

echo "========================="
echo "Generating sparse vectors"
echo "Command "mahout org.apache.mahout.clustering.conversion.InputDriver -i ${MH_DIR}/ctable -o $MH_DIR/Vec  -v org.apache.mahout.math.RandomAccessSparseVector #-c UTF-8 
echo "========================="

mahout org.apache.mahout.clustering.conversion.InputDriver -i ${MH_DIR}/ctable -o $MH_DIR/Vec -v org.apache.mahout.math.RandomAccessSparseVector #-c UTF-8 

#echo "========================="
#echo "Generating vectors"
#echo "Command "mahout seq2sparse -i $MH_DIR/Seq -o $MH_DIR/Vec -seq -ow
#echo "========================="

#mahout seq2sparse -i $MH_DIR/Seq -o $MH_DIR/Vec -seq -ow # --maxDFPercent 85 --namedVector

echo "========================="
echo "Calculating k-means"
echo "Command "mahout kmeans -i $MH_DIR/Vec -c $MH_DIR/kmeans-clusters -o $MH_DIR/results -dm org.apache.mahout.common.distance.CosineDistanceMeasure -x 10 -k 8 -ow -cl
echo "========================="

mahout kmeans -i $MH_DIR/Vec -c $MH_DIR/init-clusters -o $MH_DIR/kmeans-clusters -dm org.apache.mahout.common.distance.CosineDistanceMeasure -x 10 -k 8 -ow -cl

echo "========================="
echo "Converting result"
echo "Command "mahout clusterdump -i $MH_DIR/kmeans-clusters/clusters-*-final -o hdfs://$BIG_BENCH_HDFS_ABSOLUTE_QUERY_RESULT_DIR/q25result/result.txt -dm org.apache.mahout.common.distance.CosineDistanceMeasure -of TEXT
echo "========================="

mahout clusterdump -i $MH_DIR/kmeans-clusters/clusters-*-final  -dm org.apache.mahout.common.distance.CosineDistanceMeasure -of TEXT > hdfs://$BIG_BENCH_HDFS_ABSOLUTE_QUERY_RESULT_DIR/q25result/result.txt

#TODO copy result to ${BIG_BENCH_HDFS_ABSOLUTE_QUERY_RESULT_DIR}/${QUERY_NUM}result"


echo "========================="
echo "Clean up"
echo "========================="
hive -f ${DIRNAME}/cleanup.sql
hadoop fs -rm -r $MH_DIR

echo "======= $QUERY_NUM  result ======="
echo "results in: ${BIG_BENCH_HDFS_ABSOLUTE_QUERY_RESULT_DIR}/${QUERY_NUM}result"
echo "to display : hadoop fs -cat ${BIG_BENCH_HDFS_ABSOLUTE_QUERY_RESULT_DIR}/${QUERY_NUM}result/*"
echo "========================="


#TODO

#$hadoop fs -mkdir /mahout_io/twenty_five
#$hadoop fs -cp /user/hive/warehouse/ctable/000000_0 /mahout_io/twenty_five

# run mahout k-means on cluster
#$mahout seqdirectory -i /mahout_io/twenty_five -o /mahout_io/twenty_five/25-seqdir -c UTF-8 -chunk 5 

#$mahout seq2sparse -i /mahout_io/twenty_five/25-seqdir -o /mahout_io/twenty_five/25-seqdir-sparse-kmeans --maxDFPercent 85 --namedVector

# number of k = 8 accroding to teradata query specification
#$mahout kmeans -i /mahout_io/twenty_five/25-seqdir-sparse-kmeans/tfidf-vectors/ -c /mahout_io/twenty_five/25-kmeans-clusters -o /mahout_io/twenty_five/25-kmeans -dm org.apache.mahout.common.distance.CosineDistanceMeasure -x 10 -k 8 -ow --clustering -cl

# making local directory for cluster_result
# ..need to change this, want to store output and process it in cluster!
#mkdir ~/mahout_result/twenty_five

# all columns need to be extracted from this output file and formatted to specification
#$mahout seqdumper -s /mahout_io/twenty_five/25-kmeans/clusteredPoints/part-m-00000 > ~/mahout_result/twenty_five/cluster-points.txt

#$hadoop fs -rm -r /mahout_io/twenty_five

