#!/usr/bin/env bash

#To debug start with: 
#> run.sh <step> 
#to execute only the specified step

#EXECUTION Plan:
#step 0.  clean/create MH_DIR	:	hadoop fs rm/mkdir MH_DIR
#step 1.  hive q20.sql		:	Run hive querys to extract kmeans input data
#step 2.  mahout		:	Generating sparse vectors
#step 3.  mahout		:	Calculating k-means"
#step 4.  mahout		:	Converting result
#step 5.  hadoop fs result	: 	copy result do hdfs query result folder
#step 6.  hive && hdfs 		:	cleanup.sql && hadoop fs rm MH_DIR

QUERY_NUM="q20"
QUERY_DIRNAME="${BIG_BENCH_QUERIES_DIR}/${QUERY_NUM}"
MH_DIR=$BIG_BENCH_HDFS_ABSOLUTE_TEMP_DIR/$QUERY_NUM

resultTableName=${hiveconf:QUERY_NUM}result;
resultDIR=${env:BIG_BENCH_HDFS_ABSOLUTE_QUERY_RESULT_DIR}/${hiveconf:resultTableName};

if [ $# -lt 1 ] || [ $1 -eq 0 ] ; then
	echo "========================="
	echo "$QUERY_NUM ..initialize"
	echo "========================="
	hadoop fs -rm -r $MH_DIR
	hadoop fs -mkdir $MH_DIR
fi

if [ $# -lt 1 ] || [ $1 -eq 1 ] ; then
	echo "========================="
	echo "$QUERY_NUM Step 1/6: Executing hive queries"
	echo "========================="

	# Write input for k-means into ctable
	hive -hiveconf MH_DIR=${MH_DIR}/ctable -f ${QUERY_DIRNAME}/q20.sql
	#hadoop fs -cp $BIG_BENCH_HDFS_ABSOLUTE_TEMP_DIR/ctable/000000_0 $MH_DIR
fi

if [ $# -lt 1 ] || [ $1 -eq 2 ] ; then
	echo "========================="
	echo "$QUERY_NUM Step 2/6: Generating sparse vectors"
	echo "Command "mahout org.apache.mahout.clustering.conversion.InputDriver -i ${MH_DIR}/ctable -o $MH_DIR/Vec  -v org.apache.mahout.math.RandomAccessSparseVector #-c UTF-8 
	echo "========================="

	mahout org.apache.mahout.clustering.conversion.InputDriver -i ${MH_DIR}/ctable -o $MH_DIR/Vec -v org.apache.mahout.math.RandomAccessSparseVector #-c UTF-8 
fi

#if [ $# -lt 1 ] || [ $1 -eq 3 ] ; then
	#echo "========================="
	#echo "$QUERY_NUM: Generating vectors"
	#echo "Command "mahout seq2sparse -i $MH_DIR/Seq -o $MH_DIR/Vec -seq -ow
	#echo "========================="

	#mahout seq2sparse -i $MH_DIR/Seq -o $MH_DIR/Vec -seq -ow # --maxDFPercent 85 --namedVector
#fi

if [ $# -lt 1 ] || [ $1 -eq 3 ] ; then
	echo "========================="
	echo "$QUERY_NUM Step 3/6: Calculating k-means"
	echo "Command "mahout kmeans -i $MH_DIR/Vec/ -c $MH_DIR/kmeans-clusters -o $MH_DIR/results -dm org.apache.mahout.common.distance.CosineDistanceMeasure -x 10 -k 4 -ow -cl
	echo "========================="

	mahout kmeans -i $MH_DIR/Vec -c $MH_DIR/kmeans-clusters -o $MH_DIR/results -dm org.apache.mahout.common.distance.CosineDistanceMeasure -x 10 -k 8 -ow -cl
fi

if [ $# -lt 1 ] || [ $1 -eq 4 ] ; then
	echo "========================="
	echo "$QUERY_NUM Step 4/6: Converting result"
	echo "Command "mahout seqdump -i $MH_DIR/Vec/ -c $MH_DIR/kmeans-clusters -o $MH_DIR/results -dm org.apache.mahout.common.distance.CosineDistanceMeasure -x 10 -k 8 -ow -cl
	echo "========================="

	#mahout seqdump -i $MH_DIR/Vec/ -c $MH_DIR/kmeans-clusters -o $MH_DIR/results -dm org.apache.mahout.common.distance.CosineDistanceMeasure -x 10 -k 8 -ow -cl
	#TODO copy result to ${BIG_BENCH_HDFS_ABSOLUTE_QUERY_RESULT_DIR}/${QUERY_NUM}result"
fi

if [ $# -lt 1 ] || [ $1 -eq 5 ] ; then
	echo "========================="
	echo "$QUERY_NUM Step 5/6: Copy results to: $resultDIR"
	echo "========================="

fi

if [ $# -lt 1 ] || [ $1 -eq 6 ] ; then
	echo "========================="
	echo "$QUERY_NUM Step 6/6: Clean up"
	echo "========================="
	hive -f ${QUERY_DIRNAME}/cleanup.sql
	hadoop fs -rm -r $MH_DIR
fi


echo "======= $QUERY_NUM  result ======="
echo "results in: ${BIG_BENCH_HDFS_ABSOLUTE_QUERY_RESULT_DIR}/${QUERY_NUM}result"
echo "to display : hadoop fs -cat ${BIG_BENCH_HDFS_ABSOLUTE_QUERY_RESULT_DIR}/${QUERY_NUM}result/*"
echo "========================="

#TODO
# all columns need to be extracted from this output file and formatted to specification
#$mahout seqdumper -s /mahout_io/twenty/20-kmeans/clusteredPoints/part-m-00000 > ~/mahout_result/twenty/cluster-points.txt

