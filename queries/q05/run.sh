#!/usr/bin/env bash

QUERY_NUM="q05"
DIRNAME="${BIG_BENCH_QUERIES_DIR}/${QUERY_NUM}"
MH_DIR=$BIG_BENCH_HDFS_ABSOLUTE_TEMP_DIR/$QUERY_NUM

hadoop fs -mkdir $MH_DIR

## NOT FINISHED! mahout wrong


echo "========================="
echo "Executing hive queries"
echo "tmp result in: ${MH_DIR}/ctable"
echo "========================="

# Write input for k-means into ctable
hive -hiveconf MH_DIR=${MH_DIR}/ctable -f "${DIRNAME}/q05.sql"



echo "========================="
echo "Training sgd classifier"
echo "========================="

#mahout org.apache.mahout.classifier.sgd.TrainLogistic --input ${MH_DIR}/ctable  --output $MH_DIR/model.model --target books_interests --categories 2 --predictors c_customer_sk,  college_education, male, label --types n n --#passes 20 --features 20 --rate 1 --lambda 0.5 

echo "========================="
echo "Test model"
echo "========================="
#mahout org.apache.mahout.classifier.sgd.RunLogistic –input ${MH_DIR}/ctable –model $MH_DIR/model.model –auc –scores –confusion


#TODO display result


echo "========================="
echo "Clean up"
echo "========================="

hadoop fs -rm -r $MH_DIR/
