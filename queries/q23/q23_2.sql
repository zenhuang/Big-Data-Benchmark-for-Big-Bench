set QUERY_NUM=q23;
set resultTableName1=${hiveconf:QUERY_NUM}result1;
set resultFile1=${env:BIG_BENCH_HDFS_ABSOLUTE_QUERY_RESULT_DIR}/${hiveconf:resultTableName1};


--- RESULT PART 1--------------------------------------
--CREATE RESULT TABLE. Store query result externaly in output_dir/qXXresult/
DROP TABLE IF EXISTS ${hiveconf:resultTableName1};
CREATE TABLE ${hiveconf:resultTableName1}
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n'
STORED AS TEXTFILE LOCATION '${hiveconf:resultFile1}' 
AS
-- Beginn: the real query part
SELECT inv1.w_warehouse_sk AS inv1_w_warehouse_sk, 
	inv1.i_item_sk AS inv1_i_item_sk,
	inv1.d_moy AS inv1_d_moy,
       	inv1.mean AS inv1_mean, 
	inv1.cov AS inv1_cov, 
	inv2.w_warehouse_sk AS inv2_w_warehouse_sK,
       	inv2.i_item_sk AS inv2_i_item_sk, 
	inv2.d_moy AS inv2_d_moy, 
	inv2.mean AS inv2_mean, 
	inv2.cov AS inv2_cov
	FROM q23_tmp_inv inv1 
	JOIN q23_tmp_inv inv2  	ON inv1.i_item_sk = inv2.i_item_sk 
   				AND inv1.w_warehouse_sk = inv2.w_warehouse_sk
	WHERE inv1.d_moy=1 
	AND inv2.d_moy=1+1

ORDER BY 	inv1_w_warehouse_sk, 
		inv1_i_item_sk, 
		inv1_d_moy,
	        inv1_mean, 
		inv1_cov, 
		inv2_d_moy, 
		inv2_mean, 
		inv2_cov;
