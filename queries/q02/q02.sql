add file ${env:BIG_BENCH_HIVE_LIBS}/hive-contrib.jar;
add file ${env:BIG_BENCH_QUERIES_DIR}/Resources/bigbenchqueriesmr.jar;

set QUERY_NUM=q02;
set resultTableName=${hiveconf:QUERY_NUM}result;
set resultFile=${env:BIG_BENCH_HDFS_ABSOLUTE_QUERY_RESULT_DIR}/${hiveconf:resultTableName};

--CREATE RESULT TABLE. Store query result externaly in output_dir/qXXresult/
DROP TABLE IF EXISTS ${hiveconf:resultTableName};
CREATE TABLE ${hiveconf:resultTableName}
ROW FORMAT
DELIMITED FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION '${hiveconf:resultFile}' 
AS
-- Beginn: the real query part
SELECT  pid1, pid2, COUNT (*) AS cnt
FROM (
	--Make items basket
	FROM (
		-- Select predicate
		FROM (
		        SELECT wcs_user_sk AS cid , wcs_item_sk AS pid
		        FROM web_clickstreams
		        WHERE wcs_item_sk IS NOT NULL AND wcs_user_sk IS NOT NULL

		) temp
		MAP temp.cid, temp.pid
		USING 'cat'
		AS cid, pid
		CLUSTER BY cid
	) map_output
	REDUCE map_output.cid, map_output.pid
	USING 'java -cp bigbenchqueriesmr.jar:hive-contrib.jar de.bankmark.bigbench.queries.q02.Red'
	AS (pid1 BIGINT, pid2 BIGINT)
) temp_basket
WHERE pid1 = 1416
GROUP BY pid1, pid2
ORDER BY pid1 ,cnt ,pid2
LIMIT 30;
