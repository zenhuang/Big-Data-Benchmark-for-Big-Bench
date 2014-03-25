add file ${env:BIG_BENCH_HIVE_LIBS}/hive-contrib.jar;
add file ${env:BIG_BENCH_QUERIES_DIR}/Resources/bigbenchqueriesmr.jar;

set QUERY_NUM=q01;
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
--Find the most frequent ones
SELECT	pid1, pid2, COUNT (*) AS cnt
FROM (
	--Make items basket
	FROM (
		-- Joining two tables
		FROM (
			SELECT s.ss_ticket_number AS oid , s.ss_item_sk AS pid
			FROM store_sales s
			INNER JOIN item i ON s.ss_item_sk = i.i_item_sk
			WHERE i.i_category_id in (1 ,2 ,3) and s.ss_store_sk in (10 , 20, 33, 40, 50)
		) temp_join
		MAP temp_join.oid, temp_join.pid
		USING 'cat'
		AS oid, pid 
		CLUSTER BY oid
	) map_output
	REDUCE map_output.oid, map_output.pid
	USING 'java -cp bigbenchqueriesmr.jar:hive-contrib.jar de.bankmark.bigbench.queries.q01.Red'
	AS (pid1 BIGINT, pid2 BIGINT)
) temp_basket
GROUP BY pid1, pid2
HAVING COUNT (pid1) > 49
ORDER BY pid1 ,cnt ,pid2;
