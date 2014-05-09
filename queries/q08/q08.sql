--ADD FILE ${env:BIG_BENCH_QUERIES_DIR}/q08/q8_mapper.py;
ADD FILE ${env:BIG_BENCH_QUERIES_DIR}/q08/q8_reducer.py;


DROP TABLE if EXISTS sales_review;
DROP TABLE if EXISTS clicks;

--Prepare result storage
set QUERY_NUM=q08;
set resultTableName1=${hiveconf:QUERY_NUM}result1;
set resultTableName2=${hiveconf:QUERY_NUM}result2;
set resultFile1=${env:BIG_BENCH_HDFS_ABSOLUTE_QUERY_RESULT_DIR}/${hiveconf:resultTableName1};
set resultFile2=${env:BIG_BENCH_HDFS_ABSOLUTE_QUERY_RESULT_DIR}/${hiveconf:resultTableName2};


--PART 1 -------------------------------------
CREATE TABLE clicks AS SELECT * FROM (
	SELECT c.wcs_item_sk AS item, c.wcs_user_sk AS uid, c.wcs_click_date_sk AS c_date, c.wcs_click_time_sk AS c_time, c.wcs_sales_sk AS sales_sk, w.wp_type AS wpt
	FROM web_clickstreams c 
	INNER JOIN web_page w ON c.wcs_web_page_sk = w.wp_web_page_sk AND c.wcs_user_sk is not null
) t1;

--PART 1 result  -------------------------------------
--CREATE RESULT TABLE. Store query result externaly in output_dir/qXXresult/
DROP TABLE IF EXISTS ${hiveconf:resultTableName1};
CREATE TABLE ${hiveconf:resultTableName1}
ROW FORMAT
DELIMITED FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION '${hiveconf:resultFile1}' 
AS
-- Beginn: the real query part
SELECT * FROM clicks limit 500;

CREATE TABLE sales_review AS SELECT * FROM (
	--SELECT nPath.s_sk AS s_sk
	SELECT *	
	FROM (
		FROM (
			FROM clicks
			MAP clicks.item, clicks.uid, clicks.c_date, clicks.c_time, clicks.wpt
			-- USING 'python q8_mapper.py' || NO-OP mapper 
			USING 'cat' 
			AS item, uid, c_date, c_time, wpt
			CLUSTER BY uid
		) map_output
		REDUCE map_output.item, map_output.uid, map_output.c_date, map_output.c_time, map_output.wpt
		USING 'python q8_mapper.py'
		--AS (s_date BIGINT, s_sk STRING)
		AS(item STRING, uid STRING, c_date STRING, c_time STRING, wpt STRING)
	) nPath
	--where nPath.s_date > 2451424 and nPath.s_date <2451424+365
) t2;

--PART 2 -------------------------------------
--CREATE RESULT TABLE. Store query result externaly in output_dir/qXXresult/
DROP TABLE IF EXISTS ${hiveconf:resultTableName2};
CREATE TABLE ${hiveconf:resultTableName2}
ROW FORMAT
DELIMITED FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION '${hiveconf:resultFile2}' 
AS
-- Beginn: the real query part
SELECT * FROM sales_review limit 500;


-- cleanup -------------------------------------
DROP TABLE if EXISTS sales_review;
DROP TABLE if EXISTS clicks;


