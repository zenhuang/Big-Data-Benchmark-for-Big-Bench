ADD FILE ${env:BIG_BENCH_QUERIES_DIR}/q04/q4_mapper1.py;
ADD FILE ${env:BIG_BENCH_QUERIES_DIR}/q04/q4_mapper2.py;
ADD FILE ${env:BIG_BENCH_QUERIES_DIR}/q04/q4_reducer1.py;
ADD FILE ${env:BIG_BENCH_QUERIES_DIR}/q04/q4_reducer2.py;

set QUERY_NUM=q04;
set resultTableName=${hiveconf:QUERY_NUM}result;
set resultFile=${env:BIG_BENCH_HDFS_ABSOLUTE_QUERY_RESULT_DIR}/${hiveconf:resultTableName};


			  
DROP VIEW IF EXISTS sessions;
CREATE VIEW sessions AS 
SELECT * 
FROM (
	FROM (
		FROM (
			SELECT 	c.wcs_user_sk AS uid , 
				c.wcs_item_sk AS item , 
				w.wp_type AS wptype , 
				t.t_time+unix_timestamp(d.d_date,'yyyy-MM-dd') AS tstamp
                        FROM web_clickstreams c 
                        JOIN web_page w ON c.wcs_web_page_sk = w.wp_web_page_sk 
                        		AND c.wcs_user_sk IS NOT NULL
                        JOIN date_dim d ON c.wcs_click_date_sk = d.d_date_sk
                        JOIN time_dim t ON c.wcs_click_time_sk = t.t_time_sk
		) select_temp
		MAP select_temp.uid, select_temp.item, select_temp.wptype, select_temp.tstamp
		-- USING 'python q4_mapper1.py' AS uid, item, wptype, tstamp  ||| use cat instead beacause this is a no-op mapper
		USING 'cat' AS uid, item, wptype, tstamp
		CLUSTER BY uid
	) map_output 
        REDUCE map_output.uid, map_output.item, map_output.wptype, map_output.tstamp
        USING 'python q4_reducer1.py' AS (uid BIGINT, item BIGINT, wptype STRING, tstamp BIGINT, sessionid STRING)
) sessionize
ORDER BY uid, tstamp;
--LIMIT 2500;

DROP VIEW IF EXISTS cart_abandon;
CREATE VIEW cart_abandon AS 
SELECT * 
FROM (
	FROM (
		FROM sessions
         	MAP sessions.uid, sessions.item, sessions.wptype, sessions.tstamp, sessions.sessionid
		-- USING 'python q4_mapper2.py'   AS uid, item, wptype, tstamp, sessionid  ||| use cat instead beacause this is a no-op mapper
       		USING 'cat'   AS uid, item, wptype, tstamp, sessionid
     		CLUSTER BY sessionid
	) map_output
	REDUCE map_output.uid, map_output.item, map_output.wptype, map_output.tstamp, map_output.sessionid
 	USING 'python q4_reducer2.py'   AS (sid STRING, start_s BIGINT, end_s BIGINT)
) npath;

			
--CREATE RESULT TABLE. Store query result externaly in output_dir/qXXresult/
DROP TABLE IF EXISTS ${hiveconf:resultTableName};
CREATE TABLE ${hiveconf:resultTableName}
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n'
STORED AS TEXTFILE LOCATION '${hiveconf:resultFile}' 
AS
-- Beginn: the real query part
SELECT c.sid, COUNT (*) AS s_pages
FROM cart_abandon c JOIN sessions s ON s.sessionid = c.sid
GROUP BY c.sid;


--cleanup
DROP VIEW IF EXISTS sessions;
DROP VIEW IF EXISTS cart_abandon;
