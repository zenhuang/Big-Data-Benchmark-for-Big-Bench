
ADD FILE q2_mapper.py;
ADD FILE q2_reducer.py;

set outputTableName=q02result;
--Prepare result storage
DROP TABLE IF EXISTS ${hiveconf:outputTableName};

CREATE EXTERNAL TABLE ${hiveconf:outputTableName}(pid1  BIGINT, pid2 BIGINT, count INT) ROW FORMAT
              DELIMITED FIELDS TERMINATED BY ','
              LINES TERMINATED BY '\n'
              STORED AS TEXTFILE
              LOCATION '${env:BIG_BENCH_HDFS_ABSOLUTE_QUERY_RESULT_DIR}/${hiveconf:outputTableName}';


--Find the most frequent ones
INSERT OVERWRITE TABLE ${hiveconf:outputTableName}
SELECT	pid1, pid2, COUNT (*) AS cnt
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
		USING 'python q2_mapper.py'
		AS cid, pid 
		CLUSTER BY cid
	) map_output
	REDUCE map_output.cid, map_output.pid
	USING 'python q2_reducer.py'
	AS (pid1 BIGINT, pid2 BIGINT)
) temp_basket
WHERE pid1 = 1416
GROUP BY pid1, pid2
ORDER BY pid1 ,cnt ,pid2
LIMIT 30;
