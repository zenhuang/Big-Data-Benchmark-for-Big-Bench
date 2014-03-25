
set QUERY_NUM=q05;
set resultTableName=ctable;
set resultFile=${env:BIG_BENCH_HDFS_ABSOLUTE_QUERY_RESULT_DIR}/${hiveconf:resultTableName};


--CREATE RESULT TABLE. Store query result externaly in output_dir/qXXresult/
DROP TABLE IF EXISTS ${hiveconf:resultTableName};
CREATE TABLE ${hiveconf:resultTableName} 
       ROW FORMAT DELIMITED
       FIELDS TERMINATED BY ' '
       LINES TERMINATED BY '\n'
       STORED AS TEXTFILE
       LOCATION '${hiveconf:MH_DIR}'
AS	
-- Beginn: the real query part
	SELECT 	C.c_customer_sk , 
		C.college_education , 
		C.male, 
		CASE WHEN C.clicks_in_category > 2 THEN true ELSE false END AS label
	FROM 	(
		SELECT 	s_temp.c_customer_sk AS c_customer_sk, 
			s_temp.college_education AS college_education, 
			s_temp.male AS male, 
			SUM(CASE WHEN s_temp.i_category ='Books' THEN 1 ELSE 0 END) AS clicks_in_category
		FROM 	( 
			SELECT 	ct.c_customer_sk AS c_customer_sk, 
				CASE WHEN cdt.cd_education_status IN ('Advanced Degree', 'College', '4 yr Degree', '2 yr Degree') THEN true ELSE false END AS college_education, 
				CASE WHEN cdt.cd_gender = 'M' THEN true ELSE false END AS male, it.i_category AS i_category
			FROM customer ct
			INNER JOIN customer_demographics cdt ON ct.c_current_cdemo_sk = cdt.cd_demo_sk
			INNER JOIN web_clickstreams wcst ON wcst.wcs_user_sk = ct.c_customer_sk  
			INNER JOIN item it ON wcst.wcs_item_sk = it.i_item_sk
			) s_temp
		GROUP BY 	s_temp.c_customer_sk, 
				s_temp.college_education, 
				s_temp.male
		) C
;

