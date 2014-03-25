
set QUERY_NUM=q11;
set resultTableName=${hiveconf:QUERY_NUM}result;
set resultFile=${env:BIG_BENCH_HDFS_ABSOLUTE_QUERY_RESULT_DIR}/${hiveconf:resultTableName};

--Part 1------------------------------------------------
DROP VIEW IF EXISTS review_stats;
CREATE VIEW review_stats AS
	SELECT 
		p.pr_item_sk AS pid,
		p.r_count AS reviews_count,
		p.avg_rating AS avg_rating,
		s.revenue AS m_revenue
	FROM 
		(SELECT 
			pr_item_sk,
			count(*) AS r_count,
			avg(pr_review_rating) as avg_rating
		FROM 
			product_reviews
		WHERE pr_item_sk IS NOT null
		GROUP BY pr_item_sk) p 
                --this is GROUP BY 1 in original::same as pr_item_sk here::hive complains anyhow

		INNER JOIN 

		(SELECT 
			ws_item_sk,
			sum(ws_net_paid) AS revenue
		FROM 
			web_sales
		WHERE 
				ws_sold_date_sk > 37621-30 
				AND ws_sold_date_sk < 37621 
				AND ws_item_sk IS NOT null
		-- ws_sold_date_sk > 2003-01-02 -30 days AND ws_sold_date_sk < 2003-01-02: Days from 1900-01-01 till 2003-01-02 ==> 37621 
		GROUP BY ws_item_sk) s
		ON p.pr_item_sk = s.ws_item_sk;
                --this is GROUP BY 1 in original::same as ws_item_sk here::hive complains anyhow

	
--Part 2: compute correlation -----------------------------------------
			
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
select  corr(reviews_count,avg_rating) 
from  review_stats
;

-- cleanup -------------------------------------------------------------
DROP VIEW IF EXISTS review_stats;
