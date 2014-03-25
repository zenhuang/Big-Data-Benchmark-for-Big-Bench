set QUERY_NUM=q07;
set resultTableName=${hiveconf:QUERY_NUM}result;
set resultFile=${env:BIG_BENCH_HDFS_ABSOLUTE_QUERY_RESULT_DIR}/${hiveconf:resultTableName};



DROP TABLE IF EXISTS q06_specific_month_88;
DROP TABLE IF EXISTS q06_cat_avg_price_88;

CREATE TABLE q06_specific_month_88 AS
       SELECT DISTINCT(d_month_seq) AS d_month_seq
         FROM date_dim 
        WHERE d_year = 2002 AND d_moy = 7;

CREATE TABLE q06_cat_avg_price_88 AS
       SELECT i_category AS i_category, 
              AVG(i_current_price) * 1.2 AS avg_price
         FROM item
        GROUP BY i_category;

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
SELECT temp.ca_state AS state, COUNT(*) AS cnt
  FROM (SELECT a.ca_state AS ca_state, i.i_current_price AS i_current_price, 
               p.avg_price AS avg_price 
          FROM customer_address a
               JOIN customer c ON a.ca_address_sk = c.c_current_addr_sk
               JOIN store_sales s ON c.c_customer_sk = s.ss_customer_sk
               JOIN date_dim d ON s.ss_sold_date_sk = d.d_date_sk
               JOIN item i ON s.ss_item_sk = i.i_item_sk
               JOIN q06_specific_month_88 m ON d.d_month_seq = m.d_month_seq
               JOIN q06_cat_avg_price_88 p ON p.i_category = i.i_category
       ) temp 
 WHERE temp.i_current_price > temp.avg_price
 GROUP BY temp.ca_state
HAVING COUNT(*) >= 10
 ORDER BY cnt
 LIMIT 100;

DROP TABLE IF EXISTS q06_specific_month_88;
DROP TABLE IF EXISTS q06_cat_avg_price_88;
