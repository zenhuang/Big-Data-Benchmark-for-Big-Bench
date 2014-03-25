set QUERY_NUM=q24;
set resultTableName=${hiveconf:QUERY_NUM}result;
set resultFile=${env:BIG_BENCH_HDFS_ABSOLUTE_QUERY_RESULT_DIR}/${hiveconf:resultTableName};


-- compute the price change % for the competitor 

DROP VIEW IF EXISTS competitor_price_view;
CREATE VIEW competitor_price_view AS 
  SELECT i_item_sk, (imp_competitor_price - i_current_price)/i_current_price AS price_change,
         imp_start_date, (imp_end_date - imp_start_date) AS no_days
    FROM item i JOIN item_marketprices imp ON i.i_item_sk = imp.imp_item_sk
   WHERE (i.i_item_sk >= 7 AND i.i_item_sk <= 17)
     AND imp.imp_competitor_price < i.i_current_price;



DROP TABLE IF EXISTS self_ws_view;
CREATE TABLE self_ws_view AS 
  SELECT ws_item_sk, 
         SUM(CASE WHEN ws_sold_date_sk >= c.imp_start_date
                   AND ws_sold_date_sk < c.imp_start_date + c.no_days
                  THEN ws_quantity 
                  ELSE 0 END) AS current_ws,
         SUM(CASE WHEN ws_sold_date_sk >= c.imp_start_date - c.no_days 
                   AND ws_sold_date_sk < c.imp_start_date
                  THEN ws_quantity 
                  ELSE 0 END) AS prev_ws
    FROM web_sales ws JOIN competitor_price_view c ON ws.ws_item_sk = c.i_item_sk
   GROUP BY ws_item_sk;



DROP TABLE IF EXISTS self_ss_view;
CREATE TABLE self_ss_view AS 
  SELECT ss_item_sk,
         SUM(CASE WHEN ss_sold_date_sk >= c.imp_start_date
                   AND ss_sold_date_sk < c.imp_start_date + c.no_days 
                  THEN ss_quantity 
                  ELSE 0 END) AS current_ss,
         SUM(CASE WHEN ss_sold_date_sk >= c.imp_start_date - c.no_days 
                   AND ss_sold_date_sk < c.imp_start_date
                  THEN ss_quantity 
                  ELSE 0 END) AS prev_ss
    FROM store_sales ss JOIN competitor_price_view c ON c.i_item_sk = ss.ss_item_sk
   GROUP BY ss_item_sk;


---- Result ------------------------------------------

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
SELECT i_item_sk, (current_ss + current_ws - prev_ss - prev_ws) / ((prev_ss + prev_ws) * price_change) AS cross_price_elasticity
  FROM competitor_price_view c JOIN self_ws_view ws ON c.i_item_sk = ws.ws_item_sk
  JOIN self_ss_view ss ON c.i_item_sk = ss.ss_item_sk;


-- clean up -----------------------------------
DROP TABLE self_ws_view;
DROP TABLE self_ss_view;
DROP VIEW competitor_price_view;
