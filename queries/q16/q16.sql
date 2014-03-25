
set QUERY_NUM=q16;
set resultTableName=${hiveconf:QUERY_NUM}result;
set resultFile=${env:BIG_BENCH_HDFS_ABSOLUTE_QUERY_RESULT_DIR}/${hiveconf:resultTableName};

-- More testing needed


DROP TABLE IF EXISTS amal_one;
CREATE TABLE amal_one AS
  SELECT * 
  FROM web_sales ws LEFT OUTER JOIN web_returns wr ON
         (ws.ws_order_number = wr.wr_order_number
          AND ws.ws_item_sk = wr.wr_item_sk);


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
SELECT w_state, i_item_id,
       SUM(CASE 
           WHEN (unix_timestamp(d_date,'yyyy-MM-dd') < unix_timestamp('1998-03-16','yyyy-MM-dd')) 
           THEN ws_sales_price - COALESCE(wr_refunded_cash,0) 
           ELSE 0.0 END) as sales_before,
       SUM(CASE 
           WHEN (unix_timestamp(d_date,'yyyy-MM-dd') >= unix_timestamp('1998-03-16','yyyy-MM-dd')) 
           THEN ws_sales_price - coalesce(wr_refunded_cash,0) 
           ELSE 0.0 END) as sales_after
  FROM amal_one a1
  JOIN item i ON a1.ws_item_sk = i.i_item_sk
  JOIN warehouse w ON a1.ws_warehouse_sk = w.w_warehouse_sk
  JOIN date_dim d ON a1.ws_sold_date_sk = d.d_date_sk
   AND unix_timestamp(d.d_date, 'yyyy-MM-dd') >= unix_timestamp('1998-02-16', 'yyyy-MM-dd') 
   AND unix_timestamp(d.d_date, 'yyyy-MM-dd') <= unix_timestamp('1998-04-16', 'yyyy-MM-dd')
 GROUP BY w_state,i_item_id
 ORDER BY w_state,i_item_id;

-- cleaning up ---------------------------------------
DROP TABLE IF EXISTS amal_one;
