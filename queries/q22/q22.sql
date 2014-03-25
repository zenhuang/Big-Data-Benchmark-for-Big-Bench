set QUERY_NUM=q22;
set resultTableName=${hiveconf:QUERY_NUM}result;
set resultFile=${env:BIG_BENCH_HDFS_ABSOLUTE_QUERY_RESULT_DIR}/${hiveconf:resultTableName};




DROP TABLE IF EXISTS coalition_22;
CREATE TABLE coalition_22 AS
  SELECT *
    FROM inventory inv
    JOIN (SELECT * 
            FROM item i 
           WHERE i.i_current_price > 0.98 
             AND i.i_current_price < 1.5) i
         ON inv.inv_item_sk = i.i_item_sk
    JOIN warehouse w ON inv.inv_warehouse_sk = w.w_warehouse_sk
    JOIN date_dim d ON inv.inv_date_sk = d.d_date_sk
   WHERE datediff(d_date, '2000-05-08') >= -30 
     AND datediff(d_date, '2000-05-08') <= 30;


DROP TABLE IF EXISTS inner;
CREATE TABLE inner AS
  SELECT w_warehouse_name, i_item_id,
         sum(CASE WHEN datediff(d_date, '2000-05-08') < 0 
                  THEN inv_quantity_on_hand 
                  ELSE 0 END) AS inv_before,
         sum(CASE WHEN datediff(d_date, '2000-05-08') >= 0 
                  THEN inv_quantity_on_hand
                  ELSE 0 END) AS inv_after
       FROM coalition_22 
      GROUP BY w_warehouse_name, i_item_id;

DROP TABLE coalition_22; 

DROP TABLE IF EXISTS pre_ratio;
CREATE TABLE pre_ratio AS
  SELECT * 
    FROM inner
   WHERE inv_before > 0;


DROP TABLE IF EXISTS conditional_ratio;
CREATE TABLE conditional_ratio AS
  SELECT w_warehouse_name, inv_after/inv_before
    FROM pre_ratio
   WHERE inv_after/inv_before >= 2.0/3.0 
     AND inv_after/inv_before <= 3.0/2.0;

DROP TABLE pre_ratio;


----- RESULT --------------------------------
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
  SELECT name.w_warehouse_name, i_item_id, inv_before, inv_after
    FROM inner name 
    JOIN conditional_ratio nombre 
      ON name.w_warehouse_name = nombre.w_warehouse_name
   ORDER BY w_warehouse_name, i_item_id;

---- cleanup ----------------
DROP TABLE conditional_ratio;
DROP TABLE inner;

