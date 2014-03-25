set QUERY_NUM=q18;
set QUERY_TMP_DIR=${env:BIG_BENCH_HDFS_ABSOLUTE_TEMP_DIR}/${hiveconf:QUERY_NUM}tmp;


--ss_sold_date_sk between 1999-09-02 and 1999-05-02: Days from 1900-01-01 till 1999-09-02 ==> 36403 
DROP VIEW IF EXISTS time_series_store;
CREATE VIEW time_series_store AS 
  SELECT 
    ss_store_sk AS store,
    ss_sold_date_sk AS d,
    sum(ss_net_paid) AS sales
  FROM store_sales
  WHERE ss_sold_date_sk > 36403 
        and ss_sold_date_sk < 36403+90
  GROUP BY ss_store_sk, ss_sold_date_sk;

--SELECT * FROM store_sales WHERE ss_store_sk =1 AND  ss_sold_date_sk > 35840 and ss_sold_date_sk < 35840+90;


DROP TABLE IF EXISTS matrix1;
DROP TABLE IF EXISTS matrix2;
DROP TABLE IF EXISTS matrix3;
DROP TABLE IF EXISTS matrix4;
DROP TABLE IF EXISTS matrix5;
DROP TABLE IF EXISTS matrix6;
DROP TABLE IF EXISTS matrix7;
DROP TABLE IF EXISTS matrix8;
DROP TABLE IF EXISTS matrix9;
DROP TABLE IF EXISTS matrix10;
DROP TABLE IF EXISTS matrix11;
DROP TABLE IF EXISTS matrix12;



CREATE TABLE matrix1 ROW FORMAT DELIMITED FIELDS TERMINATED BY ' ' 
STORED AS TEXTFILE LOCATION '${hiveconf:QUERY_TMP_DIR}/matrix1'
  AS SELECT d, sales FROM time_series_store WHERE store = 1;

CREATE TABLE matrix2 ROW FORMAT DELIMITED FIELDS TERMINATED BY ' ' 
STORED AS TEXTFILE LOCATION '${hiveconf:QUERY_TMP_DIR}/matrix2'
  AS SELECT d, sales FROM time_series_store WHERE store = 2;

CREATE TABLE matrix3 ROW FORMAT DELIMITED FIELDS TERMINATED BY ' ' 
STORED AS TEXTFILE LOCATION '${hiveconf:QUERY_TMP_DIR}/matrix3'
  AS SELECT d, sales FROM time_series_store WHERE store = 3;

CREATE TABLE matrix4 ROW FORMAT DELIMITED FIELDS TERMINATED BY ' ' 
STORED AS TEXTFILE LOCATION '${hiveconf:QUERY_TMP_DIR}/matrix4'
  AS SELECT d, sales FROM time_series_store WHERE store = 4;

CREATE TABLE matrix5 ROW FORMAT DELIMITED FIELDS TERMINATED BY ' ' 
STORED AS TEXTFILE LOCATION '${hiveconf:QUERY_TMP_DIR}/matrix5'
  AS SELECT d, sales FROM time_series_store WHERE store = 5;

CREATE TABLE matrix6 ROW FORMAT DELIMITED FIELDS TERMINATED BY ' ' 
STORED AS TEXTFILE LOCATION '${hiveconf:QUERY_TMP_DIR}/matrix6'
  AS SELECT d, sales FROM time_series_store WHERE store = 6;

CREATE TABLE matrix7 ROW FORMAT DELIMITED FIELDS TERMINATED BY ' ' 
STORED AS TEXTFILE LOCATION '${hiveconf:QUERY_TMP_DIR}/matrix7'
  AS SELECT d, sales FROM time_series_store WHERE store = 7;

CREATE TABLE matrix8 ROW FORMAT DELIMITED FIELDS TERMINATED BY ' ' 
STORED AS TEXTFILE LOCATION '${hiveconf:QUERY_TMP_DIR}/matrix8'
  AS SELECT d, sales FROM time_series_store WHERE store = 8;

CREATE TABLE matrix9 ROW FORMAT DELIMITED FIELDS TERMINATED BY ' ' 
STORED AS TEXTFILE LOCATION '${hiveconf:QUERY_TMP_DIR}/matrix9'
  AS SELECT d, sales FROM time_series_store WHERE store = 9;

CREATE TABLE matrix10 ROW FORMAT DELIMITED FIELDS TERMINATED BY ' ' 
STORED AS TEXTFILE LOCATION '${hiveconf:QUERY_TMP_DIR}/matrix10'
  AS SELECT d, sales FROM time_series_store WHERE store = 10;

CREATE TABLE matrix11 ROW FORMAT DELIMITED FIELDS TERMINATED BY ' ' 
STORED AS TEXTFILE LOCATION '${hiveconf:QUERY_TMP_DIR}/matrix11'
  AS SELECT d, sales FROM time_series_store WHERE store = 11;

CREATE TABLE matrix12 ROW FORMAT DELIMITED FIELDS TERMINATED BY ' ' 
STORED AS TEXTFILE LOCATION '${hiveconf:QUERY_TMP_DIR}/matrix12'
  AS SELECT d, sales FROM time_series_store WHERE store = 12;


DROP VIEW IF EXISTS time_series_store;

