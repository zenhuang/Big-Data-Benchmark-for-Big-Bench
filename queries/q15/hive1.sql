
set QUERY_NUM=q15;
set QUERY_TMP_DIR=${env:BIG_BENCH_HDFS_ABSOLUTE_TEMP_DIR}/${hiveconf:QUERY_NUM}tmp;

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

----store time series------------------------------------------------------------------
DROP VIEW IF EXISTS time_series_category;
CREATE VIEW time_series_category AS
SELECT  i.i_category_id AS cat, -- ranges from 1 to 10
        s.ss_sold_date_sk AS d,
        sum(s.ss_net_paid) AS sales
FROM    store_sales s
INNER JOIN item i ON s.ss_item_sk = i.i_item_sk 
WHERE   i.i_category_id IS NOT NULL
        -- and date is within range ss_sold_date_sk > 1999-09-02  AND s.ss_sold_date_sk < 1999-09-02 + 365 days
        AND s.ss_sold_date_sk > 36403 AND s.ss_sold_date_sk < 36403+365
        -- and for a given store
        AND s.ss_store_sk = 10 -- ranges from 1 to 12

GROUP BY i.i_category_id, s.ss_sold_date_sk;
-------------------------------------------------------------------------------

  
CREATE TABLE matrix1
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ' ' 
STORED AS TEXTFILE 
LOCATION '${hiveconf:QUERY_TMP_DIR}/matrix1'
AS
	SELECT d, sales FROM time_series_category
	WHERE cat = 1;


CREATE TABLE matrix2
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ' ' 
STORED AS TEXTFILE 
LOCATION '${hiveconf:QUERY_TMP_DIR}/matrix2'
AS
SELECT d, sales FROM time_series_category
WHERE cat = 2;


CREATE TABLE matrix3
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ' ' 
STORED AS TEXTFILE 
LOCATION '${hiveconf:QUERY_TMP_DIR}/matrix3'
AS
SELECT d, sales FROM time_series_category
WHERE cat = 3;


CREATE TABLE matrix4
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ' ' 
STORED AS TEXTFILE 
LOCATION '${hiveconf:QUERY_TMP_DIR}/matrix4'
AS
SELECT d, sales FROM time_series_category
WHERE cat = 4;


CREATE TABLE matrix5
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ' ' 
STORED AS TEXTFILE 
LOCATION '${hiveconf:QUERY_TMP_DIR}/matrix5'
AS
SELECT d, sales FROM time_series_category
WHERE cat = 5;


CREATE TABLE matrix6
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ' ' 
STORED AS TEXTFILE 
LOCATION '${hiveconf:QUERY_TMP_DIR}/matrix6'
AS
SELECT d, sales FROM time_series_category
WHERE cat = 6;


CREATE TABLE matrix7
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ' ' 
STORED AS TEXTFILE 
LOCATION '${hiveconf:QUERY_TMP_DIR}/matrix7'
AS
SELECT d, sales FROM time_series_category
WHERE cat = 7;


CREATE TABLE matrix8
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ' ' 
STORED AS TEXTFILE 
LOCATION '${hiveconf:QUERY_TMP_DIR}/matrix8'
AS
SELECT d, sales FROM time_series_category
WHERE cat = 8;


CREATE TABLE matrix9
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ' ' 
STORED AS TEXTFILE 
LOCATION '${hiveconf:QUERY_TMP_DIR}/matrix9'
AS
SELECT d, sales FROM time_series_category
WHERE cat = 9;


CREATE TABLE matrix10
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ' ' 
STORED AS TEXTFILE 
LOCATION '${hiveconf:QUERY_TMP_DIR}/matrix10'
AS
SELECT d, sales FROM time_series_category
WHERE cat = 10;

--cleaning up -------------------------------------------------
DROP VIEW time_series_category;


