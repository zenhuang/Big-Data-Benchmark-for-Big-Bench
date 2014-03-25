set QUERY_NUM=q06;
set resultTableName=${hiveconf:QUERY_NUM}result;
set resultFile=${env:BIG_BENCH_HDFS_ABSOLUTE_QUERY_RESULT_DIR}/${hiveconf:resultTableName};


  DROP TABLE IF EXISTS q04_year_total_8;
  CREATE TABLE q04_year_total_8
  (customer_id               STRING,
   customer_first_name       STRING,
   customer_last_name        STRING,
   c_preferred_cust_flag     STRING,
   c_birth_country           STRING,
   c_login                   STRING,
   c_email_address           STRING,
   dyear                     INT,
   year_total                DOUBLE,
   sale_type                 STRING
  );


  DROP TABLE IF EXISTS sub1_grouped;
  CREATE TABLE sub1_grouped AS
  SELECT ss.ss_customer_sk AS customer_sk,
         dt.d_year AS d_year,
         sum(((ss_ext_list_price - ss_ext_wholesale_cost
                                 - ss_ext_discount_amt)
                                 + ss_ext_sales_price) / 2) AS year_total
  FROM store_sales ss INNER JOIN date_dim dt ON ss.ss_sold_date_sk = dt.d_date_sk
  GROUP BY ss.ss_customer_sk, dt.d_year;


  INSERT OVERWRITE TABLE q04_year_total_8
  SELECT c_customer_id AS customer_id,
         c_first_name  AS customer_first_name,
         c_last_name   AS customer_last_name,
         c_preferred_cust_flag,
         c_birth_country,
         c_login,
         c_email_address,
         sv.d_year     AS dyear,
         sv.year_total AS year_total,
         's' AS sale_type
  FROM customer c INNER JOIN sub1_grouped sv ON c.c_customer_sk = sv.customer_sk;

  DROP TABLE sub1_grouped;


  DROP TABLE IF EXISTS sub2_grouped;
  CREATE TABLE sub2_grouped AS
  SELECT ws.ws_bill_customer_sk AS customer_sk,
         dt.d_year AS d_year,
         sum(((ws_ext_list_price - ws_ext_wholesale_cost
                                 - ws_ext_discount_amt)
                                 + ws_ext_sales_price) / 2) AS year_total
  FROM web_sales ws INNER JOIN date_dim dt ON ws.ws_sold_date_sk = dt.d_date_sk
  GROUP BY ws.ws_bill_customer_sk, dt.d_year;

  INSERT INTO TABLE q04_year_total_8
  SELECT c_customer_id AS customer_id,
         c_first_name  AS customer_first_name,
         c_last_name   AS customer_last_name,
         c_preferred_cust_flag,
         c_birth_country,
         c_login,
         c_email_address,
         sv.d_year     AS dyear,
         sv.year_total AS year_total,
         'c' AS sale_type
  FROM customer c INNER JOIN sub2_grouped sv ON c.c_customer_sk = sv.customer_sk;

--dropping Part1 helper tables
DROP TABLE IF EXISTS sub1_grouped;
DROP TABLE IF EXISTS sub2_grouped;


--Part2: self-joins
----hive 0.8.1 does not support self-joins
-----so creating 4 different tables with same values to carry out task
DROP VIEW IF EXISTS t_s_firstyear;
DROP VIEW IF EXISTS t_s_secyear;
DROP VIEW IF EXISTS t_c_firstyear;
DROP VIEW IF EXISTS t_c_secyear;

CREATE VIEW IF NOT EXISTS t_s_firstyear AS SELECT * FROM q04_year_total_8;
CREATE VIEW IF NOT EXISTS t_s_secyear   AS SELECT * FROM q04_year_total_8;
CREATE VIEW IF NOT EXISTS t_c_firstyear AS SELECT * FROM q04_year_total_8;
CREATE VIEW IF NOT EXISTS t_c_secyear   AS SELECT * FROM q04_year_total_8;

-------------------------------------------------------------------------------
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
SELECT
  ts_s.customer_id,
  ts_s.customer_first_name,
  ts_s.customer_last_name,
  ts_s.c_preferred_cust_flag,
  ts_s.c_birth_country,
  ts_s.c_login, 
  CASE WHEN tc_f.year_total > 0
    THEN tc_s.year_total / tc_f.year_total
  ELSE null END,
  CASE WHEN ts_f.year_total > 0
    THEN ts_s.year_total / ts_f.year_total
  ELSE null END

FROM t_s_firstyear ts_f
  INNER JOIN t_s_secyear ts_s   ON ts_f.customer_id = ts_s.customer_id
  INNER JOIN t_c_secyear tc_s   ON ts_f.customer_id = tc_s.customer_id
  INNER JOIN t_c_firstyear tc_f ON ts_f.customer_id = tc_f.customer_id

WHERE ts_f.sale_type    = 's'
  AND tc_f.sale_type    = 'c'
  AND ts_s.sale_type    = 's'
  AND tc_s.sale_type    = 'c'
  AND ts_f.dyear        = 1999
  AND ts_s.dyear        = 1999 + 1
  AND tc_f.dyear        = 1999
  AND tc_s.dyear        = 1999 + 1
  AND ts_f.year_total   > 0
  AND tc_f.year_total   > 0

ORDER BY
  ts_s.customer_id,
  ts_s.customer_first_name,
  ts_s.customer_last_name,
  ts_s.c_preferred_cust_flag,
  ts_s.c_birth_country,
  ts_s.c_login
LIMIT 100;


---Cleanup-------------------------------------------------------------------  

DROP VIEW IF EXISTS t_s_firstyear;
DROP VIEW IF EXISTS t_s_secyear;
DROP VIEW IF EXISTS t_c_firstyear;
DROP VIEW IF EXISTS t_c_secyear;
DROP TABLE IF EXISTS q04_year_total_8;
