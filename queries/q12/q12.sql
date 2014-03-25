set QUERY_NUM=q12;
set resultTableName=${hiveconf:QUERY_NUM}result;
set resultFile=${env:BIG_BENCH_HDFS_ABSOLUTE_QUERY_RESULT_DIR}/${hiveconf:resultTableName};



--Find all customers who viewed items of a given category on the web
--in a given month and year that was followed by an in-store purchase in the three
--consecutive months.

--1)

CREATE VIEW IF NOT EXISTS click AS
 SELECT c.wcs_item_sk AS item,
        c.wcs_user_sk AS uid,
        c.wcs_click_date_sk AS c_date,
        c.wcs_click_time_sk AS c_time
   FROM web_clickstreams c JOIN item i ON c.wcs_item_sk = i.i_item_sk
  WHERE (i.i_category = 'Books' OR i.i_category = 'Electronics')
    AND c.wcs_user_sk is not null
    AND c.wcs_click_date_sk > 36403
    AND c.wcs_click_date_sk < 36403 + 30
  ORDER BY c_date, c_time;

CREATE VIEW IF NOT EXISTS sale AS 
 SELECT ss.ss_item_sk AS item,
        ss.ss_customer_sk AS uid,
        ss.ss_sold_date_sk AS s_date,
        ss.ss_sold_time_sk AS s_time
   FROM store_sales ss JOIN item i ON ss.ss_item_sk = i.i_item_sk
  WHERE (i.i_category = 'Books' OR i.i_category = 'Electronics')
    AND ss.ss_customer_sk is not null
    AND ss.ss_sold_date_sk > 36403
    AND ss.ss_sold_date_sk < 36403 + 120
  ORDER BY s_date, s_time;


DROP TABLE IF EXISTS ${hiveconf:resultTableName};
CREATE TABLE ${hiveconf:resultTableName}
ROW FORMAT
DELIMITED FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION '${hiveconf:resultFile}' 
AS
SELECT c_date, s_date, s.uid
  FROM click c JOIN sale s
    ON c.uid = s.uid
 WHERE c.c_date < s.s_date;

--have to fix partition

------------------------ REGEX CODE GOES HERE ---------------------------------
--TODO check this: Npath seams to be unnecssary. so this query should be done
-------------------------------------------------------------------------------

DROP VIEW IF EXISTS click;
DROP VIEW IF EXISTS sale;

