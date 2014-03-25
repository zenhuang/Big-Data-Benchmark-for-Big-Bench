set QUERY_NUM=q17;
set resultTableName=${hiveconf:QUERY_NUM}result;
set resultFile=${env:BIG_BENCH_HDFS_ABSOLUTE_QUERY_RESULT_DIR}/${hiveconf:resultTableName};

--TODO Empty result - needs more testing


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

SELECT promotions, total, promotions/total*100 
--no need to cast promotions/total: sum(COL) return DOUBLE
  FROM (SELECT SUM(ss_ext_sales_price) promotions
          FROM store_sales ss
          JOIN store s ON ss.ss_store_sk = s.s_store_sk
          JOIN promotion p ON ss.ss_promo_sk = p.p_promo_sk
          JOIN date_dim dd ON ss.ss_sold_date_sk = dd.d_date_sk
          JOIN (SELECT * 
                  FROM customer c 
                  JOIN customer_address ca ON c.c_current_addr_sk = ca.ca_address_sk) cc 
            ON ss.ss_customer_sk = cc.c_customer_sk
          JOIN item i ON ss.ss_item_sk = i.i_item_sk
         WHERE ca_gmt_offset = -7
           AND i_category = 'Jewelry'
           AND (p_channel_dmail = 'Y' OR p_channel_email = 'Y' OR p_channel_tv = 'Y')
           AND s_gmt_offset = -7
           AND d_year = 2001
           AND d_moy  = 12) promotional_sales
  JOIN (SELECT SUM(ss_ext_sales_price) total
          FROM store_sales ss
          JOIN store s ON ss.ss_store_sk = s.s_store_sk
          JOIN date_dim dd ON ss.ss_sold_date_SK = dd.d_date_sk
          JOIN (SELECT * 
                  FROM customer c 
                  JOIN customer_address ca ON c.c_current_addr_sk = ca.ca_address_sk) cc
            ON ss.ss_customer_sk = cc.c_customer_sk
          JOIN item i ON ss.ss_item_sk = i.i_item_sk
         WHERE ca_gmt_offset = -7
           AND i_category = 'Jewelry'
           AND s_gmt_offset = -7
           AND d_year = 2001
           AND d_moy  = 12) all_sales
 ORDER BY promotions, total;

