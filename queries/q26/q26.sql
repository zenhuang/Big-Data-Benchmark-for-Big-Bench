DROP TABLE IF EXISTS ctable2;

CREATE EXTERNAL TABLE ctable2 (cid INT, id1 INT, id3 INT, id5 INT, id7 INT, id9 INT, id11 INT, id13 INT, id15 INT, id2 INT, id4 INT, id6 INT, id8 INT, id10 INT, id14 INT, id16 INT) 
       ROW FORMAT DELIMITED
       FIELDS TERMINATED BY ' '
       LINES TERMINATED BY '\n'
       STORED AS TEXTFILE
       LOCATION '${hiveconf:MH_DIR}';

INSERT OVERWRITE TABLE ctable2
 SELECT ss.ss_customer_sk AS cid,
        count(CASE WHEN i.i_class_id=1  THEN 1 ELSE NULL END) AS id1,
        count(CASE WHEN i.i_class_id=3  THEN 1 ELSE NULL END) AS id3,
        count(CASE WHEN i.i_class_id=5  THEN 1 ELSE NULL END) AS id5,
        count(CASE WHEN i.i_class_id=7  THEN 1 ELSE NULL END) AS id7,
        count(CASE WHEN i.i_class_id=9  THEN 1 ELSE NULL END) AS id9,
        count(CASE WHEN i.i_class_id=11 THEN 1 ELSE NULL END) AS id11,
        count(CASE WHEN i.i_class_id=13 THEN 1 ELSE NULL END) AS id13,
        count(CASE WHEN i.i_class_id=15 THEN 1 ELSE NULL END) AS id15,
        count(CASE WHEN i.i_class_id=2  THEN 1 ELSE NULL END) AS id2,
        count(CASE WHEN i.i_class_id=4  THEN 1 ELSE NULL END) AS id4,
        count(CASE WHEN i.i_class_id=6  THEN 1 ELSE NULL END) AS id6,
        count(CASE WHEN i.i_class_id=8  THEN 1 ELSE NULL END) AS id8,
        count(CASE WHEN i.i_class_id=10 THEN 1 ELSE NULL END) AS id10,
        count(CASE WHEN i.i_class_id=14 THEN 1 ELSE NULL END) AS id14,
        count(CASE WHEN i.i_class_id=16 THEN 1 ELSE NULL END) AS id16
 FROM store_sales ss
      INNER JOIN item i ON ss.ss_item_sk = i.i_item_sk
 WHERE
   i.i_category = 'Books'
   AND ss.ss_customer_sk IS NOT NULL
 GROUP BY ss.ss_customer_sk
 HAVING count(ss.ss_item_sk) > 5;

-------------------------------------------------------------------------------

--CREATE TABLE IF NOT EXISTS twenty_six
--ROW FORMAT DELIMITED
--FIELDS TERMINATED BY ' '
--LINES TERMINATED BY '\n' AS 
--STORED AS TEXTFILE LOCATION '/home/saruman/temp/hive_tables/26' AS
--SELECT * FROM clusteringtable;

--SELECT clusterif, cid
--FROM kmeansplot
--ORDER BY clusterid, cid;
