
--1st part: creating SALES_RETURNS view -------------------------------------------
DROP VIEW IF EXISTS sales_returns;
CREATE VIEW IF NOT EXISTS sales_returns AS
SELECT s.ss_sold_date_sk 	AS s_date,
       r.sr_returned_date_sk 	AS r_date,
       s.ss_item_sk 		AS item,
       s.ss_ticket_number 	AS oid,
       s.ss_net_paid 		AS s_amount,
       r.sr_return_amt 		AS r_amount,
       (CASE WHEN s.ss_customer_sk IS NULL 
          THEN r.sr_customer_sk 
        ELSE s.ss_customer_sk END) AS cid,
       s.ss_customer_sk 	AS s_cid,
       sr_customer_sk 		AS r_cid
FROM store_sales s
--LEFT JOIN = LEFT OUTER JOIN
     LEFT OUTER JOIN store_returns r 	ON s.ss_item_sk = r.sr_item_sk
                                  	AND s.ss_ticket_number = r.sr_ticket_number
WHERE s.ss_sold_date_sk IS NOT NULL;


-- Why should the following tmp table be helpful?
--temp table: SELECT * FROM sales_returns
--CREATE TABLE IF NOT EXISTS all_sales_returns AS
--SELECT * FROM sales_returns;

------- Part 2 create imput table for mahout --------------------------------------
DROP TABLE IF EXISTS twenty;
CREATE EXTERNAL TABLE twenty 	(
				cid INT, 
				r_order_ratio DOUBLE, 
				r_item_ratio DOUBLE, 
				r_amount_ratio DOUBLE, 
				r_freq INT
				)
       ROW FORMAT DELIMITED 
       FIELDS TERMINATED BY ' '
       LINES TERMINATED BY '\n'
       STORED AS TEXTFILE
       LOCATION '${hiveconf:MH_DIR}';

INSERT OVERWRITE TABLE twenty
SELECT cid,
       100.0 * COUNT(distinct (CASE WHEN r_date IS NOT NULL THEN oid ELSE NULL end))/COUNT(distinct oid) AS r_order_ratio, 
       SUM(CASE WHEN r_date IS NOT NULL THEN 1 ELSE 0 END)/COUNT(item)*100 AS r_item_ratio,
       SUM(CASE WHEN r_date IS NOT NULL THEN r_amount ELSE 0.0 END)/SUM(s_amount)*100 AS r_amount_ratio,
       COUNT(distinct (CASE WHEN r_date IS NOT NULL THEN r_date ELSE NULL END)) AS r_freq 
--FROM all_sales_returns
FROM sales_returns
WHERE cid IS NOT NULL
--Hive does not support GROUP BY 1 (any)
GROUP BY cid
--(below) error in data: returns NULL
--HAVING COUNT(distinct (CASE WHEN r_date IS NOT NULL THEN r_date ELSE NULL END)) > 1
;

------- Cleanup --------------------------------------
--drop temp table
DROP VIEW IF EXISTS sales_returns;
--DROP TABLE IF EXISTS all_sales_returns;


