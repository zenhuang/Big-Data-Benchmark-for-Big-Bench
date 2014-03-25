DROP TABLE IF EXISTS usersegments;
DROP TABLE IF EXISTS ctable;

-- ss_sold_date_sk > 2002-01-02

CREATE TABLE usersegments AS
SELECT 
    ss_customer_sk AS cid,
    ss_ticket_number AS oid,
    ss_sold_date_sk AS dateid,
    sum(ss_net_paid) AS amount
FROM
    store_sales
WHERE 
    ss_sold_date_sk > 37256
    AND ss_customer_sk IS NOT NULL
GROUP BY ss_customer_sk, ss_ticket_number, ss_sold_date_sk;

INSERT INTO TABLE usersegments
SELECT 
    ws_bill_customer_sk AS cid,
    ws_order_number AS oid,
    ws_sold_date_sk AS dateid,
    sum(ws_net_paid) AS amount
FROM
    web_sales
WHERE 
    ws_sold_date_sk > 37256
    AND ws_bill_customer_sk IS NOT NULL
GROUP BY ws_bill_customer_sk, ws_order_number, ws_sold_date_sk;

-------------------------------------------------------------------------------
--2003-01-02 == date_sk 37621

CREATE EXTERNAL TABLE ctable (cid INT, recency INT, frequency INT, totalspend INT) 
       ROW FORMAT DELIMITED
       FIELDS TERMINATED BY ' '
       LINES TERMINATED BY '\n'
       STORED AS TEXTFILE
       LOCATION '${hiveconf:MH_DIR}';

INSERT OVERWRITE TABLE ctable 
SELECT 
    cid AS id,
    CASE WHEN 37621 - max(dateid) < 60 
           THEN 1.0 
         ELSE 0.0 END AS recency,
    count(oid) AS frequency,
    sum(amount) AS totalspend
FROM usersegments
GROUP BY cid;

DROP TABLE usersegments;



