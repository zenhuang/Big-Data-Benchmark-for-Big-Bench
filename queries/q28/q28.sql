--ADD FILE ${env:BIG_BENCH_QUERIES_DIR}/q28/mapper_q28.py;

DROP TABLE IF EXISTS q28training;

CREATE EXTERNAL TABLE q28training (pr_review_sk INT, pr_rating INT, pr_item_sk INT, pr_review_content STRING) 
       ROW FORMAT DELIMITED
       FIELDS TERMINATED BY '\t'
       LINES TERMINATED BY '\n'
       STORED AS TEXTFILE
       LOCATION '${hiveconf:MH_DIR}';

INSERT OVERWRITE TABLE q28training
SELECT pr_review_sk,
    (case pr_review_rating
        when 1 then 'NEG' 
        when 2 then 'NEG'
        when 3 then 'NEU'
        when 4 then 'POS'
        when 5 then 'POS'
    end) AS pr_rating,
    pr_item_sk,
    pr_review_content
    from product_reviews
    where pmod(pr_review_sk, 5) in (1,2,3);

DROP TABLE IF EXISTS q28testing;

CREATE EXTERNAL TABLE q28testing (pr_review_sk INT, pr_rating INT, pr_item_sk INT, pr_review_content STRING) 
       ROW FORMAT DELIMITED
       FIELDS TERMINATED BY '\t'
       LINES TERMINATED BY '\n'
       STORED AS TEXTFILE
       LOCATION '${hiveconf:MH_DIR}2';

INSERT OVERWRITE TABLE q28testing
SELECT pr_review_sk,
    (case pr_review_rating
        when 1 then 'NEG' 
        when 2 then 'NEG'
        when 3 then 'NEU'
        when 4 then 'POS'
        when 5 then 'POS'
    end) AS pr_rating,
    pr_item_sk,
    pr_review_content
    from product_reviews
    where pmod(pr_review_sk, 5) in (0,4);

--SELECT review_sk, review_rating, item_sk, review_content
--  FROM (FROM product_reviews pr
--         MAP pr.pr_review_sk, pr.pr_review_rating, 
--             pr.pr_item_sk, pr.pr_review_content
--       USING 'python mapper_q28.py'
--          AS review_sk, review_rating, 
--             item_sk, review_content
--     CLUSTER BY review_sk) mo;

--    pr_review_sk,
--    (case pr_review_rating
--        when 1 then 'NEG' 
--        when 2 then 'NEG'
--        when 3 then 'NEU'
--        when 4 then 'POS'
--        when 5 then 'POS'
--    end) AS pr_rating,
--    pr_item_sk,
--    pr_review_content
--    from product_reviews;

-------------------------------------------------------------------------------

--CREATE TABLE a32_testingt
--AS SELECT 
--  pr_review_sk,
--    (case pr_review_rating
--        when 1 then 'NEG' 
--        when 2 then 'NEG'
--        when 3 then 'NEU'
--        when 4 then 'POS'
--        when 5 then 'POS'
--    end) AS pr_rating,
--    pr_review_content,
--    pr_item_sk
--    from product_reviews
--    where pmod(pr_review_sk, 5) in (0, 4);

--remaining: implement Text Classifire using q28.py
