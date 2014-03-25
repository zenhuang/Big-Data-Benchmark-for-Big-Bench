ADD JAR ${env:BIG_BENCH_QUERIES_DIR}/Resources/bigbenchqueriesmr.jar;
CREATE TEMPORARY FUNCTION extract_sentiment AS 'de.bankmark.bigbench.queries.q10.SentimentUDF';

set QUERY_NUM=q10;
set resultTableName=${hiveconf:QUERY_NUM}result;
set resultFile=${env:BIG_BENCH_HDFS_ABSOLUTE_QUERY_RESULT_DIR}/${hiveconf:resultTableName};

--Prepare result storage
DROP TABLE IF EXISTS ${hiveconf:resultTableName};
CREATE TABLE ${hiveconf:resultTableName}
	ROW FORMAT DELIMITED FIELDS TERMINATED BY ','	LINES TERMINATED BY '\n'
	STORED AS TEXTFILE LOCATION '${hiveconf:resultFile}' 
AS
-- Beginn: the real query part
  SELECT extract_sentiment(pr_item_sk,pr_review_content) AS (pr_item_sk, review_sentence, sentiment, sentiment_word)  
  FROM product_reviews;
