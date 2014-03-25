
set QUERY_NUM=q18;
set QUERY_TMP_DIR=${env:BIG_BENCH_HDFS_ABSOLUTE_TEMP_DIR}/${hiveconf:QUERY_NUM}tmp;
set resultTableName=${hiveconf:QUERY_NUM}result;
set resultFile=${env:BIG_BENCH_HDFS_ABSOLUTE_QUERY_RESULT_DIR}/${hiveconf:resultTableName};


-------------------------------------------------------------------------------

DROP TABLE IF EXISTS lm1;
DROP TABLE IF EXISTS lm2;
DROP TABLE IF EXISTS lm3;
DROP TABLE IF EXISTS lm4;
DROP TABLE IF EXISTS lm5;
DROP TABLE IF EXISTS lm6;
DROP TABLE IF EXISTS lm7;
DROP TABLE IF EXISTS lm8;
DROP TABLE IF EXISTS lm9;
DROP TABLE IF EXISTS lm10;
DROP TABLE IF EXISTS lm11;
DROP TABLE IF EXISTS lm12;

CREATE EXTERNAL TABLE lm1 (
    intercept            DOUBLE,
    slope                DOUBLE
 )
 ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
 STORED AS TEXTFILE LOCATION '${hiveconf:QUERY_TMP_DIR}/output1/'
;

CREATE EXTERNAL TABLE lm2 (
    intercept            DOUBLE,
    slope                DOUBLE
 )
 ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
 STORED AS TEXTFILE LOCATION '${hiveconf:QUERY_TMP_DIR}/output2/'
;

CREATE EXTERNAL TABLE lm3 (
    intercept            DOUBLE,
    slope                DOUBLE
 )
 ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
 STORED AS TEXTFILE LOCATION '${hiveconf:QUERY_TMP_DIR}/output3/'
;

CREATE EXTERNAL TABLE lm4 (
    intercept            DOUBLE,
    slope                DOUBLE
 )
 ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
 STORED AS TEXTFILE LOCATION '${hiveconf:QUERY_TMP_DIR}/output4/'
;

CREATE EXTERNAL TABLE lm5 (
    intercept            DOUBLE,
    slope                DOUBLE
 )
 ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
 STORED AS TEXTFILE LOCATION '${hiveconf:QUERY_TMP_DIR}/output5/'
;

CREATE EXTERNAL TABLE lm6 (
    intercept            DOUBLE,
    slope                DOUBLE
 )
 ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
 STORED AS TEXTFILE LOCATION '${hiveconf:QUERY_TMP_DIR}/output6/'
;

CREATE EXTERNAL TABLE lm7 (
    intercept            DOUBLE,
    slope                DOUBLE
 )
 ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
 STORED AS TEXTFILE LOCATION '${hiveconf:QUERY_TMP_DIR}/output7/'
;

CREATE EXTERNAL TABLE lm8 (
    intercept            DOUBLE,
    slope                DOUBLE
 )
 ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
 STORED AS TEXTFILE LOCATION '${hiveconf:QUERY_TMP_DIR}/output8/'
;

CREATE EXTERNAL TABLE lm9 (
    intercept            DOUBLE,
    slope                DOUBLE
 )
 ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
 STORED AS TEXTFILE LOCATION '${hiveconf:QUERY_TMP_DIR}/output9/'
;

CREATE EXTERNAL TABLE lm10 (
    intercept            DOUBLE,
    slope                DOUBLE
 )
 ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
 STORED AS TEXTFILE LOCATION '${hiveconf:QUERY_TMP_DIR}/output10/'
;

CREATE EXTERNAL TABLE lm11 (
    intercept            DOUBLE,
    slope                DOUBLE
 )
 ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
 STORED AS TEXTFILE LOCATION '${hiveconf:QUERY_TMP_DIR}/output11/'
;

CREATE EXTERNAL TABLE  lm12 (
    intercept            DOUBLE,
    slope                DOUBLE
 )
 ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
 STORED AS TEXTFILE LOCATION '${hiveconf:QUERY_TMP_DIR}/output12/'
;




-- (!) Set Paths --------------------------------------------------------------
--LOAD DATA INPATH '${hiveconf:QUERY_TMP_DIR}/output1/part-r-00000' OVERWRITE INTO TABLE lm1;
--LOAD DATA INPATH '${hiveconf:QUERY_TMP_DIR}/output2/part-r-00000' OVERWRITE INTO TABLE lm2;
--LOAD DATA INPATH '${hiveconf:QUERY_TMP_DIR}/output3/part-r-00000' OVERWRITE INTO TABLE lm3;
--LOAD DATA INPATH '${hiveconf:QUERY_TMP_DIR}/output4/part-r-00000' OVERWRITE INTO TABLE lm4;
--LOAD DATA INPATH '${hiveconf:QUERY_TMP_DIR}/output5/part-r-00000' OVERWRITE INTO TABLE lm5;
--LOAD DATA INPATH '${hiveconf:QUERY_TMP_DIR}/output6/part-r-00000' OVERWRITE INTO TABLE lm6;
--LOAD DATA INPATH '${hiveconf:QUERY_TMP_DIR}/output7/part-r-00000' OVERWRITE INTO TABLE lm7;
--LOAD DATA INPATH '${hiveconf:QUERY_TMP_DIR}/output8/part-r-00000' OVERWRITE INTO TABLE lm8;
--LOAD DATA INPATH '${hiveconf:QUERY_TMP_DIR}/output9/part-r-00000' OVERWRITE INTO TABLE lm9;
--LOAD DATA INPATH '${hiveconf:QUERY_TMP_DIR}/output10/part-r-00000' OVERWRITE INTO TABLE lm10;
--LOAD DATA INPATH '${hiveconf:QUERY_TMP_DIR}/output11/part-r-00000' OVERWRITE INTO TABLE lm11;
--LOAD DATA INPATH '${hiveconf:QUERY_TMP_DIR}/output12/part-r-00000' OVERWRITE INTO TABLE lm12;


-------------------------------------------------------------------------------

--CREATE RESULT TABLE. Store query result externaly in output_dir/qXXresult/
DROP TABLE IF EXISTS store_coefficient;

CREATE TABLE store_coefficient
(
    cat             INT,
    intercept	    DOUBLE,
    slope           DOUBLE
);



INSERT INTO TABLE store_coefficient
    SELECT 1,
           intercept,
           slope        
    FROM lm1;

INSERT INTO TABLE store_coefficient
    SELECT 2,
           intercept,
           slope        
    FROM lm2;

INSERT INTO TABLE store_coefficient
    SELECT 3,
           intercept,
           slope        
    FROM lm3;

INSERT INTO TABLE store_coefficient
    SELECT 4,
           intercept,
           slope        
    FROM lm4;

INSERT INTO TABLE store_coefficient
    SELECT 5,
           intercept,
           slope        
    FROM lm5;

INSERT INTO TABLE store_coefficient
    SELECT 6,
           intercept,
           slope        
    FROM lm6;

INSERT INTO TABLE store_coefficient
    SELECT 7,
           intercept,
           slope        
    FROM lm7;

INSERT INTO TABLE store_coefficient
    SELECT 8,
           intercept,
           slope        
    FROM lm8;

INSERT INTO TABLE store_coefficient
    SELECT 9,
           intercept,
           slope        
    FROM lm9;

INSERT INTO TABLE store_coefficient
    SELECT 10,
           intercept,
           slope        
    FROM lm10;

INSERT INTO TABLE store_coefficient
    SELECT 11,
           intercept,
           slope        
    FROM lm11;

INSERT INTO TABLE store_coefficient
    SELECT 12,
           intercept,
           slope        
    FROM lm12;

-------------------------------------------------------------------------------



--Cleanup




--Display result
--SELECT * FROM ${hiveconf:resultTableName};
