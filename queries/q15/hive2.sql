
set QUERY_NUM=q15;
set QUERY_TMP_DIR=${env:BIG_BENCH_HDFS_ABSOLUTE_TEMP_DIR}/${hiveconf:QUERY_NUM}tmp;
set resultTableName=${hiveconf:QUERY_NUM}result;
set resultFile=${env:BIG_BENCH_HDFS_ABSOLUTE_QUERY_RESULT_DIR}/${hiveconf:resultTableName};

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


CREATE TABLE lm1 (
    intercept            DOUBLE,
    slope                DOUBLE
 )
 ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
;

CREATE TABLE lm2 (
    intercept            DOUBLE,
    slope                DOUBLE
 )
 ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
;

CREATE TABLE lm3 (
    intercept            DOUBLE,
    slope                DOUBLE
 )
 ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
;

CREATE TABLE lm4 (
    intercept            DOUBLE,
    slope                DOUBLE
 )
 ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
;

CREATE TABLE lm5 (
    intercept            DOUBLE,
    slope                DOUBLE
 )
 ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
;

CREATE TABLE lm6 (
    intercept            DOUBLE,
    slope                DOUBLE
 )
 ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
;

CREATE TABLE lm7 (
    intercept            DOUBLE,
    slope                DOUBLE
 )
 ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
;

CREATE TABLE lm8 (
    intercept            DOUBLE,
    slope                DOUBLE
 )
 ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
;

CREATE TABLE lm9 (
    intercept            DOUBLE,
    slope                DOUBLE
 )
 ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
;

CREATE TABLE lm10 (
    intercept            DOUBLE,
    slope                DOUBLE
 )
 ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
;

-- (!) Set Paths --------------------------------------------------------------
LOAD DATA INPATH '${hiveconf:QUERY_TMP_DIR}/output1/part-r-00000' OVERWRITE INTO TABLE lm1;
LOAD DATA INPATH '${hiveconf:QUERY_TMP_DIR}/output2/part-r-00000' OVERWRITE INTO TABLE lm2;
LOAD DATA INPATH '${hiveconf:QUERY_TMP_DIR}/output3/part-r-00000' OVERWRITE INTO TABLE lm3;
LOAD DATA INPATH '${hiveconf:QUERY_TMP_DIR}/output4/part-r-00000' OVERWRITE INTO TABLE lm4;
LOAD DATA INPATH '${hiveconf:QUERY_TMP_DIR}/output5/part-r-00000' OVERWRITE INTO TABLE lm5;
LOAD DATA INPATH '${hiveconf:QUERY_TMP_DIR}/output6/part-r-00000' OVERWRITE INTO TABLE lm6;
LOAD DATA INPATH '${hiveconf:QUERY_TMP_DIR}/output7/part-r-00000' OVERWRITE INTO TABLE lm7;
LOAD DATA INPATH '${hiveconf:QUERY_TMP_DIR}/output8/part-r-00000' OVERWRITE INTO TABLE lm8;
LOAD DATA INPATH '${hiveconf:QUERY_TMP_DIR}/output9/part-r-00000' OVERWRITE INTO TABLE lm9;
LOAD DATA INPATH '${hiveconf:QUERY_TMP_DIR}/output10/part-r-00000' OVERWRITE INTO TABLE lm10;

-------------------------------------------------------------------------------
--CREATE RESULT TABLE. Store query result externaly in output_dir/qXXresult/
DROP TABLE IF EXISTS ${hiveconf:resultTableName};

CREATE TABLE ${hiveconf:resultTableName}
(
    cat             INT,
    intercept	    DOUBLE,
    slope           DOUBLE
)
ROW FORMAT
DELIMITED FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION '${hiveconf:resultFile}' 
;



INSERT INTO TABLE ${hiveconf:resultTableName}
    SELECT 1,
           intercept,
           slope        
    FROM lm1
    WHERE slope < 0;

INSERT INTO TABLE ${hiveconf:resultTableName}
    SELECT 2,
           intercept,
           slope        
    FROM lm2
    WHERE slope < 0;

INSERT INTO TABLE ${hiveconf:resultTableName}
    SELECT 3,
           intercept,
           slope        
    FROM lm3
    WHERE slope < 0;

INSERT INTO TABLE ${hiveconf:resultTableName}
    SELECT 4,
           intercept,
           slope        
    FROM lm4
    WHERE slope < 0;

INSERT INTO TABLE ${hiveconf:resultTableName}
    SELECT 5,
           intercept,
           slope        
    FROM lm5
    WHERE slope < 0;

INSERT INTO TABLE ${hiveconf:resultTableName}
    SELECT 6,
           intercept,
           slope        
    FROM lm6
    WHERE slope < 0;

INSERT INTO TABLE ${hiveconf:resultTableName}
    SELECT 7,
           intercept,
           slope        
    FROM lm7
    WHERE slope < 0;

INSERT INTO TABLE ${hiveconf:resultTableName}
    SELECT 8,
           intercept,
           slope        
    FROM lm8
    WHERE slope < 0;

INSERT INTO TABLE ${hiveconf:resultTableName}
    SELECT 9,
           intercept,
           slope        
    FROM lm9
    WHERE slope < 0;

INSERT INTO TABLE ${hiveconf:resultTableName}
    SELECT 10,
           intercept,
           slope        
    FROM lm10
    WHERE slope < 0;

----cleaning up ----------------------------------------------------------------------

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



