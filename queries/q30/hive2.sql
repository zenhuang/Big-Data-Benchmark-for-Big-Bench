-------------------------------------------------------------------------------    
--Part 3
-------------------------------------------------------------------------------    

DROP TABLE IF EXISTS c_affinity_out;

CREATE TABLE c_affinity_out (
    customer_id    STRING,
    score          STRING
) STORED AS TEXTFILE;


LOAD DATA INPATH '/mahout_result/thirty/part-r-00000' OVERWRITE INTO TABLE c_affinity_out;

