-- @order_sensitive=true
-- IV3-8: $files lists data files with stats; cross-checks file/record count.

-- query 1
-- @skip_result_check=true
CREATE DATABASE iceberg_cat_${suite_uuid0}.iv38f_db_${uuid0};

-- query 2
-- @skip_result_check=true
CREATE TABLE iceberg_cat_${suite_uuid0}.iv38f_db_${uuid0}.t_${uuid0} (id INT, v INT)
TBLPROPERTIES ("format-version" = "3");

-- query 3
-- @skip_result_check=true
INSERT INTO iceberg_cat_${suite_uuid0}.iv38f_db_${uuid0}.t_${uuid0} VALUES (1,10);

-- query 4
-- @skip_result_check=true
INSERT INTO iceberg_cat_${suite_uuid0}.iv38f_db_${uuid0}.t_${uuid0} VALUES (2,20);

-- query 5
-- two appends -> two data files, all content=0
SELECT count(*) AS n_data_files
  FROM iceberg_cat_${suite_uuid0}.iv38f_db_${uuid0}.t_${uuid0}$files
  WHERE content = 0;

-- query 6
-- total record_count across data files == 2 rows inserted
SELECT sum(record_count) AS total_records
  FROM iceberg_cat_${suite_uuid0}.iv38f_db_${uuid0}.t_${uuid0}$files
  WHERE content = 0;

-- query 7
-- @skip_result_check=true
DROP DATABASE iceberg_cat_${suite_uuid0}.iv38f_db_${uuid0};
