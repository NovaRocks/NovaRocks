-- @order_sensitive=true
-- IV3-8: $manifests lists manifest files with file counts.

-- query 1
-- @skip_result_check=true
CREATE DATABASE iceberg_cat_${suite_uuid0}.iv38m_db_${uuid0};

-- query 2
-- @skip_result_check=true
CREATE TABLE iceberg_cat_${suite_uuid0}.iv38m_db_${uuid0}.t_${uuid0} (id INT, v INT)
TBLPROPERTIES ("format-version" = "3");

-- query 3
-- @skip_result_check=true
INSERT INTO iceberg_cat_${suite_uuid0}.iv38m_db_${uuid0}.t_${uuid0} VALUES (1,10);

-- query 4
-- @skip_result_check=true
INSERT INTO iceberg_cat_${suite_uuid0}.iv38m_db_${uuid0}.t_${uuid0} VALUES (2,20);

-- query 5
-- $manifests is non-empty and reports added data files.
SELECT count(*) > 0 AS has_manifests, sum(added_data_files_count) > 0 AS has_added
  FROM iceberg_cat_${suite_uuid0}.iv38m_db_${uuid0}.t_${uuid0}$manifests;

-- query 6
-- @skip_result_check=true
DROP DATABASE iceberg_cat_${suite_uuid0}.iv38m_db_${uuid0};
