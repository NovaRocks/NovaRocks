-- @order_sensitive=true
-- IV3-8: $entries exposes manifest entries with sequence numbers and status.

-- query 1
-- @skip_result_check=true
CREATE DATABASE iceberg_cat_${suite_uuid0}.iv38e_db_${uuid0};

-- query 2
-- @skip_result_check=true
CREATE TABLE iceberg_cat_${suite_uuid0}.iv38e_db_${uuid0}.t_${uuid0} (id INT, v INT)
TBLPROPERTIES ("format-version" = "3");

-- query 3
-- @skip_result_check=true
INSERT INTO iceberg_cat_${suite_uuid0}.iv38e_db_${uuid0}.t_${uuid0} VALUES (1,10);

-- query 4
-- @skip_result_check=true
INSERT INTO iceberg_cat_${suite_uuid0}.iv38e_db_${uuid0}.t_${uuid0} VALUES (2,20);

-- query 5
-- $entries exposes entries with non-null sequence numbers; added entries exist.
SELECT count(*) > 0 AS has_entries
  FROM iceberg_cat_${suite_uuid0}.iv38e_db_${uuid0}.t_${uuid0}$entries;

-- query 6
SELECT count(*) > 0 AS has_seq
  FROM iceberg_cat_${suite_uuid0}.iv38e_db_${uuid0}.t_${uuid0}$entries
  WHERE sequence_number IS NOT NULL;

-- query 7
SELECT count(*) > 0 AS has_added
  FROM iceberg_cat_${suite_uuid0}.iv38e_db_${uuid0}.t_${uuid0}$entries
  WHERE status = 1;

-- query 8
-- @skip_result_check=true
DROP DATABASE iceberg_cat_${suite_uuid0}.iv38e_db_${uuid0};
