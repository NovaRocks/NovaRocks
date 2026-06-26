-- @order_sensitive=true
-- IV3-2: validate that snapshot summaries carry total-* and engine identity.
-- Numeric carry-forward correctness is unit-tested in commit/*.rs; here we
-- assert the surfaced summary carries exact totals across operations.

-- query 1
-- @skip_result_check=true
CREATE DATABASE iceberg_cat_${suite_uuid0}.iv32_db_${uuid0};

-- query 2
-- @skip_result_check=true
CREATE TABLE iceberg_cat_${suite_uuid0}.iv32_db_${uuid0}.t_${uuid0} (id INT, v INT)
TBLPROPERTIES ("format-version" = "3");

-- query 3
-- @skip_result_check=true
INSERT INTO iceberg_cat_${suite_uuid0}.iv32_db_${uuid0}.t_${uuid0} VALUES (1, 10), (2, 20);

-- query 4
-- @skip_result_check=true
INSERT INTO iceberg_cat_${suite_uuid0}.iv32_db_${uuid0}.t_${uuid0} VALUES (3, 30);

-- query 5
-- Latest append summary carries exact total records/data-files + engine-name.
-- Use a subquery so ORDER BY can reference committed_at without it appearing
-- in the golden output.
SELECT total_data_files_is_2, total_records_is_3, has_total_files_size, has_engine_name
FROM (
  SELECT
    committed_at,
    summary LIKE '%"total-data-files":"2"%'  AS total_data_files_is_2,
    summary LIKE '%"total-records":"3"%'     AS total_records_is_3,
    summary LIKE '%total-files-size%'        AS has_total_files_size,
    summary LIKE '%engine-name%'             AS has_engine_name
  FROM iceberg_cat_${suite_uuid0}.iv32_db_${uuid0}.t_${uuid0}$snapshots
  ORDER BY committed_at DESC
  LIMIT 1
) t;

-- query 6
-- @skip_result_check=true
DELETE FROM iceberg_cat_${suite_uuid0}.iv32_db_${uuid0}.t_${uuid0} WHERE id = 1;

-- query 7
-- Delete snapshot carries the decremented total-records + engine-name.
SELECT total_records_is_2, has_engine_name
FROM (
  SELECT
    committed_at,
    summary LIKE '%"total-records":"2"%' AS total_records_is_2,
    summary LIKE '%engine-name%'         AS has_engine_name
  FROM iceberg_cat_${suite_uuid0}.iv32_db_${uuid0}.t_${uuid0}$snapshots
  ORDER BY committed_at DESC
  LIMIT 1
) t;

-- query 8
-- @skip_result_check=true
DROP DATABASE iceberg_cat_${suite_uuid0}.iv32_db_${uuid0};
