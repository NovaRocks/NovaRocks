-- @order_sensitive=true
-- IV3-2: validate that snapshot summaries carry total-* and engine identity.
-- Numeric carry-forward correctness is unit-tested in commit/*.rs; here we
-- assert the keys are present in the surfaced summary across operations.

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
-- Latest append summary carries all expected total-* keys + engine-name.
-- Use a subquery so ORDER BY can reference snapshot_id without it appearing
-- in the golden output.
SELECT has_total_data_files, has_total_records, has_total_files_size, has_engine_name
FROM (
  SELECT
    snapshot_id,
    summary LIKE '%total-data-files%'        AS has_total_data_files,
    summary LIKE '%total-records%'           AS has_total_records,
    summary LIKE '%total-files-size%'        AS has_total_files_size,
    summary LIKE '%engine-name%'             AS has_engine_name
  FROM iceberg_cat_${suite_uuid0}.iv32_db_${uuid0}.t_${uuid0}$snapshots
  ORDER BY snapshot_id DESC
  LIMIT 1
) t;

-- query 6
-- @skip_result_check=true
DELETE FROM iceberg_cat_${suite_uuid0}.iv32_db_${uuid0}.t_${uuid0} WHERE id = 1;

-- query 7
-- Delete snapshot also carries totals + engine-name.
SELECT has_total_records, has_engine_name
FROM (
  SELECT
    snapshot_id,
    summary LIKE '%total-records%'   AS has_total_records,
    summary LIKE '%engine-name%'     AS has_engine_name
  FROM iceberg_cat_${suite_uuid0}.iv32_db_${uuid0}.t_${uuid0}$snapshots
  ORDER BY snapshot_id DESC
  LIMIT 1
) t;

-- query 8
-- @skip_result_check=true
DROP DATABASE iceberg_cat_${suite_uuid0}.iv32_db_${uuid0};
