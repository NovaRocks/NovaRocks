-- @order_sensitive=true
-- @tags=write_path,iceberg,row_lineage,overwrite
-- Test Point:
--   Iceberg v3 full-table INSERT OVERWRITE is delete+insert, not
--   row-lineage carry-forward. Even when business columns are identical,
--   overwritten rows receive fresh _row_id values.

-- query 1
-- @skip_result_check=true
CREATE TABLE ${case_db}.t_full_overwrite_lineage (
  id INT,
  v INT
)
TBLPROPERTIES (
  "format-version" = "3",
  "write.row-lineage" = "true"
);
INSERT INTO ${case_db}.t_full_overwrite_lineage VALUES
  (1, 10),
  (2, 20);
ALTER TABLE iceberg_dml_cat_${suite_uuid0}.${case_db}.t_full_overwrite_lineage CREATE TAG before_overwrite;

-- query 2
SELECT id, v
FROM ${case_db}.t_full_overwrite_lineage
ORDER BY id;

-- query 3
-- @skip_result_check=true
INSERT OVERWRITE ${case_db}.t_full_overwrite_lineage VALUES
  (1, 10),
  (2, 20);

-- query 4
SELECT id, v
FROM ${case_db}.t_full_overwrite_lineage
ORDER BY id;

-- query 5
SELECT
  SUM(CASE WHEN cur.cur_row_id = old.old_row_id THEN 1 ELSE 0 END) AS same_row_ids,
  SUM(CASE WHEN cur.cur_row_id <> old.old_row_id THEN 1 ELSE 0 END) AS changed_row_ids,
  COUNT(cur.id) AS current_rows,
  COUNT(old.id) AS historical_rows
FROM (
  SELECT id, _row_id AS cur_row_id
  FROM ${case_db}.t_full_overwrite_lineage
) cur
JOIN (
  SELECT id, _row_id AS old_row_id
  FROM ${case_db}.t_full_overwrite_lineage FOR VERSION AS OF 'before_overwrite'
) old
ON cur.id = old.id;

-- query 6
-- @skip_result_check=true
ALTER TABLE iceberg_dml_cat_${suite_uuid0}.${case_db}.t_full_overwrite_lineage DROP TAG before_overwrite;
DROP TABLE ${case_db}.t_full_overwrite_lineage FORCE;
