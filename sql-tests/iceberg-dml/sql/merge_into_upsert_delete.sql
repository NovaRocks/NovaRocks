-- @order_sensitive=true
-- @tags=iceberg_dml,merge,delete
-- Test Objective:
-- 1. Validate MERGE INTO on an Iceberg v3 table updates existing rows and inserts new ones.
-- 2. Validate a follow-up DELETE removes the target row from visible result.

-- query 1
-- @skip_result_check=true
DROP TABLE IF EXISTS ${case_db}.t_merge_target;
DROP TABLE IF EXISTS ${case_db}.t_merge_source;
CREATE TABLE ${case_db}.t_merge_target (
  city_id INT,
  population INT,
  city STRING
) TBLPROPERTIES ("format-version" = "3", "write.row-lineage" = "true");
CREATE TABLE ${case_db}.t_merge_source (
  city_id INT,
  population INT,
  city STRING
) TBLPROPERTIES ("format-version" = "3");
INSERT INTO ${case_db}.t_merge_target VALUES
  (1, 100, 'Beijing'),
  (2, 200, 'Shanghai');
INSERT INTO ${case_db}.t_merge_source VALUES
  (2, 250, 'Shanghai-updated'),
  (3, 300, 'Shenzhen');

-- query 2
-- @skip_result_check=true
MERGE INTO ${case_db}.t_merge_target AS t
  USING ${case_db}.t_merge_source AS s
  ON t.city_id = s.city_id
  WHEN MATCHED THEN UPDATE SET population = s.population, city = s.city
  WHEN NOT MATCHED THEN INSERT (city_id, population, city) VALUES (s.city_id, s.population, s.city);

-- query 3
SELECT city_id, population, city
FROM ${case_db}.t_merge_target
ORDER BY city_id;

-- query 4
-- @skip_result_check=true
DELETE FROM ${case_db}.t_merge_target
WHERE city_id = 1;

-- query 5
SELECT city_id, population, city
FROM ${case_db}.t_merge_target
ORDER BY city_id;
