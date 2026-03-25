-- @order_sensitive=true
-- Test Objective:
-- 1. Validate Iceberg partition evolution from month -> day using DROP + ADD.
-- 2. Verify data written under the old spec is still readable after replace.
-- 3. Verify new inserts go into the new partition spec.
-- 4. Validate query correctness across both old and new partition specs.
-- 5. Validate evolving identity partition to bucket partition using DROP + ADD.
-- This case uses `${case_db}` so the sql-tests runner can provide per-case
-- database isolation under the suite catalog.

-- query 1
-- Create table with month-level partitioning, insert data, then evolve to day-level partitioning.
DROP TABLE IF EXISTS ${case_db}.t_replace_partition;
CREATE TABLE ${case_db}.t_replace_partition (
  id BIGINT,
  ts DATETIME,
  val DOUBLE
)
PARTITION BY month(ts);
INSERT INTO ${case_db}.t_replace_partition VALUES
  (1, '2024-01-15 10:00:00', 10.0),
  (2, '2024-01-20 11:00:00', 20.0),
  (3, '2024-02-10 12:00:00', 30.0);
ALTER TABLE ${case_db}.t_replace_partition DROP PARTITION COLUMN month(ts);
ALTER TABLE ${case_db}.t_replace_partition ADD PARTITION COLUMN day(ts);
INSERT INTO ${case_db}.t_replace_partition VALUES
  (4, '2024-01-15 13:00:00', 40.0),
  (5, '2024-02-10 14:00:00', 50.0),
  (6, '2024-03-05 15:00:00', 60.0);
SELECT COUNT(*) AS cnt, SUM(val) AS sum_val FROM ${case_db}.t_replace_partition;

-- query 2
-- Verify grouping by month still works across both partition specs.
SELECT DATE_FORMAT(ts, '%Y-%m') AS ym, COUNT(*) AS cnt, SUM(val) AS sum_val
FROM ${case_db}.t_replace_partition
GROUP BY DATE_FORMAT(ts, '%Y-%m')
ORDER BY ym;

-- query 3
-- Filter on a specific date range spanning both old and new specs.
SELECT id, val FROM ${case_db}.t_replace_partition
WHERE ts >= '2024-01-15 00:00:00' AND ts < '2024-01-21 00:00:00'
ORDER BY id;

-- query 4
-- Evolve identity partition column to a bucket partition column.
DROP TABLE IF EXISTS ${case_db}.t_replace_identity_to_bucket;
CREATE TABLE ${case_db}.t_replace_identity_to_bucket (
  id BIGINT,
  city STRING,
  amount DOUBLE
)
PARTITION BY (city);
INSERT INTO ${case_db}.t_replace_identity_to_bucket VALUES
  (1, 'Beijing', 100.0),
  (2, 'Shanghai', 200.0),
  (3, 'Beijing', 150.0);
ALTER TABLE ${case_db}.t_replace_identity_to_bucket DROP PARTITION COLUMN city;
ALTER TABLE ${case_db}.t_replace_identity_to_bucket ADD PARTITION COLUMN bucket(city, 4);
INSERT INTO ${case_db}.t_replace_identity_to_bucket VALUES
  (4, 'Guangzhou', 300.0),
  (5, 'Beijing', 250.0);
SELECT COUNT(*) AS cnt, SUM(amount) AS total FROM ${case_db}.t_replace_identity_to_bucket;

-- query 5
-- Verify group-by on the original column works after replace.
SELECT city, COUNT(*) AS cnt, SUM(amount) AS total
FROM ${case_db}.t_replace_identity_to_bucket
GROUP BY city
ORDER BY city;

-- query 6
-- @skip_result_check=true
-- Cleanup
DROP TABLE ${case_db}.t_replace_partition FORCE;
DROP TABLE ${case_db}.t_replace_identity_to_bucket FORCE;
