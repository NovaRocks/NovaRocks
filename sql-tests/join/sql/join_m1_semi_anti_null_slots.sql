-- @order_sensitive=true
-- @tags=join,m1,semi,anti,subquery
-- Test Objective:
-- 1. Validate M1 semi/anti selection paths for correlated EXISTS and NOT EXISTS.
-- 2. Ensure semi/anti output keeps only left-side slots.
-- 3. Prevent matched and unmatched probe rows from crossing between the two paths.
-- Test Flow:
-- 1. Create/reset left and right tables.
-- 2. Insert deterministic matching and non-matching keys.
-- 3. Query with EXISTS and NOT EXISTS and assert ordered output.
DROP TABLE IF EXISTS ${case_db}.m1_left_side;
DROP TABLE IF EXISTS ${case_db}.m1_right_side;
CREATE TABLE ${case_db}.m1_left_side (
  k INT,
  v INT
)
TBLPROPERTIES ("format-version" = "3");
CREATE TABLE ${case_db}.m1_right_side (
  k INT,
  payload INT
)
TBLPROPERTIES ("format-version" = "3");
INSERT INTO ${case_db}.m1_left_side VALUES
  (1, 10),
  (2, 20),
  (3, 30);
INSERT INTO ${case_db}.m1_right_side VALUES
  (1, 100),
  (3, 300);
SELECT v
FROM ${case_db}.m1_left_side
WHERE EXISTS (
  SELECT 1
  FROM ${case_db}.m1_right_side
  WHERE m1_left_side.k = m1_right_side.k
)
ORDER BY v;
SELECT v
FROM ${case_db}.m1_left_side
WHERE NOT EXISTS (
  SELECT 1
  FROM ${case_db}.m1_right_side
  WHERE m1_left_side.k = m1_right_side.k
)
ORDER BY v;
