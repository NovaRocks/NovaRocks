-- @order_sensitive=true
-- @tags=join,m1,residual,selection
-- Test Objective:
-- 1. Validate M1 selection-path residual predicates after hash-key matching.
-- 2. Preserve duplicate build-row matches that pass the residual predicate.
-- 3. Prevent null-key rows from leaking into normal equality join output.
-- Test Flow:
-- 1. Create/reset probe and build tables.
-- 2. Insert duplicate keys, residual-pass/residual-fail rows, and NULL keys.
-- 3. Execute INNER JOIN with a residual predicate and assert ordered output.
DROP TABLE IF EXISTS ${case_db}.m1_probe;
DROP TABLE IF EXISTS ${case_db}.m1_build;
CREATE TABLE ${case_db}.m1_probe (
  k INT,
  v INT
)
TBLPROPERTIES ("format-version" = "3");
CREATE TABLE ${case_db}.m1_build (
  k INT,
  threshold_v INT,
  tag INT
)
TBLPROPERTIES ("format-version" = "3");
INSERT INTO ${case_db}.m1_probe VALUES
  (1, 10),
  (1, 30),
  (2, 20),
  (3, 40),
  (NULL, 50);
INSERT INTO ${case_db}.m1_build VALUES
  (1, 15, 100),
  (1, 25, 200),
  (2, 10, 300),
  (NULL, 0, 400);
SELECT p.k, p.v, b.threshold_v, b.tag
FROM ${case_db}.m1_probe p
JOIN ${case_db}.m1_build b
  ON p.k = b.k AND p.v > b.threshold_v
ORDER BY p.k, p.v, b.threshold_v, b.tag;
