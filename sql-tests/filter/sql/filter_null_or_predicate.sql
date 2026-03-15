-- @order_sensitive=true
-- @tags=filter,null
-- Test Objective:
-- 1. Validate OR predicate behavior when one side depends on IS NULL checks.
-- 2. Prevent regressions in boolean short-circuit style evaluation for nullable columns.
-- Test Flow:
-- 1. Create/reset source table.
-- 2. Insert rows covering NULL and non-NULL combinations.
-- 3. Filter with OR predicate and assert deterministic ordering.
DROP TABLE IF EXISTS ${case_db}.t_filter_null_or_predicate;
CREATE TABLE ${case_db}.t_filter_null_or_predicate (
  id INT,
  a INT,
  b STRING
);
INSERT INTO ${case_db}.t_filter_null_or_predicate VALUES
  (1, 1, 'x'),
  (2, NULL, 'x'),
  (3, 2, 'y'),
  (4, NULL, NULL),
  (5, 5, 'z');
SELECT id, a, b
FROM ${case_db}.t_filter_null_or_predicate
WHERE a IS NULL OR b = 'y'
ORDER BY id;
