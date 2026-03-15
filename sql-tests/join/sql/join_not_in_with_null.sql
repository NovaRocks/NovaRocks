-- @order_sensitive=true
-- @tags=join,anti,not_in,null
-- Test Objective:
-- 1. Validate NOT IN semantics when subquery set contains NULL.
-- 2. Prevent regressions in null-aware anti behavior for scalar predicates.
-- Test Flow:
-- 1. Create/reset left and right tables.
-- 2. Insert deterministic left values and right values including NULL.
-- 3. Query with NOT IN and assert expected empty result set directly.
DROP TABLE IF EXISTS ${case_db}.t_join_not_in_with_null_l;
DROP TABLE IF EXISTS ${case_db}.t_join_not_in_with_null_r;
CREATE TABLE ${case_db}.t_join_not_in_with_null_l (
  id INT
);
CREATE TABLE ${case_db}.t_join_not_in_with_null_r (
  id INT
);
INSERT INTO ${case_db}.t_join_not_in_with_null_l VALUES
  (1),
  (2),
  (3);
INSERT INTO ${case_db}.t_join_not_in_with_null_r VALUES
  (2),
  (NULL);
SELECT id
FROM ${case_db}.t_join_not_in_with_null_l
WHERE id NOT IN (
  SELECT id FROM ${case_db}.t_join_not_in_with_null_r
)
ORDER BY id;
