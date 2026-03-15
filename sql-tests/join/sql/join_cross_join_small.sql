-- @order_sensitive=true
-- @tags=join,cross,nestloop
-- Test Objective:
-- 1. Validate CROSS JOIN cartesian semantics.
-- 2. Prevent regressions in nested-loop style cross product generation.
-- Test Flow:
-- 1. Create/reset two tiny tables.
-- 2. Insert deterministic rows on each side.
-- 3. Execute CROSS JOIN and assert ordered cartesian output.
DROP TABLE IF EXISTS ${case_db}.t_join_cross_a;
DROP TABLE IF EXISTS ${case_db}.t_join_cross_b;
CREATE TABLE ${case_db}.t_join_cross_a (
  id INT
);
CREATE TABLE ${case_db}.t_join_cross_b (
  c STRING
);
INSERT INTO ${case_db}.t_join_cross_a VALUES
  (1),
  (2);
INSERT INTO ${case_db}.t_join_cross_b VALUES
  ('x'),
  ('y');
SELECT a.id, b.c
FROM ${case_db}.t_join_cross_a a
CROSS JOIN ${case_db}.t_join_cross_b b
ORDER BY a.id, b.c;
