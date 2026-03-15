-- @order_sensitive=true
-- @tags=join,anti,subquery
-- Test Objective:
-- 1. Validate NOT EXISTS subquery semantics equivalent to anti-join behavior.
-- 2. Prevent regressions where matched rows are incorrectly retained.
-- Test Flow:
-- 1. Create/reset left and right tables.
-- 2. Insert deterministic overlapping keys.
-- 3. Query with NOT EXISTS and assert ordered output.
DROP TABLE IF EXISTS ${case_db}.t_join_not_exists_l;
DROP TABLE IF EXISTS ${case_db}.t_join_not_exists_r;
CREATE TABLE ${case_db}.t_join_not_exists_l (
  id INT,
  v STRING
);
CREATE TABLE ${case_db}.t_join_not_exists_r (
  id INT
);
INSERT INTO ${case_db}.t_join_not_exists_l VALUES
  (1, 'a'),
  (2, 'b'),
  (3, 'c'),
  (4, 'd');
INSERT INTO ${case_db}.t_join_not_exists_r VALUES
  (2),
  (4);
SELECT l.id, l.v
FROM ${case_db}.t_join_not_exists_l l
WHERE NOT EXISTS (
  SELECT 1
  FROM ${case_db}.t_join_not_exists_r r
  WHERE r.id = l.id
)
ORDER BY l.id;
