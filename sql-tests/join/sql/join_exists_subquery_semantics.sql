-- @order_sensitive=true
-- @tags=join,semi,subquery
-- Test Objective:
-- 1. Validate EXISTS subquery semantics equivalent to semi-join behavior.
-- 2. Prevent regressions in correlated subquery key matching.
-- Test Flow:
-- 1. Create/reset left and right tables.
-- 2. Insert deterministic overlapping keys.
-- 3. Query with EXISTS and assert ordered output.
DROP TABLE IF EXISTS ${case_db}.t_join_exists_l;
DROP TABLE IF EXISTS ${case_db}.t_join_exists_r;
CREATE TABLE ${case_db}.t_join_exists_l (
  id INT,
  v STRING
);
CREATE TABLE ${case_db}.t_join_exists_r (
  id INT
);
INSERT INTO ${case_db}.t_join_exists_l VALUES
  (1, 'a'),
  (2, 'b'),
  (3, 'c'),
  (4, 'd');
INSERT INTO ${case_db}.t_join_exists_r VALUES
  (2),
  (4);
SELECT l.id, l.v
FROM ${case_db}.t_join_exists_l l
WHERE EXISTS (
  SELECT 1
  FROM ${case_db}.t_join_exists_r r
  WHERE r.id = l.id
)
ORDER BY l.id;
