-- @order_sensitive=true
-- @tags=join,left_outer,residual,null
-- Test Objective:
-- 1. Validate LEFT OUTER JOIN semantics when residual predicate evaluates to NULL.
-- 2. Prevent regressions where residual-NULL candidates are treated as matched rows.
-- Test Flow:
-- 1. Create/reset left and right tables.
-- 2. Insert rows that produce TRUE/FALSE/NULL outcomes for residual predicates.
-- 3. Execute LEFT OUTER JOIN with residual predicate and assert unmatched null-fill output.
CREATE DATABASE IF NOT EXISTS sql_tests_d05;
DROP TABLE IF EXISTS sql_tests_d05.t_join_left_outer_residual_nullable_l;
DROP TABLE IF EXISTS sql_tests_d05.t_join_left_outer_residual_nullable_r;
CREATE TABLE sql_tests_d05.t_join_left_outer_residual_nullable_l (
  id INT,
  lv INT
);
CREATE TABLE sql_tests_d05.t_join_left_outer_residual_nullable_r (
  id INT,
  rv INT,
  flag STRING
);
INSERT INTO sql_tests_d05.t_join_left_outer_residual_nullable_l VALUES
  (1, 5),
  (2, 8),
  (3, 10);
INSERT INTO sql_tests_d05.t_join_left_outer_residual_nullable_r VALUES
  (1, 7, 'Y'),
  (1, 9, NULL),
  (2, 6, NULL),
  (4, 1, 'Y');
SELECT l.id, l.lv, r.rv, r.flag
FROM sql_tests_d05.t_join_left_outer_residual_nullable_l l
LEFT OUTER JOIN sql_tests_d05.t_join_left_outer_residual_nullable_r r
  ON l.id = r.id AND l.lv < r.rv AND r.flag = 'Y'
ORDER BY l.id, r.rv;
