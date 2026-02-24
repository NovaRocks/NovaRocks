-- @order_sensitive=true
-- @tags=join,inner,null
-- Test Objective:
-- 1. Validate that NULL join keys do not match under equality INNER JOIN.
-- 2. Prevent regressions where NULL=NULL is treated as a match in join keys.
-- Test Flow:
-- 1. Create/reset left and right tables.
-- 2. Insert rows including NULL and non-NULL keys.
-- 3. Execute INNER JOIN and assert only non-NULL matched rows remain.
CREATE DATABASE IF NOT EXISTS sql_tests_d05;
DROP TABLE IF EXISTS sql_tests_d05.t_join_null_key_inner_l;
DROP TABLE IF EXISTS sql_tests_d05.t_join_null_key_inner_r;
CREATE TABLE sql_tests_d05.t_join_null_key_inner_l (
  k INT,
  vl STRING
);
CREATE TABLE sql_tests_d05.t_join_null_key_inner_r (
  k INT,
  vr STRING
);
INSERT INTO sql_tests_d05.t_join_null_key_inner_l VALUES
  (NULL, 'LN'),
  (2, 'L2');
INSERT INTO sql_tests_d05.t_join_null_key_inner_r VALUES
  (NULL, 'RN'),
  (2, 'R2');
SELECT l.k, l.vl, r.vr
FROM sql_tests_d05.t_join_null_key_inner_l l
INNER JOIN sql_tests_d05.t_join_null_key_inner_r r
  ON l.k = r.k
ORDER BY l.k;
