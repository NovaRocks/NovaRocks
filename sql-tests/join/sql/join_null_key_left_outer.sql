-- @order_sensitive=true
-- @tags=join,left_outer,null
-- Test Objective:
-- 1. Validate NULL-key behavior under LEFT OUTER JOIN.
-- 2. Prevent regressions where NULL-key left rows incorrectly match right NULL keys.
-- Test Flow:
-- 1. Create/reset left and right tables.
-- 2. Insert rows including NULL and non-NULL keys.
-- 3. Execute LEFT OUTER JOIN and assert null-fill behavior.
CREATE DATABASE IF NOT EXISTS sql_tests_d05;
DROP TABLE IF EXISTS sql_tests_d05.t_join_null_key_left_outer_l;
DROP TABLE IF EXISTS sql_tests_d05.t_join_null_key_left_outer_r;
CREATE TABLE sql_tests_d05.t_join_null_key_left_outer_l (
  k INT,
  vl STRING
);
CREATE TABLE sql_tests_d05.t_join_null_key_left_outer_r (
  k INT,
  vr STRING
);
INSERT INTO sql_tests_d05.t_join_null_key_left_outer_l VALUES
  (NULL, 'LN'),
  (2, 'L2');
INSERT INTO sql_tests_d05.t_join_null_key_left_outer_r VALUES
  (NULL, 'RN'),
  (2, 'R2');
SELECT
  COALESCE(CAST(l.k AS STRING), 'NULL') AS lk,
  l.vl,
  r.vr
FROM sql_tests_d05.t_join_null_key_left_outer_l l
LEFT OUTER JOIN sql_tests_d05.t_join_null_key_left_outer_r r
  ON l.k = r.k
ORDER BY (l.k IS NULL) ASC, l.k ASC;
