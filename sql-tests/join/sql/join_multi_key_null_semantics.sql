-- @order_sensitive=true
-- @tags=join,inner,multi_key,null
-- Test Objective:
-- 1. Validate multi-column INNER JOIN key matching semantics with NULL keys.
-- 2. Prevent regressions where rows with any NULL join key are incorrectly matched.
-- Test Flow:
-- 1. Create/reset left and right tables with two join-key columns.
-- 2. Insert deterministic rows including NULL-containing keys on both sides.
-- 3. Execute multi-key INNER JOIN and assert only fully non-NULL key matches.
CREATE DATABASE IF NOT EXISTS sql_tests_d05;
DROP TABLE IF EXISTS sql_tests_d05.t_join_multi_key_null_l;
DROP TABLE IF EXISTS sql_tests_d05.t_join_multi_key_null_r;
CREATE TABLE sql_tests_d05.t_join_multi_key_null_l (
  k1 INT,
  k2 INT,
  lv STRING
);
CREATE TABLE sql_tests_d05.t_join_multi_key_null_r (
  k1 INT,
  k2 INT,
  rv STRING
);
INSERT INTO sql_tests_d05.t_join_multi_key_null_l VALUES
  (1, 1, 'l11'),
  (1, NULL, 'l1n'),
  (NULL, 1, 'ln1'),
  (2, 2, 'l22'),
  (2, 3, 'l23');
INSERT INTO sql_tests_d05.t_join_multi_key_null_r VALUES
  (1, 1, 'r11'),
  (1, NULL, 'r1n'),
  (NULL, 1, 'rn1'),
  (2, 2, 'r22'),
  (2, 4, 'r24');
SELECT l.k1, l.k2, l.lv, r.rv
FROM sql_tests_d05.t_join_multi_key_null_l l
INNER JOIN sql_tests_d05.t_join_multi_key_null_r r
  ON l.k1 = r.k1 AND l.k2 = r.k2
ORDER BY l.lv;
