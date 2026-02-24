-- @order_sensitive=true
-- @tags=join,inner,null_safe
-- Test Objective:
-- 1. Validate NULL-safe equality join (`<=>`) matches NULL join keys in hash join paths.
-- 2. Prevent regressions where probe/build NULL keys are skipped as unmatched.
-- Test Flow:
-- 1. Create/reset left and right tables with nullable join keys.
-- 2. Insert deterministic rows including duplicated NULL keys on both sides.
-- 3. Execute INNER JOIN with `<=>` and assert NULL-key multiplicity is preserved.
CREATE DATABASE IF NOT EXISTS sql_tests_d05;
DROP TABLE IF EXISTS sql_tests_d05.t_join_null_safe_equal_l;
DROP TABLE IF EXISTS sql_tests_d05.t_join_null_safe_equal_r;
CREATE TABLE sql_tests_d05.t_join_null_safe_equal_l (
  k INT,
  v STRING
);
CREATE TABLE sql_tests_d05.t_join_null_safe_equal_r (
  k INT,
  v STRING
);
INSERT INTO sql_tests_d05.t_join_null_safe_equal_l VALUES
  (1, 'l1'),
  (NULL, 'ln1'),
  (NULL, 'ln2');
INSERT INTO sql_tests_d05.t_join_null_safe_equal_r VALUES
  (NULL, 'rn1'),
  (NULL, 'rn2'),
  (1, 'r1'),
  (2, 'r2');
SELECT l.v AS lv, r.v AS rv
FROM sql_tests_d05.t_join_null_safe_equal_l l
INNER JOIN sql_tests_d05.t_join_null_safe_equal_r r
  ON l.k <=> r.k
ORDER BY lv, rv;
