-- @order_sensitive=true
-- @tags=runtime_filter,left_join
-- Test Objective:
-- 1. Validate LEFT JOIN unmatched-row preservation with runtime filter enabled.
-- 2. Prevent regressions where probe-side rows are incorrectly dropped.
-- Test Flow:
-- 1. Create/reset left/right tables.
-- 2. Insert partially overlapping keys.
-- 3. Execute LEFT JOIN and assert ordered NULL-fill output.
CREATE DATABASE IF NOT EXISTS sql_tests_d10;
DROP TABLE IF EXISTS sql_tests_d10.t_rf_left_join_preserve_l;
DROP TABLE IF EXISTS sql_tests_d10.t_rf_left_join_preserve_r;
CREATE TABLE sql_tests_d10.t_rf_left_join_preserve_l (
    id INT,
    k INT
);
CREATE TABLE sql_tests_d10.t_rf_left_join_preserve_r (
    k INT,
    tag VARCHAR(20)
);

INSERT INTO sql_tests_d10.t_rf_left_join_preserve_l VALUES
    (1, 10),
    (2, 20),
    (3, 30);

INSERT INTO sql_tests_d10.t_rf_left_join_preserve_r VALUES
    (20, 'r20'),
    (30, 'r30');

SELECT l.id, l.k, r.tag
FROM sql_tests_d10.t_rf_left_join_preserve_l l
LEFT JOIN sql_tests_d10.t_rf_left_join_preserve_r r
  ON l.k = r.k
ORDER BY l.id;
