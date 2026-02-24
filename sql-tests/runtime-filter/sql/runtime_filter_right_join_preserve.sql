-- @order_sensitive=true
-- @tags=runtime_filter,right_join
-- Test Objective:
-- 1. Validate RIGHT JOIN unmatched-row preservation.
-- 2. Prevent regressions where build-side unmatched rows are lost.
-- Test Flow:
-- 1. Create/reset left/right tables.
-- 2. Insert partially overlapping keys.
-- 3. Execute RIGHT JOIN and assert ordered output.
CREATE DATABASE IF NOT EXISTS sql_tests_d10;
DROP TABLE IF EXISTS sql_tests_d10.t_rf_right_join_preserve_l;
DROP TABLE IF EXISTS sql_tests_d10.t_rf_right_join_preserve_r;
CREATE TABLE sql_tests_d10.t_rf_right_join_preserve_l (
    k INT,
    tag_l VARCHAR(20)
);
CREATE TABLE sql_tests_d10.t_rf_right_join_preserve_r (
    k INT,
    tag_r VARCHAR(20)
);

INSERT INTO sql_tests_d10.t_rf_right_join_preserve_l VALUES
    (1, 'l1'),
    (2, 'l2');

INSERT INTO sql_tests_d10.t_rf_right_join_preserve_r VALUES
    (2, 'r2'),
    (3, 'r3');

SELECT l.k AS lk, l.tag_l, r.k AS rk, r.tag_r
FROM sql_tests_d10.t_rf_right_join_preserve_l l
RIGHT JOIN sql_tests_d10.t_rf_right_join_preserve_r r
  ON l.k = r.k
ORDER BY rk;
