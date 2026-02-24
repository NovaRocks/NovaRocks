-- @order_sensitive=true
-- @tags=runtime_filter,right_semi,residual
-- Test Objective:
-- 1. Validate RIGHT SEMI JOIN behavior with additional residual predicate.
-- 2. Prevent regressions where runtime-filter application or match marking ignores residual checks.
-- Test Flow:
-- 1. Create/reset left/right tables.
-- 2. Insert deterministic rows with passing and failing residual conditions.
-- 3. Execute RIGHT SEMI JOIN and assert right-side output rows.
CREATE DATABASE IF NOT EXISTS sql_tests_d10;
DROP TABLE IF EXISTS sql_tests_d10.t_rf_right_semi_residual_l;
DROP TABLE IF EXISTS sql_tests_d10.t_rf_right_semi_residual_r;
CREATE TABLE sql_tests_d10.t_rf_right_semi_residual_l (
    k INT,
    score INT
);
CREATE TABLE sql_tests_d10.t_rf_right_semi_residual_r (
    k INT,
    threshold INT,
    tag VARCHAR(20)
);

INSERT INTO sql_tests_d10.t_rf_right_semi_residual_l VALUES
    (1, 10),
    (1, 2),
    (2, 7),
    (3, 5);

INSERT INTO sql_tests_d10.t_rf_right_semi_residual_r VALUES
    (1, 5, 'r1_pass'),
    (1, 15, 'r1_fail'),
    (2, 8, 'r2_fail'),
    (3, 3, 'r3_pass'),
    (4, 1, 'r4_nomatch'),
    (NULL, 1, 'rnull');

SELECT r.k, r.threshold, r.tag
FROM sql_tests_d10.t_rf_right_semi_residual_l l
RIGHT SEMI JOIN sql_tests_d10.t_rf_right_semi_residual_r r
  ON l.k = r.k AND l.score > r.threshold
ORDER BY r.k, r.threshold;
