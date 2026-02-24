-- @order_sensitive=true
-- @tags=analytic,rows_frame,empty_frame
-- Test Objective:
-- 1. Validate ROWS FOLLOWING window-frame behavior when tail rows produce empty frames.
-- 2. Prevent regressions where empty frames return non-NULL SUM or non-zero COUNT(expr).
-- Test Flow:
-- 1. Create/reset source table.
-- 2. Insert deterministic ordered rows with nullable values.
-- 3. Compute SUM/COUNT over FOLLOWING frame and assert ordered output.
CREATE DATABASE IF NOT EXISTS sql_tests_d07;
DROP TABLE IF EXISTS sql_tests_d07.t_analytic_rows_following_empty_frame;
CREATE TABLE sql_tests_d07.t_analytic_rows_following_empty_frame (
    grp VARCHAR(10),
    ts INT,
    v INT
);

INSERT INTO sql_tests_d07.t_analytic_rows_following_empty_frame VALUES
    ('A', 1, 10),
    ('A', 2, 20),
    ('A', 3, NULL),
    ('A', 4, 40);

SELECT
    grp,
    ts,
    v,
    SUM(v) OVER (
        PARTITION BY grp ORDER BY ts
        ROWS BETWEEN 2 FOLLOWING AND 3 FOLLOWING
    ) AS sum_follow,
    COUNT(v) OVER (
        PARTITION BY grp ORDER BY ts
        ROWS BETWEEN 2 FOLLOWING AND 3 FOLLOWING
    ) AS cnt_follow
FROM sql_tests_d07.t_analytic_rows_following_empty_frame
ORDER BY grp, ts;
