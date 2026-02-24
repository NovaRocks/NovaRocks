-- @order_sensitive=true
-- @tags=analytic,range_frame,unbounded_following
-- Test Objective:
-- 1. Validate RANGE UNBOUNDED PRECEDING TO UNBOUNDED FOLLOWING semantics.
-- 2. Prevent regressions where RANGE full-partition frame is truncated by peer groups.
-- Test Flow:
-- 1. Create/reset source table.
-- 2. Insert deterministic rows with duplicate order keys and NULL value.
-- 3. Compute COUNT/SUM over full RANGE frame and assert stable output.
CREATE DATABASE IF NOT EXISTS sql_tests_d07;
DROP TABLE IF EXISTS sql_tests_d07.t_analytic_range_unbounded_following_full_partition;
CREATE TABLE sql_tests_d07.t_analytic_range_unbounded_following_full_partition (
    grp VARCHAR(10),
    ord_key INT,
    v INT
);

INSERT INTO sql_tests_d07.t_analytic_range_unbounded_following_full_partition VALUES
    ('A', 1, 10),
    ('A', 2, 20),
    ('A', 2, 30),
    ('A', 3, NULL);

SELECT
    grp,
    ord_key,
    v,
    COUNT(v) OVER (
        PARTITION BY grp ORDER BY ord_key
        RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS cnt_all_non_null,
    SUM(v) OVER (
        PARTITION BY grp ORDER BY ord_key
        RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS sum_all
FROM sql_tests_d07.t_analytic_range_unbounded_following_full_partition
ORDER BY grp, ord_key, v;
