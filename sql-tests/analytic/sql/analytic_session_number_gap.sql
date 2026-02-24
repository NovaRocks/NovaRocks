-- @order_sensitive=true
-- @tags=analytic,session_number
-- Test Objective:
-- 1. Validate session_number gap-splitting semantics under ordered partitions.
-- 2. Prevent regressions where session boundaries are miscomputed across large gaps.
-- Test Flow:
-- 1. Create/reset source table.
-- 2. Insert deterministic timestamp-like integer sequences by group.
-- 3. Compute session_number with fixed gap threshold and assert ordered output.
CREATE DATABASE IF NOT EXISTS sql_tests_d07;
DROP TABLE IF EXISTS sql_tests_d07.t_analytic_session_number_gap;
CREATE TABLE sql_tests_d07.t_analytic_session_number_gap (
    grp VARCHAR(10),
    ts INT
);

INSERT INTO sql_tests_d07.t_analytic_session_number_gap VALUES
    ('A', 1),
    ('A', 3),
    ('A', 10),
    ('A', 11),
    ('A', 20),
    ('B', 5),
    ('B', 7),
    ('B', 30);

SELECT
    grp,
    ts,
    session_number(ts, 2) OVER (PARTITION BY grp ORDER BY ts) AS sess_id
FROM sql_tests_d07.t_analytic_session_number_gap
ORDER BY grp, ts;
