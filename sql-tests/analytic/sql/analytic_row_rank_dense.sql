-- @order_sensitive=true
-- @tags=analytic,row_number,rank,dense_rank
-- Test Objective:
-- 1. Validate ROW_NUMBER/RANK/DENSE_RANK semantics with ties.
-- 2. Prevent regressions in ranking gap behavior under duplicate sort keys.
-- Test Flow:
-- 1. Create/reset analytic source table.
-- 2. Insert deterministic rows with tie scores.
-- 3. Compute ranking functions and assert ordered output.
CREATE DATABASE IF NOT EXISTS sql_tests_d07;
DROP TABLE IF EXISTS sql_tests_d07.t_analytic_row_rank_dense;
CREATE TABLE sql_tests_d07.t_analytic_row_rank_dense (
    grp VARCHAR(10),
    id INT,
    score INT
);

INSERT INTO sql_tests_d07.t_analytic_row_rank_dense VALUES
    ('A', 1, 100),
    ('A', 2, 100),
    ('A', 3, 90),
    ('B', 4, 80),
    ('B', 5, 70);

SELECT
    grp,
    id,
    score,
    ROW_NUMBER() OVER (PARTITION BY grp ORDER BY score DESC, id) AS rn,
    RANK() OVER (PARTITION BY grp ORDER BY score DESC) AS rnk,
    DENSE_RANK() OVER (PARTITION BY grp ORDER BY score DESC) AS drnk
FROM sql_tests_d07.t_analytic_row_rank_dense
ORDER BY grp, rn;
