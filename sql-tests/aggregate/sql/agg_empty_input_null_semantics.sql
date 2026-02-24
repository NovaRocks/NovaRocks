-- @order_sensitive=true
-- @tags=aggregate,empty_input
-- Test Objective:
-- 1. Validate aggregate output on empty filtered input.
-- 2. Prevent regressions in COUNT vs nullable aggregate semantics.
-- Test Flow:
-- 1. Create/reset source table.
-- 2. Insert deterministic rows.
-- 3. Aggregate on an empty predicate and assert scalar output.
CREATE DATABASE IF NOT EXISTS sql_tests_d06;
DROP TABLE IF EXISTS sql_tests_d06.t_agg_empty_input_null_semantics;
CREATE TABLE sql_tests_d06.t_agg_empty_input_null_semantics (
    g INT,
    v INT
);

INSERT INTO sql_tests_d06.t_agg_empty_input_null_semantics VALUES
    (1, 10),
    (2, NULL);

SELECT
    COUNT(*) AS c_all,
    COUNT(v) AS c_not_null,
    SUM(v) AS s_v,
    AVG(v) AS avg_v,
    MIN(v) AS min_v,
    MAX(v) AS max_v
FROM sql_tests_d06.t_agg_empty_input_null_semantics
WHERE g = 999;
