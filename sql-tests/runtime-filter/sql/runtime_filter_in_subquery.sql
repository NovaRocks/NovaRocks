-- @order_sensitive=true
-- @tags=runtime_filter,in_subquery
-- Test Objective:
-- 1. Validate IN-subquery semantics in runtime-filter-enabled paths.
-- 2. Prevent regressions where IN filtering diverges from join semantics.
-- Test Flow:
-- 1. Create/reset source tables.
-- 2. Insert deterministic rows.
-- 3. Filter probe rows with IN subquery and assert ordered output.
CREATE DATABASE IF NOT EXISTS sql_tests_d10;
DROP TABLE IF EXISTS sql_tests_d10.t_rf_in_subquery_l;
DROP TABLE IF EXISTS sql_tests_d10.t_rf_in_subquery_r;
CREATE TABLE sql_tests_d10.t_rf_in_subquery_l (
    id INT,
    k INT
);
CREATE TABLE sql_tests_d10.t_rf_in_subquery_r (
    k INT,
    keep_flag INT
);

INSERT INTO sql_tests_d10.t_rf_in_subquery_l VALUES
    (1, 10),
    (2, 20),
    (3, 30),
    (4, 40);

INSERT INTO sql_tests_d10.t_rf_in_subquery_r VALUES
    (20, 1),
    (30, 1),
    (50, 1),
    (10, 0);

SELECT id, k
FROM sql_tests_d10.t_rf_in_subquery_l
WHERE k IN (
    SELECT k
    FROM sql_tests_d10.t_rf_in_subquery_r
    WHERE keep_flag = 1
)
ORDER BY id;
