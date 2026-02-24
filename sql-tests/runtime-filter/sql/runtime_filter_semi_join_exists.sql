-- @order_sensitive=true
-- @tags=runtime_filter,semi_join,exists
-- Test Objective:
-- 1. Validate EXISTS-semi semantics in join-like filtering path.
-- 2. Prevent regressions where runtime-filter pruning changes semi-join result.
-- Test Flow:
-- 1. Create/reset source tables.
-- 2. Insert deterministic overlapping keys.
-- 3. Filter left rows via EXISTS subquery and assert ordered output.
CREATE DATABASE IF NOT EXISTS sql_tests_d10;
DROP TABLE IF EXISTS sql_tests_d10.t_rf_semi_join_exists_l;
DROP TABLE IF EXISTS sql_tests_d10.t_rf_semi_join_exists_r;
CREATE TABLE sql_tests_d10.t_rf_semi_join_exists_l (
    id INT,
    k INT
);
CREATE TABLE sql_tests_d10.t_rf_semi_join_exists_r (
    k INT
);

INSERT INTO sql_tests_d10.t_rf_semi_join_exists_l VALUES
    (1, 10),
    (2, 20),
    (3, 30);

INSERT INTO sql_tests_d10.t_rf_semi_join_exists_r VALUES
    (20),
    (30),
    (40);

SELECT l.id, l.k
FROM sql_tests_d10.t_rf_semi_join_exists_l l
WHERE EXISTS (
    SELECT 1
    FROM sql_tests_d10.t_rf_semi_join_exists_r r
    WHERE r.k = l.k
)
ORDER BY l.id;
