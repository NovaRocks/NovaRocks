-- @order_sensitive=true
-- @tags=set_op,union,decimal,null
-- Test Objective:
-- 1. Validate UNION DISTINCT semantics on DECIMAL values with NULLs.
-- 2. Prevent regressions in decimal key comparison and deduplication logic.
-- Test Flow:
-- 1. Build two DECIMAL row sets with duplicates and NULLs.
-- 2. Apply UNION (DISTINCT).
-- 3. Assert deterministic ordered output.
CREATE DATABASE IF NOT EXISTS sql_tests_d08;
DROP TABLE IF EXISTS sql_tests_d08.t_set_union_decimal_l;
DROP TABLE IF EXISTS sql_tests_d08.t_set_union_decimal_r;
CREATE TABLE sql_tests_d08.t_set_union_decimal_l (
    d DECIMAL(10, 2)
);
CREATE TABLE sql_tests_d08.t_set_union_decimal_r (
    d DECIMAL(10, 2)
);

INSERT INTO sql_tests_d08.t_set_union_decimal_l VALUES
    (1.20),
    (NULL),
    (2.50);

INSERT INTO sql_tests_d08.t_set_union_decimal_r VALUES
    (1.20),
    (3.00),
    (NULL);

SELECT d
FROM (
    (
        SELECT d
        FROM sql_tests_d08.t_set_union_decimal_l
    )
    UNION
    (
        SELECT d
        FROM sql_tests_d08.t_set_union_decimal_r
    )
) t
ORDER BY d IS NULL, d;
