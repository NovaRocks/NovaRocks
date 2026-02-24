-- @order_sensitive=true
-- @tags=runtime_filter,multi_join
-- Test Objective:
-- 1. Validate multi-join chain semantics under runtime filter propagation.
-- 2. Prevent regressions where intermediate runtime filters over-prune downstream joins.
-- Test Flow:
-- 1. Create/reset three join tables.
-- 2. Insert deterministic chain keys.
-- 3. Execute chained inner joins and assert ordered output.
CREATE DATABASE IF NOT EXISTS sql_tests_d10;
DROP TABLE IF EXISTS sql_tests_d10.t_rf_three_table_chain_a;
DROP TABLE IF EXISTS sql_tests_d10.t_rf_three_table_chain_b;
DROP TABLE IF EXISTS sql_tests_d10.t_rf_three_table_chain_c;
CREATE TABLE sql_tests_d10.t_rf_three_table_chain_a (
    id INT,
    k1 INT
);
CREATE TABLE sql_tests_d10.t_rf_three_table_chain_b (
    k1 INT,
    k2 INT
);
CREATE TABLE sql_tests_d10.t_rf_three_table_chain_c (
    k2 INT,
    payload VARCHAR(20)
);

INSERT INTO sql_tests_d10.t_rf_three_table_chain_a VALUES
    (1, 10),
    (2, 20),
    (3, 30);

INSERT INTO sql_tests_d10.t_rf_three_table_chain_b VALUES
    (10, 100),
    (20, 200),
    (40, 400);

INSERT INTO sql_tests_d10.t_rf_three_table_chain_c VALUES
    (100, 'c100'),
    (200, 'c200'),
    (300, 'c300');

SELECT a.id, a.k1, b.k2, c.payload
FROM sql_tests_d10.t_rf_three_table_chain_a a
JOIN sql_tests_d10.t_rf_three_table_chain_b b
  ON a.k1 = b.k1
JOIN sql_tests_d10.t_rf_three_table_chain_c c
  ON b.k2 = c.k2
ORDER BY a.id;
