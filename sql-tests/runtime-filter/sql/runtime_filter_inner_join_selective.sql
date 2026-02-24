-- @order_sensitive=true
-- @tags=runtime_filter,inner_join
-- Test Objective:
-- 1. Validate inner-join semantics under selective build-side filter.
-- 2. Prevent regressions where runtime filter drops valid probe rows.
-- Test Flow:
-- 1. Create/reset probe/build tables.
-- 2. Insert deterministic keys with partial overlap.
-- 3. Join with build-side predicate and assert ordered output.
CREATE DATABASE IF NOT EXISTS sql_tests_d10;
DROP TABLE IF EXISTS sql_tests_d10.t_rf_inner_join_selective_l;
DROP TABLE IF EXISTS sql_tests_d10.t_rf_inner_join_selective_r;
CREATE TABLE sql_tests_d10.t_rf_inner_join_selective_l (
    id INT,
    k INT,
    v VARCHAR(20)
);
CREATE TABLE sql_tests_d10.t_rf_inner_join_selective_r (
    k INT,
    tag VARCHAR(20)
);

INSERT INTO sql_tests_d10.t_rf_inner_join_selective_l VALUES
    (1, 10, 'l1'),
    (2, 20, 'l2'),
    (3, 30, 'l3'),
    (4, 40, 'l4');

INSERT INTO sql_tests_d10.t_rf_inner_join_selective_r VALUES
    (10, 'drop'),
    (20, 'keep'),
    (30, 'keep'),
    (50, 'keep');

SELECT l.id, l.k, r.tag
FROM sql_tests_d10.t_rf_inner_join_selective_l l
JOIN sql_tests_d10.t_rf_inner_join_selective_r r
  ON l.k = r.k
WHERE r.tag = 'keep'
ORDER BY l.id;
