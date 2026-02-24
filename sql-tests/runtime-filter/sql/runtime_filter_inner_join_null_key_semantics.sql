-- @order_sensitive=true
-- @tags=runtime_filter,inner_join,null_key
-- Test Objective:
-- 1. Validate INNER JOIN key semantics with NULL keys under runtime-filter-enabled path.
-- 2. Prevent regressions where NULL-key rows are incorrectly matched or retained.
-- Test Flow:
-- 1. Create/reset probe/build tables.
-- 2. Insert deterministic rows including NULL keys on both sides.
-- 3. Execute INNER JOIN and assert only non-NULL key matches remain.
CREATE DATABASE IF NOT EXISTS sql_tests_d10;
DROP TABLE IF EXISTS sql_tests_d10.t_rf_inner_null_key_l;
DROP TABLE IF EXISTS sql_tests_d10.t_rf_inner_null_key_r;
CREATE TABLE sql_tests_d10.t_rf_inner_null_key_l (
    id INT,
    k INT
);
CREATE TABLE sql_tests_d10.t_rf_inner_null_key_r (
    k INT,
    tag VARCHAR(20)
);

INSERT INTO sql_tests_d10.t_rf_inner_null_key_l VALUES
    (1, 10),
    (2, 20),
    (3, NULL),
    (4, 30),
    (5, NULL);

INSERT INTO sql_tests_d10.t_rf_inner_null_key_r VALUES
    (10, 'r10'),
    (NULL, 'rnull'),
    (30, 'r30');

SELECT l.id, l.k, r.tag
FROM sql_tests_d10.t_rf_inner_null_key_l l
INNER JOIN sql_tests_d10.t_rf_inner_null_key_r r
  ON l.k = r.k
ORDER BY l.id;
