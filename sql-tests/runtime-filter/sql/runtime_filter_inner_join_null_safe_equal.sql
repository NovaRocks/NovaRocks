-- @order_sensitive=true
-- @tags=runtime_filter,inner_join,null_safe
-- Test Objective:
-- 1. Validate NULL-safe equality join (`<=>`) keeps NULL-key matches on runtime-filter-enabled hash-join path.
-- 2. Prevent regressions where runtime filter incorrectly prunes probe NULL keys under null-safe join semantics.
-- Test Flow:
-- 1. Create/reset probe and build tables with nullable join keys.
-- 2. Insert deterministic rows where only NULL keys can match across sides.
-- 3. Execute INNER JOIN with `<=>` and assert NULL-key matches are preserved.
CREATE DATABASE IF NOT EXISTS sql_tests_d10;
DROP TABLE IF EXISTS sql_tests_d10.t_rf_inner_null_safe_l;
DROP TABLE IF EXISTS sql_tests_d10.t_rf_inner_null_safe_r;
CREATE TABLE sql_tests_d10.t_rf_inner_null_safe_l (
    id INT,
    k INT,
    v VARCHAR(20)
);
CREATE TABLE sql_tests_d10.t_rf_inner_null_safe_r (
    k INT,
    tag VARCHAR(20)
);

INSERT INTO sql_tests_d10.t_rf_inner_null_safe_l VALUES
    (1, NULL, 'ln1'),
    (2, NULL, 'ln2'),
    (3, 10, 'l10');

INSERT INTO sql_tests_d10.t_rf_inner_null_safe_r VALUES
    (NULL, 'rn1'),
    (20, 'r20');

SELECT l.id, l.v, r.tag
FROM sql_tests_d10.t_rf_inner_null_safe_l l
INNER JOIN sql_tests_d10.t_rf_inner_null_safe_r r
  ON l.k <=> r.k
ORDER BY l.id, l.v, r.tag;
