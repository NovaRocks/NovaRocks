-- @order_sensitive=true
-- @tags=runtime_filter,inner_join,null_safe,string
-- Test Objective:
-- 1. Validate NULL-safe equality (`<=>`) on STRING join key in runtime-filter-enabled hash-join path.
-- 2. Prevent regressions where one-string hash strategy drops NULL-key matches.
-- Test Flow:
-- 1. Create/reset probe and build tables with nullable STRING keys.
-- 2. Insert deterministic rows including NULL and non-NULL keys on both sides.
-- 3. Execute INNER JOIN with `<=>` and assert NULL-key and non-NULL-key matches.
CREATE DATABASE IF NOT EXISTS sql_tests_d10;
DROP TABLE IF EXISTS sql_tests_d10.t_rf_inner_null_safe_str_l;
DROP TABLE IF EXISTS sql_tests_d10.t_rf_inner_null_safe_str_r;
CREATE TABLE sql_tests_d10.t_rf_inner_null_safe_str_l (
    id INT,
    k VARCHAR(20),
    v VARCHAR(20)
);
CREATE TABLE sql_tests_d10.t_rf_inner_null_safe_str_r (
    k VARCHAR(20),
    tag VARCHAR(20)
);

INSERT INTO sql_tests_d10.t_rf_inner_null_safe_str_l VALUES
    (1, NULL, 'ln1'),
    (2, NULL, 'ln2'),
    (3, 'a', 'la'),
    (4, 'b', 'lb');

INSERT INTO sql_tests_d10.t_rf_inner_null_safe_str_r VALUES
    (NULL, 'rn1'),
    ('a', 'ra1'),
    ('c', 'rc1');

SELECT l.id, l.v, r.tag
FROM sql_tests_d10.t_rf_inner_null_safe_str_l l
INNER JOIN sql_tests_d10.t_rf_inner_null_safe_str_r r
  ON l.k <=> r.k
ORDER BY l.id, l.v, r.tag;
