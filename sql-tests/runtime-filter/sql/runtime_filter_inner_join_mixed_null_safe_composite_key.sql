-- @order_sensitive=true
-- @tags=runtime_filter,inner_join,null_safe,composite_key
-- Test Objective:
-- 1. Validate mixed composite-key semantics: `<=>` on key1 and `=` on key2.
-- 2. Prevent regressions where mixed null-safe/non-null-safe keys produce false matches or false pruning.
-- Test Flow:
-- 1. Create/reset probe and build tables with composite join keys.
-- 2. Insert deterministic rows including NULLs on key1 and key2.
-- 3. Execute INNER JOIN with mixed predicates and assert expected matched rows only.
CREATE DATABASE IF NOT EXISTS sql_tests_d10;
DROP TABLE IF EXISTS sql_tests_d10.t_rf_inner_mixed_null_safe_l;
DROP TABLE IF EXISTS sql_tests_d10.t_rf_inner_mixed_null_safe_r;
CREATE TABLE sql_tests_d10.t_rf_inner_mixed_null_safe_l (
    id INT,
    k1 VARCHAR(20),
    k2 INT,
    v VARCHAR(20)
);
CREATE TABLE sql_tests_d10.t_rf_inner_mixed_null_safe_r (
    k1 VARCHAR(20),
    k2 INT,
    tag VARCHAR(20)
);

INSERT INTO sql_tests_d10.t_rf_inner_mixed_null_safe_l VALUES
    (1, NULL, 1, 'ln1'),
    (2, NULL, 2, 'ln2'),
    (3, 'a', 1, 'la1'),
    (4, 'a', 2, 'la2'),
    (5, 'b', 1, 'lb1');

INSERT INTO sql_tests_d10.t_rf_inner_mixed_null_safe_r VALUES
    (NULL, 1, 'rn1'),
    (NULL, 2, 'rn2'),
    ('a', 1, 'ra1'),
    ('a', 3, 'ra3'),
    ('b', NULL, 'rbn'),
    ('b', 1, 'rb1');

SELECT l.id, l.v, r.tag
FROM sql_tests_d10.t_rf_inner_mixed_null_safe_l l
INNER JOIN sql_tests_d10.t_rf_inner_mixed_null_safe_r r
  ON l.k1 <=> r.k1
 AND l.k2 = r.k2
ORDER BY l.id, l.v, r.tag;
