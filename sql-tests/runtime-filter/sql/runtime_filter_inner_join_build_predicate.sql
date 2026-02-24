-- @order_sensitive=true
-- @tags=runtime_filter,inner_join,predicate
-- Test Objective:
-- 1. Validate join results with additional build-side range predicate.
-- 2. Prevent regressions where runtime-filter pruning changes join semantics.
-- Test Flow:
-- 1. Create/reset probe/build tables.
-- 2. Insert deterministic rows.
-- 3. Apply join with build-side predicate and assert ordered output.
CREATE DATABASE IF NOT EXISTS sql_tests_d10;
DROP TABLE IF EXISTS sql_tests_d10.t_rf_inner_join_build_predicate_l;
DROP TABLE IF EXISTS sql_tests_d10.t_rf_inner_join_build_predicate_r;
CREATE TABLE sql_tests_d10.t_rf_inner_join_build_predicate_l (
    id INT,
    k INT
);
CREATE TABLE sql_tests_d10.t_rf_inner_join_build_predicate_r (
    k INT,
    score INT
);

INSERT INTO sql_tests_d10.t_rf_inner_join_build_predicate_l VALUES
    (1, 1),
    (2, 2),
    (3, 3),
    (4, 4);

INSERT INTO sql_tests_d10.t_rf_inner_join_build_predicate_r VALUES
    (1, 10),
    (2, 20),
    (3, 30),
    (4, 40);

SELECT l.id, l.k, r.score
FROM sql_tests_d10.t_rf_inner_join_build_predicate_l l
JOIN sql_tests_d10.t_rf_inner_join_build_predicate_r r
  ON l.k = r.k
WHERE r.score >= 25
ORDER BY l.id;
