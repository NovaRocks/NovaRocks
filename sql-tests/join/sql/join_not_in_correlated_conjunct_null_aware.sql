-- @order_sensitive=true
-- @tags=join,not_in,correlated,null_aware
-- Test Objective:
-- 1. Validate correlated NOT IN semantics when FE lowers to NULL_AWARE_LEFT_ANTI_JOIN with an extra join conjunct.
-- 2. Prevent regressions where execution rejects or mis-evaluates plans containing both eq_join_conjunct and other_join_conjuncts.
-- Test Flow:
-- 1. Create/reset probe and build tables with nullable group/key columns.
-- 2. Insert deterministic rows covering empty-subquery, NULL-in-subquery, and direct-match cases.
-- 3. Execute correlated NOT IN and assert deterministic row ids.
CREATE DATABASE IF NOT EXISTS sql_tests_d05;
DROP TABLE IF EXISTS sql_tests_d05.t_naaj_corr_not_in_l;
DROP TABLE IF EXISTS sql_tests_d05.t_naaj_corr_not_in_r;
CREATE TABLE sql_tests_d05.t_naaj_corr_not_in_l (
    id INT,
    g INT,
    k INT
);
CREATE TABLE sql_tests_d05.t_naaj_corr_not_in_r (
    g INT,
    k INT
);

INSERT INTO sql_tests_d05.t_naaj_corr_not_in_l VALUES
    (1, 1, 2),
    (2, NULL, 0),
    (3, NULL, 1),
    (4, 3, 1),
    (5, 3, -1),
    (6, NULL, 1),
    (7, NULL, NULL),
    (8, 3, 2),
    (9, 2, 2);

INSERT INTO sql_tests_d05.t_naaj_corr_not_in_r VALUES
    (NULL, 1),
    (1, 1),
    (NULL, 2),
    (3, 2),
    (2, NULL);

SELECT l.id
FROM sql_tests_d05.t_naaj_corr_not_in_l l
WHERE l.k NOT IN (
    SELECT r.k
    FROM sql_tests_d05.t_naaj_corr_not_in_r r
    WHERE r.g = l.g
)
ORDER BY l.id;
