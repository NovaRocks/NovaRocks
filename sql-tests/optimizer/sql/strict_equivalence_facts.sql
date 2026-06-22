-- @tags=optimizer,equivalence,join_reorder
-- Test Objective:
-- 1. Strict equivalence facts are the only facts exposed through logical properties.
-- 2. Null-safe equality remains a null-safe atom predicate in the final plan;
--    Rust unit tests cover the transitive edge synthesis guard directly.
DROP TABLE IF EXISTS ${case_db}.strict_eq_a;
DROP TABLE IF EXISTS ${case_db}.strict_eq_b;
DROP TABLE IF EXISTS ${case_db}.strict_eq_c;
SET cbo_max_reorder_node_use_exhaustive = 2;
CREATE TABLE ${case_db}.strict_eq_a (a1 BIGINT, a2 BIGINT, payload BIGINT);
CREATE TABLE ${case_db}.strict_eq_b (b BIGINT, payload BIGINT);
CREATE TABLE ${case_db}.strict_eq_c (c BIGINT, payload BIGINT);
INSERT INTO ${case_db}.strict_eq_a
    SELECT generate_series, generate_series, generate_series * 10
    FROM TABLE(generate_series(1, 10000));
INSERT INTO ${case_db}.strict_eq_b VALUES (1, 100), (2, 200), (NULL, 300);
INSERT INTO ${case_db}.strict_eq_c VALUES (1, 1000), (2, 2000), (NULL, 3000);
ANALYZE TABLE ${case_db}.strict_eq_a;
ANALYZE TABLE ${case_db}.strict_eq_b;
ANALYZE TABLE ${case_db}.strict_eq_c;

EXPLAIN VERBOSE
SELECT a.payload, b.payload, c.payload
FROM (
    SELECT *
    FROM ${case_db}.strict_eq_a
    WHERE a1 = a2
) a
JOIN ${case_db}.strict_eq_b b ON a.a1 = b.b
JOIN ${case_db}.strict_eq_c c ON a.a2 = c.c;

SET cbo_max_reorder_node_use_exhaustive = 4;

EXPLAIN VERBOSE
SELECT a.payload, b.payload, c.payload
FROM (
    SELECT *
    FROM ${case_db}.strict_eq_a
    WHERE a1 <=> a2
) a
JOIN ${case_db}.strict_eq_b b ON a.a1 = b.b
JOIN ${case_db}.strict_eq_c c ON a.a2 = c.c;
