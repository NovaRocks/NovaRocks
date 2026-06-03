-- @tags=optimizer,oq6,subquery_alias_fold
-- @order_sensitive=true
-- Test Objective:
-- 1. Derived-table aliases are carried as an identity Project's output_qualifier
--    (predicate-pushdown rework), not as a dedicated alias plan operator.
-- 2. Derived-table column aliases still expose the renamed output column.
-- 3. Single-use CTE inline keeps a real join plan (no dedicated alias operator).
DROP TABLE IF EXISTS ${case_db}.oq6_alias_base;
CREATE TABLE ${case_db}.oq6_alias_base (k INT, v INT);
INSERT INTO ${case_db}.oq6_alias_base VALUES (1, 10), (2, 20), (3, 30);

EXPLAIN VERBOSE
SELECT s.k
FROM (SELECT k, v FROM ${case_db}.oq6_alias_base) s
WHERE s.v > 10;

EXPLAIN VERBOSE
SELECT renamed_k
FROM (SELECT k FROM ${case_db}.oq6_alias_base) s(renamed_k)
ORDER BY renamed_k;

SELECT renamed_k
FROM (SELECT k FROM ${case_db}.oq6_alias_base) s(renamed_k)
ORDER BY renamed_k;

EXPLAIN VERBOSE WITH w AS (
    SELECT k, v FROM ${case_db}.oq6_alias_base WHERE k < 3
)
SELECT count(*)
FROM ${case_db}.oq6_alias_base b
JOIN w w_alias ON b.k = w_alias.k;
