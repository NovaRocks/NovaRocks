-- @order_sensitive=true
-- @tags=aggregate,empty_set
-- Test Objective:
-- 1. Validate EMPTY_SET_NODE lowering for global aggregate queries.
-- 2. Prevent regressions where EMPTY_SET_NODE fails at plan lowering.
-- Test Flow:
-- 1. Build a deterministic inline relation with one non-null and one null row.
-- 2. Force an empty input with a constant-false predicate.
-- 3. Assert aggregate null/count semantics on zero rows.
SELECT
    COUNT(*) AS c_all,
    COUNT(v) AS c_not_null,
    SUM(v) AS s_v,
    AVG(v) AS avg_v,
    MIN(v) AS min_v,
    MAX(v) AS max_v
FROM (
    SELECT 1 AS v
    UNION ALL
    SELECT NULL AS v
) t
WHERE 1 = 0;
