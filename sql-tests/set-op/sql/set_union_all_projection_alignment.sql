-- @order_sensitive=true
-- @tags=set_op,union_all,projection
-- Test Objective:
-- 1. Validate UNION ALL output remains stable when child branches require projection alignment.
-- 2. Prevent regressions in child projection mapping for UNION_NODE lowering.
-- Test Flow:
-- 1. Build deterministic scalar branches with casted expressions.
-- 2. Apply UNION ALL across branches.
-- 3. Assert ordered final output.
SELECT k, v
FROM (
    SELECT CAST(1 AS BIGINT) AS k, CAST('10' AS INT) AS v
    UNION ALL
    SELECT CAST(2 AS BIGINT) AS k, CAST('20' AS INT) AS v
    UNION ALL
    SELECT CAST(1 AS BIGINT) AS k, CAST('30' AS INT) AS v
) t
ORDER BY k, v;
