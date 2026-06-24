-- @tags=optimizer,p8,distributed_ir_explain
EXPLAIN VERBOSE
SELECT k, SUM(v) AS total_v
FROM (
    SELECT 1 AS k, 10 AS v
    UNION ALL
    SELECT 1 AS k, 20 AS v
) t
GROUP BY k;

-- @skip_result_check=true
-- @result_contains=Planning:
-- @result_contains=Rows: 1
-- @result_contains=Profile: fragments=
-- @result_contains=HASH AGGREGATE
-- @result_contains=UNION ALL
EXPLAIN ANALYZE SELECT k, SUM(v) AS total_v
FROM (
    SELECT 1 AS k, 10 AS v
    UNION ALL
    SELECT 1 AS k, 20 AS v
) t
GROUP BY k;

-- @explain_contains=PLAN FRAGMENT
-- @explain_contains=EXCHANGE ID:
-- @explain_not_contains=Boundary Schemas:
SELECT k, SUM(v) AS total_v
FROM (
    SELECT 1 AS k, 10 AS v
    UNION ALL
    SELECT 1 AS k, 20 AS v
) t
GROUP BY k;
