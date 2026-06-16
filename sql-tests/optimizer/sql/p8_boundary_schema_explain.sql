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
-- @normalize_explain_timing=true
-- @result_contains=Planning:
-- @result_contains=PLAN FRAGMENT 0
-- @result_contains=PLAN FRAGMENT 1
-- @result_contains=EXCHANGE ID:
-- @result_contains=HASH AGGREGATE (GLOBAL
-- @result_contains=HASH AGGREGATE (LOCAL
-- @result_contains=PROJECT [k, sum(v) AS total_v] stats={rows=2 conf=estimated} act={rows=1
-- @result_contains=HASH AGGREGATE (GLOBAL, group by: [k]) stats={rows=2 conf=estimated} act={rows=1
-- @result_contains=HASH AGGREGATE (LOCAL, group by: [k]) stats={rows=2 conf=estimated} act={rows=2
-- @result_contains=VALUES (1 rows) stats={rows=1} act={rows=1
-- @result_not_contains=Boundary Schemas:
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
