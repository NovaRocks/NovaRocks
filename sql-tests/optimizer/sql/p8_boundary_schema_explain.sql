-- @tags=optimizer,p8,boundary_schema
EXPLAIN VERBOSE
SELECT k, SUM(v) AS total_v
FROM (
    SELECT 1 AS k, 10 AS v
    UNION ALL
    SELECT 1 AS k, 20 AS v
) t
GROUP BY k;

-- @explain_contains=Boundary Schemas:
-- @explain_contains=EXCHANGE_
SELECT k, SUM(v) AS total_v
FROM (
    SELECT 1 AS k, 10 AS v
    UNION ALL
    SELECT 1 AS k, 20 AS v
) t
GROUP BY k;
