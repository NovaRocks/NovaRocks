-- @order_sensitive=true
-- @tags=aggregate,repeat,multicast,pipeline
-- Test Objective:
-- 1. Validate CUBE semantics under multi-cast data stream sinks with pipeline parallelism.
-- 2. Prevent regressions where early EOS truncates exchange data when pipeline_dop > 1.
-- Test Flow:
-- 1. Force pipeline parallel execution with pipeline_dop = 5.
-- 2. Run a minimal CUBE query that triggers MULTI_CAST_DATA_STREAM_SINK and REPEAT paths.
-- 3. Assert deterministic full result set with detail, subtotal, and grand-total rows.
SET pipeline_dop = 5;

WITH t AS (
    SELECT 1 AS a, 'x' AS b
    UNION ALL
    SELECT 1, 'y'
    UNION ALL
    SELECT 2, 'z'
)
SELECT a, b
FROM t
GROUP BY CUBE(a, b)
ORDER BY a, b;

SET pipeline_dop = 0;
