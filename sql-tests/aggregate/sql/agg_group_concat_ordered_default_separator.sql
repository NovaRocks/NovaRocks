-- @order_sensitive=true
-- @tags=aggregate,group_concat
-- Test Objective:
-- 1. Validate ordered group_concat with default separator under two-phase finalize execution.
-- 2. Prevent regressions where merge-stage input typing breaks intermediate ARRAY decoding.
-- Test Flow:
-- 1. Build a deterministic inline input with unordered integer values.
-- 2. Run ordered group_concat without explicit separator.
-- 3. Assert deterministic ordered output.
WITH t AS (
    SELECT 3 AS c1
    UNION ALL
    SELECT 1
    UNION ALL
    SELECT 2
)
SELECT group_concat(c1 ORDER BY c1) AS gc
FROM t;
