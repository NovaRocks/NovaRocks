-- @tags=optimizer,oq1w,window_pruning
-- Test Objective:
-- 1. An unused window result is pruned from the Window node.
-- 2. A Window node is removed when no window result is required by its parent.
DROP TABLE IF EXISTS ${case_db}.oq1w_sales;
CREATE TABLE ${case_db}.oq1w_sales (region VARCHAR(20), amount INT);
INSERT INTO ${case_db}.oq1w_sales VALUES
    ('A', 10), ('A', 20), ('B', 15), ('B', 30);
ANALYZE TABLE ${case_db}.oq1w_sales;

-- The outer query reads rn but never reads rk, so rank() must be pruned.
-- @explain_contains=WINDOW [row_number
-- @explain_not_contains=rank(
SELECT region, rn
FROM (
    SELECT
        region,
        row_number() OVER (PARTITION BY region ORDER BY amount) AS rn,
        rank() OVER (PARTITION BY region ORDER BY amount) AS rk
    FROM ${case_db}.oq1w_sales
) t
ORDER BY region, rn;

-- The outer query reads only region, so the entire Window node is removable.
-- @explain_not_contains=WINDOW [
SELECT region
FROM (
    SELECT
        region,
        row_number() OVER (PARTITION BY region ORDER BY amount) AS rn
    FROM ${case_db}.oq1w_sales
) t
ORDER BY region;
