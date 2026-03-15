-- @order_sensitive=true
-- @tags=aggregate,sum_distinct
-- Test Objective:
-- 1. Validate SUM(DISTINCT) per group.
-- 2. Prevent regressions where duplicate values are summed multiple times.
-- Test Flow:
-- 1. Create/reset source table.
-- 2. Insert duplicated numeric rows per group.
-- 3. Aggregate with SUM(DISTINCT) and assert ordered output.
DROP TABLE IF EXISTS ${case_db}.t_agg_sum_distinct;
CREATE TABLE ${case_db}.t_agg_sum_distinct (
    g INT,
    v INT
);

INSERT INTO ${case_db}.t_agg_sum_distinct VALUES
    (1, 10),
    (1, 10),
    (1, 20),
    (2, 5),
    (2, 5),
    (2, NULL);

SELECT
    g,
    SUM(DISTINCT v) AS sd_v
FROM ${case_db}.t_agg_sum_distinct
GROUP BY g
ORDER BY g;
