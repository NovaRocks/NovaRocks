-- @order_sensitive=true
-- @tags=aggregate,having
-- Test Objective:
-- 1. Validate HAVING filtering on aggregate outputs.
-- 2. Prevent regressions in post-aggregation predicate evaluation.
-- Test Flow:
-- 1. Create/reset source table.
-- 2. Insert deterministic rows across groups.
-- 3. Apply GROUP BY + HAVING and assert ordered output.
DROP TABLE IF EXISTS ${case_db}.t_agg_having_threshold;
CREATE TABLE ${case_db}.t_agg_having_threshold (
    g INT,
    v INT
);

INSERT INTO ${case_db}.t_agg_having_threshold VALUES
    (1, 5),
    (1, 7),
    (2, 9),
    (2, 15),
    (3, 30);

SELECT
    g,
    SUM(v) AS s_v
FROM ${case_db}.t_agg_having_threshold
GROUP BY g
HAVING SUM(v) >= 20
ORDER BY g;
