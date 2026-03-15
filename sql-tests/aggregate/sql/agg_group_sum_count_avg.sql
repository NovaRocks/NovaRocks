-- @order_sensitive=true
-- @tags=aggregate,basic
-- Test Objective:
-- 1. Validate grouped COUNT/SUM/AVG semantics with nullable inputs.
-- 2. Prevent regressions where NULL handling changes aggregate outputs.
-- Test Flow:
-- 1. Create/reset aggregate source table.
-- 2. Insert deterministic rows across groups with NULLs.
-- 3. Aggregate by group and assert ordered output.
DROP TABLE IF EXISTS ${case_db}.t_agg_group_sum_count_avg;
CREATE TABLE ${case_db}.t_agg_group_sum_count_avg (
    g INT,
    v INT
);

INSERT INTO ${case_db}.t_agg_group_sum_count_avg VALUES
    (1, 10),
    (1, 20),
    (1, NULL),
    (2, 5),
    (2, 15),
    (3, NULL);

SELECT
    g,
    COUNT(*) AS c_all,
    COUNT(v) AS c_not_null,
    SUM(v) AS s_v,
    AVG(v) AS avg_v
FROM ${case_db}.t_agg_group_sum_count_avg
GROUP BY g
ORDER BY g;
