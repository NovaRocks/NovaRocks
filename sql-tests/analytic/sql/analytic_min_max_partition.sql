-- @order_sensitive=true
-- @tags=analytic,min,max
-- Test Objective:
-- 1. Validate MIN/MAX as partition windows.
-- 2. Prevent regressions in partition-scoped extrema propagation.
-- Test Flow:
-- 1. Create/reset source table.
-- 2. Insert deterministic partitioned rows.
-- 3. Compute partition MIN/MAX and assert ordered output.
DROP TABLE IF EXISTS ${case_db}.t_analytic_min_max_partition;
CREATE TABLE ${case_db}.t_analytic_min_max_partition (
    grp VARCHAR(10),
    id INT,
    v INT
);

INSERT INTO ${case_db}.t_analytic_min_max_partition VALUES
    ('A', 1, 9),
    ('A', 2, 3),
    ('A', 3, 7),
    ('B', 4, NULL),
    ('B', 5, 8);

SELECT
    grp,
    id,
    v,
    MIN(v) OVER (PARTITION BY grp) AS min_v,
    MAX(v) OVER (PARTITION BY grp) AS max_v
FROM ${case_db}.t_analytic_min_max_partition
ORDER BY grp, id;
