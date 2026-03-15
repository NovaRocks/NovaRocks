-- @order_sensitive=true
-- @tags=analytic,range_frame
-- Test Objective:
-- 1. Validate RANGE UNBOUNDED PRECEDING TO CURRENT ROW semantics.
-- 2. Prevent regressions in RANGE frame ordering behavior.
-- Test Flow:
-- 1. Create/reset source table.
-- 2. Insert deterministic rows with duplicate order keys.
-- 3. Compute RANGE-frame SUM and assert output.
DROP TABLE IF EXISTS ${case_db}.t_analytic_range_unbounded_current;
CREATE TABLE ${case_db}.t_analytic_range_unbounded_current (
    grp VARCHAR(10),
    ord_key INT,
    v INT
);

INSERT INTO ${case_db}.t_analytic_range_unbounded_current VALUES
    ('A', 1, 10),
    ('A', 2, 20),
    ('A', 2, 30),
    ('A', 3, 40);

SELECT
    grp,
    ord_key,
    v,
    SUM(v) OVER (
        PARTITION BY grp ORDER BY ord_key
        RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS range_sum
FROM ${case_db}.t_analytic_range_unbounded_current
ORDER BY grp, ord_key, v;
