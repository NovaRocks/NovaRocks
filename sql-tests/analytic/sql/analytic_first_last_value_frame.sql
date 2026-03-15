-- @order_sensitive=true
-- @tags=analytic,first_value,last_value
-- Test Objective:
-- 1. Validate FIRST_VALUE/LAST_VALUE under full window frame.
-- 2. Prevent regressions where LAST_VALUE incorrectly uses current-row frame.
-- Test Flow:
-- 1. Create/reset source table.
-- 2. Insert deterministic ordered rows.
-- 3. Compute FIRST_VALUE/LAST_VALUE with full frame and assert output.
DROP TABLE IF EXISTS ${case_db}.t_analytic_first_last_value_frame;
CREATE TABLE ${case_db}.t_analytic_first_last_value_frame (
    grp VARCHAR(10),
    ts INT,
    v INT
);

INSERT INTO ${case_db}.t_analytic_first_last_value_frame VALUES
    ('A', 1, 5),
    ('A', 2, 6),
    ('A', 3, 7),
    ('B', 1, NULL),
    ('B', 2, 9);

SELECT
    grp,
    ts,
    v,
    FIRST_VALUE(v) OVER (
        PARTITION BY grp ORDER BY ts
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS first_v,
    LAST_VALUE(v) OVER (
        PARTITION BY grp ORDER BY ts
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS last_v
FROM ${case_db}.t_analytic_first_last_value_frame
ORDER BY grp, ts;
