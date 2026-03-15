-- @order_sensitive=true
-- @tags=aggregate,count_distinct,multi_column
-- Test Objective:
-- 1. Validate COUNT(DISTINCT a,b) for composite key distinctness.
-- 2. Prevent regressions in multi-column distinct cardinality.
-- Test Flow:
-- 1. Create/reset source table.
-- 2. Insert deterministic duplicated and unique key pairs.
-- 3. Compute COUNT(DISTINCT a,b) and assert scalar output.
DROP TABLE IF EXISTS ${case_db}.t_agg_count_distinct_multi_column;
CREATE TABLE ${case_db}.t_agg_count_distinct_multi_column (
    a INT,
    b VARCHAR(20)
);

INSERT INTO ${case_db}.t_agg_count_distinct_multi_column VALUES
    (1, 'x'),
    (1, 'x'),
    (1, 'y'),
    (2, 'x'),
    (2, 'x');

SELECT COUNT(DISTINCT a, b) AS cd_ab
FROM ${case_db}.t_agg_count_distinct_multi_column;
