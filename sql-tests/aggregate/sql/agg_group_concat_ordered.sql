-- @order_sensitive=true
-- @tags=aggregate,group_concat
-- Test Objective:
-- 1. Validate group_concat ORDER BY behavior with explicit separator.
-- 2. Prevent regressions where merge-stage extra separator args break intermediate decoding.
-- Test Flow:
-- 1. Create/reset source table with nullable string input.
-- 2. Insert deterministic rows with duplicates and NULL.
-- 3. Assert ordered global group_concat output.
DROP TABLE IF EXISTS ${case_db}.t_agg_group_concat_ordered;
CREATE TABLE ${case_db}.t_agg_group_concat_ordered (
    k INT,
    s STRING
);

INSERT INTO ${case_db}.t_agg_group_concat_ordered VALUES
    (1, 'b'),
    (2, 'a'),
    (3, 'b'),
    (4, NULL),
    (5, 'c');

SELECT group_concat(s ORDER BY s SEPARATOR '|') AS gc
FROM ${case_db}.t_agg_group_concat_ordered;
