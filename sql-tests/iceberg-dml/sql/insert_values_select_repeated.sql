-- @order_sensitive=false
-- @tags=iceberg_dml,insert_values
-- Test Objective:
-- 1. Validate repeated INSERT ... VALUES writes are visible through SELECT.
-- 2. Prevent regressions where first insert succeeds but subsequent inserts are lost.
DROP TABLE IF EXISTS ${case_db}.t_insert_values_select_repeated;
CREATE TABLE ${case_db}.t_insert_values_select_repeated (
  c1 BIGINT
);
INSERT INTO ${case_db}.t_insert_values_select_repeated VALUES (11);
INSERT INTO ${case_db}.t_insert_values_select_repeated VALUES (22), (33);
INSERT INTO ${case_db}.t_insert_values_select_repeated VALUES (44);
SELECT c1
FROM ${case_db}.t_insert_values_select_repeated;
