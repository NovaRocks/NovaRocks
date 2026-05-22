-- @order_sensitive=true
-- @tags=iceberg_dml
-- Test Objective:
-- 1. Validate runner auto-creates ${case_db_3} under the iceberg catalog before execution.
-- 2. Validate metadata visibility immediately after database creation.
SELECT COUNT(*) AS db_exists
FROM iceberg_dml_cat_${suite_uuid0}.information_schema.schemata
WHERE schema_name = '${case_db_3}';
