-- @order_sensitive=true
-- @tags=iceberg_dml
-- Test Objective:
-- 1. Validate runner auto-creates ${case_db_3} under the iceberg catalog before execution.
-- 2. Validate metadata visibility immediately after database creation.
-- @result_contains=${case_db_3}
-- @skip_result_check=true
SELECT catalog_name, schema_name
FROM iceberg_dml_cat_${suite_uuid0}.information_schema.schemata
ORDER BY schema_name;
