-- @order_sensitive=true
-- Test Objective:
-- 1. Validate basic DDL execution in the hadoop catalog.
-- 2. Validate metadata visibility immediately after database creation.
-- Test Flow:
-- 1. The runner auto-creates ${case_db_3} before execution.
-- 2. Query information_schema to confirm the database is visible.
SELECT COUNT(*) AS db_exists
FROM default_catalog.information_schema.schemata
WHERE schema_name = '${case_db_3}';
