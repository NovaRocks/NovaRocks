-- @order_sensitive=true
-- @tags=write_path,primary_key,delete
-- Test Objective:
-- 1. Validate PRIMARY KEY table can apply INSERT then DELETE and expose correct visible rows.
-- 2. Prevent regression where DELETE is accepted but publish path keeps deleted key visible.
-- Test Flow:
-- 1. Switch to internal catalog and create/reset a PRIMARY KEY table.
-- 2. Insert deterministic seed rows.
-- 3. Delete one key and read final visible rows with ORDER BY.
SET catalog default_catalog;
CREATE DATABASE IF NOT EXISTS sql_tests_write_path_internal;
DROP TABLE IF EXISTS sql_tests_write_path_internal.t_primary_key_insert_delete_select;
CREATE TABLE sql_tests_write_path_internal.t_primary_key_insert_delete_select (
  city_id INT NOT NULL,
  population INT NOT NULL,
  city STRING NOT NULL
)
PRIMARY KEY (city_id)
DISTRIBUTED BY HASH(city_id)
PROPERTIES ("replication_num" = "1");
INSERT INTO sql_tests_write_path_internal.t_primary_key_insert_delete_select VALUES
  (1, 100, 'Beijing'),
  (2, 200, 'Shanghai'),
  (3, 300, 'Shenzhen');
DELETE FROM sql_tests_write_path_internal.t_primary_key_insert_delete_select
WHERE city_id = 2;
SELECT city_id, population, city
FROM sql_tests_write_path_internal.t_primary_key_insert_delete_select
ORDER BY city_id;
