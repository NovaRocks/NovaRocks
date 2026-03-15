-- @order_sensitive=true
-- @tags=write_path,primary_key,upsert,delete
-- Test Objective:
-- 1. Validate PRIMARY KEY upsert replaces old value for duplicated key.
-- 2. Validate DELETE after upsert removes the target key from visible result.
-- Test Flow:
-- 1. Switch to internal catalog and create/reset a PRIMARY KEY table.
-- 2. Insert seed rows, then insert duplicated key to trigger upsert.
-- 3. Delete one key and query deterministic final rows.
SET catalog default_catalog;
DROP TABLE IF EXISTS ${case_db_2}.t_primary_key_upsert_delete_select;
CREATE TABLE ${case_db_2}.t_primary_key_upsert_delete_select (
  city_id INT NOT NULL,
  population INT NOT NULL,
  city STRING NOT NULL
)
PRIMARY KEY (city_id)
DISTRIBUTED BY HASH(city_id)
PROPERTIES ("replication_num" = "1");
INSERT INTO ${case_db_2}.t_primary_key_upsert_delete_select VALUES
  (1, 100, 'Beijing'),
  (2, 200, 'Shanghai');
INSERT INTO ${case_db_2}.t_primary_key_upsert_delete_select VALUES
  (2, 250, 'Shanghai-updated'),
  (3, 300, 'Shenzhen');
DELETE FROM ${case_db_2}.t_primary_key_upsert_delete_select
WHERE city_id = 1;
SELECT city_id, population, city
FROM ${case_db_2}.t_primary_key_upsert_delete_select
ORDER BY city_id;
