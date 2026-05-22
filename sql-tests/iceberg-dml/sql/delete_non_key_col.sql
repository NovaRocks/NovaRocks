-- @order_sensitive=true
-- @tags=iceberg_dml,delete
-- Test Objective:
-- DELETE WHERE on a non-PK-equivalent column on an Iceberg v3 table.
DROP TABLE IF EXISTS ${case_db}.t_delete_non_key;
CREATE TABLE ${case_db}.t_delete_non_key (
  id INT,
  v INT
) TBLPROPERTIES ("format-version" = "3");
INSERT INTO ${case_db}.t_delete_non_key VALUES (1, 10), (2, 20), (3, 30);
DELETE FROM ${case_db}.t_delete_non_key WHERE v = 20;
SELECT id, v FROM ${case_db}.t_delete_non_key ORDER BY id;
