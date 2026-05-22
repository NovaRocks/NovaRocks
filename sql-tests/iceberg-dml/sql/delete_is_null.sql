-- @order_sensitive=true
-- @tags=iceberg_dml,delete
-- Test Objective:
-- DELETE WHERE col IS NULL removes rows whose column is NULL on an Iceberg v3 table.
DROP TABLE IF EXISTS ${case_db}.t_delete_is_null;
CREATE TABLE ${case_db}.t_delete_is_null (
  id INT,
  v INT
) TBLPROPERTIES ("format-version" = "3");
INSERT INTO ${case_db}.t_delete_is_null VALUES (1, 10), (2, NULL), (3, 30), (4, NULL);
DELETE FROM ${case_db}.t_delete_is_null WHERE v IS NULL;
SELECT id, v FROM ${case_db}.t_delete_is_null ORDER BY id;
