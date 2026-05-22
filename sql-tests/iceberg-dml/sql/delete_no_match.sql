-- @order_sensitive=true
-- @tags=iceberg_dml,delete
-- Test Objective:
-- DELETE matching zero rows on an Iceberg v3 table is a no-op: no error, all
-- original rows remain visible.
DROP TABLE IF EXISTS ${case_db}.t_delete_no_match;
CREATE TABLE ${case_db}.t_delete_no_match (
  id INT,
  v INT
) TBLPROPERTIES ("format-version" = "3");
INSERT INTO ${case_db}.t_delete_no_match VALUES (1, 100);
DELETE FROM ${case_db}.t_delete_no_match WHERE id = 999;
SELECT id, v FROM ${case_db}.t_delete_no_match ORDER BY id;
