-- @order_sensitive=true
-- @tags=iceberg_dml,delete
-- Test Objective:
-- DELETE WHERE col IN (...) removes matching rows on an Iceberg v3 table.
DROP TABLE IF EXISTS ${case_db}.t_delete_in_list;
CREATE TABLE ${case_db}.t_delete_in_list (
  id INT,
  v INT
) TBLPROPERTIES ("format-version" = "3");
INSERT INTO ${case_db}.t_delete_in_list VALUES (1, 10), (2, 20), (3, 30), (4, 40);
DELETE FROM ${case_db}.t_delete_in_list WHERE id IN (1, 3);
SELECT id, v FROM ${case_db}.t_delete_in_list ORDER BY id;
