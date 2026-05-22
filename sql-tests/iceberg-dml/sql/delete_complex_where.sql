-- @order_sensitive=true
-- @tags=iceberg_dml,delete
-- Test Objective:
-- DELETE with a function-call WHERE on an Iceberg v3 table.
DROP TABLE IF EXISTS ${case_db}.t_delete_complex_where;
CREATE TABLE ${case_db}.t_delete_complex_where (
  id INT,
  k INT,
  label STRING
) TBLPROPERTIES ("format-version" = "3");
INSERT INTO ${case_db}.t_delete_complex_where VALUES (1, 10, 'X'), (2, 20, 'Y'), (3, 30, 'Z');
DELETE FROM ${case_db}.t_delete_complex_where WHERE LOWER(label) = 'y';
SELECT id, k, label FROM ${case_db}.t_delete_complex_where ORDER BY id;
