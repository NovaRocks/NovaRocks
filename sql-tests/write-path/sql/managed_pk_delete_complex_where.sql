-- @order_sensitive=true
-- @tags=write_path,managed,primary_key,delete
-- Test Objective:
-- PRIMARY KEY DELETE supports WHERE forms that DUP/UNIQUE/AGG reject:
-- in this case a function call on a non-key column.
SET catalog default_catalog;
DROP TABLE IF EXISTS ${case_db}.t_pk_complex;
CREATE TABLE ${case_db}.t_pk_complex (
  id INT NOT NULL,
  k INT,
  label STRING
)
PRIMARY KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 2
PROPERTIES ("replication_num" = "1");
INSERT INTO ${case_db}.t_pk_complex VALUES (1, 10, 'X'), (2, 20, 'Y'), (3, 30, 'Z');
DELETE FROM ${case_db}.t_pk_complex WHERE LOWER(label) = 'y';
SELECT id, k, label FROM ${case_db}.t_pk_complex ORDER BY id;
