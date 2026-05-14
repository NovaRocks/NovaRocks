-- @order_sensitive=true
-- @tags=write_path,managed,dup_keys,delete
-- Test Objective:
-- DUP_KEYS DELETE WHERE col IS NULL removes rows whose column is NULL.
SET catalog default_catalog;
DROP TABLE IF EXISTS ${case_db}.t_dup_delete_is_null;
CREATE TABLE ${case_db}.t_dup_delete_is_null (
  id INT NOT NULL,
  v INT NULL
)
DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 2
PROPERTIES ("replication_num" = "1");
INSERT INTO ${case_db}.t_dup_delete_is_null VALUES (1, 10), (2, NULL), (3, 30), (4, NULL);
DELETE FROM ${case_db}.t_dup_delete_is_null WHERE v IS NULL;
SELECT id, v FROM ${case_db}.t_dup_delete_is_null ORDER BY id;
