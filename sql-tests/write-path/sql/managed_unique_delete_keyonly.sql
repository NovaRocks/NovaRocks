-- @order_sensitive=true
-- @tags=write_path,managed,unique_keys,delete
-- Test Objective:
-- UNIQUE_KEYS DELETE WHERE on a key column is allowed and removes the row.
SET catalog default_catalog;
DROP TABLE IF EXISTS ${case_db}.t_unique_delete_keyonly;
CREATE TABLE ${case_db}.t_unique_delete_keyonly (
  id INT NOT NULL,
  v INT NOT NULL
)
UNIQUE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 2
PROPERTIES ("replication_num" = "1");
INSERT INTO ${case_db}.t_unique_delete_keyonly VALUES (1, 10), (2, 20), (3, 30);
DELETE FROM ${case_db}.t_unique_delete_keyonly WHERE id = 2;
SELECT id, v FROM ${case_db}.t_unique_delete_keyonly ORDER BY id;
