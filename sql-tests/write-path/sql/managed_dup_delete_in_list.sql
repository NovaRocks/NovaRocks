-- @order_sensitive=true
-- @tags=write_path,managed,dup_keys,delete
-- Test Objective:
-- DUP_KEYS DELETE WHERE col IN (...) removes matching rows.
SET catalog default_catalog;
DROP TABLE IF EXISTS ${case_db}.t_dup_delete_in_list;
CREATE TABLE ${case_db}.t_dup_delete_in_list (
  id INT NOT NULL,
  v INT NOT NULL
)
DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 2
PROPERTIES ("replication_num" = "1");
INSERT INTO ${case_db}.t_dup_delete_in_list VALUES (1, 10), (2, 20), (3, 30), (4, 40);
DELETE FROM ${case_db}.t_dup_delete_in_list WHERE id IN (1, 3);
SELECT id, v FROM ${case_db}.t_dup_delete_in_list ORDER BY id;
