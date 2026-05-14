-- @order_sensitive=true
-- @tags=write_path,managed,dup_keys,delete
-- Test Objective:
-- 1. DUP_KEYS allows DELETE WHERE on a non-key column (unlike UNIQUE/AGG).
-- 2. The DeletePredicate path filters the matching rows at scan time.
SET catalog default_catalog;
DROP TABLE IF EXISTS ${case_db}.t_dup_delete_non_key;
CREATE TABLE ${case_db}.t_dup_delete_non_key (
  id INT NOT NULL,
  v INT NOT NULL
)
DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 2
PROPERTIES ("replication_num" = "1");
INSERT INTO ${case_db}.t_dup_delete_non_key VALUES (1, 10), (2, 20), (3, 30);
DELETE FROM ${case_db}.t_dup_delete_non_key WHERE v = 20;
SELECT id, v FROM ${case_db}.t_dup_delete_non_key ORDER BY id;
