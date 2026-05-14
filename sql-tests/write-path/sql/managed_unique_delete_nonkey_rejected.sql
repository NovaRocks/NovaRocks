-- @order_sensitive=true
-- @tags=write_path,managed,unique_keys,delete,error
-- Test Objective:
-- UNIQUE_KEYS rejects DELETE on a non-key column with the StarRocks message
-- "Where clause only supports key column on this table model".
SET catalog default_catalog;
DROP TABLE IF EXISTS ${case_db}.t_unique_delete_nonkey;
CREATE TABLE ${case_db}.t_unique_delete_nonkey (
  id INT NOT NULL,
  v INT NOT NULL
)
UNIQUE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 2
PROPERTIES ("replication_num" = "1");
INSERT INTO ${case_db}.t_unique_delete_nonkey VALUES (1, 10), (2, 20), (3, 30);
-- @expect_error=key column
DELETE FROM ${case_db}.t_unique_delete_nonkey WHERE v = 20;
