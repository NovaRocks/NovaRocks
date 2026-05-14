-- @order_sensitive=true
-- @tags=write_path,managed,dup_keys,delete,error
-- Test Objective:
-- DELETE on DUP/UNIQUE/AGG rejects OR (StarRocks-aligned: only AND of
-- comparisons / IN / IS NULL).
SET catalog default_catalog;
DROP TABLE IF EXISTS ${case_db}.t_dup_delete_or;
CREATE TABLE ${case_db}.t_dup_delete_or (
  id INT NOT NULL,
  v INT NOT NULL
)
DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 2
PROPERTIES ("replication_num" = "1");
INSERT INTO ${case_db}.t_dup_delete_or VALUES (1, 10), (2, 20), (3, 30);
-- @expect_error=OR
DELETE FROM ${case_db}.t_dup_delete_or WHERE id = 1 OR id = 2;
