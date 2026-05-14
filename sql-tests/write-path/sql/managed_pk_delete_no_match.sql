-- @order_sensitive=true
-- @tags=write_path,managed,primary_key,delete
-- Test Objective:
-- PRIMARY KEY DELETE matching zero rows is a no-op: no error, all
-- original rows remain visible.
SET catalog default_catalog;
DROP TABLE IF EXISTS ${case_db}.t_pk_nomatch;
CREATE TABLE ${case_db}.t_pk_nomatch (
  id INT NOT NULL,
  v INT
)
PRIMARY KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES ("replication_num" = "1");
INSERT INTO ${case_db}.t_pk_nomatch VALUES (1, 100);
DELETE FROM ${case_db}.t_pk_nomatch WHERE id = 999;
SELECT id, v FROM ${case_db}.t_pk_nomatch ORDER BY id;
