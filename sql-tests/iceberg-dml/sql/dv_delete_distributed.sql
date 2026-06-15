-- @order_sensitive=true
-- @tags=iceberg_dml,delete,dv
-- Test Point: Iceberg v3 deletion-vector DELETE writes distributed BE-side Puffin DVs and merges a second delete into the existing DV.
-- Method: create a bucket-partitioned v3 row-lineage table, insert multiple files, run two DELETE statements, then verify the remaining rows.
-- Scope: standalone Iceberg v3 DELETE, distributed DeletionVectors sink, RowDeltaDvFromFiles commit

-- query 1
-- @skip_result_check=true
DROP TABLE IF EXISTS ${case_db}.t_dv_delete_distributed FORCE;
CREATE TABLE ${case_db}.t_dv_delete_distributed (
  id BIGINT,
  g BIGINT,
  v INT
)
PARTITION BY bucket(g, 4)
TBLPROPERTIES (
  "format-version" = "3",
  "write.row-lineage" = "true"
);
INSERT INTO ${case_db}.t_dv_delete_distributed VALUES
  (1, 10, 100),
  (2, 20, 200),
  (3, 30, 300),
  (4, 40, 400),
  (5, 50, 500),
  (6, 60, 600);
DELETE FROM ${case_db}.t_dv_delete_distributed WHERE id IN (2, 5);
DELETE FROM ${case_db}.t_dv_delete_distributed WHERE v > 550;

-- query 2
SELECT COUNT(*) AS cnt
FROM ${case_db}.t_dv_delete_distributed;

-- query 3
SELECT id, g, v
FROM ${case_db}.t_dv_delete_distributed
ORDER BY id;

-- query 4
-- @skip_result_check=true
DROP TABLE ${case_db}.t_dv_delete_distributed FORCE;
