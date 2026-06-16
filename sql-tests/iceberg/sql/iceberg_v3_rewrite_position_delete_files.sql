-- @sequential=true
-- @order_sensitive=true
-- @tags=iceberg,procedures,rewrite_position_delete_files,v3,dv
-- Repack a V3 Puffin deletion vector through Spark-style CALL and
-- verify the visible row set is unchanged.

-- query 1
-- @skip_result_check=true
CREATE EXTERNAL CATALOG dv_proc_${uuid0}
PROPERTIES (
  "type" = "iceberg",
  "iceberg.catalog.type" = "hadoop",
  "iceberg.catalog.warehouse" = "${starrocks_table_warehouse}/dv_proc_${uuid0}",
  "aws.s3.endpoint" = "${oss_endpoint}",
  "aws.s3.access_key" = "${oss_ak}",
  "aws.s3.secret_key" = "${oss_sk}",
  "aws.s3.enable_path_style_access" = "true"
);
CREATE DATABASE dv_proc_${uuid0}.ns_${uuid0};
CREATE TABLE dv_proc_${uuid0}.ns_${uuid0}.orders (
  id INT,
  user_id INT,
  amount INT
) TBLPROPERTIES (
  "format-version" = "3",
  "write.row-lineage" = "true"
);
INSERT INTO dv_proc_${uuid0}.ns_${uuid0}.orders VALUES (1, 10, 100), (2, 20, 200), (3, 30, 300), (4, 40, 400);
DELETE FROM dv_proc_${uuid0}.ns_${uuid0}.orders WHERE id = 2;
DELETE FROM dv_proc_${uuid0}.ns_${uuid0}.orders WHERE id = 4;

-- query 2
-- @db=dv_proc_${uuid0}.ns_${uuid0}
SELECT id, user_id, amount FROM orders ORDER BY id;

-- query 3
-- @db=dv_proc_${uuid0}.ns_${uuid0}
-- @skip_result_check=true
CALL dv_proc_${uuid0}.system.rewrite_position_delete_files(table => 'ns_${uuid0}.orders', options => map('rewrite-all', 'true'));

-- query 4
-- @db=dv_proc_${uuid0}.ns_${uuid0}
SELECT id, user_id, amount FROM orders ORDER BY id;

-- query 5
-- @skip_result_check=true
DROP TABLE dv_proc_${uuid0}.ns_${uuid0}.orders FORCE;
DROP DATABASE dv_proc_${uuid0}.ns_${uuid0};
DROP CATALOG dv_proc_${uuid0};
