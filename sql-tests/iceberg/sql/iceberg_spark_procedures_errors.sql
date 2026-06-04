-- @sequential=true
-- @order_sensitive=true
-- @tags=iceberg,procedures,errors
-- Validate Spark-style Iceberg procedure rejection paths.

-- query 1
-- @skip_result_check=true
CREATE EXTERNAL CATALOG proc_err_${uuid0}
PROPERTIES (
  "type" = "iceberg",
  "iceberg.catalog.type" = "hadoop",
  "iceberg.catalog.warehouse" = "${starrocks_table_warehouse}/proc_err_${uuid0}",
  "aws.s3.endpoint" = "${oss_endpoint}",
  "aws.s3.access_key" = "${oss_ak}",
  "aws.s3.secret_key" = "${oss_sk}",
  "aws.s3.enable_path_style_access" = "true"
);
CREATE DATABASE proc_err_${uuid0}.ns_${uuid0};
CREATE TABLE proc_err_${uuid0}.ns_${uuid0}.orders (
  id INT
) TBLPROPERTIES (
  "format-version" = "3",
  "write.row-lineage" = "true"
);
CREATE TABLE proc_err_${uuid0}.ns_${uuid0}.v2_orders (
  id INT
) TBLPROPERTIES (
  "format-version" = "2"
);
INSERT INTO proc_err_${uuid0}.ns_${uuid0}.v2_orders VALUES (1), (2), (3);
DELETE FROM proc_err_${uuid0}.ns_${uuid0}.v2_orders WHERE id = 2;

-- query 2
-- @expect_error=Iceberg procedures must use system namespace
CALL proc_err_${uuid0}.admin.rewrite_manifests(table => 'ns_${uuid0}.orders');

-- query 3
-- @expect_error=unsupported Iceberg system procedure
CALL proc_err_${uuid0}.system.unknown_proc(table => 'ns_${uuid0}.orders');

-- query 4
-- @expect_error=where is not supported
CALL proc_err_${uuid0}.system.rewrite_position_delete_files(table => 'ns_${uuid0}.orders', where => 'id = 1');

-- query 5
-- @expect_error=unsupported rewrite_position_delete_files option
CALL proc_err_${uuid0}.system.rewrite_position_delete_files(table => 'ns_${uuid0}.orders', options => map('partial-progress.enabled', 'true'));

-- query 6
-- @expect_error=target-file-size-bytes
CALL proc_err_${uuid0}.system.rewrite_position_delete_files(table => 'ns_${uuid0}.orders', options => map('target-file-size-bytes', '134217728'));

-- query 7
-- @expect_error=V2 Parquet position delete rewrite is not supported
CALL proc_err_${uuid0}.system.rewrite_position_delete_files(table => 'ns_${uuid0}.v2_orders', options => map('rewrite-all', 'true'));

-- query 8
-- @db=proc_err_${uuid0}.ns_${uuid0}
SELECT COUNT(*) AS n FROM v2_orders;

-- query 9
-- @skip_result_check=true
DROP TABLE proc_err_${uuid0}.ns_${uuid0}.v2_orders FORCE;
DROP TABLE proc_err_${uuid0}.ns_${uuid0}.orders FORCE;
DROP DATABASE proc_err_${uuid0}.ns_${uuid0};
DROP CATALOG proc_err_${uuid0};
