-- @sequential=true
-- @order_sensitive=true
-- @tags=iceberg,procedures,spark
-- Test Spark-style Iceberg procedure CALL routing for manifest rewrite and
-- no-candidate position-delete rewrite.

-- query 1
-- @skip_result_check=true
CREATE EXTERNAL CATALOG proc_ice_${uuid0}
PROPERTIES (
  "type" = "iceberg",
  "iceberg.catalog.type" = "hadoop",
  "iceberg.catalog.warehouse" = "${starrocks_table_warehouse}/proc_ice_${uuid0}",
  "aws.s3.endpoint" = "${oss_endpoint}",
  "aws.s3.access_key" = "${oss_ak}",
  "aws.s3.secret_key" = "${oss_sk}",
  "aws.s3.enable_path_style_access" = "true"
);
CREATE DATABASE proc_ice_${uuid0}.ns_${uuid0};
CREATE TABLE proc_ice_${uuid0}.ns_${uuid0}.orders (
  id INT,
  amount INT
) TBLPROPERTIES (
  "format-version" = "3",
  "write.row-lineage" = "true"
);
INSERT INTO proc_ice_${uuid0}.ns_${uuid0}.orders VALUES (1, 10), (2, 20);

-- query 2
-- @db=proc_ice_${uuid0}.ns_${uuid0}
CALL proc_ice_${uuid0}.system.rewrite_manifests(table => 'ns_${uuid0}.orders');

-- query 3
-- @db=proc_ice_${uuid0}.ns_${uuid0}
CALL proc_ice_${uuid0}.system.rewrite_position_delete_files(table => 'ns_${uuid0}.orders');

-- query 4
-- @skip_result_check=true
DROP TABLE proc_ice_${uuid0}.ns_${uuid0}.orders FORCE;
DROP DATABASE proc_ice_${uuid0}.ns_${uuid0};
DROP CATALOG proc_ice_${uuid0};
