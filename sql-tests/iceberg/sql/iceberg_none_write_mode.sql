-- @order_sensitive=true
-- Validate Iceberg writes when write.metadata.metrics.default is set to none.
CREATE EXTERNAL CATALOG iceberg_none_cat_${uuid0}
PROPERTIES (
  "type" = "iceberg",
  "iceberg.catalog.type" = "${iceberg_catalog_type}",
  "iceberg.catalog.warehouse" = "${iceberg_catalog_warehouse}",
  "aws.s3.access_key" = "${oss_ak}",
  "aws.s3.secret_key" = "${oss_sk}",
  "aws.s3.endpoint" = "${oss_endpoint}",
  "aws.s3.enable_path_style_access" = "true"
);
CREATE DATABASE iceberg_none_cat_${uuid0}.iceberg_none_db_${uuid0};
CREATE TABLE iceberg_none_cat_${uuid0}.iceberg_none_db_${uuid0}.iceberg_none_tbl_${uuid0} (
  k1 INT
) PROPERTIES ("write.metadata.metrics.default" = "none");
INSERT INTO iceberg_none_cat_${uuid0}.iceberg_none_db_${uuid0}.iceberg_none_tbl_${uuid0}
SELECT 1;
SELECT k1
FROM iceberg_none_cat_${uuid0}.iceberg_none_db_${uuid0}.iceberg_none_tbl_${uuid0};
SET catalog default_catalog;
DROP TABLE iceberg_none_cat_${uuid0}.iceberg_none_db_${uuid0}.iceberg_none_tbl_${uuid0} FORCE;
DROP DATABASE iceberg_none_cat_${uuid0}.iceberg_none_db_${uuid0};
DROP CATALOG iceberg_none_cat_${uuid0};
