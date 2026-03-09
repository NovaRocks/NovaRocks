-- @order_sensitive=true
-- Validate Iceberg TIME round-trip through the Parquet write/read path.
CREATE EXTERNAL CATALOG iceberg_time_cat_${uuid0}
PROPERTIES (
  "type" = "iceberg",
  "iceberg.catalog.type" = "${iceberg_catalog_type}",
  "iceberg.catalog.warehouse" = "${iceberg_catalog_warehouse}",
  "aws.s3.access_key" = "${oss_ak}",
  "aws.s3.secret_key" = "${oss_sk}",
  "aws.s3.endpoint" = "${oss_endpoint}",
  "aws.s3.enable_path_style_access" = "true"
);
CREATE DATABASE iceberg_time_cat_${uuid0}.iceberg_time_db_${uuid0};
CREATE TABLE iceberg_time_cat_${uuid0}.iceberg_time_db_${uuid0}.iceberg_time_tbl_${uuid0} (
  col_id INT,
  col_time TIME
);
INSERT INTO iceberg_time_cat_${uuid0}.iceberg_time_db_${uuid0}.iceberg_time_tbl_${uuid0} VALUES
  (1, '01:02:03'),
  (2, '20:20:20'),
  (3, NULL);
SELECT col_id, col_time
FROM iceberg_time_cat_${uuid0}.iceberg_time_db_${uuid0}.iceberg_time_tbl_${uuid0}
ORDER BY col_id;
SET catalog default_catalog;
DROP TABLE iceberg_time_cat_${uuid0}.iceberg_time_db_${uuid0}.iceberg_time_tbl_${uuid0} FORCE;
DROP DATABASE iceberg_time_cat_${uuid0}.iceberg_time_db_${uuid0};
DROP CATALOG iceberg_time_cat_${uuid0};
