-- @catalog=iceberg_cat_${uuid0}
-- @db=ssb
CREATE EXTERNAL CATALOG IF NOT EXISTS `iceberg_cat_${uuid0}`
PROPERTIES (
    "type"="iceberg",
    "iceberg.catalog.type"="${iceberg_catalog_type}",
    "iceberg.catalog.warehouse"="${iceberg_catalog_warehouse}",
    "aws.s3.access_key"="${oss_ak}",
    "aws.s3.secret_key"="${oss_sk}",
    "aws.s3.endpoint"="${oss_endpoint}",
    "aws.s3.enable_path_style_access"="true"
);

USE `iceberg_cat_${uuid0}`.`ssb`;
SHOW CREATE TABLE `customer`;
SHOW CREATE TABLE `dates`;
SHOW CREATE TABLE `lineorder`;
SHOW CREATE TABLE `part`;
SHOW CREATE TABLE `supplier`;
