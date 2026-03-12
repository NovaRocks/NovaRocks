-- @catalog=iceberg_cat_${uuid0}
-- @db=tpch
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

USE `iceberg_cat_${uuid0}`.`tpch`;
SELECT 1 FROM `customer` LIMIT 1;
SELECT 1 FROM `lineitem` LIMIT 1;
SELECT 1 FROM `nation` LIMIT 1;
SELECT 1 FROM `orders` LIMIT 1;
SELECT 1 FROM `part` LIMIT 1;
SELECT 1 FROM `partsupp` LIMIT 1;
SELECT 1 FROM `region` LIMIT 1;
SELECT 1 FROM `supplier` LIMIT 1;
