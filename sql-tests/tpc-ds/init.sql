-- @catalog=iceberg_cat_${uuid0}
-- @db=tpcds
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

USE `iceberg_cat_${uuid0}`.`tpcds`;
SELECT 1 FROM `call_center` LIMIT 1;
SELECT 1 FROM `catalog_page` LIMIT 1;
SELECT 1 FROM `catalog_returns` LIMIT 1;
SELECT 1 FROM `catalog_sales` LIMIT 1;
SELECT 1 FROM `customer` LIMIT 1;
SELECT 1 FROM `customer_address` LIMIT 1;
SELECT 1 FROM `customer_demographics` LIMIT 1;
SELECT 1 FROM `date_dim` LIMIT 1;
SELECT 1 FROM `household_demographics` LIMIT 1;
SELECT 1 FROM `income_band` LIMIT 1;
SELECT 1 FROM `inventory` LIMIT 1;
SELECT 1 FROM `item` LIMIT 1;
SELECT 1 FROM `promotion` LIMIT 1;
SELECT 1 FROM `reason` LIMIT 1;
SELECT 1 FROM `ship_mode` LIMIT 1;
SELECT 1 FROM `store` LIMIT 1;
SELECT 1 FROM `store_returns` LIMIT 1;
SELECT 1 FROM `store_sales` LIMIT 1;
SELECT 1 FROM `time_dim` LIMIT 1;
SELECT 1 FROM `warehouse` LIMIT 1;
SELECT 1 FROM `web_page` LIMIT 1;
SELECT 1 FROM `web_returns` LIMIT 1;
SELECT 1 FROM `web_sales` LIMIT 1;
SELECT 1 FROM `web_site` LIMIT 1;
