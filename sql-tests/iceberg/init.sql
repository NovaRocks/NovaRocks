-- @catalog=iceberg_cat_${suite_uuid0}
CREATE EXTERNAL CATALOG IF NOT EXISTS `iceberg_cat_${suite_uuid0}`
PROPERTIES (
    "type"="iceberg",
    "iceberg.catalog.type"="${iceberg_catalog_type}",
    "iceberg.catalog.warehouse"="${iceberg_catalog_warehouse}",
    "aws.s3.access_key"="${oss_ak}",
    "aws.s3.secret_key"="${oss_sk}",
    "aws.s3.endpoint"="${oss_endpoint}",
    "aws.s3.enable_path_style_access"="true"
);
