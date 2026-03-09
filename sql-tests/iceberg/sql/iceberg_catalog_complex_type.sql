-- @order_sensitive=true
-- Validate Iceberg complex-type readback and nested-field pruning explain text.
-- query 1
CREATE EXTERNAL CATALOG iceberg_cat_${uuid0}
PROPERTIES (
  "type" = "iceberg",
  "iceberg.catalog.type" = "${iceberg_catalog_type}",
  "iceberg.catalog.warehouse" = "${iceberg_catalog_warehouse}",
  "aws.s3.access_key" = "${oss_ak}",
  "aws.s3.secret_key" = "${oss_sk}",
  "aws.s3.endpoint" = "${oss_endpoint}",
  "aws.s3.enable_path_style_access" = "true"
);
CREATE DATABASE iceberg_cat_${uuid0}.iceberg_db_${uuid0};
CREATE TABLE iceberg_cat_${uuid0}.iceberg_db_${uuid0}.ice_tbl_${uuid0} (
  name ARRAY<STRUCT<
    user STRING,
    family STRING,
    given ARRAY<STRING>,
    prefix ARRAY<STRING>,
    suffix ARRAY<STRING>
  >>
);
INSERT INTO iceberg_cat_${uuid0}.iceberg_db_${uuid0}.ice_tbl_${uuid0} VALUES
([named_struct('user', 'official', 'family', 'Glover433', 'given', ['Kira861'], 'prefix', ['Ms.'], 'suffix', NULL)]);
SELECT array_filter(x -> x.`user` = 'official', name)[1].family AS family_name
FROM iceberg_cat_${uuid0}.iceberg_db_${uuid0}.ice_tbl_${uuid0};

-- query 2
-- @result_contains=Pruned type: 1 <-> [ARRAY<struct<user varchar(1073741824), family varchar(1073741824), given array<varchar(1073741824)>, prefix array<varchar(1073741824)>, suffix array<varchar(1073741824)>>>]
EXPLAIN VERBOSE
SELECT array_filter(x -> x.`user` = 'official', name)[1].family AS family_name
FROM iceberg_cat_${uuid0}.iceberg_db_${uuid0}.ice_tbl_${uuid0};
SET catalog default_catalog;
DROP TABLE iceberg_cat_${uuid0}.iceberg_db_${uuid0}.ice_tbl_${uuid0} FORCE;
DROP DATABASE iceberg_cat_${uuid0}.iceberg_db_${uuid0};
DROP CATALOG iceberg_cat_${uuid0};
