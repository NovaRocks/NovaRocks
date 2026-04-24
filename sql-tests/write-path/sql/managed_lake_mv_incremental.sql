-- @order_sensitive=true
-- @tags=write_path,managed_lake,mv,iceberg,incremental
-- Test Objective:
-- 1. Validate first refresh materializes projection/filter MV over Iceberg.
-- 2. Validate second refresh with appended Iceberg rows adds only matching new rows.

-- query 1
-- @skip_result_check=true
CREATE EXTERNAL CATALOG mv_inc_ice_${uuid0}
PROPERTIES (
  "type" = "iceberg",
  "iceberg.catalog.type" = "hadoop",
  "iceberg.catalog.warehouse" = "${managed_lake_warehouse}/iceberg_inc_${uuid0}",
  "aws.s3.endpoint" = "${oss_endpoint}",
  "aws.s3.access_key" = "${oss_ak}",
  "aws.s3.secret_key" = "${oss_sk}",
  "aws.s3.enable_path_style_access" = "true"
);
CREATE DATABASE mv_inc_ice_${uuid0}.ns_${uuid0};
CREATE TABLE mv_inc_ice_${uuid0}.ns_${uuid0}.orders (
  k1 INT,
  v2 BIGINT
);
INSERT INTO mv_inc_ice_${uuid0}.ns_${uuid0}.orders VALUES
  (1, 10),
  (2, 20);
CREATE MATERIALIZED VIEW ${case_db}.orders_mv
DISTRIBUTED BY HASH(k1) BUCKETS 2
AS SELECT k1, v2 FROM mv_inc_ice_${uuid0}.ns_${uuid0}.orders WHERE v2 >= 20;

-- query 2
-- @skip_result_check=true
REFRESH MATERIALIZED VIEW ${case_db}.orders_mv;

-- query 3
SELECT k1, v2 FROM ${case_db}.orders_mv ORDER BY k1;

-- query 4
-- @skip_result_check=true
INSERT INTO mv_inc_ice_${uuid0}.ns_${uuid0}.orders VALUES (3, 30), (4, 5);
REFRESH MATERIALIZED VIEW ${case_db}.orders_mv;

-- query 5
SELECT k1, v2 FROM ${case_db}.orders_mv ORDER BY k1;

-- query 6
-- @skip_result_check=true
DROP MATERIALIZED VIEW ${case_db}.orders_mv;
DROP TABLE mv_inc_ice_${uuid0}.ns_${uuid0}.orders FORCE;
DROP DATABASE mv_inc_ice_${uuid0}.ns_${uuid0};
DROP CATALOG mv_inc_ice_${uuid0};
