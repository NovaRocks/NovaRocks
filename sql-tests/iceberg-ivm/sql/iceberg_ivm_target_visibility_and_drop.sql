-- @sequential=true
-- @order_sensitive=true
-- @tags=mv,iceberg,ivm,storage_engine_iceberg,target_catalog,drop
-- Test Objective:
-- 1. Validate an Iceberg-backed MV target is queryable as an Iceberg relation in the active catalog.
-- 2. Validate SHOW MATERIALIZED VIEWS exposes the Iceberg storage engine.
-- 3. Validate DROP MATERIALIZED VIEW removes the target table from the Iceberg catalog.

-- query 1
-- @skip_result_check=true
CREATE EXTERNAL CATALOG ice_ivm_target_${uuid0}
PROPERTIES (
  "type" = "iceberg",
  "iceberg.catalog.type" = "hadoop",
  "iceberg.catalog.warehouse" = "${iceberg_catalog_warehouse}/iceberg_ivm_target_${uuid0}",
  "aws.s3.endpoint" = "${oss_endpoint}",
  "aws.s3.access_key" = "${oss_ak}",
  "aws.s3.secret_key" = "${oss_sk}",
  "aws.s3.enable_path_style_access" = "true"
);
CREATE DATABASE ice_ivm_target_${uuid0}.ns_${uuid0};
CREATE TABLE ice_ivm_target_${uuid0}.ns_${uuid0}.base_orders (
  order_id INT,
  amount BIGINT
);
INSERT INTO ice_ivm_target_${uuid0}.ns_${uuid0}.base_orders VALUES
  (1, 10),
  (2, 20);

SET CATALOG ice_ivm_target_${uuid0};
USE ns_${uuid0};

CREATE MATERIALIZED VIEW target_mv
DISTRIBUTED BY HASH(order_id) BUCKETS 2
PROPERTIES ('storage_engine' = 'iceberg')
AS SELECT order_id, amount FROM base_orders;

REFRESH MATERIALIZED VIEW target_mv;

-- query 2
SELECT order_id, amount FROM target_mv ORDER BY order_id;

-- query 3
-- @result_contains=target_mv
-- @result_contains=iceberg
SHOW MATERIALIZED VIEWS;

-- query 4
SELECT COUNT(*) FROM target_mv;

-- query 5
-- @skip_result_check=true
DROP MATERIALIZED VIEW target_mv;

-- query 6
-- @expect_error=no metadata files
SELECT COUNT(*) FROM target_mv;

-- query 7
-- @skip_result_check=true
DROP TABLE ice_ivm_target_${uuid0}.ns_${uuid0}.base_orders FORCE;
DROP DATABASE ice_ivm_target_${uuid0}.ns_${uuid0};
DROP CATALOG ice_ivm_target_${uuid0};
