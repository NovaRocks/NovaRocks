-- @sequential=true
-- @order_sensitive=true
-- @tags=mv,iceberg,ivm,storage_engine_iceberg,noop,stale
-- Test Objective:
-- 1. Validate an Iceberg-backed MV is empty before its first manual refresh.
-- 2. Validate a no-op refresh with no base snapshot change keeps target rows stable.
-- 3. Validate append-only base changes stay invisible until the next refresh.
-- 4. Validate the refreshed target contains only rows matching the MV predicate.

-- query 1
-- @skip_result_check=true
CREATE EXTERNAL CATALOG ice_ivm_noop_${uuid0}
PROPERTIES (
  "type" = "iceberg",
  "iceberg.catalog.type" = "hadoop",
  "iceberg.catalog.warehouse" = "${iceberg_catalog_warehouse}/iceberg_ivm_noop_${uuid0}",
  "aws.s3.endpoint" = "${oss_endpoint}",
  "aws.s3.access_key" = "${oss_ak}",
  "aws.s3.secret_key" = "${oss_sk}",
  "aws.s3.enable_path_style_access" = "true"
);
CREATE DATABASE ice_ivm_noop_${uuid0}.ns_${uuid0};
CREATE TABLE ice_ivm_noop_${uuid0}.ns_${uuid0}.orders (
  order_id INT,
  amount BIGINT,
  region STRING
);
INSERT INTO ice_ivm_noop_${uuid0}.ns_${uuid0}.orders VALUES
  (1, 10, 'east'),
  (2, 20, 'west'),
  (3, 0, 'skip');

SET CATALOG ice_ivm_noop_${uuid0};
USE ns_${uuid0};

CREATE MATERIALIZED VIEW orders_mv
DISTRIBUTED BY HASH(order_id) BUCKETS 2
PROPERTIES ('storage_engine' = 'iceberg')
AS SELECT order_id, amount, region
FROM orders
WHERE amount > 0;

-- query 2
SELECT order_id, amount, region FROM orders_mv ORDER BY order_id;

-- query 3
-- @skip_result_check=true
REFRESH MATERIALIZED VIEW orders_mv;

-- query 4
SELECT order_id, amount, region FROM orders_mv ORDER BY order_id;

-- query 5
-- @skip_result_check=true
REFRESH MATERIALIZED VIEW orders_mv;

-- query 6
SELECT order_id, amount, region FROM orders_mv ORDER BY order_id;

-- query 7
-- @skip_result_check=true
INSERT INTO ice_ivm_noop_${uuid0}.ns_${uuid0}.orders VALUES
  (4, 40, 'north'),
  (5, 0, 'skip');

-- query 8
SELECT order_id, amount, region FROM orders_mv ORDER BY order_id;

-- query 9
-- @skip_result_check=true
REFRESH MATERIALIZED VIEW orders_mv;

-- query 10
SELECT order_id, amount, region FROM orders_mv ORDER BY order_id;

-- query 11
-- @skip_result_check=true
DROP MATERIALIZED VIEW orders_mv;
DROP TABLE ice_ivm_noop_${uuid0}.ns_${uuid0}.orders FORCE;
DROP DATABASE ice_ivm_noop_${uuid0}.ns_${uuid0};
DROP CATALOG ice_ivm_noop_${uuid0};
