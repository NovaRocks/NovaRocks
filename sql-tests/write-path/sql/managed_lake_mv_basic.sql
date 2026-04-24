-- @order_sensitive=true
-- @tags=write_path,managed_lake,mv,iceberg
-- Test Objective:
-- 1. Validate standalone CREATE / REFRESH / SELECT / DROP MATERIALIZED VIEW over an Iceberg base table.
-- 2. Confirm MV contents stay stale until the next manual REFRESH.

-- query 1
-- @skip_result_check=true
CREATE EXTERNAL CATALOG mv_ice_${uuid0}
PROPERTIES (
  "type" = "iceberg",
  "iceberg.catalog.type" = "hadoop",
  "iceberg.catalog.warehouse" = "${managed_lake_warehouse}/iceberg_${uuid0}",
  "aws.s3.endpoint" = "${oss_endpoint}",
  "aws.s3.access_key" = "${oss_ak}",
  "aws.s3.secret_key" = "${oss_sk}",
  "aws.s3.enable_path_style_access" = "true"
);
CREATE DATABASE mv_ice_${uuid0}.ns_${uuid0};
CREATE TABLE mv_ice_${uuid0}.ns_${uuid0}.orders (
  k1 INT,
  v2 BIGINT
);
INSERT INTO mv_ice_${uuid0}.ns_${uuid0}.orders VALUES
  (1, 10),
  (2, 20),
  (3, 50);
CREATE MATERIALIZED VIEW ${case_db}.orders_mv
DISTRIBUTED BY HASH(k1) BUCKETS 2
AS SELECT k1, v2 FROM mv_ice_${uuid0}.ns_${uuid0}.orders;

-- query 2
SELECT k1, v2 FROM ${case_db}.orders_mv ORDER BY k1;

-- query 3
-- @skip_result_check=true
REFRESH MATERIALIZED VIEW ${case_db}.orders_mv;

-- query 4
SELECT k1, v2 FROM ${case_db}.orders_mv ORDER BY k1;

-- query 5
-- @skip_result_check=true
INSERT INTO mv_ice_${uuid0}.ns_${uuid0}.orders VALUES (4, 70);

-- query 6
SELECT k1, v2 FROM ${case_db}.orders_mv ORDER BY k1;

-- query 7
-- @skip_result_check=true
REFRESH MATERIALIZED VIEW ${case_db}.orders_mv;

-- query 8
SELECT k1, v2 FROM ${case_db}.orders_mv ORDER BY k1;

-- query 9
-- @result_contains=orders_mv
SHOW MATERIALIZED VIEWS FROM ${case_db};

-- query 10
-- @skip_result_check=true
DROP MATERIALIZED VIEW ${case_db}.orders_mv;
DROP TABLE mv_ice_${uuid0}.ns_${uuid0}.orders FORCE;
DROP DATABASE mv_ice_${uuid0}.ns_${uuid0};
DROP CATALOG mv_ice_${uuid0};
