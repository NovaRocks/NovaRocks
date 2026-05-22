-- @sequential=true
-- @order_sensitive=true
-- @tags=mv,iceberg,ivm,aggregate,min_max,detail_state,delete,boundary
-- Test Point (headline IVM-P5 case): DELETE of the CURRENT MIN row in an
-- Iceberg-backed MIN/MAX MV correctly re-derives the new MIN from the value-
-- count detail-state map -- WITHOUT triggering a full refresh, which is the
-- core win of IVM-P5.
-- Method: 5-row base (amounts 10, 20, 30, 40, 50). Build MV; verify MIN=10,
-- MAX=50. DELETE amount=10 (the current MIN). REFRESH. Verify MIN=20, MAX=50.
-- Also DELETE amount=50 (the current MAX) in a second step. Verify MIN=20,
-- MAX=40.
-- Scope: Iceberg target MV, single-base aggregate, MIN/MAX detail-state
-- boundary-DELETE path (the core IVM-P5 correctness signal).

-- query 1
-- @skip_result_check=true
CREATE EXTERNAL CATALOG ice_ivm_minmax_db_${uuid0}
PROPERTIES (
  "type" = "iceberg",
  "iceberg.catalog.type" = "hadoop",
  "iceberg.catalog.warehouse" = "${iceberg_catalog_warehouse}/iceberg_ivm_minmax_db_${uuid0}",
  "aws.s3.endpoint" = "${oss_endpoint}",
  "aws.s3.access_key" = "${oss_ak}",
  "aws.s3.secret_key" = "${oss_sk}",
  "aws.s3.enable_path_style_access" = "true"
);
CREATE DATABASE ice_ivm_minmax_db_${uuid0}.ns_${uuid0};
CREATE TABLE ice_ivm_minmax_db_${uuid0}.ns_${uuid0}.orders (
  region STRING,
  amount BIGINT
) TBLPROPERTIES (
  "format-version" = "3",
  "write.row-lineage" = "true"
);
SET CATALOG ice_ivm_minmax_db_${uuid0};
USE ns_${uuid0};
CREATE MATERIALIZED VIEW minmax_db_mv_${uuid0}
DISTRIBUTED BY HASH(region) BUCKETS 1
PROPERTIES ('storage_engine' = 'iceberg')
AS
SELECT region,
       MIN(amount) AS mn,
       MAX(amount) AS mx,
       COUNT(*) AS c
FROM ice_ivm_minmax_db_${uuid0}.ns_${uuid0}.orders
GROUP BY region;

-- query 2
-- @skip_result_check=true
INSERT INTO ice_ivm_minmax_db_${uuid0}.ns_${uuid0}.orders VALUES
  ('east', 10),
  ('east', 20),
  ('east', 30),
  ('east', 40),
  ('east', 50);
REFRESH MATERIALIZED VIEW minmax_db_mv_${uuid0};

-- query 3
SELECT region, mn, mx, c
FROM minmax_db_mv_${uuid0}
ORDER BY region;

-- query 4
-- @skip_result_check=true
DELETE FROM ice_ivm_minmax_db_${uuid0}.ns_${uuid0}.orders WHERE amount = 10;
REFRESH MATERIALIZED VIEW minmax_db_mv_${uuid0};

-- query 5
SELECT region, mn, mx, c
FROM minmax_db_mv_${uuid0}
ORDER BY region;

-- query 6
SELECT region,
       MIN(amount) AS mn,
       MAX(amount) AS mx,
       COUNT(*) AS c
FROM ice_ivm_minmax_db_${uuid0}.ns_${uuid0}.orders
GROUP BY region
ORDER BY region;

-- query 7
-- @skip_result_check=true
DELETE FROM ice_ivm_minmax_db_${uuid0}.ns_${uuid0}.orders WHERE amount = 50;
REFRESH MATERIALIZED VIEW minmax_db_mv_${uuid0};

-- query 8
SELECT region, mn, mx, c
FROM minmax_db_mv_${uuid0}
ORDER BY region;

-- query 9
SELECT region,
       MIN(amount) AS mn,
       MAX(amount) AS mx,
       COUNT(*) AS c
FROM ice_ivm_minmax_db_${uuid0}.ns_${uuid0}.orders
GROUP BY region
ORDER BY region;

-- query 10
-- @skip_result_check=true
DROP MATERIALIZED VIEW minmax_db_mv_${uuid0};
DROP TABLE ice_ivm_minmax_db_${uuid0}.ns_${uuid0}.orders FORCE;
DROP DATABASE ice_ivm_minmax_db_${uuid0}.ns_${uuid0};
DROP CATALOG ice_ivm_minmax_db_${uuid0};
