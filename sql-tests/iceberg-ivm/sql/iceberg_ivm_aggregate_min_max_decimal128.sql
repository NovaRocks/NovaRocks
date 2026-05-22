-- @sequential=true
-- @order_sensitive=true
-- @tags=mv,iceberg,ivm,aggregate,min_max,detail_state,decimal128
-- Test Point (IVM-P5 follow-up: non-Int64 SQL coverage): MIN/MAX over a
-- DECIMAL(18, 2) base column round-trips correctly through Map<Decimal128, Int64>
-- detail-state, Iceberg parquet write, and Iceberg parquet read. Boundary
-- DELETE (delete the current MIN row) re-derives MIN from the merged
-- detail-map without full refresh.
-- Scope: validate Decimal128 key Arrow ↔ Iceberg field-id round-trip (the
-- type combo most at risk after the four IVM-P5 integration bugs in #160).

-- query 1
-- @skip_result_check=true
CREATE EXTERNAL CATALOG ice_ivm_minmax_decimal_db_${uuid0}
PROPERTIES (
  "type" = "iceberg",
  "iceberg.catalog.type" = "hadoop",
  "iceberg.catalog.warehouse" = "${iceberg_catalog_warehouse}/ice_ivm_minmax_decimal_db_${uuid0}",
  "aws.s3.endpoint" = "${oss_endpoint}",
  "aws.s3.access_key" = "${oss_ak}",
  "aws.s3.secret_key" = "${oss_sk}",
  "aws.s3.enable_path_style_access" = "true"
);
CREATE DATABASE ice_ivm_minmax_decimal_db_${uuid0}.ns_${uuid0};
CREATE TABLE ice_ivm_minmax_decimal_db_${uuid0}.ns_${uuid0}.sales (
  id BIGINT,
  region STRING,
  amount DECIMAL(18, 2)
) TBLPROPERTIES (
  "format-version" = "3",
  "write.row-lineage" = "true"
);
SET CATALOG ice_ivm_minmax_decimal_db_${uuid0};
USE ns_${uuid0};
CREATE MATERIALIZED VIEW decimal_mv_${uuid0}
DISTRIBUTED BY HASH(region) BUCKETS 1
PROPERTIES ('storage_engine' = 'iceberg')
AS
SELECT region,
       MIN(amount) AS mn,
       MAX(amount) AS mx,
       COUNT(*) AS c
FROM ice_ivm_minmax_decimal_db_${uuid0}.ns_${uuid0}.sales
GROUP BY region;

-- query 2
-- @skip_result_check=true
-- 5 rows in 'east' with deliberately non-trivial decimal precision so
-- a precision/scale round-trip bug would surface (e.g. trailing-zero
-- representation differences between Arrow and Iceberg parquet).
INSERT INTO ice_ivm_minmax_decimal_db_${uuid0}.ns_${uuid0}.sales VALUES
  (1, 'east', 10.50),
  (2, 'east', 20.75),
  (3, 'east', 30.00),
  (4, 'east', 40.25),
  (5, 'east', 50.99);
REFRESH MATERIALIZED VIEW decimal_mv_${uuid0};

-- query 3
-- MV: MIN=10.50, MAX=50.99, COUNT=5.
SELECT region, mn, mx, c
FROM decimal_mv_${uuid0}
ORDER BY region;

-- query 4
-- Plain GROUP BY verification.
SELECT region,
       MIN(amount) AS mn,
       MAX(amount) AS mx,
       COUNT(*) AS c
FROM ice_ivm_minmax_decimal_db_${uuid0}.ns_${uuid0}.sales
GROUP BY region
ORDER BY region;

-- query 5
-- @skip_result_check=true
-- DELETE id=1 (current MIN=10.50). REFRESH. Boundary DELETE forces the
-- visible deriver to re-scan the detail map and pick the next smallest
-- Decimal128 entry.
DELETE FROM ice_ivm_minmax_decimal_db_${uuid0}.ns_${uuid0}.sales WHERE id = 1;
REFRESH MATERIALIZED VIEW decimal_mv_${uuid0};

-- query 6
-- MV after boundary DELETE: MIN=20.75, MAX=50.99, COUNT=4.
SELECT region, mn, mx, c
FROM decimal_mv_${uuid0}
ORDER BY region;

-- query 7
-- Plain verification.
SELECT region,
       MIN(amount) AS mn,
       MAX(amount) AS mx,
       COUNT(*) AS c
FROM ice_ivm_minmax_decimal_db_${uuid0}.ns_${uuid0}.sales
GROUP BY region
ORDER BY region;

-- query 8
-- @skip_result_check=true
-- DELETE id=5 (current MAX=50.99). REFRESH. Re-derive MAX from detail map.
DELETE FROM ice_ivm_minmax_decimal_db_${uuid0}.ns_${uuid0}.sales WHERE id = 5;
REFRESH MATERIALIZED VIEW decimal_mv_${uuid0};

-- query 9
-- MV after second boundary DELETE: MIN=20.75, MAX=40.25, COUNT=3.
SELECT region, mn, mx, c
FROM decimal_mv_${uuid0}
ORDER BY region;

-- query 10
-- Plain verification.
SELECT region,
       MIN(amount) AS mn,
       MAX(amount) AS mx,
       COUNT(*) AS c
FROM ice_ivm_minmax_decimal_db_${uuid0}.ns_${uuid0}.sales
GROUP BY region
ORDER BY region;

-- query 11
-- @skip_result_check=true
DROP MATERIALIZED VIEW decimal_mv_${uuid0};
DROP TABLE ice_ivm_minmax_decimal_db_${uuid0}.ns_${uuid0}.sales FORCE;
DROP DATABASE ice_ivm_minmax_decimal_db_${uuid0}.ns_${uuid0};
DROP CATALOG ice_ivm_minmax_decimal_db_${uuid0};
