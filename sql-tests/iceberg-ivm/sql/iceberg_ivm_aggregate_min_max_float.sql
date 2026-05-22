-- @sequential=true
-- @order_sensitive=true
-- @tags=mv,iceberg,ivm,aggregate,min_max,detail_state,float,nan
-- Test Point (IVM-P5 Float follow-up): MIN/MAX(DOUBLE) is now supported in
-- detail-state aggregate IMVs. NaN inputs are correctly handled — they
-- don't participate in the visible MIN/MAX (SQL standard "ignore NaN")
-- and they don't crash the merge / sort / derive path.
-- Method: base table with BIGINT id + DOUBLE reading. Insert rows including
-- finite values, NULL, and NaN. Verify MV mirrors plain GROUP BY (which also
-- ignores NaN/NULL). DELETE the current MIN row (by id). REFRESH. Verify new
-- MIN. DELETE the NaN row (by id). REFRESH. Verify MIN/MAX unchanged, count
-- drops. Uses id-based DELETE because Iceberg DELETE WHERE on DOUBLE column
-- is a separate unsupported predicate path in NovaRocks.
-- Scope: Iceberg target MV, single-base aggregate, Float MIN/MAX
-- detail-state path with NaN keys.

-- query 1
-- @skip_result_check=true
CREATE EXTERNAL CATALOG ice_ivm_float_db_${uuid0}
PROPERTIES (
  "type" = "iceberg",
  "iceberg.catalog.type" = "hadoop",
  "iceberg.catalog.warehouse" = "${iceberg_catalog_warehouse}/ice_ivm_float_db_${uuid0}",
  "aws.s3.endpoint" = "${oss_endpoint}",
  "aws.s3.access_key" = "${oss_ak}",
  "aws.s3.secret_key" = "${oss_sk}",
  "aws.s3.enable_path_style_access" = "true"
);
CREATE DATABASE ice_ivm_float_db_${uuid0}.ns_${uuid0};
CREATE TABLE ice_ivm_float_db_${uuid0}.ns_${uuid0}.measurements (
  id BIGINT,
  region STRING,
  reading DOUBLE
) TBLPROPERTIES (
  "format-version" = "3",
  "write.row-lineage" = "true"
);
SET CATALOG ice_ivm_float_db_${uuid0};
USE ns_${uuid0};
CREATE MATERIALIZED VIEW float_mv_${uuid0}
DISTRIBUTED BY HASH(region) BUCKETS 1
PROPERTIES ('storage_engine' = 'iceberg')
AS
SELECT region,
       MIN(reading) AS mn,
       MAX(reading) AS mx,
       COUNT(*) AS c
FROM ice_ivm_float_db_${uuid0}.ns_${uuid0}.measurements
GROUP BY region;

-- query 2
-- @skip_result_check=true
-- 6 rows in 'north': finite values + 1 NaN + 1 NULL. The NaN is constructed
-- via CAST('NaN' AS DOUBLE) which both NovaRocks and Iceberg parquet
-- round-trip as IEEE 754 quiet NaN.
INSERT INTO ice_ivm_float_db_${uuid0}.ns_${uuid0}.measurements VALUES
  (1, 'north', 10.5),
  (2, 'north', 20.25),
  (3, 'north', CAST('NaN' AS DOUBLE)),
  (4, 'north', 30.0),
  (5, 'north', 50.5),
  (6, 'north', NULL);
REFRESH MATERIALIZED VIEW float_mv_${uuid0};

-- query 3
-- MV: MIN/MAX skip NaN and NULL; COUNT counts all 6 rows.
-- Expected: north, 10.5, 50.5, 6.
SELECT region, mn, mx, c
FROM float_mv_${uuid0}
ORDER BY region;

-- query 4
-- Plain GROUP BY for comparison. NovaRocks's MIN/MAX over DOUBLE skips NaN
-- (matches StarRocks IEEE 754 OP() behaviour). Expected identical to query 3.
SELECT region,
       MIN(reading) AS mn,
       MAX(reading) AS mx,
       COUNT(*) AS c
FROM ice_ivm_float_db_${uuid0}.ns_${uuid0}.measurements
GROUP BY region
ORDER BY region;

-- query 5
-- @skip_result_check=true
-- DELETE the current MIN row (id=1, reading=10.5). After REFRESH, new
-- visible MIN must advance to 20.25 via the detail-map re-derive path
-- (the headline IVM-P5 boundary-DELETE case, here exercised on Float keys).
DELETE FROM ice_ivm_float_db_${uuid0}.ns_${uuid0}.measurements WHERE id = 1;
REFRESH MATERIALIZED VIEW float_mv_${uuid0};

-- query 6
-- MV after boundary DELETE: new MIN=20.25, MAX=50.5, COUNT=5.
SELECT region, mn, mx, c
FROM float_mv_${uuid0}
ORDER BY region;

-- query 7
-- Base verification.
SELECT region,
       MIN(reading) AS mn,
       MAX(reading) AS mx,
       COUNT(*) AS c
FROM ice_ivm_float_db_${uuid0}.ns_${uuid0}.measurements
GROUP BY region
ORDER BY region;

-- query 8
-- @skip_result_check=true
-- DELETE the NaN row (id=3). After REFRESH, MIN/MAX unchanged (NaN never
-- participated anyway), but COUNT drops by 1. This exercises the
-- merge_value_count_map_state path: the NaN entry has its count decremented
-- to zero and is pruned, but the visible MIN/MAX deriver skipped NaN keys
-- in both REFRESH passes, so the result is stable.
DELETE FROM ice_ivm_float_db_${uuid0}.ns_${uuid0}.measurements WHERE id = 3;
REFRESH MATERIALIZED VIEW float_mv_${uuid0};

-- query 9
-- MV after NaN row deleted: MIN=20.25, MAX=50.5, COUNT=4.
SELECT region, mn, mx, c
FROM float_mv_${uuid0}
ORDER BY region;

-- query 10
-- Base verification.
SELECT region,
       MIN(reading) AS mn,
       MAX(reading) AS mx,
       COUNT(*) AS c
FROM ice_ivm_float_db_${uuid0}.ns_${uuid0}.measurements
GROUP BY region
ORDER BY region;

-- query 11
-- @skip_result_check=true
DROP MATERIALIZED VIEW float_mv_${uuid0};
DROP TABLE ice_ivm_float_db_${uuid0}.ns_${uuid0}.measurements FORCE;
DROP DATABASE ice_ivm_float_db_${uuid0}.ns_${uuid0};
DROP CATALOG ice_ivm_float_db_${uuid0};
