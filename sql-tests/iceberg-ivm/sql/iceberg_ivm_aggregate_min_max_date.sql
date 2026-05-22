-- @sequential=true
-- @order_sensitive=true
-- @tags=mv,iceberg,ivm,aggregate,min_max,detail_state,date
-- Test Point (IVM-P5 follow-up: non-Int64 SQL coverage): MIN/MAX over a
-- DATE (Date32) base column round-trips through Map<Date32, Int64>
-- detail-state. Boundary DELETE re-derives correctly. Date ordering
-- matches calendar ordering.
-- Scope: validate Date32 key Arrow ↔ Iceberg field-id round-trip
-- (underlying int32 representation; lower-risk than Decimal128/Utf8 but
-- still uncovered by Int64-only fixtures).

-- query 1
-- @skip_result_check=true
CREATE EXTERNAL CATALOG ice_ivm_minmax_date_db_${uuid0}
PROPERTIES (
  "type" = "iceberg",
  "iceberg.catalog.type" = "hadoop",
  "iceberg.catalog.warehouse" = "${iceberg_catalog_warehouse}/ice_ivm_minmax_date_db_${uuid0}",
  "aws.s3.endpoint" = "${oss_endpoint}",
  "aws.s3.access_key" = "${oss_ak}",
  "aws.s3.secret_key" = "${oss_sk}",
  "aws.s3.enable_path_style_access" = "true"
);
CREATE DATABASE ice_ivm_minmax_date_db_${uuid0}.ns_${uuid0};
CREATE TABLE ice_ivm_minmax_date_db_${uuid0}.ns_${uuid0}.events (
  id BIGINT,
  region STRING,
  event_date DATE
) TBLPROPERTIES (
  "format-version" = "3",
  "write.row-lineage" = "true"
);
SET CATALOG ice_ivm_minmax_date_db_${uuid0};
USE ns_${uuid0};
CREATE MATERIALIZED VIEW date_mv_${uuid0}
DISTRIBUTED BY HASH(region) BUCKETS 1
PROPERTIES ('storage_engine' = 'iceberg')
AS
SELECT region,
       MIN(event_date) AS mn,
       MAX(event_date) AS mx,
       COUNT(*) AS c
FROM ice_ivm_minmax_date_db_${uuid0}.ns_${uuid0}.events
GROUP BY region;

-- query 2
-- @skip_result_check=true
-- 5 rows in 'east' spread across one calendar year.
INSERT INTO ice_ivm_minmax_date_db_${uuid0}.ns_${uuid0}.events VALUES
  (1, 'east', '2026-01-15'),
  (2, 'east', '2026-03-22'),
  (3, 'east', '2026-06-30'),
  (4, 'east', '2026-09-10'),
  (5, 'east', '2026-12-25');
REFRESH MATERIALIZED VIEW date_mv_${uuid0};

-- query 3
-- MV: MIN=2026-01-15, MAX=2026-12-25, COUNT=5.
SELECT region, mn, mx, c
FROM date_mv_${uuid0}
ORDER BY region;

-- query 4
-- Plain verification.
SELECT region,
       MIN(event_date) AS mn,
       MAX(event_date) AS mx,
       COUNT(*) AS c
FROM ice_ivm_minmax_date_db_${uuid0}.ns_${uuid0}.events
GROUP BY region
ORDER BY region;

-- query 5
-- @skip_result_check=true
-- DELETE id=1 (current MIN). REFRESH.
DELETE FROM ice_ivm_minmax_date_db_${uuid0}.ns_${uuid0}.events WHERE id = 1;
REFRESH MATERIALIZED VIEW date_mv_${uuid0};

-- query 6
-- MV after boundary DELETE: MIN=2026-03-22, MAX=2026-12-25, COUNT=4.
SELECT region, mn, mx, c
FROM date_mv_${uuid0}
ORDER BY region;

-- query 7
SELECT region,
       MIN(event_date) AS mn,
       MAX(event_date) AS mx,
       COUNT(*) AS c
FROM ice_ivm_minmax_date_db_${uuid0}.ns_${uuid0}.events
GROUP BY region
ORDER BY region;

-- query 8
-- @skip_result_check=true
-- DELETE id=5 (current MAX). REFRESH.
DELETE FROM ice_ivm_minmax_date_db_${uuid0}.ns_${uuid0}.events WHERE id = 5;
REFRESH MATERIALIZED VIEW date_mv_${uuid0};

-- query 9
-- MV after second boundary DELETE: MIN=2026-03-22, MAX=2026-09-10, COUNT=3.
SELECT region, mn, mx, c
FROM date_mv_${uuid0}
ORDER BY region;

-- query 10
SELECT region,
       MIN(event_date) AS mn,
       MAX(event_date) AS mx,
       COUNT(*) AS c
FROM ice_ivm_minmax_date_db_${uuid0}.ns_${uuid0}.events
GROUP BY region
ORDER BY region;

-- query 11
-- @skip_result_check=true
DROP MATERIALIZED VIEW date_mv_${uuid0};
DROP TABLE ice_ivm_minmax_date_db_${uuid0}.ns_${uuid0}.events FORCE;
DROP DATABASE ice_ivm_minmax_date_db_${uuid0}.ns_${uuid0};
DROP CATALOG ice_ivm_minmax_date_db_${uuid0};
