-- @sequential=true
-- @order_sensitive=true
-- @tags=mv,iceberg,ivm,aggregate,min_max,detail_state,boolean
-- Test Point (IVM-BoolAgg 2026-05-23 — DDL gate lift): MIN/MAX over a
-- BOOLEAN base column round-trips through Map<Boolean, Int64> detail-state.
-- Prior to this work, the DDL gate explicitly rejected Boolean MIN/MAX with
-- a stale "AggScalarValue does not support Boolean" comment; this fixture
-- proves the gate is unlocked AND incremental refresh semantics match plain
-- GROUP BY (false < true).

-- query 1
-- @skip_result_check=true
CREATE EXTERNAL CATALOG ice_ivm_mmbool_db_${uuid0}
PROPERTIES (
  "type" = "iceberg",
  "iceberg.catalog.type" = "hadoop",
  "iceberg.catalog.warehouse" = "${iceberg_catalog_warehouse}/ice_ivm_mmbool_db_${uuid0}",
  "aws.s3.endpoint" = "${oss_endpoint}",
  "aws.s3.access_key" = "${oss_ak}",
  "aws.s3.secret_key" = "${oss_sk}",
  "aws.s3.enable_path_style_access" = "true"
);
CREATE DATABASE ice_ivm_mmbool_db_${uuid0}.ns_${uuid0};
CREATE TABLE ice_ivm_mmbool_db_${uuid0}.ns_${uuid0}.events (
  id BIGINT,
  region STRING,
  flag BOOLEAN
) TBLPROPERTIES (
  "format-version" = "3",
  "write.row-lineage" = "true"
);
SET CATALOG ice_ivm_mmbool_db_${uuid0};
USE ns_${uuid0};
CREATE MATERIALIZED VIEW mmbool_mv_${uuid0}
DISTRIBUTED BY HASH(region) BUCKETS 1
PROPERTIES ('storage_engine' = 'iceberg')
AS
SELECT region,
       MIN(flag) AS mn,
       MAX(flag) AS mx,
       COUNT(*) AS c
FROM ice_ivm_mmbool_db_${uuid0}.ns_${uuid0}.events
GROUP BY region;

-- query 2
-- @skip_result_check=true
-- Mix of true / false / NULL. NULL is skipped from MIN/MAX (standard SQL)
-- but counted in COUNT(*).
INSERT INTO ice_ivm_mmbool_db_${uuid0}.ns_${uuid0}.events VALUES
  (1, 'east', false),
  (2, 'east', true),
  (3, 'east', true),
  (4, 'east', NULL),
  (5, 'west', true),
  (6, 'west', true);
REFRESH MATERIALIZED VIEW mmbool_mv_${uuid0};

-- query 3
-- MV: east MIN=false, MAX=true, c=4; west MIN=true, MAX=true, c=2.
SELECT region, mn, mx, c
FROM mmbool_mv_${uuid0}
ORDER BY region;

-- query 4
-- Plain GROUP BY verification.
SELECT region,
       MIN(flag) AS mn,
       MAX(flag) AS mx,
       COUNT(*) AS c
FROM ice_ivm_mmbool_db_${uuid0}.ns_${uuid0}.events
GROUP BY region
ORDER BY region;

-- query 5
-- @skip_result_check=true
-- DELETE id=1 (the only false row in 'east'). REFRESH. Boundary DELETE
-- forces MIN to be re-derived from the detail map: MIN flips to true
-- (no false entries left).
DELETE FROM ice_ivm_mmbool_db_${uuid0}.ns_${uuid0}.events WHERE id = 1;
REFRESH MATERIALIZED VIEW mmbool_mv_${uuid0};

-- query 6
-- MV after boundary DELETE: east MIN=true, MAX=true, c=3; west unchanged.
SELECT region, mn, mx, c
FROM mmbool_mv_${uuid0}
ORDER BY region;

-- query 7
SELECT region,
       MIN(flag) AS mn,
       MAX(flag) AS mx,
       COUNT(*) AS c
FROM ice_ivm_mmbool_db_${uuid0}.ns_${uuid0}.events
GROUP BY region
ORDER BY region;

-- query 8
-- @skip_result_check=true
-- Re-INSERT a false row into 'east'. MIN flips back to false.
INSERT INTO ice_ivm_mmbool_db_${uuid0}.ns_${uuid0}.events VALUES
  (7, 'east', false);
REFRESH MATERIALIZED VIEW mmbool_mv_${uuid0};

-- query 9
-- MV after re-INSERT: east MIN=false MAX=true c=4.
SELECT region, mn, mx, c
FROM mmbool_mv_${uuid0}
ORDER BY region;

-- query 10
-- @skip_result_check=true
-- DELETE all true rows from 'west'. With no rows left, the 'west' group
-- disappears from the MV entirely (P5 §3.7 empty-group elimination).
DELETE FROM ice_ivm_mmbool_db_${uuid0}.ns_${uuid0}.events WHERE region = 'west';
REFRESH MATERIALIZED VIEW mmbool_mv_${uuid0};

-- query 11
-- MV after 'west' wiped: only 'east' remains.
SELECT region, mn, mx, c
FROM mmbool_mv_${uuid0}
ORDER BY region;

-- query 12
SELECT region,
       MIN(flag) AS mn,
       MAX(flag) AS mx,
       COUNT(*) AS c
FROM ice_ivm_mmbool_db_${uuid0}.ns_${uuid0}.events
GROUP BY region
ORDER BY region;

-- query 13
-- @skip_result_check=true
DROP MATERIALIZED VIEW mmbool_mv_${uuid0};
DROP TABLE ice_ivm_mmbool_db_${uuid0}.ns_${uuid0}.events FORCE;
DROP DATABASE ice_ivm_mmbool_db_${uuid0}.ns_${uuid0};
DROP CATALOG ice_ivm_mmbool_db_${uuid0};
