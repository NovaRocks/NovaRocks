-- @sequential=true
-- @order_sensitive=true
-- @tags=mv,iceberg,ivm,aggregate,bool_or,detail_state,boolean
-- Test Point (IVM-BoolAgg 2026-05-23): BOOL_OR over a BOOLEAN base column
-- round-trips through Map<Boolean, Int64> detail-state. Boundary DELETE
-- re-derives correctly: deleting the last `true` row flips visible to
-- `false`; deleting all non-null rows yields NULL.
-- Scope: validate Boolean key Arrow ↔ Iceberg field-id round-trip and
-- standard SQL BOOL_OR null/empty semantics under incremental refresh.

-- query 1
-- @skip_result_check=true
CREATE EXTERNAL CATALOG ice_ivm_bool_or_db_${uuid0}
PROPERTIES (
  "type" = "iceberg",
  "iceberg.catalog.type" = "hadoop",
  "iceberg.catalog.warehouse" = "${iceberg_catalog_warehouse}/ice_ivm_bool_or_db_${uuid0}",
  "aws.s3.endpoint" = "${oss_endpoint}",
  "aws.s3.access_key" = "${oss_ak}",
  "aws.s3.secret_key" = "${oss_sk}",
  "aws.s3.enable_path_style_access" = "true"
);
CREATE DATABASE ice_ivm_bool_or_db_${uuid0}.ns_${uuid0};
CREATE TABLE ice_ivm_bool_or_db_${uuid0}.ns_${uuid0}.events (
  id BIGINT,
  region STRING,
  flag BOOLEAN
) TBLPROPERTIES (
  "format-version" = "3",
  "write.row-lineage" = "true"
);
SET CATALOG ice_ivm_bool_or_db_${uuid0};
USE ns_${uuid0};
CREATE MATERIALIZED VIEW bool_or_mv_${uuid0}
DISTRIBUTED BY HASH(region) BUCKETS 1
PROPERTIES ('storage_engine' = 'iceberg')
AS
SELECT region,
       BOOL_OR(flag) AS any_true,
       COUNT(*) AS c
FROM ice_ivm_bool_or_db_${uuid0}.ns_${uuid0}.events
GROUP BY region;

-- query 2
-- @skip_result_check=true
-- 6 rows: 'east' has one true, three false, one NULL; 'west' has only false;
-- this exercises the (true>0 → true) and (only false → false) branches in
-- one refresh.
INSERT INTO ice_ivm_bool_or_db_${uuid0}.ns_${uuid0}.events VALUES
  (1, 'east', true),
  (2, 'east', false),
  (3, 'east', false),
  (4, 'east', false),
  (5, 'east', NULL),
  (6, 'west', false),
  (7, 'west', false);
REFRESH MATERIALIZED VIEW bool_or_mv_${uuid0};

-- query 3
-- MV: east any_true=true (because id=1 is true), c=5;
-- west any_true=false (no true rows at all), c=2.
SELECT region, any_true, c
FROM bool_or_mv_${uuid0}
ORDER BY region;

-- query 4
-- Plain GROUP BY verification.
SELECT region,
       BOOL_OR(flag) AS any_true,
       COUNT(*) AS c
FROM ice_ivm_bool_or_db_${uuid0}.ns_${uuid0}.events
GROUP BY region
ORDER BY region;

-- query 5
-- @skip_result_check=true
-- DELETE id=1 (the only true row in 'east'). REFRESH. Boundary DELETE
-- forces re-derivation from the detail map: with no true entries remaining,
-- BOOL_OR must flip to false.
DELETE FROM ice_ivm_bool_or_db_${uuid0}.ns_${uuid0}.events WHERE id = 1;
REFRESH MATERIALIZED VIEW bool_or_mv_${uuid0};

-- query 6
-- MV after boundary DELETE: east any_true=false (no true left), c=4;
-- west unchanged (any_true=false, c=2).
SELECT region, any_true, c
FROM bool_or_mv_${uuid0}
ORDER BY region;

-- query 7
SELECT region,
       BOOL_OR(flag) AS any_true,
       COUNT(*) AS c
FROM ice_ivm_bool_or_db_${uuid0}.ns_${uuid0}.events
GROUP BY region
ORDER BY region;

-- query 8
-- @skip_result_check=true
-- DELETE the NULL row (id=5). BOOL_OR unchanged (NULL was already
-- skipped from the detail map); COUNT drops by 1.
DELETE FROM ice_ivm_bool_or_db_${uuid0}.ns_${uuid0}.events WHERE id = 5;
REFRESH MATERIALIZED VIEW bool_or_mv_${uuid0};

-- query 9
-- MV after NULL row deleted: east any_true=false, c=3; west unchanged.
SELECT region, any_true, c
FROM bool_or_mv_${uuid0}
ORDER BY region;

-- query 10
-- @skip_result_check=true
-- DELETE every remaining 'east' row. The group's detail map becomes empty
-- → BOOL_OR must be NULL, but the row stays in the MV with COUNT=0?
-- Actually NovaRocks drops empty groups (P5 spec §3.7), so the 'east' row
-- disappears entirely.
DELETE FROM ice_ivm_bool_or_db_${uuid0}.ns_${uuid0}.events WHERE region = 'east';
REFRESH MATERIALIZED VIEW bool_or_mv_${uuid0};

-- query 11
-- MV after all 'east' rows deleted: only 'west' remains.
SELECT region, any_true, c
FROM bool_or_mv_${uuid0}
ORDER BY region;

-- query 12
SELECT region,
       BOOL_OR(flag) AS any_true,
       COUNT(*) AS c
FROM ice_ivm_bool_or_db_${uuid0}.ns_${uuid0}.events
GROUP BY region
ORDER BY region;

-- query 13
-- @skip_result_check=true
-- Re-insert a single true row into 'east'. This recreates the group; MV
-- should report any_true=true, c=1.
INSERT INTO ice_ivm_bool_or_db_${uuid0}.ns_${uuid0}.events VALUES
  (8, 'east', true);
REFRESH MATERIALIZED VIEW bool_or_mv_${uuid0};

-- query 14
-- MV after re-INSERT: east any_true=true c=1; west any_true=false c=2.
SELECT region, any_true, c
FROM bool_or_mv_${uuid0}
ORDER BY region;

-- query 15
-- @skip_result_check=true
DROP MATERIALIZED VIEW bool_or_mv_${uuid0};
DROP TABLE ice_ivm_bool_or_db_${uuid0}.ns_${uuid0}.events FORCE;
DROP DATABASE ice_ivm_bool_or_db_${uuid0}.ns_${uuid0};
DROP CATALOG ice_ivm_bool_or_db_${uuid0};
