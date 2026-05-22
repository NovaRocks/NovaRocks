-- @sequential=true
-- @order_sensitive=true
-- @tags=mv,iceberg,ivm,aggregate,bool_and,detail_state,boolean
-- Test Point (IVM-BoolAgg 2026-05-23): BOOL_AND over a BOOLEAN base column
-- round-trips through Map<Boolean, Int64> detail-state. Boundary DELETE
-- re-derives correctly: deleting the last `false` row flips visible to
-- `true`; deleting all non-null rows yields NULL.

-- query 1
-- @skip_result_check=true
CREATE EXTERNAL CATALOG ice_ivm_bool_and_db_${uuid0}
PROPERTIES (
  "type" = "iceberg",
  "iceberg.catalog.type" = "hadoop",
  "iceberg.catalog.warehouse" = "${iceberg_catalog_warehouse}/ice_ivm_bool_and_db_${uuid0}",
  "aws.s3.endpoint" = "${oss_endpoint}",
  "aws.s3.access_key" = "${oss_ak}",
  "aws.s3.secret_key" = "${oss_sk}",
  "aws.s3.enable_path_style_access" = "true"
);
CREATE DATABASE ice_ivm_bool_and_db_${uuid0}.ns_${uuid0};
CREATE TABLE ice_ivm_bool_and_db_${uuid0}.ns_${uuid0}.events (
  id BIGINT,
  region STRING,
  flag BOOLEAN
) TBLPROPERTIES (
  "format-version" = "3",
  "write.row-lineage" = "true"
);
SET CATALOG ice_ivm_bool_and_db_${uuid0};
USE ns_${uuid0};
CREATE MATERIALIZED VIEW bool_and_mv_${uuid0}
DISTRIBUTED BY HASH(region) BUCKETS 1
PROPERTIES ('storage_engine' = 'iceberg')
AS
SELECT region,
       BOOL_AND(flag) AS all_true,
       COUNT(*) AS c
FROM ice_ivm_bool_and_db_${uuid0}.ns_${uuid0}.events
GROUP BY region;

-- query 2
-- @skip_result_check=true
-- 7 rows: 'east' has three true + one false + one NULL; 'west' has only
-- true. This exercises the (false>0 → false) and (only true → true) branches.
INSERT INTO ice_ivm_bool_and_db_${uuid0}.ns_${uuid0}.events VALUES
  (1, 'east', true),
  (2, 'east', true),
  (3, 'east', true),
  (4, 'east', false),
  (5, 'east', NULL),
  (6, 'west', true),
  (7, 'west', true);
REFRESH MATERIALIZED VIEW bool_and_mv_${uuid0};

-- query 3
-- MV: east all_true=false (because id=4 is false), c=5;
-- west all_true=true (no false rows), c=2.
SELECT region, all_true, c
FROM bool_and_mv_${uuid0}
ORDER BY region;

-- query 4
-- Plain GROUP BY verification.
SELECT region,
       BOOL_AND(flag) AS all_true,
       COUNT(*) AS c
FROM ice_ivm_bool_and_db_${uuid0}.ns_${uuid0}.events
GROUP BY region
ORDER BY region;

-- query 5
-- @skip_result_check=true
-- DELETE id=4 (the only false row in 'east'). REFRESH. Boundary DELETE
-- forces re-derivation: with no false entries remaining, BOOL_AND must
-- flip to true.
DELETE FROM ice_ivm_bool_and_db_${uuid0}.ns_${uuid0}.events WHERE id = 4;
REFRESH MATERIALIZED VIEW bool_and_mv_${uuid0};

-- query 6
-- MV after boundary DELETE: east all_true=true (no false left), c=4;
-- west unchanged (all_true=true, c=2).
SELECT region, all_true, c
FROM bool_and_mv_${uuid0}
ORDER BY region;

-- query 7
SELECT region,
       BOOL_AND(flag) AS all_true,
       COUNT(*) AS c
FROM ice_ivm_bool_and_db_${uuid0}.ns_${uuid0}.events
GROUP BY region
ORDER BY region;

-- query 8
-- @skip_result_check=true
-- DELETE the NULL row (id=5). BOOL_AND unchanged; COUNT drops by 1.
DELETE FROM ice_ivm_bool_and_db_${uuid0}.ns_${uuid0}.events WHERE id = 5;
REFRESH MATERIALIZED VIEW bool_and_mv_${uuid0};

-- query 9
-- MV after NULL row deleted: east all_true=true, c=3.
SELECT region, all_true, c
FROM bool_and_mv_${uuid0}
ORDER BY region;

-- query 10
-- @skip_result_check=true
-- Re-INSERT another false row into 'east'. BOOL_AND flips back to false.
INSERT INTO ice_ivm_bool_and_db_${uuid0}.ns_${uuid0}.events VALUES
  (8, 'east', false);
REFRESH MATERIALIZED VIEW bool_and_mv_${uuid0};

-- query 11
-- MV after re-INSERT: east all_true=false c=4; west unchanged.
SELECT region, all_true, c
FROM bool_and_mv_${uuid0}
ORDER BY region;

-- query 12
SELECT region,
       BOOL_AND(flag) AS all_true,
       COUNT(*) AS c
FROM ice_ivm_bool_and_db_${uuid0}.ns_${uuid0}.events
GROUP BY region
ORDER BY region;

-- query 13
-- @skip_result_check=true
DROP MATERIALIZED VIEW bool_and_mv_${uuid0};
DROP TABLE ice_ivm_bool_and_db_${uuid0}.ns_${uuid0}.events FORCE;
DROP DATABASE ice_ivm_bool_and_db_${uuid0}.ns_${uuid0};
DROP CATALOG ice_ivm_bool_and_db_${uuid0};
