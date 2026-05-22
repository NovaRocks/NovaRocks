-- @sequential=true
-- @order_sensitive=true
-- @tags=mv,iceberg,ivm,aggregate,min_max,detail_state,utf8,string
-- Test Point (IVM-P5 follow-up: non-Int64 SQL coverage): MIN/MAX over a
-- STRING (Utf8) base column round-trips through Map<Utf8, Int64>
-- detail-state. Boundary DELETE re-derives correctly. NULL inputs are
-- skipped from MIN/MAX as per SQL standard.
-- Scope: validate Utf8 key Arrow ↔ Iceberg field-id round-trip including
-- variable-width keys + NULL semantics + lexicographic ordering.

-- query 1
-- @skip_result_check=true
CREATE EXTERNAL CATALOG ice_ivm_minmax_string_db_${uuid0}
PROPERTIES (
  "type" = "iceberg",
  "iceberg.catalog.type" = "hadoop",
  "iceberg.catalog.warehouse" = "${iceberg_catalog_warehouse}/ice_ivm_minmax_string_db_${uuid0}",
  "aws.s3.endpoint" = "${oss_endpoint}",
  "aws.s3.access_key" = "${oss_ak}",
  "aws.s3.secret_key" = "${oss_sk}",
  "aws.s3.enable_path_style_access" = "true"
);
CREATE DATABASE ice_ivm_minmax_string_db_${uuid0}.ns_${uuid0};
CREATE TABLE ice_ivm_minmax_string_db_${uuid0}.ns_${uuid0}.items (
  id BIGINT,
  region STRING,
  name STRING
) TBLPROPERTIES (
  "format-version" = "3",
  "write.row-lineage" = "true"
);
SET CATALOG ice_ivm_minmax_string_db_${uuid0};
USE ns_${uuid0};
CREATE MATERIALIZED VIEW string_mv_${uuid0}
DISTRIBUTED BY HASH(region) BUCKETS 1
PROPERTIES ('storage_engine' = 'iceberg')
AS
SELECT region,
       MIN(name) AS mn,
       MAX(name) AS mx,
       COUNT(*) AS c
FROM ice_ivm_minmax_string_db_${uuid0}.ns_${uuid0}.items
GROUP BY region;

-- query 2
-- @skip_result_check=true
-- 6 rows in 'east': 5 alphabetical names + 1 NULL to validate NULL is
-- skipped from MIN/MAX (counted in COUNT(*) though).
INSERT INTO ice_ivm_minmax_string_db_${uuid0}.ns_${uuid0}.items VALUES
  (1, 'east', 'apple'),
  (2, 'east', 'banana'),
  (3, 'east', 'cherry'),
  (4, 'east', 'date'),
  (5, 'east', 'elderberry'),
  (6, 'east', NULL);
REFRESH MATERIALIZED VIEW string_mv_${uuid0};

-- query 3
-- MV: MIN='apple', MAX='elderberry', COUNT=6.
SELECT region, mn, mx, c
FROM string_mv_${uuid0}
ORDER BY region;

-- query 4
-- Plain GROUP BY verification.
SELECT region,
       MIN(name) AS mn,
       MAX(name) AS mx,
       COUNT(*) AS c
FROM ice_ivm_minmax_string_db_${uuid0}.ns_${uuid0}.items
GROUP BY region
ORDER BY region;

-- query 5
-- @skip_result_check=true
-- DELETE id=1 (current MIN='apple'). REFRESH. Boundary DELETE re-derives
-- visible MIN from the detail map.
DELETE FROM ice_ivm_minmax_string_db_${uuid0}.ns_${uuid0}.items WHERE id = 1;
REFRESH MATERIALIZED VIEW string_mv_${uuid0};

-- query 6
-- MV after boundary DELETE: MIN='banana', MAX='elderberry', COUNT=5.
SELECT region, mn, mx, c
FROM string_mv_${uuid0}
ORDER BY region;

-- query 7
SELECT region,
       MIN(name) AS mn,
       MAX(name) AS mx,
       COUNT(*) AS c
FROM ice_ivm_minmax_string_db_${uuid0}.ns_${uuid0}.items
GROUP BY region
ORDER BY region;

-- query 8
-- @skip_result_check=true
-- DELETE id=5 (current MAX='elderberry'). REFRESH.
DELETE FROM ice_ivm_minmax_string_db_${uuid0}.ns_${uuid0}.items WHERE id = 5;
REFRESH MATERIALIZED VIEW string_mv_${uuid0};

-- query 9
-- MV after second boundary DELETE: MIN='banana', MAX='date', COUNT=4.
SELECT region, mn, mx, c
FROM string_mv_${uuid0}
ORDER BY region;

-- query 10
SELECT region,
       MIN(name) AS mn,
       MAX(name) AS mx,
       COUNT(*) AS c
FROM ice_ivm_minmax_string_db_${uuid0}.ns_${uuid0}.items
GROUP BY region
ORDER BY region;

-- query 11
-- @skip_result_check=true
-- DELETE the NULL row. MIN/MAX unchanged; COUNT drops by 1.
DELETE FROM ice_ivm_minmax_string_db_${uuid0}.ns_${uuid0}.items WHERE id = 6;
REFRESH MATERIALIZED VIEW string_mv_${uuid0};

-- query 12
-- MV after NULL row deleted: MIN='banana', MAX='date', COUNT=3.
SELECT region, mn, mx, c
FROM string_mv_${uuid0}
ORDER BY region;

-- query 13
-- @skip_result_check=true
DROP MATERIALIZED VIEW string_mv_${uuid0};
DROP TABLE ice_ivm_minmax_string_db_${uuid0}.ns_${uuid0}.items FORCE;
DROP DATABASE ice_ivm_minmax_string_db_${uuid0}.ns_${uuid0};
DROP CATALOG ice_ivm_minmax_string_db_${uuid0};
