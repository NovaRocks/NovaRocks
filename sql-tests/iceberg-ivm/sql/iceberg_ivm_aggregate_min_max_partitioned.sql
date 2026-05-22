-- @sequential=true
-- @order_sensitive=true
-- @tags=mv,iceberg,ivm,aggregate,min_max,detail_state,partitioned
-- Test Point: MIN/MAX detail-state IMV refresh combined with the
-- affected-partition planner (PR #145). Deltas that touch only a subset of
-- partitions should re-derive MIN/MAX only for those partitions; untouched
-- partitions remain bit-identical across REFRESH.
-- Method: 3 partitions ('SF', 'NY', 'LA') x 5 rows each. PARTITION BY region
-- MV with MIN(amount), MAX(amount), COUNT(*). DELETE the current MIN row in
-- 'SF' and 'NY' partitions only -- 'LA' must remain unchanged.
-- Scope: Iceberg target MV, partitioned aggregate, MIN/MAX detail-state,
-- partition-pruned touched-group lookup + boundary DELETE.

-- query 1
-- @skip_result_check=true
CREATE EXTERNAL CATALOG ice_ivm_minmax_p_${uuid0}
PROPERTIES (
  "type" = "iceberg",
  "iceberg.catalog.type" = "hadoop",
  "iceberg.catalog.warehouse" = "${iceberg_catalog_warehouse}/iceberg_ivm_minmax_p_${uuid0}",
  "aws.s3.endpoint" = "${oss_endpoint}",
  "aws.s3.access_key" = "${oss_ak}",
  "aws.s3.secret_key" = "${oss_sk}",
  "aws.s3.enable_path_style_access" = "true"
);
CREATE DATABASE ice_ivm_minmax_p_${uuid0}.ns_${uuid0};
CREATE TABLE ice_ivm_minmax_p_${uuid0}.ns_${uuid0}.orders (
  region STRING,
  amount BIGINT
) TBLPROPERTIES (
  "format-version" = "3",
  "write.row-lineage" = "true"
);
SET CATALOG ice_ivm_minmax_p_${uuid0};
USE ns_${uuid0};
CREATE MATERIALIZED VIEW minmax_p_mv_${uuid0}
PARTITION BY region
DISTRIBUTED BY HASH(region) BUCKETS 1
PROPERTIES ('storage_engine' = 'iceberg')
AS
SELECT region,
       MIN(amount) AS mn,
       MAX(amount) AS mx,
       COUNT(*) AS c
FROM ice_ivm_minmax_p_${uuid0}.ns_${uuid0}.orders
GROUP BY region;

-- query 2
-- @skip_result_check=true
INSERT INTO ice_ivm_minmax_p_${uuid0}.ns_${uuid0}.orders VALUES
  ('SF', 10),
  ('SF', 20),
  ('SF', 30),
  ('SF', 40),
  ('SF', 50),
  ('NY', 11),
  ('NY', 22),
  ('NY', 33),
  ('NY', 44),
  ('NY', 55),
  ('LA', 7),
  ('LA', 17),
  ('LA', 27),
  ('LA', 37),
  ('LA', 47);
REFRESH MATERIALIZED VIEW minmax_p_mv_${uuid0};

-- query 3
SELECT region, mn, mx, c
FROM minmax_p_mv_${uuid0}
ORDER BY region;

-- query 4
-- @skip_result_check=true
DELETE FROM ice_ivm_minmax_p_${uuid0}.ns_${uuid0}.orders
WHERE region = 'SF' AND amount = 10;
DELETE FROM ice_ivm_minmax_p_${uuid0}.ns_${uuid0}.orders
WHERE region = 'NY' AND amount = 11;
REFRESH MATERIALIZED VIEW minmax_p_mv_${uuid0};

-- query 5
SELECT region, mn, mx, c
FROM minmax_p_mv_${uuid0}
ORDER BY region;

-- query 6
SELECT region,
       MIN(amount) AS mn,
       MAX(amount) AS mx,
       COUNT(*) AS c
FROM ice_ivm_minmax_p_${uuid0}.ns_${uuid0}.orders
GROUP BY region
ORDER BY region;

-- query 7
-- @skip_result_check=true
DROP MATERIALIZED VIEW minmax_p_mv_${uuid0};
DROP TABLE ice_ivm_minmax_p_${uuid0}.ns_${uuid0}.orders FORCE;
DROP DATABASE ice_ivm_minmax_p_${uuid0}.ns_${uuid0};
DROP CATALOG ice_ivm_minmax_p_${uuid0};
