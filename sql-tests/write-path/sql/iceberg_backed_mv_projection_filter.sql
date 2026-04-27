-- @order_sensitive=true
-- @tags=write_path,managed_lake,mv,iceberg,phase4a
-- Test Objective:
-- 1. CREATE MATERIALIZED VIEW with PROPERTIES('storage_engine' = 'iceberg') succeeds.
-- 2. First REFRESH writes visible projection/filter result into the iceberg-backed MV.
-- 3. Append-only incremental REFRESH appends only new rows (only rows matching WHERE v2 > 0).
-- 4. SHOW MATERIALIZED VIEWS includes StorageEngine column with value 'iceberg'.
-- 5. DROP cleans up sqlite + iceberg.

-- query 1
-- @skip_result_check=true
CREATE EXTERNAL CATALOG mv_phase4a_${uuid0}
PROPERTIES (
  "type" = "iceberg",
  "iceberg.catalog.type" = "hadoop",
  "iceberg.catalog.warehouse" = "${managed_lake_warehouse}/iceberg_mv_phase4a_${uuid0}",
  "aws.s3.endpoint" = "${oss_endpoint}",
  "aws.s3.access_key" = "${oss_ak}",
  "aws.s3.secret_key" = "${oss_sk}",
  "aws.s3.enable_path_style_access" = "true"
);
CREATE DATABASE mv_phase4a_${uuid0}.ns_${uuid0};
CREATE TABLE mv_phase4a_${uuid0}.ns_${uuid0}.orders (
  k1 INT,
  v2 BIGINT
);
INSERT INTO mv_phase4a_${uuid0}.ns_${uuid0}.orders VALUES
  (1, 10), (1, 20), (2, 40), (3, 0);

CREATE MATERIALIZED VIEW ${case_db}.proj_mv
DISTRIBUTED BY HASH(k1) BUCKETS 2
PROPERTIES ('storage_engine' = 'iceberg')
AS SELECT k1, v2 FROM mv_phase4a_${uuid0}.ns_${uuid0}.orders WHERE v2 > 0;

-- query 2
-- @skip_result_check=true
REFRESH MATERIALIZED VIEW ${case_db}.proj_mv;

-- query 3
SELECT k1, v2 FROM ${case_db}.proj_mv ORDER BY k1, v2;

-- query 4
-- @skip_result_check=true
INSERT INTO mv_phase4a_${uuid0}.ns_${uuid0}.orders VALUES
  (1, 70), (4, 5);

-- query 5
-- @skip_result_check=true
REFRESH MATERIALIZED VIEW ${case_db}.proj_mv;

-- query 6
SELECT k1, v2 FROM ${case_db}.proj_mv ORDER BY k1, v2;

-- query 7
-- SHOW MATERIALIZED VIEWS includes StorageEngine column with value 'iceberg'.
-- @result_contains=proj_mv
-- @result_contains=iceberg
SHOW MATERIALIZED VIEWS FROM ${case_db};

-- query 8
-- @skip_result_check=true
DROP MATERIALIZED VIEW ${case_db}.proj_mv;
DROP TABLE mv_phase4a_${uuid0}.ns_${uuid0}.orders FORCE;
DROP DATABASE mv_phase4a_${uuid0}.ns_${uuid0};
DROP CATALOG mv_phase4a_${uuid0};
