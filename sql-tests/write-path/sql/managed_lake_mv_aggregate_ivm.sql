-- @order_sensitive=true
-- @tags=write_path,managed_lake,mv,iceberg,aggregate
-- Test Objective:
-- 1. Validate aggregate MV incremental refresh over an appended Iceberg base table.
-- 2. Cover COUNT(*), COUNT(nullable column), SUM(nullable column), and hidden state isolation.

-- query 1
-- @skip_result_check=true
CREATE EXTERNAL CATALOG mv_agg_ice_${uuid0}
PROPERTIES (
  "type" = "iceberg",
  "iceberg.catalog.type" = "hadoop",
  "iceberg.catalog.warehouse" = "${managed_lake_warehouse}/iceberg_agg_${uuid0}",
  "aws.s3.endpoint" = "${oss_endpoint}",
  "aws.s3.access_key" = "${oss_ak}",
  "aws.s3.secret_key" = "${oss_sk}",
  "aws.s3.enable_path_style_access" = "true"
);
CREATE DATABASE mv_agg_ice_${uuid0}.ns_${uuid0};
CREATE TABLE mv_agg_ice_${uuid0}.ns_${uuid0}.orders (
  k1 INT,
  v2 BIGINT
);
INSERT INTO mv_agg_ice_${uuid0}.ns_${uuid0}.orders VALUES
  (1, 10),
  (1, 20),
  (2, 40),
  (3, NULL);
CREATE MATERIALIZED VIEW ${case_db}.orders_agg_mv
DISTRIBUTED BY HASH(k1) BUCKETS 2
AS SELECT
  k1,
  count(*) AS c_all,
  count(v2) AS c_v2,
  sum(v2) AS s_v2
FROM mv_agg_ice_${uuid0}.ns_${uuid0}.orders
GROUP BY k1;

-- query 2
-- @skip_result_check=true
REFRESH MATERIALIZED VIEW ${case_db}.orders_agg_mv;

-- query 3
SELECT k1, c_all, c_v2, s_v2
FROM ${case_db}.orders_agg_mv
ORDER BY k1;

-- query 4
-- @skip_result_check=true
INSERT INTO mv_agg_ice_${uuid0}.ns_${uuid0}.orders VALUES
  (1, 70),
  (2, 60),
  (4, 5);

-- query 5
-- @skip_result_check=true
REFRESH MATERIALIZED VIEW ${case_db}.orders_agg_mv;

-- query 6
SELECT k1, c_all, c_v2, s_v2
FROM ${case_db}.orders_agg_mv
ORDER BY k1;

-- query 7
-- @expect_error=Column '__row_id__' cannot be resolved
SELECT __row_id__
FROM ${case_db}.orders_agg_mv;

-- query 8
-- @expect_error=Column '__agg_state_c_all' cannot be resolved
SELECT __agg_state_c_all
FROM ${case_db}.orders_agg_mv;

-- query 9
-- @expect_error=Column '__agg_state_c_v2' cannot be resolved
SELECT __agg_state_c_v2
FROM ${case_db}.orders_agg_mv;

-- query 10
-- @expect_error=Column '__agg_state_s_v2' cannot be resolved
SELECT __agg_state_s_v2
FROM ${case_db}.orders_agg_mv;

-- query 11
-- @skip_result_check=true
DROP MATERIALIZED VIEW ${case_db}.orders_agg_mv;
DROP TABLE mv_agg_ice_${uuid0}.ns_${uuid0}.orders FORCE;
DROP DATABASE mv_agg_ice_${uuid0}.ns_${uuid0};
DROP CATALOG mv_agg_ice_${uuid0};
