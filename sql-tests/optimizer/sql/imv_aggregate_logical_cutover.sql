-- @sequential=true
-- @order_sensitive=true
-- @tags=optimizer,iceberg,imv,aggregate,logical_cutover
-- Test Objective:
-- Validate optimizer-visible plan evidence for single-table aggregate IMV
-- incremental refresh after the refresh path has cut over to IMV rewrite
-- execution. The case intentionally builds a real previous snapshot by
-- refreshing once before mutating the Iceberg v3 row-lineage base table.

-- query 1
-- @skip_result_check=true
CREATE EXTERNAL CATALOG imv_agg_cut_${uuid0}
PROPERTIES (
  "type" = "iceberg",
  "iceberg.catalog.type" = "hadoop",
  "iceberg.catalog.warehouse" = "${iceberg_catalog_warehouse}/imv_agg_cut_${uuid0}",
  "aws.s3.endpoint" = "${oss_endpoint}",
  "aws.s3.access_key" = "${oss_ak}",
  "aws.s3.secret_key" = "${oss_sk}",
  "aws.s3.enable_path_style_access" = "true"
);
CREATE DATABASE imv_agg_cut_${uuid0}.ns_${uuid0};
CREATE TABLE imv_agg_cut_${uuid0}.ns_${uuid0}.orders (
  id BIGINT NOT NULL,
  region STRING,
  amount BIGINT
) TBLPROPERTIES (
  "format-version" = "3",
  "write.row-lineage" = "true"
);
INSERT INTO imv_agg_cut_${uuid0}.ns_${uuid0}.orders VALUES
  (1, 'east', 10),
  (2, 'east', 20),
  (3, 'west', 30);
SET CATALOG imv_agg_cut_${uuid0};
USE ns_${uuid0};
CREATE MATERIALIZED VIEW agg_mv_${uuid0}
DISTRIBUTED BY HASH(region) BUCKETS 1
PROPERTIES ('storage_engine' = 'iceberg')
AS
SELECT region, COUNT(*) AS c, SUM(amount) AS s
FROM orders
GROUP BY region;

-- query 2
-- Build the real previous snapshot required by incremental refresh planning.
-- @skip_result_check=true
REFRESH MATERIALIZED VIEW agg_mv_${uuid0};

-- query 3
-- Mutate both an existing group and a new group before checking the
-- refresh-time IMV rewrite plan.
-- @skip_result_check=true
INSERT INTO imv_agg_cut_${uuid0}.ns_${uuid0}.orders VALUES
  (4, 'east', 5),
  (5, 'north', 40);

-- query 4
-- @skip_result_check=true
-- @explain_contains=AggregateStateMerge
-- @explain_contains=IcebergMvTargetState
-- @explain_contains=count_state_signed
-- @explain_contains=sum_state_signed
EXPLAIN REFRESH MATERIALIZED VIEW agg_mv_${uuid0};

-- query 5
-- @skip_result_check=true
DROP MATERIALIZED VIEW agg_mv_${uuid0};
DROP TABLE imv_agg_cut_${uuid0}.ns_${uuid0}.orders FORCE;
DROP DATABASE imv_agg_cut_${uuid0}.ns_${uuid0};
DROP CATALOG imv_agg_cut_${uuid0};
