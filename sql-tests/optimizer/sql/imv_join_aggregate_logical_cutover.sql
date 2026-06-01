-- @sequential=true
-- @order_sensitive=true
-- @tags=optimizer,iceberg,imv,join,aggregate,logical_cutover
-- Test Objective:
-- Validate optimizer-visible plan evidence for join aggregate IMV
-- incremental refresh after the refresh path has cut over to IMV rewrite
-- execution. The case intentionally builds real previous snapshots before
-- appending to the fact table so the join delta rewrite must plan version
-- scans and a UNION branch shape.

-- query 1
-- @skip_result_check=true
CREATE EXTERNAL CATALOG imv_jagg_cut_${uuid0}
PROPERTIES (
  "type" = "iceberg",
  "iceberg.catalog.type" = "hadoop",
  "iceberg.catalog.warehouse" = "${iceberg_catalog_warehouse}/imv_jagg_cut_${uuid0}",
  "aws.s3.endpoint" = "${oss_endpoint}",
  "aws.s3.access_key" = "${oss_ak}",
  "aws.s3.secret_key" = "${oss_sk}",
  "aws.s3.enable_path_style_access" = "true"
);
CREATE DATABASE imv_jagg_cut_${uuid0}.ns_${uuid0};
CREATE TABLE imv_jagg_cut_${uuid0}.ns_${uuid0}.fact (
  id BIGINT NOT NULL,
  dim_id BIGINT,
  amount BIGINT
) TBLPROPERTIES (
  "format-version" = "3",
  "write.row-lineage" = "true"
);
CREATE TABLE imv_jagg_cut_${uuid0}.ns_${uuid0}.dim (
  id BIGINT NOT NULL,
  region STRING
) TBLPROPERTIES (
  "format-version" = "3",
  "write.row-lineage" = "true"
);
INSERT INTO imv_jagg_cut_${uuid0}.ns_${uuid0}.dim VALUES
  (10, 'east'),
  (20, 'west');
INSERT INTO imv_jagg_cut_${uuid0}.ns_${uuid0}.fact VALUES
  (1, 10, 100),
  (2, 10, 200),
  (3, 20, 50);
SET CATALOG imv_jagg_cut_${uuid0};
USE ns_${uuid0};
CREATE MATERIALIZED VIEW join_agg_mv_${uuid0}
DISTRIBUTED BY HASH(region) BUCKETS 1
PROPERTIES ('storage_engine' = 'iceberg')
AS
SELECT d.region, SUM(f.amount) AS s
FROM fact AS f
JOIN dim AS d ON f.dim_id = d.id
GROUP BY d.region;

-- query 2
-- Build the real previous snapshots required by incremental refresh planning.
-- @skip_result_check=true
REFRESH MATERIALIZED VIEW join_agg_mv_${uuid0};

-- query 3
-- Append fact rows so join aggregate refresh needs the IMV join-delta rewrite
-- over delta and pinned version scans.
-- @skip_result_check=true
INSERT INTO imv_jagg_cut_${uuid0}.ns_${uuid0}.fact VALUES
  (4, 20, 80),
  (5, 10, 25);

-- query 4
-- @skip_result_check=true
-- @explain_contains=AggregateStateMerge
-- @explain_contains=IcebergMvTargetState
-- @explain_contains=IcebergVersionTable
-- @explain_contains=UNION
-- @explain_contains=sum_state_signed
REFRESH MATERIALIZED VIEW join_agg_mv_${uuid0};

-- query 5
-- @skip_result_check=true
DROP MATERIALIZED VIEW join_agg_mv_${uuid0};
DROP TABLE imv_jagg_cut_${uuid0}.ns_${uuid0}.fact FORCE;
DROP TABLE imv_jagg_cut_${uuid0}.ns_${uuid0}.dim FORCE;
DROP DATABASE imv_jagg_cut_${uuid0}.ns_${uuid0};
DROP CATALOG imv_jagg_cut_${uuid0};
