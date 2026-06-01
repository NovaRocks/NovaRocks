-- @sequential=true
-- @tags=mv,iceberg,ivm,aggregate,rewrite

-- query 1
-- @skip_result_check=true
CREATE EXTERNAL CATALOG ice_imv_agg_${uuid0}
PROPERTIES (
  "type" = "iceberg",
  "iceberg.catalog.type" = "hadoop",
  "iceberg.catalog.warehouse" = "${iceberg_catalog_warehouse}/imv_agg_cutover_${uuid0}",
  "aws.s3.endpoint" = "${oss_endpoint}",
  "aws.s3.access_key" = "${oss_ak}",
  "aws.s3.secret_key" = "${oss_sk}",
  "aws.s3.enable_path_style_access" = "true"
);
CREATE DATABASE ice_imv_agg_${uuid0}.ns_${uuid0};
CREATE TABLE ice_imv_agg_${uuid0}.ns_${uuid0}.imv_agg_base (
  k BIGINT,
  region STRING,
  amount BIGINT
) TBLPROPERTIES (
  "format-version" = "3",
  "write.row-lineage" = "true"
);
SET CATALOG ice_imv_agg_${uuid0};
USE ns_${uuid0};

CREATE MATERIALIZED VIEW imv_agg_mv
DISTRIBUTED BY HASH(region) BUCKETS 1
PROPERTIES ('storage_engine' = 'iceberg')
AS
SELECT region, COUNT(*) AS c, SUM(amount) AS s
FROM imv_agg_base
GROUP BY region;

INSERT INTO imv_agg_base VALUES
  (1, 'east', 10),
  (2, 'west', 20);
INSERT INTO imv_agg_base VALUES
  (3, 'east', 30);

-- query 2
-- @skip_result_check=true
-- @explain_contains=AggregateStateMerge
-- @explain_contains=IcebergMvTargetState
-- @explain_contains=count_state_signed
-- @explain_contains=sum_state_signed
EXPLAIN REFRESH MATERIALIZED VIEW imv_agg_mv;

-- query 3
-- @skip_result_check=true
DROP MATERIALIZED VIEW imv_agg_mv;
DROP TABLE ice_imv_agg_${uuid0}.ns_${uuid0}.imv_agg_base FORCE;
DROP DATABASE ice_imv_agg_${uuid0}.ns_${uuid0};
DROP CATALOG ice_imv_agg_${uuid0};
