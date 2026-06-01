-- @sequential=true
-- @tags=mv,iceberg,ivm,join,aggregate,rewrite

-- query 1
-- @skip_result_check=true
CREATE EXTERNAL CATALOG ice_imv_join_agg_${uuid0}
PROPERTIES (
  "type" = "iceberg",
  "iceberg.catalog.type" = "hadoop",
  "iceberg.catalog.warehouse" = "${iceberg_catalog_warehouse}/imv_join_agg_cutover_${uuid0}",
  "aws.s3.endpoint" = "${oss_endpoint}",
  "aws.s3.access_key" = "${oss_ak}",
  "aws.s3.secret_key" = "${oss_sk}",
  "aws.s3.enable_path_style_access" = "true"
);
CREATE DATABASE ice_imv_join_agg_${uuid0}.ns_${uuid0};
CREATE TABLE ice_imv_join_agg_${uuid0}.ns_${uuid0}.imv_join_fact (
  id BIGINT,
  dim_id BIGINT,
  amount BIGINT
) TBLPROPERTIES (
  "format-version" = "3",
  "write.row-lineage" = "true"
);

CREATE TABLE ice_imv_join_agg_${uuid0}.ns_${uuid0}.imv_join_dim (
  id BIGINT,
  region STRING
) TBLPROPERTIES (
  "format-version" = "3",
  "write.row-lineage" = "true"
);
SET CATALOG ice_imv_join_agg_${uuid0};
USE ns_${uuid0};

CREATE MATERIALIZED VIEW imv_join_agg_mv
DISTRIBUTED BY HASH(region) BUCKETS 1
PROPERTIES ('storage_engine' = 'iceberg')
AS
SELECT d.region, COUNT(*) AS c, SUM(f.amount) AS s
FROM imv_join_fact AS f
JOIN imv_join_dim AS d ON f.dim_id = d.id
GROUP BY d.region;

INSERT INTO imv_join_dim VALUES
  (10, 'east'),
  (20, 'west');
INSERT INTO imv_join_fact VALUES
  (1, 10, 100),
  (2, 20, 50);
INSERT INTO imv_join_fact VALUES
  (3, 10, 25);

-- query 2
-- @skip_result_check=true
-- @explain_contains=AggregateStateMerge
-- @explain_contains=IcebergMvTargetState
-- @explain_contains=IcebergVersionTable
-- @explain_contains=UNION
-- @explain_contains=sum_state_signed
EXPLAIN REFRESH MATERIALIZED VIEW imv_join_agg_mv;

-- query 3
-- @skip_result_check=true
DROP MATERIALIZED VIEW imv_join_agg_mv;
DROP TABLE ice_imv_join_agg_${uuid0}.ns_${uuid0}.imv_join_fact FORCE;
DROP TABLE ice_imv_join_agg_${uuid0}.ns_${uuid0}.imv_join_dim FORCE;
DROP DATABASE ice_imv_join_agg_${uuid0}.ns_${uuid0};
DROP CATALOG ice_imv_join_agg_${uuid0};
