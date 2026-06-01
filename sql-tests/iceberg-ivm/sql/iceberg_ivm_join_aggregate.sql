-- @sequential=true
-- @order_sensitive=true
-- @tags=mv,iceberg,ivm,join,aggregate,target_state
-- Test Point: Iceberg-backed join aggregate IMV supports two-sided base retract changes.
-- Method: Create fact/dim v3 row-lineage tables, refresh join aggregate MV, mutate both bases, and compare MV with base query.
-- Scope: Iceberg target MV, two-table inner equi-join aggregate, telescoping delta, group row-id apply.

-- query 1
-- @skip_result_check=true
CREATE EXTERNAL CATALOG ice_ivm_join_agg_${uuid0}
PROPERTIES (
  "type" = "iceberg",
  "iceberg.catalog.type" = "hadoop",
  "iceberg.catalog.warehouse" = "${iceberg_catalog_warehouse}/iceberg_ivm_join_agg_${uuid0}",
  "aws.s3.endpoint" = "${oss_endpoint}",
  "aws.s3.access_key" = "${oss_ak}",
  "aws.s3.secret_key" = "${oss_sk}",
  "aws.s3.enable_path_style_access" = "true"
);
CREATE DATABASE ice_ivm_join_agg_${uuid0}.ns_${uuid0};
CREATE TABLE ice_ivm_join_agg_${uuid0}.ns_${uuid0}.fact (
  id BIGINT NOT NULL,
  dim_id BIGINT,
  amount BIGINT
) TBLPROPERTIES (
  "format-version" = "3",
  "write.row-lineage" = "true"
);
CREATE TABLE ice_ivm_join_agg_${uuid0}.ns_${uuid0}.dim (
  id BIGINT NOT NULL,
  region STRING
) TBLPROPERTIES (
  "format-version" = "3",
  "write.row-lineage" = "true"
);
SET CATALOG ice_ivm_join_agg_${uuid0};
USE ns_${uuid0};
CREATE MATERIALIZED VIEW join_agg_mv_${uuid0}
DISTRIBUTED BY HASH(region) BUCKETS 1
PROPERTIES ('storage_engine' = 'iceberg')
AS
SELECT d.region, COUNT(*) AS c, SUM(f.amount) AS s
FROM ice_ivm_join_agg_${uuid0}.ns_${uuid0}.fact AS f
JOIN ice_ivm_join_agg_${uuid0}.ns_${uuid0}.dim AS d ON f.dim_id = d.id
GROUP BY d.region;

-- query 2
-- @skip_result_check=true
INSERT INTO ice_ivm_join_agg_${uuid0}.ns_${uuid0}.dim VALUES
  (10, 'east'),
  (20, 'west'),
  (30, 'south');
INSERT INTO ice_ivm_join_agg_${uuid0}.ns_${uuid0}.fact VALUES
  (1, 10, 100),
  (2, 10, 200),
  (3, 20, 50),
  (4, 30, 70);
REFRESH MATERIALIZED VIEW join_agg_mv_${uuid0};

-- query 3
SELECT region, c, s
FROM join_agg_mv_${uuid0}
ORDER BY region;

-- query 4
-- @skip_result_check=true
INSERT INTO ice_ivm_join_agg_${uuid0}.ns_${uuid0}.fact VALUES (5, 20, 80);
UPDATE ice_ivm_join_agg_${uuid0}.ns_${uuid0}.fact SET amount = 150 WHERE id = 1;
UPDATE ice_ivm_join_agg_${uuid0}.ns_${uuid0}.dim SET region = 'north' WHERE id = 10;
DELETE FROM ice_ivm_join_agg_${uuid0}.ns_${uuid0}.fact WHERE id = 4;
DELETE FROM ice_ivm_join_agg_${uuid0}.ns_${uuid0}.dim WHERE id = 30;
-- @explain_contains=AggregateStateMerge
-- @explain_contains=IcebergVersionTable
-- @explain_contains=IcebergMvTargetState
REFRESH MATERIALIZED VIEW join_agg_mv_${uuid0};

-- query 5
SELECT region, c, s
FROM join_agg_mv_${uuid0}
ORDER BY region;

-- query 6
SELECT d.region, COUNT(*) AS c, SUM(f.amount) AS s
FROM ice_ivm_join_agg_${uuid0}.ns_${uuid0}.fact AS f
JOIN ice_ivm_join_agg_${uuid0}.ns_${uuid0}.dim AS d ON f.dim_id = d.id
GROUP BY d.region
ORDER BY d.region;

-- query 7
-- @skip_result_check=true
DROP MATERIALIZED VIEW join_agg_mv_${uuid0};
DROP TABLE ice_ivm_join_agg_${uuid0}.ns_${uuid0}.fact FORCE;
DROP TABLE ice_ivm_join_agg_${uuid0}.ns_${uuid0}.dim FORCE;
DROP DATABASE ice_ivm_join_agg_${uuid0}.ns_${uuid0};
DROP CATALOG ice_ivm_join_agg_${uuid0};
