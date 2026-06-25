-- @sequential=true
-- @order_sensitive=true
-- @tags=mv,iceberg,ivm,join,partitioned,affected_partitions
-- Test Point: partitioned Iceberg join MV keeps correctness when target partition
-- column is produced from the non-mutated side; refresh explain reports the
-- join affected partition derivation state instead of the legacy not-implemented reason.

-- query 1
-- @skip_result_check=true
CREATE EXTERNAL CATALOG ice_ivm_join_part_${uuid0}
PROPERTIES (
  "type" = "iceberg",
  "iceberg.catalog.type" = "hadoop",
  "iceberg.catalog.warehouse" = "${iceberg_catalog_warehouse}/iceberg_ivm_join_part_${uuid0}",
  "aws.s3.endpoint" = "${oss_endpoint}",
  "aws.s3.access_key" = "${oss_ak}",
  "aws.s3.secret_key" = "${oss_sk}",
  "aws.s3.enable_path_style_access" = "true"
);
CREATE DATABASE ice_ivm_join_part_${uuid0}.ns_${uuid0};
CREATE TABLE ice_ivm_join_part_${uuid0}.ns_${uuid0}.fact (
  id BIGINT NOT NULL,
  dim_id BIGINT,
  amount INT
) TBLPROPERTIES (
  "format-version" = "3",
  "write.row-lineage" = "true"
);
CREATE TABLE ice_ivm_join_part_${uuid0}.ns_${uuid0}.dim (
  id BIGINT NOT NULL,
  region STRING
) TBLPROPERTIES (
  "format-version" = "3",
  "write.row-lineage" = "true"
);
SET CATALOG ice_ivm_join_part_${uuid0};
USE ns_${uuid0};
CREATE MATERIALIZED VIEW join_part_mv_${uuid0}
PARTITION BY region
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES ('storage_engine' = 'iceberg')
AS
SELECT f.id, d.region, f.amount
FROM ice_ivm_join_part_${uuid0}.ns_${uuid0}.fact AS f
JOIN ice_ivm_join_part_${uuid0}.ns_${uuid0}.dim AS d ON f.dim_id = d.id
WHERE f.amount >= 10;

-- query 2
-- @skip_result_check=true
INSERT INTO ice_ivm_join_part_${uuid0}.ns_${uuid0}.dim VALUES
  (10, 'east'),
  (20, 'west');
INSERT INTO ice_ivm_join_part_${uuid0}.ns_${uuid0}.fact VALUES
  (1, 10, 100),
  (2, 20, 200);
REFRESH MATERIALIZED VIEW join_part_mv_${uuid0};

-- query 3
SELECT id, region, amount
FROM join_part_mv_${uuid0}
ORDER BY id;

-- query 4
-- @skip_result_check=true
INSERT INTO ice_ivm_join_part_${uuid0}.ns_${uuid0}.fact VALUES
  (3, 20, 300);

-- query 5
-- @skip_result_check=true
-- @explain_contains=MV Refresh affected partitions: not-derived(join MV affected partition planning requires row-derived delta rows)
-- @explain_not_contains=affected partition planning is not implemented
EXPLAIN REFRESH MATERIALIZED VIEW join_part_mv_${uuid0};

-- query 6
-- @skip_result_check=true
REFRESH MATERIALIZED VIEW join_part_mv_${uuid0};

-- query 7
SELECT id, region, amount
FROM join_part_mv_${uuid0}
ORDER BY id;

-- query 8
SELECT f.id, d.region, f.amount
FROM ice_ivm_join_part_${uuid0}.ns_${uuid0}.fact AS f
JOIN ice_ivm_join_part_${uuid0}.ns_${uuid0}.dim AS d ON f.dim_id = d.id
WHERE f.amount >= 10
ORDER BY f.id;

-- query 9
-- @skip_result_check=true
DROP MATERIALIZED VIEW join_part_mv_${uuid0};
DROP TABLE ice_ivm_join_part_${uuid0}.ns_${uuid0}.fact FORCE;
DROP TABLE ice_ivm_join_part_${uuid0}.ns_${uuid0}.dim FORCE;
DROP DATABASE ice_ivm_join_part_${uuid0}.ns_${uuid0};
DROP CATALOG ice_ivm_join_part_${uuid0};
