-- @sequential=true
-- @order_sensitive=true
-- @tags=mv,iceberg,ivm,join,aggregate,partitioned,target_state
-- Test Point: Iceberg-backed partitioned join aggregate MV correctly handles dim-side
-- group key moves by emitting retract+append signed delta rows that span both the
-- old and new MV partition.
-- Method: Create v3 row-lineage fact/dim tables, create PARTITION BY region join
-- aggregate MV. Initial refresh seeds 3 regions. A dim UPDATE moves one fact's
-- region from 'east' to 'north', which must (a) retract the 'east' group's
-- amount contribution and (b) append it to the 'north' group, with the MV
-- result matching the base query after refresh.
-- Scope: Iceberg target MV, two-base join aggregate, identity-partition pruning
-- spanning two partitions per dim move, group row-id apply.

-- query 1
-- @skip_result_check=true
CREATE EXTERNAL CATALOG ice_ivm_pjagg_${uuid0}
PROPERTIES (
  "type" = "iceberg",
  "iceberg.catalog.type" = "hadoop",
  "iceberg.catalog.warehouse" = "${iceberg_catalog_warehouse}/iceberg_ivm_pjagg_${uuid0}",
  "aws.s3.endpoint" = "${oss_endpoint}",
  "aws.s3.access_key" = "${oss_ak}",
  "aws.s3.secret_key" = "${oss_sk}",
  "aws.s3.enable_path_style_access" = "true"
);
CREATE DATABASE ice_ivm_pjagg_${uuid0}.ns_${uuid0};
CREATE TABLE ice_ivm_pjagg_${uuid0}.ns_${uuid0}.fact (
  id BIGINT NOT NULL,
  dim_id BIGINT,
  amount BIGINT
) TBLPROPERTIES (
  "format-version" = "3",
  "write.row-lineage" = "true"
);
CREATE TABLE ice_ivm_pjagg_${uuid0}.ns_${uuid0}.dim (
  id BIGINT NOT NULL,
  region STRING
) TBLPROPERTIES (
  "format-version" = "3",
  "write.row-lineage" = "true"
);
SET CATALOG ice_ivm_pjagg_${uuid0};
USE ns_${uuid0};
CREATE MATERIALIZED VIEW pjagg_mv_${uuid0}
PARTITION BY region
DISTRIBUTED BY HASH(region) BUCKETS 1
PROPERTIES ('storage_engine' = 'iceberg')
AS
SELECT d.region, COUNT(*) AS c, SUM(f.amount) AS s
FROM ice_ivm_pjagg_${uuid0}.ns_${uuid0}.fact AS f
JOIN ice_ivm_pjagg_${uuid0}.ns_${uuid0}.dim AS d ON f.dim_id = d.id
GROUP BY d.region;

-- query 2
-- @skip_result_check=true
INSERT INTO ice_ivm_pjagg_${uuid0}.ns_${uuid0}.dim VALUES
  (10, 'east'),
  (20, 'west'),
  (30, 'south');
INSERT INTO ice_ivm_pjagg_${uuid0}.ns_${uuid0}.fact VALUES
  (1, 10, 100),
  (2, 10, 200),
  (3, 20, 50),
  (4, 30, 70);
REFRESH MATERIALIZED VIEW pjagg_mv_${uuid0};

-- query 3
SELECT region, c, s
FROM pjagg_mv_${uuid0}
ORDER BY region;

-- query 4
-- @skip_result_check=true
UPDATE ice_ivm_pjagg_${uuid0}.ns_${uuid0}.dim SET region = 'north' WHERE id = 10;
REFRESH MATERIALIZED VIEW pjagg_mv_${uuid0};

-- query 5
SELECT region, c, s
FROM pjagg_mv_${uuid0}
ORDER BY region;

-- query 6
SELECT region, c, s
FROM pjagg_mv_${uuid0}
ORDER BY region;

SELECT d.region, COUNT(*) AS c, SUM(f.amount) AS s
FROM ice_ivm_pjagg_${uuid0}.ns_${uuid0}.fact AS f
JOIN ice_ivm_pjagg_${uuid0}.ns_${uuid0}.dim AS d ON f.dim_id = d.id
GROUP BY d.region
ORDER BY region;

-- query 7
-- @skip_result_check=true
UPDATE ice_ivm_pjagg_${uuid0}.ns_${uuid0}.fact SET amount = 500 WHERE id = 3;
REFRESH MATERIALIZED VIEW pjagg_mv_${uuid0};

-- query 8
SELECT region, c, s
FROM pjagg_mv_${uuid0}
ORDER BY region;

-- query 9
-- @skip_result_check=true
DROP MATERIALIZED VIEW pjagg_mv_${uuid0};
DROP TABLE ice_ivm_pjagg_${uuid0}.ns_${uuid0}.fact;
DROP TABLE ice_ivm_pjagg_${uuid0}.ns_${uuid0}.dim;
DROP DATABASE ice_ivm_pjagg_${uuid0}.ns_${uuid0};
DROP CATALOG ice_ivm_pjagg_${uuid0};
