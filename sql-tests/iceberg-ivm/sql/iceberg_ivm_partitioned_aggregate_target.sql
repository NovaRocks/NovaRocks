-- @sequential=true
-- @order_sensitive=true
-- @tags=mv,iceberg,ivm,aggregate,partitioned,target_state
-- Test Point: Iceberg-backed partitioned aggregate MV refreshes incrementally using
-- the partition-pruned touched-group lookup path.
-- Method: Create v3 row-lineage base table, create storage_engine='iceberg'
-- partitioned aggregate MV (PARTITION BY region), refresh after insert/update/delete
-- limited to a subset of regions, and verify result rows match the equivalent
-- aggregate over the base.
-- Scope: Iceberg target MV, single-base aggregate, COUNT/SUM/AVG, identity-partition
-- pruning + group row-id apply.

-- query 1
-- @skip_result_check=true
CREATE EXTERNAL CATALOG ice_ivm_pagg_${uuid0}
PROPERTIES (
  "type" = "iceberg",
  "iceberg.catalog.type" = "hadoop",
  "iceberg.catalog.warehouse" = "${iceberg_catalog_warehouse}/iceberg_ivm_pagg_${uuid0}",
  "aws.s3.endpoint" = "${oss_endpoint}",
  "aws.s3.access_key" = "${oss_ak}",
  "aws.s3.secret_key" = "${oss_sk}",
  "aws.s3.enable_path_style_access" = "true"
);
CREATE DATABASE ice_ivm_pagg_${uuid0}.ns_${uuid0};
CREATE TABLE ice_ivm_pagg_${uuid0}.ns_${uuid0}.orders (
  region STRING,
  amount BIGINT
) TBLPROPERTIES (
  "format-version" = "3",
  "write.row-lineage" = "true"
);
SET CATALOG ice_ivm_pagg_${uuid0};
USE ns_${uuid0};
CREATE MATERIALIZED VIEW pagg_mv_${uuid0}
PARTITION BY region
DISTRIBUTED BY HASH(region) BUCKETS 1
PROPERTIES ('storage_engine' = 'iceberg')
AS
SELECT region, COUNT(*) AS c, COUNT(amount) AS c_amount, SUM(amount) AS s, AVG(amount) AS a
FROM ice_ivm_pagg_${uuid0}.ns_${uuid0}.orders
GROUP BY region;

-- query 2
-- @skip_result_check=true
INSERT INTO ice_ivm_pagg_${uuid0}.ns_${uuid0}.orders VALUES
  ('east', 10),
  ('east', 20),
  ('west', NULL),
  ('south', 5);
REFRESH MATERIALIZED VIEW pagg_mv_${uuid0};

-- query 3
SELECT region, c, c_amount, s, a
FROM pagg_mv_${uuid0}
ORDER BY region;

-- query 4
-- @skip_result_check=true
INSERT INTO ice_ivm_pagg_${uuid0}.ns_${uuid0}.orders VALUES
  ('east', 30),
  ('south', 15);
REFRESH MATERIALIZED VIEW pagg_mv_${uuid0};

-- query 5
SELECT region, c, c_amount, s, a
FROM pagg_mv_${uuid0}
ORDER BY region;

-- query 6
-- @skip_result_check=true
DELETE FROM ice_ivm_pagg_${uuid0}.ns_${uuid0}.orders WHERE region = 'east' AND amount = 10;
UPDATE ice_ivm_pagg_${uuid0}.ns_${uuid0}.orders SET amount = 100 WHERE region = 'east' AND amount = 20;
REFRESH MATERIALIZED VIEW pagg_mv_${uuid0};

-- query 7
SELECT region, c, c_amount, s, a
FROM pagg_mv_${uuid0}
ORDER BY region;

-- query 8
-- @skip_result_check=true
INSERT INTO ice_ivm_pagg_${uuid0}.ns_${uuid0}.orders VALUES ('west', 42);
REFRESH MATERIALIZED VIEW pagg_mv_${uuid0};

-- query 9
SELECT region, c, c_amount, s, a
FROM pagg_mv_${uuid0}
ORDER BY region;

-- query 10
SELECT region, c, c_amount, s, a
FROM pagg_mv_${uuid0}
ORDER BY region;

SELECT region,
       COUNT(*) AS c,
       COUNT(amount) AS c_amount,
       SUM(amount) AS s,
       AVG(amount) AS a
FROM ice_ivm_pagg_${uuid0}.ns_${uuid0}.orders
GROUP BY region
ORDER BY region;

-- query 11
-- @skip_result_check=true
DROP MATERIALIZED VIEW pagg_mv_${uuid0};
DROP TABLE ice_ivm_pagg_${uuid0}.ns_${uuid0}.orders;
DROP DATABASE ice_ivm_pagg_${uuid0}.ns_${uuid0};
DROP CATALOG ice_ivm_pagg_${uuid0};
