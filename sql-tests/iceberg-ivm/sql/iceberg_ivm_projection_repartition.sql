-- @sequential=true
-- @order_sensitive=true
-- @tags=mv,iceberg,ivm,projection_filter,partitioned,repartition
-- Test Point: Iceberg-backed projection/filter MV can be repartitioned from one
-- target partition spec to another without losing rows, and later incremental
-- refreshes use the new partition contract.
-- Method: Create a row-lineage base, refresh a projection/filter MV partitioned
-- by bucket(id), apply insert/delete incremental changes, repartition the MV target to
-- truncate(region), then apply another incremental delta. Also verify aggregate
-- MV repartition remains rejected by the current support boundary.
-- Scope: Iceberg target MV, single-base stateless projection/filter repartition,
-- post-repartition incremental refresh, aggregate rejection.

-- query 1
-- @skip_result_check=true
CREATE EXTERNAL CATALOG ice_ivm_repart_${uuid0}
PROPERTIES (
  "type" = "iceberg",
  "iceberg.catalog.type" = "hadoop",
  "iceberg.catalog.warehouse" = "${iceberg_catalog_warehouse}/iceberg_ivm_repart_${uuid0}",
  "aws.s3.endpoint" = "${oss_endpoint}",
  "aws.s3.access_key" = "${oss_ak}",
  "aws.s3.secret_key" = "${oss_sk}",
  "aws.s3.enable_path_style_access" = "true"
);
CREATE DATABASE ice_ivm_repart_${uuid0}.ns_${uuid0};
CREATE TABLE ice_ivm_repart_${uuid0}.ns_${uuid0}.orders (
  id BIGINT NOT NULL,
  region STRING,
  amount BIGINT,
  category STRING
) TBLPROPERTIES (
  "format-version" = "3",
  "write.row-lineage" = "true"
);
SET CATALOG ice_ivm_repart_${uuid0};
USE ns_${uuid0};
CREATE MATERIALIZED VIEW pf_repart_mv_${uuid0}
PARTITION BY bucket(id, 4)
DISTRIBUTED BY HASH(id) BUCKETS 2
PROPERTIES ('storage_engine' = 'iceberg')
AS
SELECT id, region, amount, category
FROM ice_ivm_repart_${uuid0}.ns_${uuid0}.orders
WHERE amount >= 10;

-- query 2
-- @skip_result_check=true
INSERT INTO ice_ivm_repart_${uuid0}.ns_${uuid0}.orders VALUES
  (1, 'east', 10, 'books'),
  (2, 'west', 20, 'games'),
  (3, 'east', 30, 'books'),
  (4, 'north', 5, 'games');
REFRESH MATERIALIZED VIEW pf_repart_mv_${uuid0};

-- query 3
SELECT id, region, amount, category
FROM pf_repart_mv_${uuid0}
ORDER BY id;

-- query 4
-- @skip_result_check=true
INSERT INTO ice_ivm_repart_${uuid0}.ns_${uuid0}.orders VALUES
  (5, 'west', 40, 'toys'),
  (6, 'south', 8, 'toys');
DELETE FROM ice_ivm_repart_${uuid0}.ns_${uuid0}.orders WHERE id = 1;
REFRESH MATERIALIZED VIEW pf_repart_mv_${uuid0};

-- query 5
SELECT id, region, amount, category
FROM pf_repart_mv_${uuid0}
ORDER BY id;

-- query 6
-- @skip_result_check=true
ALTER MATERIALIZED VIEW pf_repart_mv_${uuid0} REPARTITION BY (truncate(region, 2));

-- query 7
SELECT id, region, amount, category
FROM pf_repart_mv_${uuid0}
ORDER BY id;

-- query 8
-- @skip_result_check=true
INSERT INTO ice_ivm_repart_${uuid0}.ns_${uuid0}.orders VALUES
  (7, 'east', 70, 'books'),
  (8, 'north', 1, 'games');
DELETE FROM ice_ivm_repart_${uuid0}.ns_${uuid0}.orders WHERE id = 3;
REFRESH MATERIALIZED VIEW pf_repart_mv_${uuid0};

-- query 9
SELECT id, region, amount, category
FROM pf_repart_mv_${uuid0}
ORDER BY id;

-- query 10
SELECT id, region, amount, category
FROM ice_ivm_repart_${uuid0}.ns_${uuid0}.orders
WHERE amount >= 10
ORDER BY id;

-- query 11
-- @skip_result_check=true
CREATE MATERIALIZED VIEW agg_repart_mv_${uuid0}
PARTITION BY region
DISTRIBUTED BY HASH(region) BUCKETS 1
PROPERTIES ('storage_engine' = 'iceberg')
AS
SELECT region, COUNT(*) AS c, SUM(amount) AS s
FROM ice_ivm_repart_${uuid0}.ns_${uuid0}.orders
GROUP BY region;
REFRESH MATERIALIZED VIEW agg_repart_mv_${uuid0};

-- query 12
-- @expect_error=currently supports single-base projection/filter Iceberg MVs only
ALTER MATERIALIZED VIEW agg_repart_mv_${uuid0} REPARTITION BY (truncate(region, 2));

-- query 13
-- @skip_result_check=true
DROP MATERIALIZED VIEW agg_repart_mv_${uuid0};
DROP MATERIALIZED VIEW pf_repart_mv_${uuid0};
DROP TABLE ice_ivm_repart_${uuid0}.ns_${uuid0}.orders;
DROP DATABASE ice_ivm_repart_${uuid0}.ns_${uuid0};
DROP CATALOG ice_ivm_repart_${uuid0};
