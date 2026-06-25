-- @sequential=true
-- @order_sensitive=true
-- @tags=mv,iceberg,ivm,projection_filter,aggregate,join,partitioned,repartition
-- Test Point: Iceberg-backed MV repartition supports projection/filter,
-- aggregate, and join full rebuild shapes while preserving post-repartition refresh.
-- Method: Repartition a projection/filter MV, an aggregate MV, and a join MV
-- from one target partition spec to another, then verify visible results match
-- a full recompute. Keep one unsupported shape assertion with a concrete
-- UnsupportedRepartitionShape error.
-- Scope: Iceberg target MV, repartition, operation lifecycle, full rebuild,
-- post-repartition incremental refresh for projection/filter.

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
-- @skip_result_check=true
ALTER MATERIALIZED VIEW agg_repart_mv_${uuid0} REPARTITION BY (truncate(region, 2));

-- query 13
SELECT region, c, s
FROM agg_repart_mv_${uuid0}
ORDER BY region;

-- query 14
SELECT region, COUNT(*) AS c, SUM(amount) AS s
FROM ice_ivm_repart_${uuid0}.ns_${uuid0}.orders
GROUP BY region
ORDER BY region;

-- query 15
-- @skip_result_check=true
CREATE TABLE ice_ivm_repart_${uuid0}.ns_${uuid0}.customers (
  customer_id BIGINT NOT NULL,
  region STRING
) TBLPROPERTIES (
  "format-version" = "3",
  "write.row-lineage" = "true"
);
INSERT INTO ice_ivm_repart_${uuid0}.ns_${uuid0}.customers VALUES
  (10, 'east'),
  (20, 'west'),
  (30, 'north');
CREATE TABLE ice_ivm_repart_${uuid0}.ns_${uuid0}.join_orders (
  id BIGINT NOT NULL,
  customer_id BIGINT,
  amount BIGINT
) TBLPROPERTIES (
  "format-version" = "3",
  "write.row-lineage" = "true"
);
INSERT INTO ice_ivm_repart_${uuid0}.ns_${uuid0}.join_orders VALUES
  (101, 10, 100),
  (102, 20, 200),
  (103, 10, 300);
CREATE MATERIALIZED VIEW join_repart_mv_${uuid0}
PARTITION BY region
DISTRIBUTED BY HASH(region) BUCKETS 1
PROPERTIES ('storage_engine' = 'iceberg')
AS
SELECT o.id, c.region, o.amount
FROM ice_ivm_repart_${uuid0}.ns_${uuid0}.join_orders o
JOIN ice_ivm_repart_${uuid0}.ns_${uuid0}.customers c
  ON o.customer_id = c.customer_id;
REFRESH MATERIALIZED VIEW join_repart_mv_${uuid0};
ALTER MATERIALIZED VIEW join_repart_mv_${uuid0} REPARTITION BY (truncate(region, 2));

-- query 16
SELECT id, region, amount
FROM join_repart_mv_${uuid0}
ORDER BY id;

-- query 17
SELECT o.id, c.region, o.amount
FROM ice_ivm_repart_${uuid0}.ns_${uuid0}.join_orders o
JOIN ice_ivm_repart_${uuid0}.ns_${uuid0}.customers c
  ON o.customer_id = c.customer_id
ORDER BY o.id;

-- query 18
-- @expect_error=UnsupportedRepartitionShape
CREATE TABLE ice_ivm_repart_${uuid0}.ns_${uuid0}.orders_extra (
  id BIGINT NOT NULL,
  region STRING,
  amount BIGINT,
  category STRING
) TBLPROPERTIES (
  "format-version" = "3",
  "write.row-lineage" = "true"
);
INSERT INTO ice_ivm_repart_${uuid0}.ns_${uuid0}.orders_extra VALUES
  (201, 'east', 11, 'books'),
  (202, 'west', 22, 'games');
CREATE MATERIALIZED VIEW unsupported_repart_mv_${uuid0}
PARTITION BY region
DISTRIBUTED BY HASH(region) BUCKETS 1
PROPERTIES ('storage_engine' = 'iceberg')
AS
SELECT region, COUNT(*) AS c, SUM(amount) AS s
FROM ice_ivm_repart_${uuid0}.ns_${uuid0}.orders
GROUP BY region
UNION ALL
SELECT region, COUNT(*) AS c, SUM(amount) AS s
FROM ice_ivm_repart_${uuid0}.ns_${uuid0}.orders_extra
GROUP BY region;
REFRESH MATERIALIZED VIEW unsupported_repart_mv_${uuid0};
ALTER MATERIALIZED VIEW unsupported_repart_mv_${uuid0} REPARTITION BY (truncate(region, 2));

-- query 19
-- @skip_result_check=true
DROP MATERIALIZED VIEW IF EXISTS unsupported_repart_mv_${uuid0};
DROP MATERIALIZED VIEW join_repart_mv_${uuid0};
DROP TABLE ice_ivm_repart_${uuid0}.ns_${uuid0}.join_orders;
DROP TABLE ice_ivm_repart_${uuid0}.ns_${uuid0}.customers;
DROP MATERIALIZED VIEW agg_repart_mv_${uuid0};
DROP MATERIALIZED VIEW pf_repart_mv_${uuid0};
DROP TABLE ice_ivm_repart_${uuid0}.ns_${uuid0}.orders_extra;
DROP TABLE ice_ivm_repart_${uuid0}.ns_${uuid0}.orders;
DROP DATABASE ice_ivm_repart_${uuid0}.ns_${uuid0};
DROP CATALOG ice_ivm_repart_${uuid0};
