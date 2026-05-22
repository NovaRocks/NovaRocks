-- @sequential=true
-- @order_sensitive=true
-- @tags=mv,iceberg,ivm,join,aggregate,min_max,detail_state
-- Test Point: Two-base inner-join aggregate IMV with MIN/MAX detail state.
-- Verifies the Phase 4 join-aggregate path correctly maintains MIN/MAX on
-- both INSERT and boundary-DELETE deltas applied to the fact side.
-- Method: orders (fact) JOIN users (dim) ON orders.user_id = users.id;
-- MV computes MIN(order_amount), MAX(order_amount), COUNT(*) GROUP BY
-- u.user_region. INSERT 3 users and 5 orders; verify. DELETE the current
-- MIN order in one region; REFRESH; verify the region's MIN re-derives to
-- the next smallest while the other regions are unchanged.
-- Scope: Iceberg target MV, two-base join aggregate, MIN/MAX detail-state
-- boundary-DELETE path on the fact side.

-- query 1
-- @skip_result_check=true
CREATE EXTERNAL CATALOG ice_ivm_jmm_${uuid0}
PROPERTIES (
  "type" = "iceberg",
  "iceberg.catalog.type" = "hadoop",
  "iceberg.catalog.warehouse" = "${iceberg_catalog_warehouse}/iceberg_ivm_jmm_${uuid0}",
  "aws.s3.endpoint" = "${oss_endpoint}",
  "aws.s3.access_key" = "${oss_ak}",
  "aws.s3.secret_key" = "${oss_sk}",
  "aws.s3.enable_path_style_access" = "true"
);
CREATE DATABASE ice_ivm_jmm_${uuid0}.ns_${uuid0};
CREATE TABLE ice_ivm_jmm_${uuid0}.ns_${uuid0}.orders (
  id BIGINT NOT NULL,
  user_id BIGINT,
  order_amount BIGINT
) TBLPROPERTIES (
  "format-version" = "3",
  "write.row-lineage" = "true"
);
CREATE TABLE ice_ivm_jmm_${uuid0}.ns_${uuid0}.users (
  id BIGINT NOT NULL,
  user_region STRING
) TBLPROPERTIES (
  "format-version" = "3",
  "write.row-lineage" = "true"
);
SET CATALOG ice_ivm_jmm_${uuid0};
USE ns_${uuid0};
CREATE MATERIALIZED VIEW jmm_mv_${uuid0}
DISTRIBUTED BY HASH(user_region) BUCKETS 1
PROPERTIES ('storage_engine' = 'iceberg')
AS
SELECT u.user_region,
       MIN(o.order_amount) AS mn,
       MAX(o.order_amount) AS mx,
       COUNT(*) AS c
FROM ice_ivm_jmm_${uuid0}.ns_${uuid0}.orders AS o
JOIN ice_ivm_jmm_${uuid0}.ns_${uuid0}.users AS u
  ON o.user_id = u.id
GROUP BY u.user_region;

-- query 2
-- @skip_result_check=true
INSERT INTO ice_ivm_jmm_${uuid0}.ns_${uuid0}.users VALUES
  (1, 'east'),
  (2, 'west'),
  (3, 'south');
INSERT INTO ice_ivm_jmm_${uuid0}.ns_${uuid0}.orders VALUES
  (100, 1, 25),
  (101, 1, 75),
  (102, 2, 40),
  (103, 2, 60),
  (104, 3, 99);
REFRESH MATERIALIZED VIEW jmm_mv_${uuid0};

-- query 3
SELECT user_region, mn, mx, c
FROM jmm_mv_${uuid0}
ORDER BY user_region;

-- query 4
SELECT u.user_region,
       MIN(o.order_amount) AS mn,
       MAX(o.order_amount) AS mx,
       COUNT(*) AS c
FROM ice_ivm_jmm_${uuid0}.ns_${uuid0}.orders AS o
JOIN ice_ivm_jmm_${uuid0}.ns_${uuid0}.users AS u
  ON o.user_id = u.id
GROUP BY u.user_region
ORDER BY u.user_region;

-- query 5
-- Delete the current MIN order for 'east' (user_id=1, order_amount=25).
-- After REFRESH, MIN for 'east' must become 75; other regions unchanged.
-- @skip_result_check=true
DELETE FROM ice_ivm_jmm_${uuid0}.ns_${uuid0}.orders WHERE id = 100;
REFRESH MATERIALIZED VIEW jmm_mv_${uuid0};

-- query 6
SELECT user_region, mn, mx, c
FROM jmm_mv_${uuid0}
ORDER BY user_region;

-- query 7
SELECT u.user_region,
       MIN(o.order_amount) AS mn,
       MAX(o.order_amount) AS mx,
       COUNT(*) AS c
FROM ice_ivm_jmm_${uuid0}.ns_${uuid0}.orders AS o
JOIN ice_ivm_jmm_${uuid0}.ns_${uuid0}.users AS u
  ON o.user_id = u.id
GROUP BY u.user_region
ORDER BY u.user_region;

-- query 8
-- @skip_result_check=true
DROP MATERIALIZED VIEW jmm_mv_${uuid0};
DROP TABLE ice_ivm_jmm_${uuid0}.ns_${uuid0}.orders FORCE;
DROP TABLE ice_ivm_jmm_${uuid0}.ns_${uuid0}.users FORCE;
DROP DATABASE ice_ivm_jmm_${uuid0}.ns_${uuid0};
DROP CATALOG ice_ivm_jmm_${uuid0};
