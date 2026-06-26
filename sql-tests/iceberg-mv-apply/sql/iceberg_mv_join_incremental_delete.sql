-- @sequential=true
-- @order_sensitive=true
-- @tags=mv,iceberg,ivm,join,incremental_delete,target_apply
-- Test Point: join MV incremental refresh resolves target delete rows through framework target scan.
-- Method: Create a two-base join MV, refresh initial state, then delete/update/insert base rows and refresh incrementally.
-- Scope: Join delta coalescing, framework target apply-key locator, target position delete commit.

-- query 1
-- @skip_result_check=true
CREATE EXTERNAL CATALOG ice_mv_apply_${uuid0}
PROPERTIES (
  "type" = "iceberg",
  "iceberg.catalog.type" = "hadoop",
  "iceberg.catalog.warehouse" = "${iceberg_catalog_warehouse}/ice_mv_apply_${uuid0}",
  "aws.s3.endpoint" = "${oss_endpoint}",
  "aws.s3.access_key" = "${oss_ak}",
  "aws.s3.secret_key" = "${oss_sk}",
  "aws.s3.enable_path_style_access" = "true"
);
CREATE DATABASE ice_mv_apply_${uuid0}.ns_${uuid0};
CREATE TABLE ice_mv_apply_${uuid0}.ns_${uuid0}.fact_${uuid0} (
  id BIGINT NOT NULL,
  dim_id BIGINT,
  amount INT
) TBLPROPERTIES (
  "format-version" = "3",
  "write.row-lineage" = "true"
);
CREATE TABLE ice_mv_apply_${uuid0}.ns_${uuid0}.dim_${uuid0} (
  id BIGINT NOT NULL,
  label STRING
) TBLPROPERTIES (
  "format-version" = "3",
  "write.row-lineage" = "true"
);
SET CATALOG ice_mv_apply_${uuid0};
USE ns_${uuid0};
CREATE MATERIALIZED VIEW join_apply_mv_${uuid0}
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES ('storage_engine' = 'iceberg')
AS
SELECT f.id, f.amount, d.label
FROM fact_${uuid0} AS f
JOIN dim_${uuid0} AS d ON f.dim_id = d.id
WHERE f.amount >= 100;

-- query 2
-- @skip_result_check=true
INSERT INTO ice_mv_apply_${uuid0}.ns_${uuid0}.dim_${uuid0} VALUES
  (10, 'old-a'),
  (20, 'old-b');
INSERT INTO ice_mv_apply_${uuid0}.ns_${uuid0}.fact_${uuid0} VALUES
  (1, 10, 100),
  (2, 20, 200);
REFRESH MATERIALIZED VIEW join_apply_mv_${uuid0};

-- query 3
SELECT id, amount, label
FROM join_apply_mv_${uuid0}
ORDER BY id, label;

-- query 4
-- @skip_result_check=true
DELETE FROM ice_mv_apply_${uuid0}.ns_${uuid0}.fact_${uuid0} WHERE id = 1;
UPDATE ice_mv_apply_${uuid0}.ns_${uuid0}.dim_${uuid0}
SET label = 'new-b'
WHERE id = 20;
INSERT INTO ice_mv_apply_${uuid0}.ns_${uuid0}.dim_${uuid0} VALUES
  (30, 'new-c');
INSERT INTO ice_mv_apply_${uuid0}.ns_${uuid0}.fact_${uuid0} VALUES
  (3, 30, 300);

-- query 5
-- @skip_result_check=true
-- @explain_contains=IcebergVersionTable
REFRESH MATERIALIZED VIEW join_apply_mv_${uuid0};

-- query 6
SELECT id, amount, label
FROM join_apply_mv_${uuid0}
ORDER BY id, label;

-- query 7
SELECT f.id, f.amount, d.label
FROM ice_mv_apply_${uuid0}.ns_${uuid0}.fact_${uuid0} AS f
JOIN ice_mv_apply_${uuid0}.ns_${uuid0}.dim_${uuid0} AS d ON f.dim_id = d.id
WHERE f.amount >= 100
ORDER BY f.id, d.label;

-- query 8
-- @skip_result_check=true
DROP MATERIALIZED VIEW join_apply_mv_${uuid0};
DROP TABLE ice_mv_apply_${uuid0}.ns_${uuid0}.fact_${uuid0} FORCE;
DROP TABLE ice_mv_apply_${uuid0}.ns_${uuid0}.dim_${uuid0} FORCE;
DROP DATABASE ice_mv_apply_${uuid0}.ns_${uuid0};
DROP CATALOG ice_mv_apply_${uuid0};
