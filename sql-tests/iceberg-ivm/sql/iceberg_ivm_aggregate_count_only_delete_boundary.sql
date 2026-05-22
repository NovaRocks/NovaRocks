-- @sequential=true
-- @order_sensitive=true
-- @tags=mv,iceberg,ivm,aggregate,count,boundary
-- Test Point (IVM cumulative-DV regression, scalar-only sibling of
-- iceberg_ivm_aggregate_min_max_delete_boundary): sequential boundary
-- DELETEs against a COUNT(*) MV must not double-count the carried-over
-- positions inside the Iceberg v3 Puffin deletion vector. Plain COUNT(*)
-- isolates the scan-deletes subtraction path from the MIN/MAX detail-state
-- code; both fixtures exercising different aggregate shapes prove the fix
-- is in the IVM delete-source layer.
-- Method: 5-row base (amounts 10, 20, 30, 40, 50). Build MV; verify c=5.
-- DELETE amount=10. REFRESH. Verify c=4. DELETE amount=50. REFRESH.
-- Verify c=3 -- which matches the plain SELECT and proves the cumulative
-- DV at the second snapshot was not re-applied as a fresh delete.

-- query 1
-- @skip_result_check=true
CREATE EXTERNAL CATALOG ice_ivm_ctest_db_${uuid0}
PROPERTIES (
  "type" = "iceberg",
  "iceberg.catalog.type" = "hadoop",
  "iceberg.catalog.warehouse" = "${iceberg_catalog_warehouse}/ice_ivm_ctest_db_${uuid0}",
  "aws.s3.endpoint" = "${oss_endpoint}",
  "aws.s3.access_key" = "${oss_ak}",
  "aws.s3.secret_key" = "${oss_sk}",
  "aws.s3.enable_path_style_access" = "true"
);
CREATE DATABASE ice_ivm_ctest_db_${uuid0}.ns_${uuid0};
CREATE TABLE ice_ivm_ctest_db_${uuid0}.ns_${uuid0}.orders (
  region STRING,
  amount BIGINT
) TBLPROPERTIES (
  "format-version" = "3",
  "write.row-lineage" = "true"
);
SET CATALOG ice_ivm_ctest_db_${uuid0};
USE ns_${uuid0};
CREATE MATERIALIZED VIEW ctest_db_mv_${uuid0}
DISTRIBUTED BY HASH(region) BUCKETS 1
PROPERTIES ('storage_engine' = 'iceberg')
AS
SELECT region,
       COUNT(*) AS c
FROM ice_ivm_ctest_db_${uuid0}.ns_${uuid0}.orders
GROUP BY region;

-- query 2
-- @skip_result_check=true
INSERT INTO ice_ivm_ctest_db_${uuid0}.ns_${uuid0}.orders VALUES
  ('east', 10),
  ('east', 20),
  ('east', 30),
  ('east', 40),
  ('east', 50);
REFRESH MATERIALIZED VIEW ctest_db_mv_${uuid0};

-- query 3
SELECT region, c FROM ctest_db_mv_${uuid0} ORDER BY region;

-- query 4
-- @skip_result_check=true
DELETE FROM ice_ivm_ctest_db_${uuid0}.ns_${uuid0}.orders WHERE amount = 10;
REFRESH MATERIALIZED VIEW ctest_db_mv_${uuid0};

-- query 5
SELECT region, c FROM ctest_db_mv_${uuid0} ORDER BY region;

-- query 6
-- @skip_result_check=true
DELETE FROM ice_ivm_ctest_db_${uuid0}.ns_${uuid0}.orders WHERE amount = 50;
REFRESH MATERIALIZED VIEW ctest_db_mv_${uuid0};

-- query 7
SELECT region, c FROM ctest_db_mv_${uuid0} ORDER BY region;

-- query 8
SELECT region, COUNT(*) AS c FROM ice_ivm_ctest_db_${uuid0}.ns_${uuid0}.orders GROUP BY region ORDER BY region;
