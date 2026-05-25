-- @sequential=true
-- @order_sensitive=true
-- @tags=mv,iceberg,ivm,dependency_graph,drop_guard,base_table
-- Test Objective:
-- 1. DROP TABLE on a base Iceberg table that backs a materialized view is
--    rejected by the dependency guard with the
--    "has downstream materialized views" error.
-- 2. After the MV is dropped, the base table becomes droppable.

-- query 1
-- Bootstrap iceberg catalog, base table, and seed data.
-- @skip_result_check=true
CREATE EXTERNAL CATALOG ice_ivm_dropbase_${uuid0}
PROPERTIES (
  "type" = "iceberg",
  "iceberg.catalog.type" = "rest",
  "uri" = "${iceberg_rest_uri}",
  "warehouse" = "${iceberg_rest_warehouse}",
  "aws.s3.access_key" = "${oss_ak}",
  "aws.s3.secret_key" = "${oss_sk}",
  "aws.s3.endpoint" = "${oss_endpoint}",
  "aws.s3.region" = "us-east-1",
  "aws.s3.enable_path_style_access" = "true"
);
CREATE DATABASE ice_ivm_dropbase_${uuid0}.dropbase_${uuid0};
CREATE TABLE ice_ivm_dropbase_${uuid0}.dropbase_${uuid0}.orders_${uuid0} (
  id BIGINT,
  region STRING,
  amount BIGINT
) TBLPROPERTIES (
  "format-version" = "3",
  "write.row-lineage" = "true"
);
INSERT INTO ice_ivm_dropbase_${uuid0}.dropbase_${uuid0}.orders_${uuid0} VALUES
  (1, 'east', 10),
  (2, 'west', 20);

-- query 2
-- Switch the session context to the new iceberg catalog.
-- @skip_result_check=true
SET CATALOG ice_ivm_dropbase_${uuid0};
USE dropbase_${uuid0};

-- query 3
-- Create an MV that depends on the base table.
-- @skip_result_check=true
CREATE MATERIALIZED VIEW mv_orders_${uuid0}
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES('storage_engine' = 'iceberg')
AS SELECT id, region, amount FROM orders_${uuid0};

-- query 4
-- Refresh so the MV materializes its initial snapshot.
-- @skip_result_check=true
REFRESH MATERIALIZED VIEW mv_orders_${uuid0};

-- query 5
-- Attempting to drop the base table while mv_orders depends on it must be
-- rejected by ensure_no_downstream_dependencies with the guard error.
-- @expect_error=has downstream materialized views
DROP TABLE ice_ivm_dropbase_${uuid0}.dropbase_${uuid0}.orders_${uuid0};

-- query 6
-- After the rejected drop, the base table and its MV are still intact, so
-- the MV continues to return the seeded rows.
SELECT id, region, amount FROM mv_orders_${uuid0} ORDER BY id;

-- query 7
-- Drop the MV first to remove the downstream dependency edge.
-- @skip_result_check=true
DROP MATERIALIZED VIEW mv_orders_${uuid0};

-- query 8
-- With no downstream MV, the base-table drop now succeeds.
-- @skip_result_check=true
DROP TABLE ice_ivm_dropbase_${uuid0}.dropbase_${uuid0}.orders_${uuid0} FORCE;

-- query 9
-- Cleanup: drop the database and external catalog.
-- @skip_result_check=true
DROP DATABASE ice_ivm_dropbase_${uuid0}.dropbase_${uuid0};
DROP CATALOG ice_ivm_dropbase_${uuid0};
