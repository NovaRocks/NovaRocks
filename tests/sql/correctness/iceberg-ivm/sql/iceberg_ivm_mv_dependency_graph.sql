-- Licensed to the Apache Software Foundation (ASF) under one
-- or more contributor license agreements.  See the NOTICE file
-- distributed with this work for additional information
-- regarding copyright ownership.  The ASF licenses this file
-- to you under the Apache License, Version 2.0 (the
-- "License"); you may not use this file except in compliance
-- with the License.  You may obtain a copy of the License at
--
--   http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing,
-- software distributed under the License is distributed on an
-- "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
-- KIND, either express or implied.  See the License for the
-- specific language governing permissions and limitations
-- under the License.

-- @sequential=true
-- @order_sensitive=true
-- @tags=mv,iceberg,ivm,dependency_graph,mv_on_mv
-- Test Objective:
-- 1. Cover the MV dependency graph end-to-end: an upstream Iceberg MV
--    (`mv_orders`) is consumed by a downstream Iceberg MV (`mv_region`).
-- 2. REFRESH on the downstream MV cascades through the upstream MV so that
--    the downstream sees up-to-date upstream rows after a base-table insert.
-- 3. DROP on an upstream MV that still has a downstream MV is rejected by the
--    dependency guard ("has downstream materialized views: ...").
-- 4. After dropping the downstream MV, dropping the upstream MV succeeds.

-- query 1
-- @skip_result_check=true
CREATE EXTERNAL CATALOG ice_ivm_dep_${uuid0}
PROPERTIES (
  "type" = "iceberg",
  "iceberg.catalog.type" = "rest",
  "uri" = "${iceberg_rest_uri}",
  "warehouse" = "${iceberg_rest_warehouse}",
  "credential.object-store-metadata.consumer-role" = "frontend",
  "credential.object-store-metadata.mode" = "static",
  "credential.object-store-metadata.name" = "${iceberg_object_store_credential_name}",
  "credential.object-store-metadata.generation" = "${iceberg_object_store_credential_generation}",
  "credential.object-store-data.consumer-role" = "backend",
  "credential.object-store-data.mode" = "static",
  "credential.object-store-data.name" = "${iceberg_object_store_credential_name}",
  "credential.object-store-data.generation" = "${iceberg_object_store_credential_generation}",
  "aws.s3.endpoint" = "${oss_endpoint}",
  "aws.s3.region" = "us-east-1",
  "aws.s3.enable_path_style_access" = "true"
);
CREATE DATABASE ice_ivm_dep_${uuid0}.dep_${uuid0};
CREATE TABLE ice_ivm_dep_${uuid0}.dep_${uuid0}.orders_${uuid0} (
  id BIGINT,
  region STRING,
  amount BIGINT
)
TBLPROPERTIES ("format-version" = "3",
  "write.row-lineage" = "true");
INSERT INTO ice_ivm_dep_${uuid0}.dep_${uuid0}.orders_${uuid0} VALUES
  (1, 'east', 10),
  (2, 'west', 20);

-- query 2
-- @skip_result_check=true
SET CATALOG ice_ivm_dep_${uuid0};
USE dep_${uuid0};

-- query 3
-- @skip_result_check=true
CREATE MATERIALIZED VIEW mv_orders_${uuid0}
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES('storage_engine' = 'iceberg')
AS SELECT id, region, amount FROM orders_${uuid0};

-- query 4
-- @skip_result_check=true
REFRESH MATERIALIZED VIEW mv_orders_${uuid0};

-- query 5
-- @skip_result_check=true
CREATE MATERIALIZED VIEW mv_region_${uuid0}
DISTRIBUTED BY HASH(region) BUCKETS 1
PROPERTIES('storage_engine' = 'iceberg')
AS SELECT region, SUM(amount) AS total_amount, COUNT(*) AS row_count
FROM mv_orders_${uuid0}
GROUP BY region;

-- query 6
-- SHOW MATERIALIZED VIEWS should list both MVs and surface the upstream
-- dependency of mv_region on mv_orders in the Dependencies column.
-- The `mv:` prefix only appears in the Dependencies column (added by
-- MvDependencyObjectRef::display_name for MV-on-MV edges); MV names in the
-- Name column are bare. Asserting the fully-qualified `mv:` reference
-- therefore locks the Dependencies column content, not just MV name presence.
-- @result_contains=mv_orders_${uuid0}
-- @result_contains=mv_region_${uuid0}
-- @result_contains=mv:ice_ivm_dep_${uuid0}.dep_${uuid0}.mv_orders_${uuid0}
SHOW MATERIALIZED VIEWS;

-- query 7
-- @skip_result_check=true
REFRESH MATERIALIZED VIEW mv_region_${uuid0};

-- query 8
-- After the initial cascaded refresh, mv_region reflects the 2 base rows.
SELECT region, total_amount, row_count FROM mv_region_${uuid0} ORDER BY region;

-- query 9
-- @skip_result_check=true
INSERT INTO ice_ivm_dep_${uuid0}.dep_${uuid0}.orders_${uuid0} VALUES
  (3, 'east', 7);

-- query 10
-- REFRESH on the downstream MV should cascade and refresh the upstream first,
-- so the downstream sees the new base row without an explicit upstream refresh.
-- @skip_result_check=true
REFRESH MATERIALIZED VIEW mv_region_${uuid0};

-- query 11
-- east should now reflect the appended (3, 'east', 7) row.
SELECT region, total_amount, row_count FROM mv_region_${uuid0} ORDER BY region;

-- query 12
-- Dropping the upstream MV while a downstream MV still depends on it must
-- be rejected by the dependency guard.
-- @expect_error=has downstream materialized views
DROP MATERIALIZED VIEW mv_orders_${uuid0};

-- query 13
-- Drop downstream first, then upstream becomes droppable.
-- @skip_result_check=true
DROP MATERIALIZED VIEW mv_region_${uuid0};
DROP MATERIALIZED VIEW mv_orders_${uuid0};
DROP TABLE ice_ivm_dep_${uuid0}.dep_${uuid0}.orders_${uuid0} FORCE;
DROP DATABASE ice_ivm_dep_${uuid0}.dep_${uuid0};
DROP CATALOG ice_ivm_dep_${uuid0};
