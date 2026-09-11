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
-- @tags=mv,iceberg,ivm,w6,single_table,guard
-- Test Objective:
-- 1. Validate an Iceberg-backed single-table MV is queryable after refresh.
-- 2. Validate direct table mutations and Iceberg maintenance commands are rejected for MV targets.
-- 3. Validate namespace/catalog drops are blocked while an MV exists and cleanup succeeds after DROP MATERIALIZED VIEW.

-- query 1
-- @skip_result_check=true
CREATE EXTERNAL CATALOG ice_w6_${uuid0}
PROPERTIES (
  "type" = "iceberg",
  "iceberg.catalog.type" = "hadoop",
  "iceberg.catalog.warehouse" = "${iceberg_catalog_warehouse}/ice_w6_${uuid0}",
  "aws.s3.endpoint" = "${oss_endpoint}",
  "credential.object-store-metadata.consumer-role" = "frontend",
  "credential.object-store-metadata.mode" = "static",
  "credential.object-store-metadata.name" = "${iceberg_object_store_credential_name}",
  "credential.object-store-metadata.generation" = "${iceberg_object_store_credential_generation}",
  "credential.object-store-data.consumer-role" = "backend",
  "credential.object-store-data.mode" = "static",
  "credential.object-store-data.name" = "${iceberg_object_store_credential_name}",
  "credential.object-store-data.generation" = "${iceberg_object_store_credential_generation}",
  "aws.s3.enable_path_style_access" = "true"
);
CREATE DATABASE ice_w6_${uuid0}.ns_${uuid0};
CREATE TABLE ice_w6_${uuid0}.ns_${uuid0}.base_orders (
  order_id INT,
  amount BIGINT
)
TBLPROPERTIES ("format-version" = "3",
  "write.row-lineage" = "true");
INSERT INTO ice_w6_${uuid0}.ns_${uuid0}.base_orders VALUES
  (1, 10),
  (2, 20);

SET CATALOG ice_w6_${uuid0};
USE ns_${uuid0};

CREATE MATERIALIZED VIEW target_mv
DISTRIBUTED BY HASH(order_id) BUCKETS 2
PROPERTIES ('storage_engine' = 'iceberg')
AS SELECT order_id, amount FROM base_orders;

REFRESH MATERIALIZED VIEW target_mv;

-- query 2
SELECT order_id, amount FROM target_mv ORDER BY order_id;

-- query 3
-- @expect_error=materialized view
INSERT INTO target_mv VALUES (3, 30);

-- query 4
-- @expect_error=materialized view
UPDATE target_mv SET amount = 100 WHERE order_id = 1;

-- query 5
-- @expect_error=materialized view
DELETE FROM target_mv WHERE order_id = 1;

-- query 6
-- @expect_error=materialized view
MERGE INTO target_mv AS t
USING (SELECT 1 AS order_id, 100 AS amount) AS s
ON t.order_id = s.order_id
WHEN MATCHED THEN UPDATE SET amount = s.amount;

-- query 7
-- @expect_error=materialized view
TRUNCATE TABLE target_mv;

-- query 8
-- @expect_error=materialized view
ALTER TABLE target_mv ADD COLUMN extra INT;

-- query 9
-- @expect_error=materialized view
ALTER TABLE target_mv SET TBLPROPERTIES ('comment' = 'blocked');

-- query 10
-- @expect_error=materialized view
ALTER TABLE target_mv ADD FILES FROM 's3://blocked/path';

-- query 11
-- @expect_error=materialized view
ALTER TABLE target_mv ADD EQUALITY DELETE (order_id) VALUES (1);

-- query 12
-- @expect_error=materialized view
CALL ice_w6_${uuid0}.system.rewrite_manifests(table => 'ns_${uuid0}.target_mv');

-- query 13
-- @expect_error=materialized view
DROP TABLE target_mv FORCE;

-- query 14
-- @expect_error=materialized view
ALTER TABLE ice_w6_${uuid0}.ns_${uuid0}.target_mv CREATE BRANCH blocked_branch;

-- query 15
-- @expect_error=materialized view
DROP DATABASE ice_w6_${uuid0}.ns_${uuid0};

-- query 16
-- @expect_error=materialized view
DROP CATALOG ice_w6_${uuid0};

-- query 17
SELECT order_id, amount FROM target_mv ORDER BY order_id;

-- query 18
-- @skip_result_check=true
DROP MATERIALIZED VIEW target_mv;
DROP TABLE ice_w6_${uuid0}.ns_${uuid0}.base_orders FORCE;
DROP DATABASE ice_w6_${uuid0}.ns_${uuid0};
DROP CATALOG ice_w6_${uuid0};
