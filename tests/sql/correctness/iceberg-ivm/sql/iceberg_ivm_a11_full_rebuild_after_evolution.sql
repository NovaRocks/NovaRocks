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
-- @tags=mv,iceberg,ivm,a11,refresh_full_disabled
-- Test Objective:
-- Lock in that REFRESH MATERIALIZED VIEW ... FULL is currently disabled
-- pending redesign. The previous implementation (drop target + delete
-- definition + recreate empty target) was misleading and non-atomic;
-- until a redesign lands the engine must reject this keyword fast and
-- point operators at the manual recovery path (DROP + CREATE + REFRESH).

-- query 1
-- @skip_result_check=true
CREATE EXTERNAL CATALOG ice_ivm_a11_full_disabled_${uuid0}
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
CREATE DATABASE ice_ivm_a11_full_disabled_${uuid0}.ns_${uuid0};
CREATE TABLE ice_ivm_a11_full_disabled_${uuid0}.ns_${uuid0}.base_${uuid0} (
  id INT NOT NULL,
  amount BIGINT
)
TBLPROPERTIES ("format-version" = "3",
  "write.row-lineage" = "true");
INSERT INTO ice_ivm_a11_full_disabled_${uuid0}.ns_${uuid0}.base_${uuid0} VALUES
  (1, 100),
  (2, 50);

-- query 2
-- @skip_result_check=true
SET CATALOG ice_ivm_a11_full_disabled_${uuid0};
USE ns_${uuid0};
CREATE MATERIALIZED VIEW mv_${uuid0}
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES('storage_engine' = 'iceberg')
AS SELECT id, amount FROM base_${uuid0};

-- query 3
-- @skip_result_check=true
REFRESH MATERIALIZED VIEW mv_${uuid0};

-- query 4
-- The Iceberg-backed MV refresh path must reject FULL with the
-- "currently disabled pending redesign" error.
-- @expect_error=currently disabled pending redesign
REFRESH MATERIALIZED VIEW mv_${uuid0} FULL;

-- query 5
-- EXPLAIN REFRESH ... FULL must return the same disabled error as executing
-- FULL (W6: EXPLAIN/exec same-source, no divergent "not supported").
-- @expect_error=currently disabled pending redesign
EXPLAIN REFRESH MATERIALIZED VIEW mv_${uuid0} FULL;

-- query 6
-- @skip_result_check=true
DROP MATERIALIZED VIEW mv_${uuid0};
DROP TABLE ice_ivm_a11_full_disabled_${uuid0}.ns_${uuid0}.base_${uuid0} FORCE;
DROP DATABASE ice_ivm_a11_full_disabled_${uuid0}.ns_${uuid0};
DROP CATALOG ice_ivm_a11_full_disabled_${uuid0};
