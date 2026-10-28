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
-- @tags=mv,iceberg,ivm,row_lineage,join,negative
-- Test Point: Unsupported join IMV shapes fail at CREATE time.
-- Method: Try outer join, non-equi join, projection cross join, and three-table join.
-- Scope: Iceberg-backed join IMV shape validation.

-- query 1
-- @skip_result_check=true
CREATE EXTERNAL CATALOG ice_ivm_join_reject_${uuid0}
PROPERTIES (
  "type" = "iceberg",
  "iceberg.catalog.type" = "rest",
  "uri" = "${iceberg_rest_uri}",
  "warehouse" = "${iceberg_rest_warehouse}",
  "aws.s3.endpoint" = "${oss_endpoint}",
  "credential.object-store-metadata.consumer-role" = "frontend",
  "credential.object-store-metadata.mode" = "${iceberg_object_store_credential_mode}",
  "credential.object-store-metadata.name" = "${iceberg_object_store_metadata_credential_name}",
  "credential.object-store-metadata.generation" = "${iceberg_object_store_credential_generation}",
  "credential.object-store-data.consumer-role" = "backend",
  "credential.object-store-data.mode" = "${iceberg_object_store_credential_mode}",
  "credential.object-store-data.name" = "${iceberg_object_store_data_credential_name}",
  "credential.object-store-data.generation" = "${iceberg_object_store_credential_generation}",
  "aws.s3.region" = "${oss_region}",
  "aws.s3.enable_path_style_access" = "${oss_path_style}"
);
CREATE DATABASE ice_ivm_join_reject_${uuid0}.ns_${uuid0};
CREATE TABLE ice_ivm_join_reject_${uuid0}.ns_${uuid0}.reject_left_${uuid0} (
  id BIGINT NOT NULL,
  rid BIGINT
)
TBLPROPERTIES ("format-version" = "3",
  "write.row-lineage" = "true");
CREATE TABLE ice_ivm_join_reject_${uuid0}.ns_${uuid0}.reject_right_${uuid0} (
  id BIGINT NOT NULL,
  rid BIGINT
)
TBLPROPERTIES ("format-version" = "3",
  "write.row-lineage" = "true");
CREATE TABLE ice_ivm_join_reject_${uuid0}.ns_${uuid0}.reject_extra_${uuid0} (
  id BIGINT NOT NULL,
  rid BIGINT
)
TBLPROPERTIES ("format-version" = "3",
  "write.row-lineage" = "true");
SET CATALOG ice_ivm_join_reject_${uuid0};
USE ns_${uuid0};

-- query 2
-- @expect_error=Iceberg IMV refresh contract supports only inner/cross join shapes
CREATE MATERIALIZED VIEW reject_outer_${uuid0}
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES ('storage_engine' = 'iceberg')
AS SELECT l.id
FROM ice_ivm_join_reject_${uuid0}.ns_${uuid0}.reject_left_${uuid0} AS l
LEFT JOIN ice_ivm_join_reject_${uuid0}.ns_${uuid0}.reject_right_${uuid0} AS r ON l.rid = r.rid;

-- query 3
-- @expect_error=Iceberg IMV refresh contract supports only AND-combined equi-join predicates
CREATE MATERIALIZED VIEW reject_nonequi_${uuid0}
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES ('storage_engine' = 'iceberg')
AS SELECT l.id
FROM ice_ivm_join_reject_${uuid0}.ns_${uuid0}.reject_left_${uuid0} AS l
JOIN ice_ivm_join_reject_${uuid0}.ns_${uuid0}.reject_right_${uuid0} AS r ON l.rid > r.rid;

-- query 4
-- @expect_error=requires at least one equi-join predicate
CREATE MATERIALIZED VIEW reject_cross_projection_${uuid0}
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES ('storage_engine' = 'iceberg')
AS SELECT l.id
FROM ice_ivm_join_reject_${uuid0}.ns_${uuid0}.reject_left_${uuid0} AS l
CROSS JOIN ice_ivm_join_reject_${uuid0}.ns_${uuid0}.reject_right_${uuid0} AS r;

-- query 5
-- @expect_error=requires 2 Iceberg base relation occurrences, got 3
CREATE MATERIALIZED VIEW reject_three_${uuid0}
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES ('storage_engine' = 'iceberg')
AS SELECT l.id
FROM ice_ivm_join_reject_${uuid0}.ns_${uuid0}.reject_left_${uuid0} AS l
JOIN ice_ivm_join_reject_${uuid0}.ns_${uuid0}.reject_right_${uuid0} AS r ON l.rid = r.rid
JOIN ice_ivm_join_reject_${uuid0}.ns_${uuid0}.reject_extra_${uuid0} AS x ON x.rid = r.rid;

-- query 6
-- @cleanup=true
-- @skip_result_check=true
DROP MATERIALIZED VIEW IF EXISTS reject_outer_${uuid0};
DROP MATERIALIZED VIEW IF EXISTS reject_nonequi_${uuid0};
DROP MATERIALIZED VIEW IF EXISTS reject_cross_projection_${uuid0};
DROP MATERIALIZED VIEW IF EXISTS reject_three_${uuid0};
DROP TABLE ice_ivm_join_reject_${uuid0}.ns_${uuid0}.reject_left_${uuid0} FORCE;
DROP TABLE ice_ivm_join_reject_${uuid0}.ns_${uuid0}.reject_right_${uuid0} FORCE;
DROP TABLE ice_ivm_join_reject_${uuid0}.ns_${uuid0}.reject_extra_${uuid0} FORCE;
DROP DATABASE ice_ivm_join_reject_${uuid0}.ns_${uuid0};
DROP CATALOG ice_ivm_join_reject_${uuid0};
