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
-- @tags=mv,iceberg,ivm,a11,base_uuid,identity,error
-- Test Objective:
-- Validate that replacing the base table (DROP + CREATE with same name) triggers
-- BaseTableIdentityChanged error, because the new table has a different UUID.
-- The A11 identity guard must fire before any field-level checks.
--
-- Setup: create table, create MV, refresh. Then Spark drops and re-creates the
-- base table at the same three-part name. NovaRocks refresh must fail.

-- query 1
-- @skip_result_check=true
CREATE EXTERNAL CATALOG ice_ivm_a11_uuid_${uuid0}
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
CREATE DATABASE ice_ivm_a11_uuid_${uuid0}.ns_${uuid0};
CREATE TABLE ice_ivm_a11_uuid_${uuid0}.ns_${uuid0}.base_${uuid0} (
  id INT NOT NULL,
  amount BIGINT
)
TBLPROPERTIES ("format-version" = "3",
  "write.row-lineage" = "true");
INSERT INTO ice_ivm_a11_uuid_${uuid0}.ns_${uuid0}.base_${uuid0} VALUES
  (1, 100),
  (2, 50);

-- query 2
-- @skip_result_check=true
SET CATALOG ice_ivm_a11_uuid_${uuid0};
USE ns_${uuid0};
CREATE MATERIALIZED VIEW mv_${uuid0}
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES('storage_engine' = 'iceberg')
AS SELECT id, amount FROM base_${uuid0};

-- query 3
-- @skip_result_check=true
REFRESH MATERIALIZED VIEW mv_${uuid0};

-- query 4
-- Initial MV state.
SELECT id, amount FROM mv_${uuid0} ORDER BY id;

-- query 5
-- Spark: drop and re-create the base table at the same name (new UUID).
-- @result_contains=SPARK_SQL_OK
shell: set -eu
tmp_sql="$(mktemp "${TMPDIR:-/tmp}/novarocks-a11-uuid-XXXXXX.sql")"
trap 'rm -f "$tmp_sql"' EXIT
cat > "$tmp_sql" <<'SPARK_SQL'
DROP TABLE ice_rest.ns_${uuid0}.base_${uuid0};
CREATE TABLE ice_rest.ns_${uuid0}.base_${uuid0} (
  id INT NOT NULL,
  amount BIGINT
)
TBLPROPERTIES ('format-version' = '3',
  'write.row-lineage' = 'true');
INSERT INTO ice_rest.ns_${uuid0}.base_${uuid0} VALUES (10, 999);
SPARK_SQL
"${NOVAROCKS_WORKSPACE_ROOT:-.}/docker/iceberg-rest/spark-sql.sh" "$tmp_sql"
printf 'SPARK_SQL_OK\n'

-- query 6
-- Refresh must fail: BaseTableIdentityChanged (UUID mismatch).
-- @expect_error=base table identity changed
REFRESH MATERIALIZED VIEW mv_${uuid0};

-- query 7
-- @skip_result_check=true
DROP MATERIALIZED VIEW mv_${uuid0};
DROP TABLE ice_ivm_a11_uuid_${uuid0}.ns_${uuid0}.base_${uuid0} FORCE;
DROP DATABASE ice_ivm_a11_uuid_${uuid0}.ns_${uuid0};
DROP CATALOG ice_ivm_a11_uuid_${uuid0};
