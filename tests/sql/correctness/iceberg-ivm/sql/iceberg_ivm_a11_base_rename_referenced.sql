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
-- @tags=mv,iceberg,ivm,a11,base_rename,projection,filter
-- An invalid original definition is reported as stale on refresh.

-- query 1
-- @skip_result_check=true
CREATE CATALOG ice_ivm_a11_rename_${uuid0} USING iceberg WITH ("iceberg.catalog.type" = 'rest', "uri" = '${iceberg_rest_uri}', "warehouse" = '${iceberg_rest_warehouse}', "credential.object-store-metadata.consumer-role" = 'frontend', "credential.object-store-metadata.mode" = '${iceberg_object_store_credential_mode}', "credential.object-store-metadata.name" = '${iceberg_object_store_metadata_credential_name}', "credential.object-store-metadata.generation" = '${iceberg_object_store_credential_generation}', "credential.object-store-data.consumer-role" = 'backend', "credential.object-store-data.mode" = '${iceberg_object_store_credential_mode}', "credential.object-store-data.name" = '${iceberg_object_store_data_credential_name}', "credential.object-store-data.generation" = '${iceberg_object_store_credential_generation}', "aws.s3.endpoint" = '${oss_endpoint}', "aws.s3.region" = '${oss_region}', "aws.s3.enable_path_style_access" = '${oss_path_style}');
CREATE SCHEMA ice_ivm_a11_rename_${uuid0}.ns_a11_rename_${uuid0};
CREATE TABLE ice_ivm_a11_rename_${uuid0}.ns_a11_rename_${uuid0}.base_${uuid0} (
  id INT NOT NULL,
  region VARCHAR,
  amount BIGINT
) WITH (format_version = 3);
INSERT INTO ice_ivm_a11_rename_${uuid0}.ns_a11_rename_${uuid0}.base_${uuid0} VALUES
  ROW(1, 'US', 100),
  ROW(2, 'EU', 50),
  ROW(3, 'US', 200);

-- query 2
-- @skip_result_check=true
USE ice_ivm_a11_rename_${uuid0}.ns_a11_rename_${uuid0};
CREATE MATERIALIZED VIEW mv_${uuid0} AS SELECT id, region, amount FROM base_${uuid0} WHERE region = 'US';

-- query 3
-- @skip_result_check=true
REFRESH MATERIALIZED VIEW mv_${uuid0};

-- query 4
-- Initial MV state: US rows only.
SELECT id, region, amount FROM mv_${uuid0} ORDER BY id;

-- query 5
-- Spark: rename base column `region` -> `area` (same field id).
-- The INSERT is done by NovaRocks in query 6 so that first_row_id is set
-- correctly for row-lineage tracking (Spark 1.8.x does not write first_row_id).
-- @result_contains=SPARK_SQL_OK
shell: set -eu
tmp_sql="$(mktemp "${TMPDIR:-/tmp}/novarocks-a11-rename-XXXXXX.sql")"
trap 'rm -f "$tmp_sql"' EXIT
cat > "$tmp_sql" <<'SPARK_SQL'
ALTER TABLE ice_rest.ns_a11_rename_${uuid0}.base_${uuid0} RENAME COLUMN region TO area;
SPARK_SQL
"${NOVAROCKS_WORKSPACE_ROOT:-.}/docker/iceberg-rest/spark-sql.sh" "$tmp_sql"
printf 'SPARK_SQL_OK\n'

-- query 6
-- NovaRocks INSERT using positional syntax (column order: id, area, amount after rename).
-- Positional insert bypasses the session-cached column name check and lets the
-- sink reload the schema from the REST catalog.
-- @skip_result_check=true
INSERT INTO ice_ivm_a11_rename_${uuid0}.ns_a11_rename_${uuid0}.base_${uuid0} VALUES ROW(4, 'US', 300);

-- query 7
-- @expect_trino_error=VIEW_IS_STALE,USER_ERROR
REFRESH MATERIALIZED VIEW mv_${uuid0};

-- query 8
-- The failed refresh leaves the previous published rows available.
SELECT id, region, amount FROM mv_${uuid0} ORDER BY id;

-- query 9
-- @cleanup=true
-- @skip_result_check=true
DROP MATERIALIZED VIEW mv_${uuid0};
DROP TABLE ice_ivm_a11_rename_${uuid0}.ns_a11_rename_${uuid0}.base_${uuid0} ;
DROP SCHEMA ice_ivm_a11_rename_${uuid0}.ns_a11_rename_${uuid0};
DROP CATALOG ice_ivm_a11_rename_${uuid0};
