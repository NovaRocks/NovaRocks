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
-- @tags=mv,iceberg,ivm,a11,aggregate,nullability,error
-- Test Point: An aggregate MV rebuilds after a referenced input becomes nullable.
-- Method: Relax a required filter input through Spark and verify the rebuilt count excludes NULL.
-- Scope: Iceberg target MV, single-base aggregate, schema contract.

-- query 1
-- @skip_result_check=true
CREATE CATALOG ice_ivm_agg_a11_null_${uuid0} USING iceberg WITH ("iceberg.catalog.type" = 'rest', "uri" = '${iceberg_rest_uri}', "warehouse" = '${iceberg_rest_warehouse}', "credential.object-store-metadata.consumer-role" = 'frontend', "credential.object-store-metadata.mode" = '${iceberg_object_store_credential_mode}', "credential.object-store-metadata.name" = '${iceberg_object_store_metadata_credential_name}', "credential.object-store-metadata.generation" = '${iceberg_object_store_credential_generation}', "credential.object-store-data.consumer-role" = 'backend', "credential.object-store-data.mode" = '${iceberg_object_store_credential_mode}', "credential.object-store-data.name" = '${iceberg_object_store_data_credential_name}', "credential.object-store-data.generation" = '${iceberg_object_store_credential_generation}', "aws.s3.endpoint" = '${oss_endpoint}', "aws.s3.region" = '${oss_region}', "aws.s3.enable_path_style_access" = '${oss_path_style}');
CREATE SCHEMA ice_ivm_agg_a11_null_${uuid0}.ns_${uuid0};
CREATE TABLE ice_ivm_agg_a11_null_${uuid0}.ns_${uuid0}.orders (
  region VARCHAR NOT NULL,
  amount BIGINT NOT NULL
) WITH (format_version = 3);
INSERT INTO ice_ivm_agg_a11_null_${uuid0}.ns_${uuid0}.orders VALUES ROW('east', 10), ROW('west', 5);
USE ice_ivm_agg_a11_null_${uuid0}.ns_${uuid0};
SET SESSION enable_materialized_view_rewrite = false;
CREATE MATERIALIZED VIEW agg_mv_${uuid0} AS SELECT region, COUNT(*) AS c
FROM orders
WHERE amount IS NOT NULL
GROUP BY region;

-- query 2
-- @skip_result_check=true
REFRESH MATERIALIZED VIEW agg_mv_${uuid0};

-- query 3
-- @result_contains=SPARK_SQL_OK
shell: set -eu
tmp_sql="$(mktemp "${TMPDIR:-/tmp}/novarocks-a11-agg-null-XXXXXX.sql")"
trap 'rm -f "$tmp_sql"' EXIT
cat > "$tmp_sql" <<'SPARK_SQL'
ALTER TABLE ice_rest.ns_${uuid0}.orders ALTER COLUMN amount DROP NOT NULL;
SPARK_SQL
"${NOVAROCKS_WORKSPACE_ROOT:-.}/docker/iceberg-rest/spark-sql.sh" "$tmp_sql"
printf 'SPARK_SQL_OK\n'

-- query 4
-- @skip_result_check=true
INSERT INTO orders VALUES ROW('east', NULL), ROW('west', 7);
REFRESH MATERIALIZED VIEW agg_mv_${uuid0};

-- query 5
SELECT region, c FROM agg_mv_${uuid0} ORDER BY region;

-- query 6
SELECT region, COUNT(*) AS c FROM orders WHERE amount IS NOT NULL GROUP BY region ORDER BY region;

-- query 7
-- @cleanup=true
-- @skip_result_check=true
DROP MATERIALIZED VIEW agg_mv_${uuid0};
DROP TABLE ice_ivm_agg_a11_null_${uuid0}.ns_${uuid0}.orders ;
DROP SCHEMA ice_ivm_agg_a11_null_${uuid0}.ns_${uuid0};
DROP CATALOG ice_ivm_agg_a11_null_${uuid0};
