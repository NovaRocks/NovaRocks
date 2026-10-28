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

-- @order_sensitive=true
-- Variant writes fail before staging; scalar expressions retain their value domains.
-- query 1
-- @skip_result_check=true
CREATE EXTERNAL CATALOG `iru9_variant_insert_${uuid0}`
PROPERTIES (
    "type"="iceberg",
    "iceberg.catalog.type"="rest",
    "uri"="${iceberg_rest_uri}",
    "warehouse"="${iceberg_rest_warehouse}",
    "credential.object-store-metadata.consumer-role" = "frontend",
    "credential.object-store-metadata.mode" = "${iceberg_object_store_credential_mode}",
    "credential.object-store-metadata.name" = "${iceberg_object_store_metadata_credential_name}",
    "credential.object-store-metadata.generation" = "${iceberg_object_store_credential_generation}",
    "credential.object-store-data.consumer-role" = "backend",
    "credential.object-store-data.mode" = "${iceberg_object_store_credential_mode}",
    "credential.object-store-data.name" = "${iceberg_object_store_data_credential_name}",
    "credential.object-store-data.generation" = "${iceberg_object_store_credential_generation}",
    "aws.s3.endpoint"="${oss_endpoint}",
    "aws.s3.region"="${oss_region}",
    "aws.s3.enable_path_style_access"="${oss_path_style}"
);

-- query 2
-- @result_contains=IRU9_EMPTY_INT_OK
shell: set -eu
receipt_parent="${XDG_STATE_HOME:-$HOME/.local/state}/novarocks/experiments/iceberg-variant"
mkdir -p "$receipt_parent"
receipt_dir="$(mktemp -d "$receipt_parent/$(date -u +%Y%m%dT%H%M%SZ)-XXXXXX")"
"${NOVAROCKS_WORKSPACE_ROOT:-.}/tests/sql/fixtures/iceberg-variant/run.sh" --mode empty-int --namespace iru9_${uuid0} --table t_v3_variant --output "$receipt_dir" >"$receipt_dir/run.log"
cat "$receipt_dir/run.log" >&2
grep -E '^IRU9_(EMPTY_INT_OK|FIXTURE_COMPLETE)' "$receipt_dir/run.log"

-- query 3
-- @expect_error_tier=drift
-- @expect_error=Iceberg Variant writes are not supported
INSERT INTO iru9_variant_insert_${uuid0}.iru9_${uuid0}.t_v3_variant VALUES
  (1, parse_json('{"a":1,"b":"x"}')),
  (2, parse_json('[10,20,30]')),
  (3, parse_json('null')),
  (4, NULL);

-- query 4
SELECT variant_typeof(parse_json('{"a":1}')) AS object_kind,
       variant_typeof(parse_json('[10,20,30]')) AS array_kind,
       variant_typeof(parse_json('null')) AS variant_null_kind,
       variant_typeof(CAST(NULL AS VARIANT)) AS sql_null_kind,
       parse_json('null') IS NULL AS variant_is_sql_null,
       CAST(NULL AS VARIANT) IS NULL AS sql_is_null;

-- query 5
SELECT get_json_string(parse_json('{"a":1,"b":"x"}'), '$.b') AS b;

-- query 6
SELECT COUNT(*) AS remaining_rows FROM iru9_variant_insert_${uuid0}.iru9_${uuid0}.t_v3_variant;

-- query 7
-- @skip_result_check=true
DROP TABLE iru9_variant_insert_${uuid0}.iru9_${uuid0}.t_v3_variant FORCE;
DROP CATALOG iru9_variant_insert_${uuid0};
