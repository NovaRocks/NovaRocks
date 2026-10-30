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
-- @expect_error_tier=drift
-- Data-producing writes reject Variant even when the table has no rows.
-- query 1
-- @skip_result_check=true
CREATE EXTERNAL CATALOG `iru9_variant_mutations_${uuid0}`
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
DROP TABLE IF EXISTS ${case_db}.s_v3_variant_mut FORCE;
CREATE TABLE ${case_db}.s_v3_variant_mut (id BIGINT, new_id BIGINT)
TBLPROPERTIES ("format-version"="3");
INSERT INTO ${case_db}.s_v3_variant_mut VALUES (3,30);

-- query 2
-- @result_contains=IRU9_EMPTY_BIGINT_OK
shell: set -eu
receipt_parent="${XDG_STATE_HOME:-$HOME/.local/state}/novarocks/experiments/iceberg-variant"
mkdir -p "$receipt_parent"
receipt_dir="$(mktemp -d "$receipt_parent/$(date -u +%Y%m%dT%H%M%SZ)-XXXXXX")"
"${NOVAROCKS_WORKSPACE_ROOT:-.}/tests/sql/fixtures/iceberg-variant/run.sh" --mode empty-bigint --namespace iru9_${uuid0} --table t_v3_variant_mut --output "$receipt_dir" >"$receipt_dir/run.log"
cat "$receipt_dir/run.log" >&2
grep -E '^IRU9_(EMPTY_BIGINT_OK|FIXTURE_COMPLETE)' "$receipt_dir/run.log"

-- query 3
-- @expect_error=Iceberg Variant writes are not supported
UPDATE iru9_variant_mutations_${uuid0}.iru9_${uuid0}.t_v3_variant_mut SET id = 10 WHERE id = 1;

-- query 4
-- @expect_error=Iceberg Variant writes are not supported
MERGE INTO iru9_variant_mutations_${uuid0}.iru9_${uuid0}.t_v3_variant_mut t USING ${case_db}.s_v3_variant_mut s
ON t.id = s.id WHEN MATCHED THEN UPDATE SET id = s.new_id;

-- query 5
-- @expect_error=Iceberg Variant writes are not supported
MERGE INTO iru9_variant_mutations_${uuid0}.iru9_${uuid0}.t_v3_variant_mut t USING ${case_db}.s_v3_variant_mut s
ON t.id = s.id WHEN NOT MATCHED THEN INSERT (id,v) VALUES (s.id,NULL);

-- query 6
-- @expect_error=Iceberg Variant writes are not supported
INSERT OVERWRITE iru9_variant_mutations_${uuid0}.iru9_${uuid0}.t_v3_variant_mut VALUES (20,parse_json('{"a":20}'));

-- query 7
-- @skip_result_check=true
DELETE FROM iru9_variant_mutations_${uuid0}.iru9_${uuid0}.t_v3_variant_mut WHERE id = 2;
TRUNCATE TABLE iru9_variant_mutations_${uuid0}.iru9_${uuid0}.t_v3_variant_mut;

-- query 8
SELECT COUNT(*) AS remaining_rows FROM iru9_variant_mutations_${uuid0}.iru9_${uuid0}.t_v3_variant_mut;

-- query 9
-- @expect_error=does not support writing Iceberg Variant values
ALTER TABLE iru9_variant_mutations_${uuid0}.iru9_${uuid0}.t_v3_variant_mut ADD EQUALITY DELETE (v) VALUES (NULL);

-- query 10
-- @expect_error=Iceberg Variant writes are not supported
CREATE TABLE ${case_db}.t_v3_variant_partition_bad (id BIGINT,v VARIANT)
PARTITION BY identity(v) TBLPROPERTIES ("format-version"="3");

-- query 11
-- @expect_error=variant columns cannot appear in the partition spec
ALTER TABLE iru9_variant_mutations_${uuid0}.iru9_${uuid0}.t_v3_variant_mut ADD PARTITION COLUMN v;

-- query 12
-- @result_contains=IRU9_EMPTY_REGION_OK
shell: set -eu
receipt_parent="${XDG_STATE_HOME:-$HOME/.local/state}/novarocks/experiments/iceberg-variant"
mkdir -p "$receipt_parent"
receipt_dir="$(mktemp -d "$receipt_parent/$(date -u +%Y%m%dT%H%M%SZ)-XXXXXX")"
"${NOVAROCKS_WORKSPACE_ROOT:-.}/tests/sql/fixtures/iceberg-variant/run.sh" --mode empty-region --namespace iru9_${uuid0} --table t_v3_variant_part_mut --output "$receipt_dir" >"$receipt_dir/run.log"
cat "$receipt_dir/run.log" >&2
grep -E '^IRU9_(EMPTY_REGION_OK|FIXTURE_COMPLETE)' "$receipt_dir/run.log"

-- query 13
-- @expect_error=Iceberg Variant writes are not supported
INSERT OVERWRITE PARTITIONS iru9_variant_mutations_${uuid0}.iru9_${uuid0}.t_v3_variant_part_mut
VALUES (4,'us',parse_json('{"a":4}'));

-- query 14
-- @skip_result_check=true
DROP TABLE iru9_variant_mutations_${uuid0}.iru9_${uuid0}.t_v3_variant_mut FORCE;
DROP TABLE ${case_db}.s_v3_variant_mut FORCE;
DROP TABLE iru9_variant_mutations_${uuid0}.iru9_${uuid0}.t_v3_variant_part_mut FORCE;
DROP CATALOG iru9_variant_mutations_${uuid0};
