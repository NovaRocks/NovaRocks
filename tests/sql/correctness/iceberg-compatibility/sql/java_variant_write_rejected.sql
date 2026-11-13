-- Licensed to the Apache Software Foundation (ASF) under one
-- or more contributor license agreements. See the NOTICE file
-- distributed with this work for additional information
-- regarding copyright ownership. The ASF licenses this file
-- to you under the Apache License, Version 2.0 (the
-- "License"); you may not use this file except in compliance
-- with the License. You may obtain a copy of the License at
--
-- http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing,
-- software distributed under the License is distributed on an
-- "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
-- KIND, either express or implied. See the License for the
-- specific language governing permissions and limitations
-- under the License.

-- @order_sensitive=true
-- @sequential=true
-- @expect_error_tier=drift

-- query 1
-- @result_contains=IRU9_SHREDDED_OK
shell: set -eu
receipt_parent="${XDG_STATE_HOME:-$HOME/.local/state}/novarocks/experiments/iceberg-variant"
mkdir -p "$receipt_parent"
receipt_dir="$(mktemp -d "$receipt_parent/$(date -u +%Y%m%dT%H%M%SZ)-XXXXXX")"
"${NOVAROCKS_WORKSPACE_ROOT:-.}/tests/sql/fixtures/iceberg-variant/run.sh" --mode shredded --namespace ${case_db} --table java_variant_rejected_${uuid0} --output "$receipt_dir" >"$receipt_dir/run.log"
cat "$receipt_dir/run.log" >&2
grep -E '^IRU9_(SHREDDED_OK|FIXTURE_COMPLETE)' "$receipt_dir/run.log"

-- query 2
-- @expect_error=Iceberg Variant writes are not supported
INSERT INTO iceberg_compat_${suite_uuid0}.${case_db}.java_variant_rejected_${uuid0} VALUES (9,parse_json('1'));

-- query 3
-- @expect_error=Iceberg Variant writes are not supported
UPDATE iceberg_compat_${suite_uuid0}.${case_db}.java_variant_rejected_${uuid0} SET id=99 WHERE id=1;

-- query 4
-- @expect_error=Iceberg Variant writes are not supported
MERGE INTO iceberg_compat_${suite_uuid0}.${case_db}.java_variant_rejected_${uuid0} t USING (SELECT 1 AS id) s ON t.id=s.id WHEN NOT MATCHED THEN INSERT (id,v) VALUES (s.id,NULL);

-- query 5
-- OPTIMIZE reports the provider schema refusal within the statement.
-- @expect_trino_error=NOT_SUPPORTED,USER_ERROR
ALTER TABLE iceberg_compat_${suite_uuid0}.${case_db}.java_variant_rejected_${uuid0} OPTIMIZE;

-- query 6
-- @expect_error=Iceberg Variant writes are not supported
ALTER TABLE iceberg_compat_${suite_uuid0}.${case_db}.java_variant_rejected_${uuid0} ADD FILES FROM 's3://warehouse/iru9-does-not-exist-${uuid0}';

-- query 7
-- The ordinary id column has no Variant semantics. REST staging admission
-- must still reject this property before probing or staging the target.
-- @expect_error=write.parquet.variant-shredding.
CREATE TABLE iceberg_compat_${suite_uuid0}.${case_db}.java_shredding_ctas_rejected_${uuid0}
TBLPROPERTIES ('write.parquet.variant-shredding.payload' = 'a:int') AS SELECT 1 AS id;

-- query 8
-- REST staged creation must refuse the actual logical Variant result type
-- obtained from the external Java table before staging a new target.
-- @expect_error=Iceberg Variant writes are not supported
CREATE TABLE iceberg_compat_${suite_uuid0}.${case_db}.java_variant_ctas_rejected_${uuid0}
TBLPROPERTIES ('format-version' = '3') AS
SELECT id,v FROM iceberg_compat_${suite_uuid0}.${case_db}.java_variant_rejected_${uuid0};

-- query 9
-- @result_contains=IRU9_UNCHANGED_OK
shell: set -eu
receipt_parent="${XDG_STATE_HOME:-$HOME/.local/state}/novarocks/experiments/iceberg-variant"
mkdir -p "$receipt_parent"
receipt_dir="$(mktemp -d "$receipt_parent/$(date -u +%Y%m%dT%H%M%SZ)-XXXXXX")"
"${NOVAROCKS_WORKSPACE_ROOT:-.}/tests/sql/fixtures/iceberg-variant/run.sh" --mode unchanged --namespace ${case_db} --table java_variant_rejected_${uuid0} --output "$receipt_dir"

-- query 10
-- LIKE keeps the external logical declaration and refuses its new target.
-- @expect_error=Iceberg Variant writes are not supported yet: table iceberg_compat_${suite_uuid0}.${case_db}.java_variant_like_rejected_${uuid0}, column v
CREATE TABLE iceberg_compat_${suite_uuid0}.${case_db}.java_variant_like_rejected_${uuid0} LIKE iceberg_compat_${suite_uuid0}.${case_db}.java_variant_rejected_${uuid0};

-- query 11
-- @expect_error=Iceberg Variant writes are not supported yet: table iceberg_compat_${suite_uuid0}.${case_db}.java_variant_rejected_${uuid0}, column extra_variant
ALTER TABLE iceberg_compat_${suite_uuid0}.${case_db}.java_variant_rejected_${uuid0} ADD COLUMN extra_variant VARIANT;

-- query 12
-- @expect_error=Iceberg Variant writes are not supported yet: table iceberg_compat_${suite_uuid0}.${case_db}.java_variant_rejected_${uuid0}, column extra_struct.v
ALTER TABLE iceberg_compat_${suite_uuid0}.${case_db}.java_variant_rejected_${uuid0} ADD COLUMN extra_struct STRUCT<v VARIANT>;

-- query 13
-- @expect_error=unsupported Iceberg type evolution from int to variant
ALTER TABLE iceberg_compat_${suite_uuid0}.${case_db}.java_variant_rejected_${uuid0} MODIFY COLUMN id VARIANT;

-- query 14
-- Default-format CTAS refuses the same logical Variant as explicit-v3 query9.
-- @expect_error=Iceberg Variant writes are not supported yet: table ${case_db}.java_variant_default_ctas_rejected_${uuid0}, column v
CREATE TABLE iceberg_compat_${suite_uuid0}.${case_db}.java_variant_default_ctas_rejected_${uuid0} AS SELECT id,v FROM iceberg_compat_${suite_uuid0}.${case_db}.java_variant_rejected_${uuid0};

-- query 15
-- Existing Variant does not block ordinary metadata changes or its type no-op.
-- @skip_result_check=true
ALTER TABLE iceberg_compat_${suite_uuid0}.${case_db}.java_variant_rejected_${uuid0} ADD COLUMN note_text STRING;
ALTER TABLE iceberg_compat_${suite_uuid0}.${case_db}.java_variant_rejected_${uuid0} RENAME COLUMN note_text TO note_renamed;
ALTER TABLE iceberg_compat_${suite_uuid0}.${case_db}.java_variant_rejected_${uuid0} ALTER COLUMN note_renamed COMMENT 'ordinary metadata remains supported';
ALTER TABLE iceberg_compat_${suite_uuid0}.${case_db}.java_variant_rejected_${uuid0} ALTER COLUMN v COMMENT 'external Variant remains readable';
ALTER TABLE iceberg_compat_${suite_uuid0}.${case_db}.java_variant_rejected_${uuid0} MODIFY COLUMN v VARIANT;

-- query 16
SELECT id,note_renamed FROM iceberg_compat_${suite_uuid0}.${case_db}.java_variant_rejected_${uuid0} ORDER BY id;

-- query 17
-- @skip_result_check=true
ALTER TABLE iceberg_compat_${suite_uuid0}.${case_db}.java_variant_rejected_${uuid0} DROP COLUMN note_renamed;
ALTER TABLE iceberg_compat_${suite_uuid0}.${case_db}.java_variant_rejected_${uuid0} SET ('iru9.metadata-control' = 'allowed');
ALTER TABLE iceberg_compat_${suite_uuid0}.${case_db}.java_variant_rejected_${uuid0} UNSET TBLPROPERTIES ('iru9.metadata-control');

-- query 18
-- @result_contains=IRU9_UNCHANGED_OK
shell: set -eu
receipt_parent="${XDG_STATE_HOME:-$HOME/.local/state}/novarocks/experiments/iceberg-variant"
mkdir -p "$receipt_parent"
receipt_dir="$(mktemp -d "$receipt_parent/$(date -u +%Y%m%dT%H%M%SZ)-XXXXXX")"
"${NOVAROCKS_WORKSPACE_ROOT:-.}/tests/sql/fixtures/iceberg-variant/run.sh" --mode unchanged --namespace ${case_db} --table java_variant_rejected_${uuid0} --output "$receipt_dir"

-- query 19
-- @skip_result_check=true
DROP TABLE iceberg_compat_${suite_uuid0}.${case_db}.java_variant_rejected_${uuid0} FORCE;
