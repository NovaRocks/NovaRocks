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
"${NOVAROCKS_WORKSPACE_ROOT:-.}/tests/sql/fixtures/iceberg-variant/run.sh" --mode shredded --namespace ${case_db} --table java_variant_dv_${uuid0} --output "$receipt_dir"

-- query 2
-- @skip_result_check=true
DELETE FROM iceberg_compat_${suite_uuid0}.${case_db}.java_variant_dv_${uuid0} WHERE id=1;

-- query 3
SELECT id FROM iceberg_compat_${suite_uuid0}.${case_db}.java_variant_dv_${uuid0} ORDER BY id;

-- query 4
-- The ordinary id key writes no Variant column into an equality-delete file.
-- @skip_result_check=true
ALTER TABLE iceberg_compat_${suite_uuid0}.${case_db}.java_variant_dv_${uuid0} EXECUTE add_equality_delete(columns => ARRAY['id'], rows => ARRAY[ROW(2)]);

-- query 5
SELECT id FROM iceberg_compat_${suite_uuid0}.${case_db}.java_variant_dv_${uuid0} ORDER BY id;

-- query 6
-- @skip_result_check=true
TRUNCATE TABLE iceberg_compat_${suite_uuid0}.${case_db}.java_variant_dv_${uuid0};

-- query 7
SELECT COUNT(*) AS remaining_rows FROM iceberg_compat_${suite_uuid0}.${case_db}.java_variant_dv_${uuid0};

-- query 8
-- @skip_result_check=true
DROP TABLE iceberg_compat_${suite_uuid0}.${case_db}.java_variant_dv_${uuid0} ;
