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
-- @result_contains=IRU9_WRITE_TARGET_OK
shell: set -eu
receipt_parent="${XDG_STATE_HOME:-$HOME/.local/state}/novarocks/experiments/iceberg-variant"
mkdir -p "$receipt_parent"
receipt_dir="$(mktemp -d "$receipt_parent/$(date -u +%Y%m%dT%H%M%SZ)-XXXXXX")"
"${NOVAROCKS_WORKSPACE_ROOT:-.}/tests/sql/fixtures/iceberg-variant/run.sh" --mode write-target --namespace ${case_db} --table java_timestamptz_target_${uuid0} --output "$receipt_dir"

-- query 2
-- @skip_result_check=true
INSERT INTO iceberg_compat_${suite_uuid0}.${case_db}.java_timestamptz_target_${uuid0} VALUES
 (1,'2024-01-02 03:04:05.123456',12.34,'東京',X'FF00'),
 (2,NULL,NULL,NULL,NULL);

-- query 3
SELECT id,CAST(ts AS VARCHAR) AS instant,d,s,hex(raw) AS raw_hex
FROM iceberg_compat_${suite_uuid0}.${case_db}.java_timestamptz_target_${uuid0} ORDER BY id;

-- ADD EQUALITY DELETE currently accepts scalar literals only; hexadecimal
-- binary literals are rejected by the existing statement lowering. Keep the
-- non-NULL FF00 binary data writer/read assertion above, and exercise equality
-- adaptation with expressible timestamptz, decimal and string keys.
-- query 4
-- @skip_result_check=true
ALTER TABLE iceberg_compat_${suite_uuid0}.${case_db}.java_timestamptz_target_${uuid0} ADD EQUALITY DELETE (ts,d,s)
VALUES ('2024-01-02 03:04:05.123456',12.34,'東京');

-- query 5
SELECT id FROM iceberg_compat_${suite_uuid0}.${case_db}.java_timestamptz_target_${uuid0} ORDER BY id;

-- query 6
-- @skip_result_check=true
DROP TABLE iceberg_compat_${suite_uuid0}.${case_db}.java_timestamptz_target_${uuid0} FORCE;
