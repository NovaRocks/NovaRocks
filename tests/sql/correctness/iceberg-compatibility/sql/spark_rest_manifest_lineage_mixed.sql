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
-- One external v3 manifest mixes an explicit EXISTING file with two
-- unassigned ADDED files. Bound rows consume the cursor only for A and B.

-- query 1
-- @skip_result_check=true
CREATE DATABASE IF NOT EXISTS iceberg_compat_${suite_uuid0}.${case_db};

-- query 2
-- @result_contains=UEA4G_FIXTURE_OK
shell: set -eu
repo="${NOVAROCKS_WORKSPACE_ROOT:-.}"
mkdir -p "$repo/logs/rwm14"
receipt_dir="$(mktemp -d "$repo/logs/rwm14/mixed-${uuid0}-XXXXXX")"
"$repo/tests/sql/fixtures/iceberg-delete-applicability/run.sh" --mode lineage --lineage-phase mixed --namespace ${case_db} --prefix rwm14_${uuid0} --output "$receipt_dir"

-- query 3
SELECT value, COUNT(*) AS rows_in_file, MIN(_row_id) AS first_id, MAX(_row_id) AS last_id,
       MIN(_last_updated_sequence_number) AS sequence_number
FROM iceberg_compat_${suite_uuid0}.${case_db}.rwm14_${uuid0}_manifest_mixed
WHERE id > 0 GROUP BY value ORDER BY value;

-- query 4
SELECT status, record_count, first_row_id, sequence_number, file_sequence_number
FROM iceberg_compat_${suite_uuid0}.${case_db}.rwm14_${uuid0}_manifest_mixed$entries
WHERE record_count IN (10, 25, 50) ORDER BY record_count;

-- Reclaim directly after successful verification. Early failures leave the
-- owned scratch namespace to the runner's actual-exit cleanup.
-- query 5
-- @skip_result_check=true
DROP TABLE IF EXISTS iceberg_compat_${suite_uuid0}.${case_db}.rwm14_${uuid0}_manifest_mixed FORCE;
