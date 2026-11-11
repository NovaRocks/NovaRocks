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
-- Java creates separate historical manifests. The Nova append must really
-- merge those sources. Spark and raw Avro then verify rows and output encoding.

-- query 1
-- @skip_result_check=true
CREATE DATABASE IF NOT EXISTS iceberg_compat_${suite_uuid0}.${case_db};

-- query 2
-- @result_contains=UEA4G_FIXTURE_OK
shell: set -eu
repo="${NOVAROCKS_WORKSPACE_ROOT:-.}"
mkdir -p "$repo/logs/rwm14"
receipt_dir="$repo/logs/rwm14/historical-${uuid0}"
"$repo/tests/sql/fixtures/iceberg-delete-applicability/run.sh" --mode lineage --lineage-phase historical --namespace ${case_db} --prefix rwm14_${uuid0} --output "$receipt_dir"

-- query 3
-- @skip_result_check=true
INSERT INTO iceberg_compat_${suite_uuid0}.${case_db}.rwm14_${uuid0}_manifest_old_bucket VALUES (3, 'new');
INSERT INTO iceberg_compat_${suite_uuid0}.${case_db}.rwm14_${uuid0}_manifest_promoted_identity VALUES (13, 3000000002, CAST(3.75 AS DOUBLE));

-- query 4
-- @result_contains=UEA4G_FIXTURE_OK
shell: set -eu
repo="${NOVAROCKS_WORKSPACE_ROOT:-.}"
mkdir -p "$repo/logs/rwm14"
receipt_dir="$(mktemp -d "$repo/logs/rwm14/historical-check-${uuid0}-XXXXXX")"
"$repo/tests/sql/fixtures/iceberg-delete-applicability/run.sh" --mode lineage --lineage-phase historical-check --lineage-seed-receipt "$repo/logs/rwm14/historical-${uuid0}/receipt.json" --namespace ${case_db} --prefix rwm14_${uuid0} --output "$receipt_dir"

-- query 5
SELECT id, value FROM iceberg_compat_${suite_uuid0}.${case_db}.rwm14_${uuid0}_manifest_old_bucket ORDER BY id;

-- query 6
SELECT id, p, CAST(q * 100 AS BIGINT) AS q_times_100
FROM iceberg_compat_${suite_uuid0}.${case_db}.rwm14_${uuid0}_manifest_promoted_identity ORDER BY id;

-- Reclaim directly after successful verification. Early failures leave the
-- owned scratch namespace to the runner's actual-exit cleanup.
-- query 7
-- @skip_result_check=true
DROP TABLE IF EXISTS iceberg_compat_${suite_uuid0}.${case_db}.rwm14_${uuid0}_manifest_old_bucket FORCE;
DROP TABLE IF EXISTS iceberg_compat_${suite_uuid0}.${case_db}.rwm14_${uuid0}_manifest_promoted_identity FORCE;
