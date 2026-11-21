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
-- A5. Iceberg v3, a multi-row file. Snapshot DELETE keeps the concurrent matching row.

-- query 1
-- @skip_result_check=true
CREATE SCHEMA lake_publication_${suite_uuid0}.ns_${uuid0};
CREATE TABLE lake_publication_${suite_uuid0}.ns_${uuid0}.rows_under_test (id BIGINT, value BIGINT)
WITH (format_version = 3, max_commit_retry = 3,
      extra_properties = MAP(
        ARRAY['commit.retry.min-wait-ms', 'commit.retry.max-wait-ms', 'write.delete.mode', 'write.update.mode', 'write.merge.mode', 'write.delete.isolation-level'],
        ARRAY['0', '0', 'merge-on-read', 'merge-on-read', 'merge-on-read', 'snapshot']));
INSERT INTO lake_publication_${suite_uuid0}.ns_${uuid0}.rows_under_test VALUES ROW(1, 10), ROW(2, 20), ROW(3, 30);

-- query 2
-- @result_contains=RWM12_TRAFFIC_BASELINE_OK
shell: python3 "${NOVAROCKS_WORKSPACE_ROOT:-.}/tests/sql/correctness/lake-publication/verify_interleaving.py" baseline '${iceberg_rest_uri}' rwm12_snapshot_append '${uuid0}'

-- query 3
-- @publication_catalog_fault=table-commit,before-requirement-check-hold-for-concurrent-shell
-- @publication_catalog_concurrent_shell=set -eu; tmp_sql=$(mktemp "${TMPDIR:-/tmp}/rwm12-companion-XXXXXX.sql"); trap 'rm -f "$tmp_sql"' EXIT; printf '%s\n' 'INSERT INTO ice_rest.ns_${uuid0}.rows_under_test VALUES (1, 100);' > "$tmp_sql"; "${NOVAROCKS_WORKSPACE_ROOT:-.}/docker/iceberg-rest/spark-sql.sh" "$tmp_sql"
-- @skip_result_check=true
DELETE FROM lake_publication_${suite_uuid0}.ns_${uuid0}.rows_under_test WHERE id = 1;

-- query 4
SELECT id, value FROM lake_publication_${suite_uuid0}.ns_${uuid0}.rows_under_test ORDER BY id, value;

-- query 5
-- @result_contains=RWM12_SPARK_READBACK_OK
shell: python3 "${NOVAROCKS_WORKSPACE_ROOT:-.}/tests/sql/correctness/lake-publication/verify_interleaving.py" finish '${iceberg_rest_uri}' rwm12_snapshot_append '${uuid0}' 'ns_${uuid0}.rows_under_test' 2 'id,value' '[[1, 100], [2, 20], [3, 30]]' 'false'

-- query 6
-- @cleanup=true
-- @skip_result_check=true
DROP SCHEMA IF EXISTS lake_publication_${suite_uuid0}.ns_${uuid0} CASCADE;
