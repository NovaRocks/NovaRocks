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
-- C14. An evolved spec introduces a genuine partial match and blocks publication.

-- query 1
-- @skip_result_check=true
CREATE SCHEMA lake_publication_${suite_uuid0}.ns_${uuid0};
CREATE TABLE lake_publication_${suite_uuid0}.ns_${uuid0}.rows_under_test (id BIGINT, ts TIMESTAMP(6))
WITH (partitioning = ARRAY['day(ts)'], format_version = 3, max_commit_retry = 3,
      extra_properties = MAP(ARRAY['commit.retry.min-wait-ms', 'commit.retry.max-wait-ms', 'write.delete.mode'],
                             ARRAY['0', '0', 'merge-on-read']));

-- query 2
-- @skip_result_check=true
-- @be_log_contains=NOVAROCKS_PARTICIPATION_ESTABLISH_APPLIED
-- @be_log_contains=NOVAROCKS_TASK_CREATE_APPLIED
-- @be_log_contains=NOVAROCKS_CONNECTOR_WRITER_OPENED
INSERT INTO lake_publication_${suite_uuid0}.ns_${uuid0}.rows_under_test VALUES ROW(1, TIMESTAMP '2024-01-01 04:00:00'), ROW(2, TIMESTAMP '2024-01-02 01:00:00');

-- query 3
-- @skip_result_check=true
-- @result_contains=TableDelete
-- @result_contains=rows:bigint
EXPLAIN DELETE FROM lake_publication_${suite_uuid0}.ns_${uuid0}.rows_under_test WHERE ts >= TIMESTAMP '2024-01-01 00:00:00' AND ts < TIMESTAMP '2024-01-02 00:00:00';

-- query 4
-- @skip_result_check=true
-- @result_contains=RWM12_TRAFFIC_BASELINE_OK
shell: python3 "${NOVAROCKS_WORKSPACE_ROOT:-.}/tests/sql/correctness/lake-publication/verify_interleaving.py" baseline '${iceberg_rest_uri}' rwm18_metadata_delete_evolution_conflict '${uuid0}'

-- query 5
-- @publication_catalog_fault=table-commit,before-requirement-check-hold-for-concurrent-shell
-- @publication_catalog_concurrent_shell=python3 "${NOVAROCKS_WORKSPACE_ROOT:-.}/tests/sql/correctness/lake-publication/verify_interleaving.py" maintenance-companion rwm18_metadata_delete_evolution_conflict '${uuid0}' 'ns_${uuid0}.rows_under_test' evolve_and_append_mixed '${iceberg_rest_uri}'
-- @expect_trino_error=ICEBERG_COMMIT_ERROR,EXTERNAL
-- @expect_error=Failed to commit during delete: Cannot delete file where some, but not all, rows match filter
-- @be_log_not_contains=NOVAROCKS_PARTICIPATION_ESTABLISH_APPLIED
-- @be_log_not_contains=NOVAROCKS_TASK_CREATE_APPLIED
-- @be_log_not_contains=NOVAROCKS_CONNECTOR_WRITER_OPENED
DELETE FROM lake_publication_${suite_uuid0}.ns_${uuid0}.rows_under_test WHERE ts >= TIMESTAMP '2024-01-01 00:00:00' AND ts < TIMESTAMP '2024-01-02 00:00:00';

-- query 6
-- @skip_result_check=true
-- @result_contains=RWM18_SPARK_READBACK_OK
shell: python3 "${NOVAROCKS_WORKSPACE_ROOT:-.}/tests/sql/correctness/lake-publication/verify_interleaving.py" metadata-delete-finish '${iceberg_rest_uri}' rwm18_metadata_delete_evolution_conflict '${uuid0}' 'ns_${uuid0}.rows_under_test'

-- query 7
SELECT id FROM lake_publication_${suite_uuid0}.ns_${uuid0}.rows_under_test ORDER BY id;

-- query 8
-- @cleanup=true
-- @skip_result_check=true
DROP SCHEMA IF EXISTS lake_publication_${suite_uuid0}.ns_${uuid0} CASCADE;
