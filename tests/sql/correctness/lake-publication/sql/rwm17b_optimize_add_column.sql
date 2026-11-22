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
-- B5. An added nullable field is read as NULL after OPTIMIZE writes its original columns.

-- query 1
-- @skip_result_check=true
CREATE SCHEMA lake_publication_${suite_uuid0}.ns_${uuid0};
CREATE TABLE lake_publication_${suite_uuid0}.ns_${uuid0}.rows_under_test (id BIGINT, value BIGINT)
WITH (format_version = 3, max_commit_retry = 3, extra_properties = MAP(ARRAY['commit.retry.min-wait-ms', 'commit.retry.max-wait-ms', 'write.delete.mode'], ARRAY['0', '0', 'merge-on-read']));
INSERT INTO lake_publication_${suite_uuid0}.ns_${uuid0}.rows_under_test VALUES (1, 10), (2, 20);
INSERT INTO lake_publication_${suite_uuid0}.ns_${uuid0}.rows_under_test VALUES (3, 30);

-- query 2
-- @result_contains=RWM12_TRAFFIC_BASELINE_OK
shell: python3 "${NOVAROCKS_WORKSPACE_ROOT:-.}/tests/sql/correctness/lake-publication/verify_interleaving.py" baseline '${iceberg_rest_uri}' rwm17b_optimize_add_column '${uuid0}'

-- query 3
-- @publication_catalog_fault=table-commit,before-requirement-check-hold-for-concurrent-shell
-- @publication_catalog_concurrent_shell=python3 "${NOVAROCKS_WORKSPACE_ROOT:-.}/tests/sql/correctness/lake-publication/verify_interleaving.py" maintenance-companion rwm17b_optimize_add_column '${uuid0}' 'ns_${uuid0}.rows_under_test' add_column
-- @skip_result_check=true
ALTER TABLE lake_publication_${suite_uuid0}.ns_${uuid0}.rows_under_test EXECUTE optimize;

-- query 4
SELECT id, value, new_value FROM lake_publication_${suite_uuid0}.ns_${uuid0}.rows_under_test ORDER BY id;

-- query 5
-- @result_contains=RWM17B_SPARK_READBACK_OK
shell: python3 "${NOVAROCKS_WORKSPACE_ROOT:-.}/tests/sql/correctness/lake-publication/verify_interleaving.py" maintenance-finish '${iceberg_rest_uri}' rwm17b_optimize_add_column '${uuid0}' 'ns_${uuid0}.rows_under_test' 1 0 'id,value' '[[1, 10], [2, 20], [3, 30]]' 'replace'

-- query 6
-- @cleanup=true
-- @skip_result_check=true
DROP SCHEMA IF EXISTS lake_publication_${suite_uuid0}.ns_${uuid0} CASCADE;
