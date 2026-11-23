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
-- Append freezes its output spec while remaining independent of schema-only
-- evolution. These races release a single held POST after the Spark mutation.

-- query 1
-- @skip_result_check=true
CREATE SCHEMA lake_publication_${suite_uuid0}.ns_${uuid0};
CREATE TABLE lake_publication_${suite_uuid0}.ns_${uuid0}.insert_contend (id BIGINT, value BIGINT)
WITH (format_version = 3, max_commit_retry = 0,
      extra_properties = MAP(ARRAY['commit.retry.min-wait-ms', 'commit.retry.max-wait-ms'], ARRAY['0', '0']));
INSERT INTO lake_publication_${suite_uuid0}.ns_${uuid0}.insert_contend VALUES ROW(1, 10), ROW(2, 20), ROW(3, 30);
CREATE TABLE lake_publication_${suite_uuid0}.ns_${uuid0}.insert_schema (id BIGINT, value BIGINT)
WITH (format_version = 3, max_commit_retry = 3,
      extra_properties = MAP(ARRAY['commit.retry.min-wait-ms', 'commit.retry.max-wait-ms'], ARRAY['0', '0']));
INSERT INTO lake_publication_${suite_uuid0}.ns_${uuid0}.insert_schema VALUES ROW(1, 10), ROW(2, 20), ROW(3, 30);
CREATE TABLE lake_publication_${suite_uuid0}.ns_${uuid0}.insert_spec (id BIGINT, value BIGINT)
WITH (format_version = 3, max_commit_retry = 3,
      extra_properties = MAP(ARRAY['commit.retry.min-wait-ms', 'commit.retry.max-wait-ms'], ARRAY['0', '0']));
INSERT INTO lake_publication_${suite_uuid0}.ns_${uuid0}.insert_spec VALUES ROW(1, 10), ROW(2, 20), ROW(3, 30);
CREATE TABLE lake_publication_${suite_uuid0}.ns_${uuid0}.insert_replace (id BIGINT, value BIGINT)
WITH (format_version = 3, max_commit_retry = 3,
      extra_properties = MAP(ARRAY['commit.retry.min-wait-ms', 'commit.retry.max-wait-ms'], ARRAY['0', '0']));
INSERT INTO lake_publication_${suite_uuid0}.ns_${uuid0}.insert_replace VALUES ROW(1, 10), ROW(2, 20), ROW(3, 30);
CREATE TABLE lake_publication_${suite_uuid0}.ns_${uuid0}.insert_unknown (id BIGINT, value BIGINT)
WITH (format_version = 3, max_commit_retry = 3,
      extra_properties = MAP(ARRAY['commit.retry.min-wait-ms', 'commit.retry.max-wait-ms'], ARRAY['0', '0']));
INSERT INTO lake_publication_${suite_uuid0}.ns_${uuid0}.insert_unknown VALUES ROW(1, 10), ROW(2, 20), ROW(3, 30);

-- query 2
-- A11. A competing append advances main while the original POST is held.
-- @result_contains=RWM17A_INSERT_BASE_READY
shell: python3 "${NOVAROCKS_WORKSPACE_ROOT:-.}/tests/sql/correctness/lake-publication/verify_insert_concurrency.py" baseline '${iceberg_rest_uri}' '${uuid0}' 'ns_${uuid0}.insert_contend' contend

-- query 3
-- @publication_catalog_fault=table-commit,before-requirement-check-hold-for-concurrent-shell
-- @publication_catalog_concurrent_shell=python3 "${NOVAROCKS_WORKSPACE_ROOT:-.}/tests/sql/correctness/lake-publication/verify_insert_concurrency.py" companion '${iceberg_rest_uri}' '${uuid0}' 'ns_${uuid0}.insert_contend' contend
-- @expect_trino_error=TRANSACTION_CONFLICT,USER_ERROR
-- @expect_error=Failed to commit the transaction during insert: REST catalog rejected the request with HTTP 409 (after 1 commit attempts)
INSERT INTO lake_publication_${suite_uuid0}.ns_${uuid0}.insert_contend VALUES ROW(5, 50);

-- query 4
-- @result_contains=RWM17A_INSERT_EFFECTS_OK
shell: python3 "${NOVAROCKS_WORKSPACE_ROOT:-.}/tests/sql/correctness/lake-publication/verify_insert_concurrency.py" verify '${iceberg_rest_uri}' '${uuid0}' 'ns_${uuid0}.insert_contend' contend

-- query 5
SELECT id, value FROM lake_publication_${suite_uuid0}.ns_${uuid0}.insert_contend ORDER BY id;

-- query 6
-- A12. ADD COLUMN changes only schema, so the original POST still commits.
-- @result_contains=RWM17A_INSERT_BASE_READY
shell: python3 "${NOVAROCKS_WORKSPACE_ROOT:-.}/tests/sql/correctness/lake-publication/verify_insert_concurrency.py" baseline '${iceberg_rest_uri}' '${uuid0}' 'ns_${uuid0}.insert_schema' schema

-- query 7
-- @publication_catalog_fault=table-commit,before-requirement-check-hold-for-concurrent-shell
-- @publication_catalog_concurrent_shell=python3 "${NOVAROCKS_WORKSPACE_ROOT:-.}/tests/sql/correctness/lake-publication/verify_insert_concurrency.py" companion '${iceberg_rest_uri}' '${uuid0}' 'ns_${uuid0}.insert_schema' schema
-- @skip_result_check=true
INSERT INTO lake_publication_${suite_uuid0}.ns_${uuid0}.insert_schema VALUES ROW(5, 50);

-- query 8
-- @result_contains=RWM17A_INSERT_EFFECTS_OK
shell: python3 "${NOVAROCKS_WORKSPACE_ROOT:-.}/tests/sql/correctness/lake-publication/verify_insert_concurrency.py" verify '${iceberg_rest_uri}' '${uuid0}' 'ns_${uuid0}.insert_schema' schema

-- query 9
SELECT id, value, new_value FROM lake_publication_${suite_uuid0}.ns_${uuid0}.insert_schema ORDER BY id;

-- query 10
-- A12. The original POST is rejected and reload fences the old writer spec.
-- @result_contains=RWM17A_INSERT_BASE_READY
shell: python3 "${NOVAROCKS_WORKSPACE_ROOT:-.}/tests/sql/correctness/lake-publication/verify_insert_concurrency.py" baseline '${iceberg_rest_uri}' '${uuid0}' 'ns_${uuid0}.insert_spec' spec

-- query 11
-- @publication_catalog_fault=table-commit,before-requirement-check-hold-for-concurrent-shell
-- @publication_catalog_concurrent_shell=python3 "${NOVAROCKS_WORKSPACE_ROOT:-.}/tests/sql/correctness/lake-publication/verify_insert_concurrency.py" companion '${iceberg_rest_uri}' '${uuid0}' 'ns_${uuid0}.insert_spec' spec
-- @expect_trino_error=ICEBERG_COMMIT_ERROR,EXTERNAL
-- @expect_error=Failed to commit the transaction during insert: CatalogCommitConflicts, context: { expected: 0, found: 1 } => Requirement failed: Default partition spec id does not match
INSERT INTO lake_publication_${suite_uuid0}.ns_${uuid0}.insert_spec VALUES ROW(5, 50);

-- query 12
-- @result_contains=RWM17A_INSERT_EFFECTS_OK
shell: python3 "${NOVAROCKS_WORKSPACE_ROOT:-.}/tests/sql/correctness/lake-publication/verify_insert_concurrency.py" verify '${iceberg_rest_uri}' '${uuid0}' 'ns_${uuid0}.insert_spec' spec

-- query 13
SELECT id, value FROM lake_publication_${suite_uuid0}.ns_${uuid0}.insert_spec ORDER BY id;

-- query 14
-- A13 replacement path. An old UUID POST fails, then reload sees the new UUID.
-- @result_contains=RWM17A_INSERT_BASE_READY
shell: python3 "${NOVAROCKS_WORKSPACE_ROOT:-.}/tests/sql/correctness/lake-publication/verify_insert_concurrency.py" baseline '${iceberg_rest_uri}' '${uuid0}' 'ns_${uuid0}.insert_replace' replace

-- query 15
-- @publication_catalog_fault=table-commit,before-requirement-check-hold-for-concurrent-shell
-- @publication_catalog_concurrent_shell=python3 "${NOVAROCKS_WORKSPACE_ROOT:-.}/tests/sql/correctness/lake-publication/verify_insert_concurrency.py" companion '${iceberg_rest_uri}' '${uuid0}' 'ns_${uuid0}.insert_replace' replace
-- @expect_trino_error=ICEBERG_COMMIT_ERROR,EXTERNAL
-- @expect_error=Failed to commit the transaction during insert: Iceberg commit target table UUID was replaced
INSERT INTO lake_publication_${suite_uuid0}.ns_${uuid0}.insert_replace VALUES ROW(5, 50);

-- query 16
-- @result_contains=RWM17A_INSERT_EFFECTS_OK
shell: python3 "${NOVAROCKS_WORKSPACE_ROOT:-.}/tests/sql/correctness/lake-publication/verify_insert_concurrency.py" verify '${iceberg_rest_uri}' '${uuid0}' 'ns_${uuid0}.insert_replace' replace

-- query 17
SELECT id, value FROM lake_publication_${suite_uuid0}.ns_${uuid0}.insert_replace ORDER BY id;

-- query 18
-- A14. The proxy withholds the applied reply and the exact marker readback.
-- @result_contains=RWM17A_INSERT_BASE_READY
shell: python3 "${NOVAROCKS_WORKSPACE_ROOT:-.}/tests/sql/correctness/lake-publication/verify_insert_concurrency.py" baseline '${iceberg_rest_uri}' '${uuid0}' 'ns_${uuid0}.insert_unknown' unknown

-- query 19
-- @publication_catalog_fault=table-commit,after-commit-before-response-hide-table-load
-- @publication_catalog_fault_target=namespace=ns_${uuid0},table=insert_unknown
-- @expect_trino_error=ICEBERG_COMMIT_ERROR,EXTERNAL
-- @expect_error=Failed to commit the transaction during insert: [CommitUnknown]
INSERT INTO lake_publication_${suite_uuid0}.ns_${uuid0}.insert_unknown VALUES ROW(5, 50), ROW(6, 60);

-- query 20
-- @result_contains=RWM17A_INSERT_EFFECTS_OK
shell: python3 "${NOVAROCKS_WORKSPACE_ROOT:-.}/tests/sql/correctness/lake-publication/verify_insert_concurrency.py" verify '${iceberg_rest_uri}' '${uuid0}' 'ns_${uuid0}.insert_unknown' unknown

-- query 21
SELECT id, value FROM lake_publication_${suite_uuid0}.ns_${uuid0}.insert_unknown ORDER BY id;

-- query 22
-- @cleanup=true
-- @skip_result_check=true
DROP SCHEMA IF EXISTS lake_publication_${suite_uuid0}.ns_${uuid0} CASCADE;
