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

-- query 1
-- @skip_result_check=true
CREATE SCHEMA lake_publication_${suite_uuid0}.ns_${uuid0};
CREATE TABLE lake_publication_${suite_uuid0}.ns_${uuid0}.source_rows (id INT);
INSERT INTO lake_publication_${suite_uuid0}.ns_${uuid0}.source_rows VALUES (1), (2), (3);

-- query 2
-- The held final commit guarantees that Spark creates the competing table
-- after the NovaRocks writer has produced its own files.
-- @publication_catalog_fault=table-commit,before-requirement-check-hold-for-concurrent-shell
-- @publication_catalog_concurrent_shell=python3 "${NOVAROCKS_WORKSPACE_ROOT:-.}/tests/sql/correctness/lake-publication/verify_ctas_publication.py" race 'ns_${uuid0}' '${uuid0}' '${iceberg_rest_warehouse}' raced_plain
-- @expect_trino_error=TABLE_ALREADY_EXISTS,USER_ERROR
-- @expect_error=Table ns_${uuid0}.raced_plain already exists
CREATE TABLE lake_publication_${suite_uuid0}.ns_${uuid0}.raced_plain
WITH (location = '${iceberg_rest_warehouse}/ns_${uuid0}/rwm17a_ctas_raced_plain')
AS SELECT id FROM lake_publication_${suite_uuid0}.ns_${uuid0}.source_rows;

-- query 3
-- @publication_catalog_fault=table-commit,before-requirement-check-hold-for-concurrent-shell
-- @publication_catalog_concurrent_shell=python3 "${NOVAROCKS_WORKSPACE_ROOT:-.}/tests/sql/correctness/lake-publication/verify_ctas_publication.py" race 'ns_${uuid0}' '${uuid0}' '${iceberg_rest_warehouse}' raced_if_not_exists
-- @expect_trino_error=TABLE_ALREADY_EXISTS,USER_ERROR
-- @expect_error=Table ns_${uuid0}.raced_if_not_exists already exists
CREATE TABLE IF NOT EXISTS lake_publication_${suite_uuid0}.ns_${uuid0}.raced_if_not_exists
WITH (location = '${iceberg_rest_warehouse}/ns_${uuid0}/rwm17a_ctas_raced_if_not_exists')
AS SELECT id FROM lake_publication_${suite_uuid0}.ns_${uuid0}.source_rows;

-- query 4
-- Reject the final commit rather than stage-create, after the nonempty source
-- has produced data and the operation has built its manifest updates.
-- @publication_catalog_fault=table-commit,before-dispatch-forbidden
-- @expect_trino_error=ICEBERG_COMMIT_ERROR,EXTERNAL
-- @expect_error=Failed to commit the transaction during create table:
CREATE TABLE lake_publication_${suite_uuid0}.ns_${uuid0}.forbidden_after_write
WITH (location = '${iceberg_rest_warehouse}/ns_${uuid0}/rwm17a_ctas_forbidden_after_write')
AS SELECT id FROM lake_publication_${suite_uuid0}.ns_${uuid0}.source_rows;

-- query 5
-- The real Catalog commits, but the exact marker cannot be read during the
-- original statement. The fault guard requires readback and forbids redispatch.
-- @publication_catalog_fault=table-commit,after-commit-before-response-hide-table-load
-- @publication_catalog_fault_target=namespace=ns_${uuid0},table=unobserved_commit
-- @expect_trino_error=ICEBERG_COMMIT_ERROR,EXTERNAL
-- @expect_error=Failed to commit the transaction during create table: [CommitUnknown]
CREATE TABLE lake_publication_${suite_uuid0}.ns_${uuid0}.unobserved_commit
WITH (location = '${iceberg_rest_warehouse}/ns_${uuid0}/rwm17a_ctas_unobserved_commit')
AS SELECT id FROM lake_publication_${suite_uuid0}.ns_${uuid0}.source_rows;

-- query 6
SELECT case_id, id FROM (
    SELECT 1 AS case_id, id FROM lake_publication_${suite_uuid0}.ns_${uuid0}.raced_plain
    UNION ALL
    SELECT 2 AS case_id, id FROM lake_publication_${suite_uuid0}.ns_${uuid0}.raced_if_not_exists
) AS winners ORDER BY case_id, id;

-- query 7
SELECT id FROM lake_publication_${suite_uuid0}.ns_${uuid0}.unobserved_commit ORDER BY id;

-- query 8
-- Spark bypasses the fault proxy. Check actual owned storage rather than
-- using the absence of a SQL table as evidence that its files were deleted.
-- @result_contains=RWM17A_CTAS_PUBLICATION_FILES_OK
shell: python3 "${NOVAROCKS_WORKSPACE_ROOT:-.}/tests/sql/correctness/lake-publication/verify_ctas_publication.py" verify 'ns_${uuid0}' '${uuid0}' '${iceberg_rest_warehouse}'

-- query 9
-- @cleanup=true
-- @skip_result_check=true
DROP SCHEMA IF EXISTS lake_publication_${suite_uuid0}.ns_${uuid0} CASCADE;
