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
-- A17. Iceberg v3, one two-row file without a DV. Spark proves it added a DV before NovaRocks removes the file.

-- query 1
-- @skip_result_check=true
CREATE SCHEMA lake_publication_${suite_uuid0}.ns_${uuid0};
CREATE TABLE lake_publication_${suite_uuid0}.ns_${uuid0}.rows_under_test (id BIGINT, value BIGINT)
WITH (format_version = 3, max_commit_retry = 3,
      extra_properties = MAP(
        ARRAY['commit.retry.min-wait-ms', 'commit.retry.max-wait-ms', 'write.delete.mode', 'write.update.mode', 'write.merge.mode', 'write.delete.isolation-level'],
        ARRAY['0', '0', 'merge-on-read', 'merge-on-read', 'merge-on-read', 'serializable']));
INSERT INTO lake_publication_${suite_uuid0}.ns_${uuid0}.rows_under_test VALUES ROW(1, 10), ROW(2, 20);

-- query 2
-- @result_contains=RWM12_TRAFFIC_BASELINE_OK
shell: python3 "${NOVAROCKS_WORKSPACE_ROOT:-.}/tests/sql/correctness/lake-publication/verify_interleaving.py" baseline '${iceberg_rest_uri}' rwm12_dangling_dv '${uuid0}'

-- query 3
-- @publication_catalog_fault=table-commit,before-requirement-check-hold-for-concurrent-shell
-- @publication_catalog_concurrent_shell=python3 "${NOVAROCKS_WORKSPACE_ROOT:-.}/tests/sql/correctness/lake-publication/verify_interleaving.py" companion rwm12_dangling_dv '${uuid0}' 'ns_${uuid0}.rows_under_test'
-- @skip_result_check=true
DELETE FROM lake_publication_${suite_uuid0}.ns_${uuid0}.rows_under_test WHERE id IN (1, 2);

-- query 4
SELECT (SELECT count(*) FROM lake_publication_${suite_uuid0}.ns_${uuid0}.rows_under_test) AS live_rows, (SELECT count(*) FROM lake_publication_${suite_uuid0}.ns_${uuid0}."rows_under_test$files") AS live_files;

-- query 5
-- @result_contains=RWM12_SPARK_READBACK_OK
shell: python3 "${NOVAROCKS_WORKSPACE_ROOT:-.}/tests/sql/correctness/lake-publication/verify_interleaving.py" finish '${iceberg_rest_uri}' rwm12_dangling_dv '${uuid0}' 'ns_${uuid0}.rows_under_test' 2 'id,value' '[]' 'true'

-- query 6
-- @cleanup=true
-- @skip_result_check=true
DROP SCHEMA IF EXISTS lake_publication_${suite_uuid0}.ns_${uuid0} CASCADE;
