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
-- A6. Iceberg v3, multi-row east and west files. UPDATE only affects east.

-- query 1
-- @skip_result_check=true
CREATE SCHEMA lake_publication_${suite_uuid0}.ns_${uuid0};
CREATE TABLE lake_publication_${suite_uuid0}.ns_${uuid0}.rows_under_test (id BIGINT, value BIGINT, part VARCHAR)
WITH (format_version = 3, max_commit_retry = 3, partitioning = ARRAY['part'],
      extra_properties = MAP(
        ARRAY['commit.retry.min-wait-ms', 'commit.retry.max-wait-ms', 'write.delete.mode', 'write.update.mode', 'write.merge.mode', 'write.delete.isolation-level'],
        ARRAY['0', '0', 'merge-on-read', 'merge-on-read', 'merge-on-read', 'serializable']));
INSERT INTO lake_publication_${suite_uuid0}.ns_${uuid0}.rows_under_test VALUES ROW(1, 10, 'east'), ROW(2, 20, 'east'), ROW(3, 30, 'west'), ROW(4, 40, 'west');

-- query 2
-- @result_contains=RWM12_TRAFFIC_BASELINE_OK
shell: python3 "${NOVAROCKS_WORKSPACE_ROOT:-.}/tests/sql/correctness/lake-publication/verify_interleaving.py" baseline '${iceberg_rest_uri}' rwm12_disjoint_partitions '${uuid0}'

-- query 3
-- @publication_catalog_fault=table-commit,before-requirement-check-hold-for-concurrent-shell
-- @publication_catalog_concurrent_shell=set -eu; tmp_sql=$(mktemp "${TMPDIR:-/tmp}/rwm12-companion-XXXXXX.sql"); trap 'rm -f "$tmp_sql"' EXIT; printf '%s\n' 'MERGE INTO ice_rest.ns_${uuid0}.rows_under_test t USING (SELECT 3L AS id, '"'"'west'"'"' AS part) s ON t.id = s.id AND t.part = s.part WHEN MATCHED AND t.part = '"'"'west'"'"' THEN UPDATE SET value = 31;' 'INSERT INTO ice_rest.ns_${uuid0}.rows_under_test VALUES (5, 50, '"'"'north'"'"');' > "$tmp_sql"; "${NOVAROCKS_WORKSPACE_ROOT:-.}/docker/iceberg-rest/spark-sql.sh" "$tmp_sql"
-- @skip_result_check=true
UPDATE lake_publication_${suite_uuid0}.ns_${uuid0}.rows_under_test SET value = 11 WHERE part = 'east' AND id = 1;

-- query 4
SELECT id, value, part FROM lake_publication_${suite_uuid0}.ns_${uuid0}.rows_under_test ORDER BY id, value;

-- query 5
-- @result_contains=RWM12_SPARK_READBACK_OK
shell: python3 "${NOVAROCKS_WORKSPACE_ROOT:-.}/tests/sql/correctness/lake-publication/verify_interleaving.py" finish '${iceberg_rest_uri}' rwm12_disjoint_partitions '${uuid0}' 'ns_${uuid0}.rows_under_test' 2 'id,value,part' '[[1, 11, "east"], [2, 20, "east"], [3, 31, "west"], [4, 40, "west"], [5, 50, "north"]]' 'false'

-- query 6
-- @cleanup=true
-- @skip_result_check=true
DROP SCHEMA IF EXISTS lake_publication_${suite_uuid0}.ns_${uuid0} CASCADE;
