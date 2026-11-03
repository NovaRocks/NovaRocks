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

-- @order_sensitive=true
-- @sequential=true
-- ADD FILES is an FE-only data mutation. Spark writes a plain Parquet file
-- with no embedded Iceberg field IDs, reordered columns and legacy names.

-- query 1
-- @result_contains=SPARK_SQL_OK
shell: set -eu
tmp_sql="$(mktemp "${TMPDIR:-/tmp}/novarocks-c2-add-files-XXXXXX.sql")"
trap 'rm -f "$tmp_sql"' EXIT
raw_parquet_warehouse='${iceberg_test_warehouse}'
case "$raw_parquet_warehouse" in
  s3://*) raw_parquet_warehouse="s3a://${raw_parquet_warehouse#s3://}" ;;
esac
cat > "$tmp_sql" <<SPARK_SQL
CREATE NAMESPACE IF NOT EXISTS ice_rest.${case_db};

DROP TABLE IF EXISTS ice_rest.${case_db}.c2_add_files_${uuid0};
CREATE TABLE ice_rest.${case_db}.c2_add_files_${uuid0} (
  new_id BIGINT,
  new_note STRING
) USING iceberg
TBLPROPERTIES (
  'format-version' = '3',
  'write.row-lineage' = 'true',
  'schema.name-mapping.default' = '[{"field-id":1,"names":["new_id","old_id"]},{"field-id":2,"names":["new_note","old_note"]}]'
);

INSERT OVERWRITE DIRECTORY '$raw_parquet_warehouse/c2-add-files-${uuid0}'
USING parquet
SELECT old_note, old_id
FROM VALUES
  ('alpha', CAST(11 AS BIGINT)),
  ('beta', CAST(22 AS BIGINT))
AS source(old_note, old_id);

INSERT OVERWRITE DIRECTORY '$raw_parquet_warehouse/c2-add-files-no-mapping-${uuid0}'
USING parquet
SELECT old_note, old_id
FROM VALUES
  ('gamma', CAST(33 AS BIGINT))
AS source(old_note, old_id);
SPARK_SQL
"${NOVAROCKS_WORKSPACE_ROOT:-.}/docker/iceberg-rest/spark-sql.sh" "$tmp_sql"
printf 'SPARK_SQL_OK\n'

-- query 2
-- @skip_result_check=true
-- @be_log_contains=NOVAROCKS_PARTICIPATION_ESTABLISH_APPLIED
-- @be_log_contains=NOVAROCKS_TASK_CREATE_APPLIED
-- @be_log_contains=NOVAROCKS_CONNECTOR_WRITER_OPENED
-- Prove the write markers before asserting ADD FILES stays frontend-only.
CREATE TABLE iceberg_compat_${suite_uuid0}.${case_db}.add_files_write_control_${uuid0}
  (id BIGINT) TBLPROPERTIES ("format-version" = "2");
INSERT INTO iceberg_compat_${suite_uuid0}.${case_db}.add_files_write_control_${uuid0}
  VALUES (1);
DROP TABLE iceberg_compat_${suite_uuid0}.${case_db}.add_files_write_control_${uuid0} FORCE;

-- query 3
-- @skip_result_check=true
-- @be_log_not_contains=NOVAROCKS_PARTICIPATION_ESTABLISH_APPLIED
-- @be_log_not_contains=NOVAROCKS_TASK_CREATE_APPLIED
-- @be_log_not_contains=NOVAROCKS_CONNECTOR_WRITER_OPENED
ALTER TABLE iceberg_compat_${suite_uuid0}.${case_db}.c2_add_files_${uuid0}
  ADD FILES FROM '${iceberg_test_warehouse}/c2-add-files-${uuid0}';

-- query 4
SELECT new_id, new_note
FROM iceberg_compat_${suite_uuid0}.${case_db}.c2_add_files_${uuid0}
ORDER BY new_id;

-- query 5
-- A second statement reaches the refreshed target base and is rejected by the
-- RegisteredFilesNotLive dependency against the exact target-ref live set;
-- no frontend source-scope ledger is a correctness authority.
-- @expect_error=Broken { dependency: RegisteredFilesNotLive
ALTER TABLE iceberg_compat_${suite_uuid0}.${case_db}.c2_add_files_${uuid0}
  ADD FILES FROM '${iceberg_test_warehouse}/c2-add-files-${uuid0}';

-- query 6
-- @skip_result_check=true
CREATE TABLE iceberg_compat_${suite_uuid0}.${case_db}.c2_add_files_partitioned_${uuid0}
  (id BIGINT) PARTITION BY (id)
  TBLPROPERTIES ("format-version" = "2");

-- query 7
-- @expect_error=unpartitioned
ALTER TABLE iceberg_compat_${suite_uuid0}.${case_db}.c2_add_files_partitioned_${uuid0}
  ADD FILES FROM '${iceberg_test_warehouse}/c2-add-files-${uuid0}';

-- query 8
-- @skip_result_check=true
CREATE TABLE iceberg_compat_${suite_uuid0}.${case_db}.c2_add_files_no_mapping_${uuid0}
  (new_id BIGINT, new_note VARCHAR)
  TBLPROPERTIES ("format-version" = "2");

-- query 9
-- @expect_error=schema.name-mapping.default
ALTER TABLE iceberg_compat_${suite_uuid0}.${case_db}.c2_add_files_no_mapping_${uuid0}
  ADD FILES FROM '${iceberg_test_warehouse}/c2-add-files-no-mapping-${uuid0}';

-- query 10
-- @skip_result_check=true
DROP TABLE iceberg_compat_${suite_uuid0}.${case_db}.c2_add_files_${uuid0} FORCE;

-- query 11
-- @skip_result_check=true
DROP TABLE iceberg_compat_${suite_uuid0}.${case_db}.c2_add_files_partitioned_${uuid0} FORCE;

-- query 12
-- @skip_result_check=true
DROP TABLE iceberg_compat_${suite_uuid0}.${case_db}.c2_add_files_no_mapping_${uuid0} FORCE;
