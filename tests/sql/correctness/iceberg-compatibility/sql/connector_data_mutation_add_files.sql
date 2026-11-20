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
-- Spark copies field-ID-bearing data to fixed source keys. ADD FILES owns
-- its registered paths after commit and the independent targets never share paths.

-- query 1
-- @result_contains=RWM17C_REST_SOURCE_READY
shell: set -eu
workspace="${NOVAROCKS_WORKSPACE_ROOT:-.}"
receipt_dir="$workspace/tests/sql/.runtime/iceberg-add-files/${uuid0}"
mkdir -p "$receipt_dir"
scala="$receipt_dir/prepareRest.scala"
cat "$workspace/tests/sql/fixtures/iceberg-add-files/fixture.scala" > "$scala"
cat >> "$scala" <<'SCALA'
try {
  IcebergAddFilesFixture.prepareRest(org.apache.spark.sql.SparkSession.active, "${case_db}", "${uuid0}", "${iceberg_test_warehouse}/rwm17c-rest-${uuid0}")
} catch { case failure: Throwable => failure.printStackTrace(); System.exit(1) }
SCALA
if ! "$workspace/docker/iceberg-rest/spark-shell.sh" "$scala" > "$receipt_dir/prepareRest.log" 2>&1; then
  tail -80 "$receipt_dir/prepareRest.log" >&2
  exit 1
fi
if ! tr '\r' '\n' < "$receipt_dir/prepareRest.log" | grep -x 'RWM17C_REST_SOURCE_READY'; then
  tail -80 "$receipt_dir/prepareRest.log" >&2
  exit 1
fi

-- query 2
-- @be_log_contains=NOVAROCKS_PARTICIPATION_ESTABLISH_APPLIED
-- @be_log_contains=NOVAROCKS_TASK_CREATE_APPLIED
-- @be_log_contains=NOVAROCKS_CONNECTOR_WRITER_OPENED
CREATE TABLE iceberg_compat_${suite_uuid0}.${case_db}.rwm17c_write_control_${uuid0} (id BIGINT) WITH (format_version = 2);
INSERT INTO iceberg_compat_${suite_uuid0}.${case_db}.rwm17c_write_control_${uuid0} VALUES ROW(1);
DROP TABLE iceberg_compat_${suite_uuid0}.${case_db}.rwm17c_write_control_${uuid0};

-- query 3
CREATE CATALOG IF NOT EXISTS "rwm17c_disabled_${uuid0}" USING iceberg WITH ("iceberg.catalog.type" = 'rest', "uri" = '${iceberg_rest_uri}', "warehouse" = '${iceberg_rest_warehouse}', "credential.object-store-metadata.consumer-role" = 'frontend', "credential.object-store-metadata.mode" = '${iceberg_object_store_credential_mode}', "credential.object-store-metadata.name" = '${iceberg_object_store_metadata_credential_name}', "credential.object-store-metadata.generation" = '${iceberg_object_store_credential_generation}', "credential.object-store-data.consumer-role" = 'backend', "credential.object-store-data.mode" = '${iceberg_object_store_credential_mode}', "credential.object-store-data.name" = '${iceberg_object_store_data_credential_name}', "credential.object-store-data.generation" = '${iceberg_object_store_credential_generation}', "aws.s3.endpoint" = '${oss_endpoint}', "aws.s3.region" = '${oss_region}', "aws.s3.enable_path_style_access" = '${oss_path_style}');

-- query 4
-- @expect_trino_error=PERMISSION_DENIED,USER_ERROR
ALTER TABLE rwm17c_disabled_${uuid0}.${case_db}.rwm17c_main_${uuid0} EXECUTE add_files();

-- query 5
-- @expect_trino_error=INVALID_PROCEDURE_ARGUMENT,USER_ERROR
ALTER TABLE rwm17c_disabled_${uuid0}.${case_db}.rwm17c_main_${uuid0} EXECUTE add_files(location => '${iceberg_test_warehouse}/rwm17c-rest-${uuid0}/flat/', format => 'TEXTFILE');

-- query 6
DROP CATALOG rwm17c_disabled_${uuid0};

-- query 7
-- @expect_trino_error=INVALID_PROCEDURE_ARGUMENT,USER_ERROR
ALTER TABLE iceberg_compat_${suite_uuid0}.${case_db}.rwm17c_main_${uuid0} EXECUTE add_files();

-- query 8
-- @expect_trino_error=INVALID_PROCEDURE_ARGUMENT,USER_ERROR
ALTER TABLE iceberg_compat_${suite_uuid0}.${case_db}.rwm17c_main_${uuid0} EXECUTE add_files(location => '${iceberg_test_warehouse}/rwm17c-rest-${uuid0}/does-not-exist');

-- query 9
-- @expect_trino_error=INVALID_PROCEDURE_ARGUMENT,USER_ERROR
ALTER TABLE iceberg_compat_${suite_uuid0}.${case_db}.rwm17c_main_${uuid0} EXECUTE add_files(location => '${iceberg_test_warehouse}/rwm17c-rest-${uuid0}/flat', format => 'argl');

-- query 10
-- @expect_trino_error=INVALID_PROCEDURE_ARGUMENT,USER_ERROR
ALTER TABLE iceberg_compat_${suite_uuid0}.${case_db}.rwm17c_main_${uuid0} EXECUTE add_files(location => '${iceberg_test_warehouse}/rwm17c-rest-${uuid0}/flat', format => 'TEXTFILE');

-- query 11
-- @expect_trino_error=INVALID_PROCEDURE_ARGUMENT,USER_ERROR
ALTER TABLE iceberg_compat_${suite_uuid0}.${case_db}.rwm17c_main_${uuid0} EXECUTE add_files(location => '${iceberg_test_warehouse}/rwm17c-rest-${uuid0}/flat', format => 'PARQUET', recursive_directory => 'invalid');

-- query 12
-- @expect_trino_error=NOT_SUPPORTED,USER_ERROR
ALTER TABLE iceberg_compat_${suite_uuid0}.${case_db}.rwm17c_main_${uuid0} EXECUTE add_files(location => '${iceberg_test_warehouse}/rwm17c-rest-${uuid0}/does-not-exist', format => 'ORC');

-- query 13
-- @expect_trino_error=NOT_SUPPORTED,USER_ERROR
ALTER TABLE iceberg_compat_${suite_uuid0}.${case_db}.rwm17c_main_${uuid0} EXECUTE add_files(location => '${iceberg_test_warehouse}/rwm17c-rest-${uuid0}/does-not-exist', format => 'AVRO');

-- query 14
CREATE TABLE iceberg_compat_${suite_uuid0}.${case_db}.rwm17c_partitioned_${uuid0} (id BIGINT, note VARCHAR) WITH (format_version = 2, partitioning = ARRAY['id']);
CREATE TABLE iceberg_compat_${suite_uuid0}.${case_db}.rwm17c_single_${uuid0} (id BIGINT, note VARCHAR) WITH (format_version = 2);
CREATE TABLE iceberg_compat_${suite_uuid0}.${case_db}.rwm17c_include_${uuid0} (id BIGINT, note VARCHAR) WITH (format_version = 2);
CREATE TABLE iceberg_compat_${suite_uuid0}.${case_db}.rwm17c_skip_${uuid0} (id BIGINT, note VARCHAR) WITH (format_version = 2);
CREATE TABLE iceberg_compat_${suite_uuid0}.${case_db}.rwm17c_fail_${uuid0} (id BIGINT, note VARCHAR) WITH (format_version = 2);
CREATE TABLE iceberg_compat_${suite_uuid0}.${case_db}.rwm17c_bad_${uuid0} (id BIGINT, note VARCHAR) WITH (format_version = 2);
CREATE TABLE iceberg_compat_${suite_uuid0}.${case_db}.rwm17c_mixed_${uuid0} (id BIGINT, note VARCHAR) WITH (format_version = 2);
CREATE TABLE iceberg_compat_${suite_uuid0}.${case_db}.rwm17c_empty_${uuid0} (id BIGINT, note VARCHAR) WITH (format_version = 2);
CREATE TABLE iceberg_compat_${suite_uuid0}.${case_db}.rwm17c_not_null_${uuid0} (id BIGINT NOT NULL, note VARCHAR) WITH (format_version = 2);
CREATE TABLE iceberg_compat_${suite_uuid0}.${case_db}.rwm17c_missing_column_${uuid0} (id BIGINT, note VARCHAR, required_col BIGINT NOT NULL) WITH (format_version = 2);
CREATE TABLE iceberg_compat_${suite_uuid0}.${case_db}.rwm17c_incompatible_${uuid0} (id VARCHAR, note VARCHAR) WITH (format_version = 2);
CREATE TABLE iceberg_compat_${suite_uuid0}.${case_db}.rwm17c_v1_${uuid0} (id BIGINT, note VARCHAR) WITH (format_version = 1);

-- query 15
-- @expect_trino_error=INVALID_PROCEDURE_ARGUMENT,USER_ERROR
ALTER TABLE iceberg_compat_${suite_uuid0}.${case_db}.rwm17c_partitioned_${uuid0} EXECUTE add_files(location => '${iceberg_test_warehouse}/rwm17c-rest-${uuid0}/flat', format => 'PARQUET');

-- query 16
-- @expect_trino_error=NOT_FOUND,USER_ERROR
ALTER TABLE iceberg_compat_${suite_uuid0}.${case_db}.rwm17c_main_${uuid0} EXECUTE add_files(location => '${iceberg_test_warehouse}/rwm17c-rest-${uuid0}/does-not-exist', format => 'PARQUET');

-- query 17
-- @expect_trino_error=NOT_SUPPORTED,USER_ERROR
ALTER TABLE iceberg_compat_${suite_uuid0}.${case_db}.rwm17c_main_${uuid0} EXECUTE add_files(location => '${iceberg_test_warehouse}/rwm17c-rest-${uuid0}/no-id', format => 'PARQUET');

-- query 18
-- @be_log_not_contains=NOVAROCKS_PARTICIPATION_ESTABLISH_APPLIED
-- @be_log_not_contains=NOVAROCKS_TASK_CREATE_APPLIED
-- @be_log_not_contains=NOVAROCKS_CONNECTOR_WRITER_OPENED
ALTER TABLE iceberg_compat_${suite_uuid0}.${case_db}.rwm17c_main_${uuid0} EXECUTE add_files(location => '${iceberg_test_warehouse}/rwm17c-rest-${uuid0}/flat/', format => 'PARQUET');

-- query 19
SELECT id, note FROM iceberg_compat_${suite_uuid0}.${case_db}.rwm17c_main_${uuid0} ORDER BY id, note;

-- query 20
-- @expect_trino_error=ICEBERG_COMMIT_ERROR,EXTERNAL
ALTER TABLE iceberg_compat_${suite_uuid0}.${case_db}.rwm17c_main_${uuid0} EXECUTE add_files(location => '${iceberg_test_warehouse}/rwm17c-rest-${uuid0}/flat', format => 'PARQUET');

-- query 21
ALTER TABLE iceberg_compat_${suite_uuid0}.${case_db}.rwm17c_single_${uuid0} EXECUTE add_files(location => '${iceberg_test_warehouse}/rwm17c-rest-${uuid0}/single/_one.parquet', format => 'PARQUET');

-- query 22
SELECT id, note FROM iceberg_compat_${suite_uuid0}.${case_db}.rwm17c_single_${uuid0} ORDER BY id, note;

-- query 23
ALTER TABLE iceberg_compat_${suite_uuid0}.${case_db}.rwm17c_include_${uuid0} EXECUTE add_files(location => '${iceberg_test_warehouse}/rwm17c-rest-${uuid0}/include/', format => 'PARQUET', recursive_directory => 'TRUE');

-- query 24
SELECT id, note FROM iceberg_compat_${suite_uuid0}.${case_db}.rwm17c_include_${uuid0} ORDER BY id, note;

-- query 25
ALTER TABLE iceberg_compat_${suite_uuid0}.${case_db}.rwm17c_skip_${uuid0} EXECUTE add_files(location => '${iceberg_test_warehouse}/rwm17c-rest-${uuid0}/skip/', format => 'PARQUET', recursive_directory => 'FALSE');

-- query 26
SELECT id, note FROM iceberg_compat_${suite_uuid0}.${case_db}.rwm17c_skip_${uuid0} ORDER BY id, note;

-- query 27
-- @expect_trino_error=NOT_SUPPORTED,USER_ERROR
ALTER TABLE iceberg_compat_${suite_uuid0}.${case_db}.rwm17c_fail_${uuid0} EXECUTE add_files(location => '${iceberg_test_warehouse}/rwm17c-rest-${uuid0}/fail/', format => 'PARQUET', recursive_directory => 'FAIL');

-- query 28
-- @expect_trino_error=ICEBERG_COMMIT_ERROR,EXTERNAL
ALTER TABLE iceberg_compat_${suite_uuid0}.${case_db}.rwm17c_mixed_${uuid0} EXECUTE add_files(location => '${iceberg_test_warehouse}/rwm17c-rest-${uuid0}/mixed/', format => 'PARQUET', recursive_directory => 'FAIL');

-- query 29
-- @expect_trino_error=ICEBERG_COMMIT_ERROR,EXTERNAL
ALTER TABLE iceberg_compat_${suite_uuid0}.${case_db}.rwm17c_bad_${uuid0} EXECUTE add_files(location => '${iceberg_test_warehouse}/rwm17c-rest-${uuid0}/bad', format => 'PARQUET');

-- query 30
-- @expect_trino_error=CONSTRAINT_VIOLATION,USER_ERROR
ALTER TABLE iceberg_compat_${suite_uuid0}.${case_db}.rwm17c_not_null_${uuid0} EXECUTE add_files(location => '${iceberg_test_warehouse}/rwm17c-rest-${uuid0}/nullable', format => 'PARQUET');

-- query 31
-- @expect_trino_error=CONSTRAINT_VIOLATION,USER_ERROR
ALTER TABLE iceberg_compat_${suite_uuid0}.${case_db}.rwm17c_missing_column_${uuid0} EXECUTE add_files(location => '${iceberg_test_warehouse}/rwm17c-rest-${uuid0}/missing-column', format => 'PARQUET');

-- query 32
-- @expect_trino_error=TYPE_MISMATCH,USER_ERROR
ALTER TABLE iceberg_compat_${suite_uuid0}.${case_db}.rwm17c_incompatible_${uuid0} EXECUTE add_files(location => '${iceberg_test_warehouse}/rwm17c-rest-${uuid0}/incompatible', format => 'PARQUET');

-- query 33
ALTER TABLE iceberg_compat_${suite_uuid0}.${case_db}.rwm17c_empty_${uuid0} EXECUTE add_files(location => '${iceberg_test_warehouse}/rwm17c-rest-${uuid0}/empty/', format => 'PARQUET');

-- query 34
ALTER TABLE iceberg_compat_${suite_uuid0}.${case_db}.rwm17c_v1_${uuid0} EXECUTE add_files(location => '${iceberg_test_warehouse}/rwm17c-rest-${uuid0}/v1', format => 'PARQUET');

-- query 35
SELECT id, note FROM iceberg_compat_${suite_uuid0}.${case_db}.rwm17c_v1_${uuid0} ORDER BY id, note;

-- query 36
SELECT (SELECT count(*) FROM iceberg_compat_${suite_uuid0}.${case_db}.rwm17c_fail_${uuid0}) AS fail_rows, (SELECT count(*) FROM iceberg_compat_${suite_uuid0}.${case_db}.rwm17c_mixed_${uuid0}) AS mixed_rows, (SELECT count(*) FROM iceberg_compat_${suite_uuid0}.${case_db}.rwm17c_bad_${uuid0}) AS bad_rows, (SELECT count(*) FROM iceberg_compat_${suite_uuid0}.${case_db}.rwm17c_not_null_${uuid0}) AS null_rows, (SELECT count(*) FROM iceberg_compat_${suite_uuid0}.${case_db}.rwm17c_missing_column_${uuid0}) AS missing_rows, (SELECT count(*) FROM iceberg_compat_${suite_uuid0}.${case_db}.rwm17c_incompatible_${uuid0}) AS incompatible_rows;

-- query 37
-- @result_contains=RWM17C_REST_MAPPING_AND_EMPTY_APPEND_VERIFIED
shell: set -eu
workspace="${NOVAROCKS_WORKSPACE_ROOT:-.}"
receipt_dir="$workspace/tests/sql/.runtime/iceberg-add-files/${uuid0}"
mkdir -p "$receipt_dir"
scala="$receipt_dir/observeRest.scala"
cat "$workspace/tests/sql/fixtures/iceberg-add-files/fixture.scala" > "$scala"
cat >> "$scala" <<'SCALA'
try {
  IcebergAddFilesFixture.observeRest(org.apache.spark.sql.SparkSession.active, "${case_db}", "${uuid0}")
} catch { case failure: Throwable => failure.printStackTrace(); System.exit(1) }
SCALA
if ! "$workspace/docker/iceberg-rest/spark-shell.sh" "$scala" > "$receipt_dir/observeRest.log" 2>&1; then
  tail -80 "$receipt_dir/observeRest.log" >&2
  exit 1
fi
if ! tr '\r' '\n' < "$receipt_dir/observeRest.log" | grep -x 'RWM17C_REST_MAPPING_AND_EMPTY_APPEND_VERIFIED'; then
  tail -80 "$receipt_dir/observeRest.log" >&2
  exit 1
fi

-- query 38
DROP TABLE iceberg_compat_${suite_uuid0}.${case_db}.rwm17c_main_${uuid0};
DROP TABLE iceberg_compat_${suite_uuid0}.${case_db}.rwm17c_partitioned_${uuid0};
DROP TABLE iceberg_compat_${suite_uuid0}.${case_db}.rwm17c_single_${uuid0};
DROP TABLE iceberg_compat_${suite_uuid0}.${case_db}.rwm17c_include_${uuid0};
DROP TABLE iceberg_compat_${suite_uuid0}.${case_db}.rwm17c_skip_${uuid0};
DROP TABLE iceberg_compat_${suite_uuid0}.${case_db}.rwm17c_fail_${uuid0};
DROP TABLE iceberg_compat_${suite_uuid0}.${case_db}.rwm17c_bad_${uuid0};
DROP TABLE iceberg_compat_${suite_uuid0}.${case_db}.rwm17c_mixed_${uuid0};
DROP TABLE iceberg_compat_${suite_uuid0}.${case_db}.rwm17c_empty_${uuid0};
DROP TABLE iceberg_compat_${suite_uuid0}.${case_db}.rwm17c_not_null_${uuid0};
DROP TABLE iceberg_compat_${suite_uuid0}.${case_db}.rwm17c_missing_column_${uuid0};
DROP TABLE iceberg_compat_${suite_uuid0}.${case_db}.rwm17c_incompatible_${uuid0};
DROP TABLE iceberg_compat_${suite_uuid0}.${case_db}.rwm17c_v1_${uuid0};
