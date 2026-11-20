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
-- Hadoop installs mappings and the append under the same metadata condition.
-- Metrics readback compares the same Parquet bytes used by Iceberg Java 1.12.0.

-- query 1
-- @result_contains=RWM17C_HADOOP_SOURCE_READY
shell: set -eu
workspace="${NOVAROCKS_WORKSPACE_ROOT:-.}"
receipt_dir="$workspace/tests/sql/.runtime/iceberg-add-files/${uuid0}"
mkdir -p "$receipt_dir"
scala="$receipt_dir/prepareHadoop.scala"
cat "$workspace/tests/sql/fixtures/iceberg-add-files/fixture.scala" > "$scala"
metrics="$(tr -d '\r\n' < "$workspace/tests/sql/fixtures/iceberg-add-files/metrics.parquet.b64")"
legacy="$(tr -d '\r\n' < "$workspace/tests/sql/fixtures/iceberg-add-files/legacy-array.parquet.b64")"
cat >> "$scala" <<SCALA
try {
  IcebergAddFilesFixture.prepareHadoop(org.apache.spark.sql.SparkSession.active, "${iceberg_test_warehouse}/rwm17c-hadoop-sources-${uuid0}",
    "$metrics", "$legacy", "9f6bfa14ff3516636854c83e4793f86df8d2892278fbf3a9fd0b12d1c96ce2c2")
} catch { case failure: Throwable => failure.printStackTrace(); System.exit(1) }
SCALA
if ! "$workspace/docker/iceberg-rest/spark-shell.sh" "$scala" > "$receipt_dir/prepareHadoop.log" 2>&1; then
  tail -80 "$receipt_dir/prepareHadoop.log" >&2
  exit 1
fi
grep -x 'RWM17C_HADOOP_SOURCE_READY' "$receipt_dir/prepareHadoop.log"

-- query 2
CREATE CATALOG rwm17c_hadoop_${uuid0} USING iceberg WITH ("iceberg.catalog.type" = 'hadoop', "iceberg.add-files-procedure.enabled" = 'true', "iceberg.catalog.warehouse" = '${iceberg_test_warehouse}/rwm17c-hadoop-warehouse-${uuid0}', "aws.s3.endpoint" = '${oss_endpoint}', "credential.object-store-metadata.consumer-role" = 'frontend', "credential.object-store-metadata.mode" = '${iceberg_object_store_credential_mode}', "credential.object-store-metadata.name" = '${iceberg_object_store_metadata_credential_name}', "credential.object-store-metadata.generation" = '${iceberg_object_store_credential_generation}', "credential.object-store-data.consumer-role" = 'backend', "credential.object-store-data.mode" = '${iceberg_object_store_credential_mode}', "credential.object-store-data.name" = '${iceberg_object_store_data_credential_name}', "credential.object-store-data.generation" = '${iceberg_object_store_credential_generation}', "aws.s3.region" = '${oss_region}', "aws.s3.enable_path_style_access" = '${oss_path_style}');
CREATE SCHEMA rwm17c_hadoop_${uuid0}.${case_db};
USE rwm17c_hadoop_${uuid0}.${case_db};

-- query 3
CREATE TABLE automatic_names (id BIGINT, note VARCHAR) WITH (format_version = 2);
CREATE TABLE existing_names (new_id BIGINT, new_note VARCHAR) WITH (
    format_version = 2,
    extra_properties = MAP(ARRAY['schema.name-mapping.default'], ARRAY['[{"field-id":1,"names":["new_id","old_id"]},{"field-id":2,"names":["new_note","old_note"]}]'])
);
CREATE TABLE legacy_array (items ARRAY(ROW(value INTEGER))) WITH (format_version = 2);
CREATE TABLE metrics (
    id BIGINT,
    amount DECIMAL(12, 2),
    label VARCHAR,
    payload VARBINARY,
    details ROW(flag INTEGER, comment VARCHAR),
    items ARRAY(ROW(code INTEGER, note VARCHAR))
) WITH (format_version = 2);

-- query 4
ALTER TABLE automatic_names EXECUTE add_files(location => '${iceberg_test_warehouse}/rwm17c-hadoop-sources-${uuid0}/automatic', format => 'PARQUET');

-- query 5
SELECT id, note FROM automatic_names WHERE id >= 11 ORDER BY id;

-- query 6
ALTER TABLE existing_names EXECUTE add_files(location => '${iceberg_test_warehouse}/rwm17c-hadoop-sources-${uuid0}/mapped', format => 'PARQUET');

-- query 7
SELECT new_id, new_note FROM existing_names WHERE new_id = 33;

-- query 8
ALTER TABLE legacy_array EXECUTE add_files(location => '${iceberg_test_warehouse}/rwm17c-hadoop-sources-${uuid0}/legacy.parquet', format => 'PARQUET');

-- query 9
SELECT items[1].value AS first_value, items[2] IS NULL AS null_element,
       items[3].value AS third_value FROM legacy_array;

-- query 10
ALTER TABLE metrics EXECUTE add_files(location => '${iceberg_test_warehouse}/rwm17c-hadoop-sources-${uuid0}/metrics.parquet', format => 'PARQUET');

-- query 11
SELECT id, details.flag AS flag FROM metrics ORDER BY id NULLS LAST;

-- query 12
SET SESSION enable_connector_static_predicate_pushdown = true;

-- query 13
SELECT id, details.flag AS flag FROM metrics WHERE id >= 0 OR id IS NULL ORDER BY id NULLS LAST;

-- query 14
-- @be_log_not_contains=NOVAROCKS_CONNECTOR_UNIT_READER_OPEN
SELECT id, details.flag AS flag FROM metrics WHERE id < -100 ORDER BY id NULLS LAST;

-- query 15
SET SESSION enable_connector_static_predicate_pushdown = false;

-- query 16
SELECT id, details.flag AS flag FROM metrics WHERE id >= 0 OR id IS NULL ORDER BY id NULLS LAST;

-- query 17
-- @be_log_contains=NOVAROCKS_CONNECTOR_UNIT_READER_OPEN
SELECT id, details.flag AS flag FROM metrics WHERE id < -100 ORDER BY id NULLS LAST;

-- query 18
SET SESSION enable_connector_static_predicate_pushdown = true;

-- query 19
-- @result_contains=RWM17C_HADOOP_MAPPING_AND_JAVA_METRICS_VERIFIED
shell: set -eu
workspace="${NOVAROCKS_WORKSPACE_ROOT:-.}"
receipt_dir="$workspace/tests/sql/.runtime/iceberg-add-files/${uuid0}"
mkdir -p "$receipt_dir"
scala="$receipt_dir/observeHadoop.scala"
cat "$workspace/tests/sql/fixtures/iceberg-add-files/fixture.scala" > "$scala"
mapping="$(python3 -c 'import base64, pathlib, sys; print(base64.b64encode(pathlib.Path(sys.argv[1]).read_bytes()).decode())' "$workspace/tests/sql/fixtures/iceberg-add-files/metrics-mapping.json")"
cat >> "$scala" <<SCALA
try {
  IcebergAddFilesFixture.observeHadoop(org.apache.spark.sql.SparkSession.active,
    "${case_db}", "${iceberg_test_warehouse}/rwm17c-hadoop-warehouse-${uuid0}",
    new String(java.util.Base64.getDecoder.decode("$mapping"), java.nio.charset.StandardCharsets.UTF_8))
} catch { case failure: Throwable => failure.printStackTrace(); System.exit(1) }
SCALA
if ! "$workspace/docker/iceberg-rest/spark-shell.sh" "$scala" > "$receipt_dir/observeHadoop.log" 2>&1; then
  tail -80 "$receipt_dir/observeHadoop.log" >&2
  exit 1
fi
grep -x 'RWM17C_HADOOP_MAPPING_AND_JAVA_METRICS_VERIFIED' "$receipt_dir/observeHadoop.log"

-- query 20
DROP TABLE automatic_names;
DROP TABLE existing_names;
DROP TABLE legacy_array;
DROP TABLE metrics;
USE iceberg_compat_${suite_uuid0}.${case_db};
DROP SCHEMA rwm17c_hadoop_${uuid0}.${case_db};
DROP CATALOG rwm17c_hadoop_${uuid0};
