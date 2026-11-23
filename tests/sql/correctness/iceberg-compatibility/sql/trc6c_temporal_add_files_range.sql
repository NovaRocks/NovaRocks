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

-- The raw INT64 is 2^51 × 1000 microseconds. No optional min/max is present.

-- query 1
-- @result_contains=TRC6C_WIDE_ZONED_SOURCE_READY
shell: set -eu
workspace="${NOVAROCKS_WORKSPACE_ROOT:-.}"
receipt_dir="$workspace/tests/sql/.runtime/trc6c-wide-zoned/${uuid0}"
mkdir -p "$receipt_dir"
scala="$receipt_dir/prepare.scala"
cat "$workspace/tests/sql/fixtures/iceberg-add-files/fixture.scala" > "$scala"
raw="$(tr -d '\r\n' < "$workspace/tests/sql/fixtures/iceberg-add-files/trc6c-wide-zoned-micros.parquet.b64")"
cat >> "$scala" <<SCALA
try {
  IcebergAddFilesFixture.publishReference(org.apache.spark.sql.SparkSession.active,
    "${iceberg_test_warehouse}/trc6c-wide-zoned-${uuid0}", "source", "$raw",
    "7b2862f55a4fed09a4cd6b809d242009ba051fce99474258936b7be8d5604e1f")
  println("TRC6C_WIDE_ZONED_SOURCE_READY")
} catch { case failure: Throwable => failure.printStackTrace(); System.exit(1) }
SCALA
if ! "$workspace/docker/iceberg-rest/spark-shell.sh" "$scala" > "$receipt_dir/prepare.log" 2>&1; then
  tail -80 "$receipt_dir/prepare.log" >&2
  exit 1
fi
tr -d '\r' < "$receipt_dir/prepare.log" | grep -x 'TRC6C_WIDE_ZONED_SOURCE_READY'

-- query 2
-- @skip_result_check=true
CREATE TABLE iceberg_compat_${suite_uuid0}.${case_db}.trc6c_wide_zoned_${uuid0} (
    instant TIMESTAMP(6) WITH TIME ZONE
) WITH (format_version = 2);
ALTER TABLE iceberg_compat_${suite_uuid0}.${case_db}.trc6c_wide_zoned_${uuid0}
EXECUTE add_files(location => '${iceberg_test_warehouse}/trc6c-wide-zoned-${uuid0}/source.parquet', format => 'PARQUET');

-- query 3
SELECT count(*) AS snapshots
FROM iceberg_compat_${suite_uuid0}.${case_db}."trc6c_wide_zoned_${uuid0}$snapshots";

-- query 4
-- @be_log_contains=NOVAROCKS_CONNECTOR_UNIT_READER_OPEN
-- @expect_error=Data failure (reason 4:1)
SELECT instant FROM iceberg_compat_${suite_uuid0}.${case_db}.trc6c_wide_zoned_${uuid0};

-- query 5
-- @skip_result_check=true
DROP TABLE iceberg_compat_${suite_uuid0}.${case_db}.trc6c_wide_zoned_${uuid0};
