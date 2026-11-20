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
-- Spark establishes field IDs and evolves the target schema. The registered
-- copy, old v2 file, and legacy v3 file are read by the same native reader.

-- query 1
-- @result_contains=RWM17C_NESTED_SOURCE_READY
shell: set -eu
workspace="${NOVAROCKS_WORKSPACE_ROOT:-.}"
receipt_dir="$workspace/tests/sql/.runtime/iceberg-add-files/${uuid0}"
mkdir -p "$receipt_dir"
scala="$receipt_dir/prepareNested.scala"
cat "$workspace/tests/sql/fixtures/iceberg-add-files/fixture.scala" > "$scala"
cat >> "$scala" <<'SCALA'
try {
  IcebergAddFilesFixture.prepareNested(org.apache.spark.sql.SparkSession.active,
    "${case_db}", "${uuid0}", "${iceberg_test_warehouse}/rwm17c-nested-${uuid0}")
} catch { case failure: Throwable => failure.printStackTrace(); System.exit(1) }
SCALA
if ! "$workspace/docker/iceberg-rest/spark-shell.sh" "$scala" > "$receipt_dir/prepareNested.log" 2>&1; then
  tail -80 "$receipt_dir/prepareNested.log" >&2
  exit 1
fi
grep -x 'RWM17C_NESTED_SOURCE_READY' "$receipt_dir/prepareNested.log"

-- query 2
SELECT id, details.note AS note, details.flag AS flag, details.fresh AS fresh
FROM iceberg_compat_${suite_uuid0}.${case_db}.rwm17c_old_${uuid0}
ORDER BY id;

-- query 3
ALTER TABLE iceberg_compat_${suite_uuid0}.${case_db}.rwm17c_registered_${uuid0} EXECUTE add_files(location => '${iceberg_test_warehouse}/rwm17c-nested-${uuid0}/registered.parquet', format => 'PARQUET');

-- query 4
SELECT id, details.note AS note, details.flag AS flag, details.fresh AS fresh
FROM iceberg_compat_${suite_uuid0}.${case_db}.rwm17c_registered_${uuid0}
ORDER BY id;

-- query 5
-- @expect_trino_error=CONSTRAINT_VIOLATION,USER_ERROR
ALTER TABLE iceberg_compat_${suite_uuid0}.${case_db}.rwm17c_required_add_${uuid0} EXECUTE add_files(location => '${iceberg_test_warehouse}/rwm17c-nested-${uuid0}/required-add.parquet', format => 'PARQUET');

-- query 6
-- @expect_trino_error=GENERIC_USER_ERROR,USER_ERROR
SELECT details.required_child FROM iceberg_compat_${suite_uuid0}.${case_db}.rwm17c_required_read_${uuid0};

-- query 7
-- @expect_trino_error=NOT_SUPPORTED,USER_ERROR
ALTER TABLE iceberg_compat_${suite_uuid0}.${case_db}.rwm17c_mixed_add_${uuid0} EXECUTE add_files(location => '${iceberg_test_warehouse}/rwm17c-nested-${uuid0}/mixed-add.parquet', format => 'PARQUET');

-- query 8
-- @expect_trino_error=GENERIC_USER_ERROR,USER_ERROR
SELECT id, note FROM iceberg_compat_${suite_uuid0}.${case_db}.rwm17c_mixed_read_${uuid0};

-- query 9
SELECT id, details.flag AS flag, details.absent AS absent, details.initialized AS initialized,
       CASE WHEN items IS NULL THEN NULL ELSE items[1].value END AS first_value,
       CASE WHEN items IS NULL THEN NULL ELSE items[2] IS NULL END AS null_element,
       element_at(codes, 'x') AS code
FROM iceberg_compat_${suite_uuid0}.${case_db}.rwm17c_v3_default_${uuid0}
ORDER BY id;

-- query 10
DROP TABLE iceberg_compat_${suite_uuid0}.${case_db}.rwm17c_old_${uuid0};
DROP TABLE iceberg_compat_${suite_uuid0}.${case_db}.rwm17c_registered_${uuid0};
DROP TABLE iceberg_compat_${suite_uuid0}.${case_db}.rwm17c_required_add_${uuid0};
DROP TABLE iceberg_compat_${suite_uuid0}.${case_db}.rwm17c_required_read_${uuid0};
DROP TABLE iceberg_compat_${suite_uuid0}.${case_db}.rwm17c_mixed_add_${uuid0};
DROP TABLE iceberg_compat_${suite_uuid0}.${case_db}.rwm17c_mixed_read_${uuid0};
DROP TABLE iceberg_compat_${suite_uuid0}.${case_db}.rwm17c_v3_default_${uuid0};
