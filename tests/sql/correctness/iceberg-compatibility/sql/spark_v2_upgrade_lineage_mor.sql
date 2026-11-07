-- Licensed to the Apache Software Foundation (ASF) under one
-- or more contributor license agreements. See the NOTICE file
-- distributed with this work for additional information
-- regarding copyright ownership. The ASF licenses this file
-- to you under the Apache License, Version 2.0 (the
-- "License"); you may not use this file except in compliance
-- with the License. You may obtain a copy of the License at
--
--   http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing,
-- software distributed under the License is distributed on an
-- "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
-- KIND, either express or implied. See the License for the
-- specific language governing permissions and limitations
-- under the License.

-- @order_sensitive=true
-- @sequential=true
-- Spark/Java independently prove pre-commit NULL lineage, then inspect the
-- actual committed snapshot and rows after a MOR UPDATE of a historical row.
-- COW on files without lineage is outside this MOR case's supported contract.

-- query 1
-- @result_contains=SPARK_UPGRADE_NULL_LINEAGE_READY
shell: set -eu
interop_dir="${NOVAROCKS_WORKSPACE_ROOT:-.}/logs/rwm-5/upgrade-${uuid0}"
mkdir -p "$interop_dir"
tmp_scala="$(mktemp "${TMPDIR:-/tmp}/novarocks-upgrade-null-lineage-XXXXXX.scala")"
trap 'rm -f "$tmp_scala"' EXIT
cat "${NOVAROCKS_WORKSPACE_ROOT:-.}/tests/sql/fixtures/iru5-commit-interop/SparkCommitInterop.scala" > "$tmp_scala"
cat >> "$tmp_scala" <<'SPARK_SCALA'
Iru5SparkCommitInterop.checked("SPARK_UPGRADE_NULL_LINEAGE_READY") {
  spark.sql("CREATE NAMESPACE IF NOT EXISTS ice_rest.${case_db}")
  spark.sql("CREATE TABLE ice_rest.${case_db}.upgrade_lineage_${uuid0} (id BIGINT, v INT) USING iceberg TBLPROPERTIES ('format-version'='2')")
  spark.sql("INSERT INTO ice_rest.${case_db}.upgrade_lineage_${uuid0} VALUES (1,10),(2,20)")
  spark.sql("ALTER TABLE ice_rest.${case_db}.upgrade_lineage_${uuid0} SET TBLPROPERTIES ('format-version'='3', 'novarocks.update.mode'='merge-on-read')")
  val table = Iru5SparkCommitInterop.table(spark, "ice_rest.${case_db}.upgrade_lineage_${uuid0}")
  require(Iru5SparkCommitInterop.metadata(table).formatVersion() == 3, "Spark must upgrade metadata to v3")
  require(table.currentSnapshot().firstRowId() == null, "Upgrade alone must not allocate a snapshot row range")
  require(!table.properties().containsKey("write.row-lineage"), "The source must not use a Nova lineage property")
  val rows = spark.sql("SELECT id, v, _row_id, _last_updated_sequence_number FROM ice_rest.${case_db}.upgrade_lineage_${uuid0} ORDER BY id").collect().toVector
  require(rows.map(r => (r.getLong(0), r.getInt(1))) == Vector((1L,10),(2L,20)), "Spark historical row oracle")
  require(rows.forall(r => r.isNullAt(2) && r.isNullAt(3)), "Both historical lineage values must be NULL before a v3 commit")
}
SPARK_SCALA
spark_out="$("${NOVAROCKS_WORKSPACE_ROOT:-.}/docker/iceberg-rest/spark-shell.sh" "$tmp_scala" 2>&1)" || {
  printf '%s\n' "$spark_out" | tee "$interop_dir/prepare.log"
  exit 1
}
printf '%s\n' "$spark_out" | tee "$interop_dir/prepare.log"
printf '%s\n' "$spark_out" | tr '\r' '\n' | grep -Fx 'SPARK_UPGRADE_NULL_LINEAGE_READY'

-- query 2
USE iceberg_compat_${suite_uuid0}.${case_db};
SELECT id, _row_id IS NULL AS missing_row_id,
       _last_updated_sequence_number IS NULL AS missing_sequence
FROM ${case_db}.upgrade_lineage_${uuid0} ORDER BY id;

-- query 3
-- @expect_error_tier=target
-- @expect_sql_code=query.execution.invalid_data
-- @expect_sql_phase=Execution
-- @expect_error=has no row lineage (no row range)
CREATE MATERIALIZED VIEW ${case_db}.upgrade_lineage_mv_${uuid0}
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES ('storage_engine'='iceberg')
AS SELECT id,v FROM ${case_db}.upgrade_lineage_${uuid0};

-- query 4
-- @expect_error_tier=drift
-- @expect_error=TrinoError { name=MERGE_TARGET_ROW_MULTIPLE_MATCHES type=USER_ERROR code=119 }: One MERGE target table row matched more than one source row
MERGE INTO ${case_db}.upgrade_lineage_${uuid0} t USING (SELECT 1 AS id, 100 AS v UNION ALL SELECT 1,101) s
ON t.id=s.id WHEN MATCHED THEN UPDATE SET v=s.v;

-- query 5
-- @skip_result_check=true
UPDATE ${case_db}.upgrade_lineage_${uuid0} SET v=11 WHERE id=1;

-- query 6
SELECT id,v, _row_id IS NOT NULL AS assigned_row_id,
       _last_updated_sequence_number IS NOT NULL AS assigned_sequence
FROM ${case_db}.upgrade_lineage_${uuid0} ORDER BY id;

-- query 7
-- @result_contains=SPARK_UPGRADE_MOR_LINEAGE_OK
shell: set -eu
interop_dir="${NOVAROCKS_WORKSPACE_ROOT:-.}/logs/rwm-5/upgrade-${uuid0}"
mkdir -p "$interop_dir"
tmp_scala="$(mktemp "${TMPDIR:-/tmp}/novarocks-upgrade-mor-lineage-XXXXXX.scala")"
trap 'rm -f "$tmp_scala"' EXIT
cat "${NOVAROCKS_WORKSPACE_ROOT:-.}/tests/sql/fixtures/iru5-commit-interop/SparkCommitInterop.scala" > "$tmp_scala"
cat >> "$tmp_scala" <<'SPARK_SCALA'
Iru5SparkCommitInterop.checked("SPARK_UPGRADE_MOR_LINEAGE_OK") {
  val table = Iru5SparkCommitInterop.table(spark, "ice_rest.${case_db}.upgrade_lineage_${uuid0}")
  val current = table.currentSnapshot()
  val parent = table.snapshot(current.parentId())
  require(current.firstRowId() != null && parent.firstRowId() == null, "MOR must create the first v3 row range")
  val rows = spark.sql("SELECT id, v, _row_id, _last_updated_sequence_number FROM ice_rest.${case_db}.upgrade_lineage_${uuid0} ORDER BY id").collect().toVector
  require(rows.map(r => (r.getLong(0), r.getInt(1))) == Vector((1L,11),(2L,20)), "Spark must observe exactly the selected historical-row update")
  require(rows.forall(r => !r.isNullAt(2) && !r.isNullAt(3)), "Updated and untouched rows must both receive lineage")
  require(rows.map(_.getLong(2)).distinct.size == 2, "The updated row must receive its own unique assigned ID")
  require(rows.head.getLong(3) == current.sequenceNumber(), "Updated row inherits the actual MOR commit sequence")
  require(rows(1).getLong(3) == parent.sequenceNumber(), "Untouched historical row preserves its data sequence")
  println("SPARK_UPGRADE_ACTUAL_LINEAGE snapshot=" + current.snapshotId() + " rows=" + rows)
}
SPARK_SCALA
spark_out="$("${NOVAROCKS_WORKSPACE_ROOT:-.}/docker/iceberg-rest/spark-shell.sh" "$tmp_scala" 2>&1)" || {
  printf '%s\n' "$spark_out" | tee "$interop_dir/verify.log"
  exit 1
}
printf '%s\n' "$spark_out" | tee "$interop_dir/verify.log"
printf '%s\n' "$spark_out" | tr '\r' '\n' | grep -Fx 'SPARK_UPGRADE_MOR_LINEAGE_OK'

-- query 8
-- @skip_result_check=true
CREATE MATERIALIZED VIEW ${case_db}.upgrade_lineage_mv_${uuid0}
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES ('storage_engine'='iceberg')
AS SELECT id,v FROM ${case_db}.upgrade_lineage_${uuid0};
REFRESH MATERIALIZED VIEW ${case_db}.upgrade_lineage_mv_${uuid0};

-- query 9
SELECT id,v FROM ${case_db}.upgrade_lineage_mv_${uuid0} ORDER BY id;

-- query 10
-- @cleanup=true
-- @skip_result_check=true
DROP MATERIALIZED VIEW IF EXISTS ${case_db}.upgrade_lineage_mv_${uuid0};
DROP TABLE IF EXISTS ${case_db}.upgrade_lineage_${uuid0} FORCE;
