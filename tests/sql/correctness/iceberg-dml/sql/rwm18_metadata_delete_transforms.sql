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
-- @tags=iceberg_dml,delete,metadata_delete
-- Aligned transforms select whole files, and partial partitions keep row-level DELETE.

-- query 1
-- @skip_result_check=true
CREATE TABLE ${case_db}.rwm18_day6 (id BIGINT, ts TIMESTAMP(6))
WITH (partitioning = ARRAY['day(ts)'], format_version = 3);

-- query 2
-- @skip_result_check=true
INSERT INTO ${case_db}.rwm18_day6 VALUES
  ROW(1, TIMESTAMP '2024-01-01 00:00:00.000000'),
  ROW(2, TIMESTAMP '2024-01-01 12:00:00.000000'),
  ROW(3, TIMESTAMP '2024-01-02 00:00:00.000000'),
  ROW(4, TIMESTAMP '2024-01-02 12:00:00.000001'),
  ROW(5, TIMESTAMP '2024-01-03 00:00:00.000000');

-- query 3
-- @skip_result_check=true
-- @result_contains=TableDelete
-- @result_contains=rows:bigint
-- @result_contains=constraint on [
EXPLAIN DELETE FROM ${case_db}.rwm18_day6 WHERE ts >= TIMESTAMP '2024-01-01 00:00:00.000000' AND ts < TIMESTAMP '2024-01-02 00:00:00.000000';

-- query 4
-- @skip_result_check=true
-- @expect_update_count=2
-- @be_log_not_contains=NOVAROCKS_PARTICIPATION_ESTABLISH_APPLIED
-- @be_log_not_contains=NOVAROCKS_TASK_CREATE_APPLIED
DELETE FROM ${case_db}.rwm18_day6 WHERE ts >= TIMESTAMP '2024-01-01 00:00:00.000000' AND ts < TIMESTAMP '2024-01-02 00:00:00.000000';

-- query 5
SELECT s.operation,
       CAST(element_at(s.summary, 'deleted-records') AS BIGINT) AS physical_rows,
       CAST(element_at(s.summary, 'deleted-data-files') AS BIGINT) > 0 AS removed_data_files,
       element_at(s.summary, 'added-delete-files') IS NULL AS no_new_delete_files
FROM ${case_db}."rwm18_day6$snapshots" AS s
JOIN ${case_db}."rwm18_day6$refs" AS r ON s.snapshot_id = r.snapshot_id
WHERE r.name = 'main';

-- query 6
-- @skip_result_check=true
-- @result_contains=RowLevelDelete
-- @result_contains=DeletionVector
-- @result_not_contains=TableDelete
EXPLAIN DELETE FROM ${case_db}.rwm18_day6 WHERE ts >= TIMESTAMP '2024-01-02 12:00:00.000000' AND ts < TIMESTAMP '2024-01-03 00:00:00.000000';

-- query 7
-- @skip_result_check=true
-- @expect_update_count=1
-- @be_log_contains=NOVAROCKS_PARTICIPATION_ESTABLISH_APPLIED
-- @be_log_contains=NOVAROCKS_TASK_CREATE_APPLIED
DELETE FROM ${case_db}.rwm18_day6 WHERE ts >= TIMESTAMP '2024-01-02 12:00:00.000000' AND ts < TIMESTAMP '2024-01-03 00:00:00.000000';

-- query 8
SELECT CAST(element_at(s.summary, 'added-position-deletes') AS BIGINT) > 0 AS added_positions
FROM ${case_db}."rwm18_day6$snapshots" AS s
JOIN ${case_db}."rwm18_day6$refs" AS r ON s.snapshot_id = r.snapshot_id
WHERE r.name = 'main';

-- query 9
SELECT count(*) > 0 AS has_delete_file FROM ${case_db}."rwm18_day6$files"
WHERE content = 1 AND file_format = 'PUFFIN';

-- query 10
SELECT id FROM ${case_db}.rwm18_day6 ORDER BY id;

-- query 11
-- @skip_result_check=true
CREATE TABLE ${case_db}.rwm18_hour6 (id BIGINT, ts TIMESTAMP(6))
WITH (partitioning = ARRAY['hour(ts)'], format_version = 3);

-- query 12
-- @skip_result_check=true
INSERT INTO ${case_db}.rwm18_hour6 VALUES
  ROW(1, TIMESTAMP '1969-12-31 22:59:59.999999'),
  ROW(2, TIMESTAMP '1969-12-31 23:00:00.000000'),
  ROW(3, TIMESTAMP '1969-12-31 23:59:59.999999'),
  ROW(4, TIMESTAMP '1970-01-01 00:00:00.000000'),
  ROW(5, TIMESTAMP '1970-01-01 00:00:00.000001');

-- query 13
-- @skip_result_check=true
-- @result_contains=TableDelete
-- @result_contains=rows:bigint
-- @result_contains=constraint on [
EXPLAIN DELETE FROM ${case_db}.rwm18_hour6 WHERE ts >= TIMESTAMP '1969-12-31 23:00:00.000000' AND ts < TIMESTAMP '1970-01-01 00:00:00.000000';

-- query 14
-- @skip_result_check=true
-- @expect_update_count=2
-- @be_log_not_contains=NOVAROCKS_PARTICIPATION_ESTABLISH_APPLIED
-- @be_log_not_contains=NOVAROCKS_TASK_CREATE_APPLIED
DELETE FROM ${case_db}.rwm18_hour6 WHERE ts >= TIMESTAMP '1969-12-31 23:00:00.000000' AND ts < TIMESTAMP '1970-01-01 00:00:00.000000';

-- query 15
SELECT s.operation,
       CAST(element_at(s.summary, 'deleted-records') AS BIGINT) AS physical_rows,
       CAST(element_at(s.summary, 'deleted-data-files') AS BIGINT) > 0 AS removed_data_files,
       element_at(s.summary, 'added-delete-files') IS NULL AS no_new_delete_files
FROM ${case_db}."rwm18_hour6$snapshots" AS s
JOIN ${case_db}."rwm18_hour6$refs" AS r ON s.snapshot_id = r.snapshot_id
WHERE r.name = 'main';

-- query 16
-- @skip_result_check=true
-- @result_contains=RowLevelDelete
-- @result_contains=DeletionVector
-- @result_not_contains=TableDelete
EXPLAIN DELETE FROM ${case_db}.rwm18_hour6 WHERE ts >= TIMESTAMP '1970-01-01 00:00:00.000001' AND ts < TIMESTAMP '1970-01-01 01:00:00.000000';

-- query 17
-- @skip_result_check=true
-- @expect_update_count=1
-- @be_log_contains=NOVAROCKS_PARTICIPATION_ESTABLISH_APPLIED
-- @be_log_contains=NOVAROCKS_TASK_CREATE_APPLIED
DELETE FROM ${case_db}.rwm18_hour6 WHERE ts >= TIMESTAMP '1970-01-01 00:00:00.000001' AND ts < TIMESTAMP '1970-01-01 01:00:00.000000';

-- query 18
SELECT CAST(element_at(s.summary, 'added-position-deletes') AS BIGINT) > 0 AS added_positions
FROM ${case_db}."rwm18_hour6$snapshots" AS s
JOIN ${case_db}."rwm18_hour6$refs" AS r ON s.snapshot_id = r.snapshot_id
WHERE r.name = 'main';

-- query 19
SELECT count(*) > 0 AS has_delete_file FROM ${case_db}."rwm18_hour6$files"
WHERE content = 1 AND file_format = 'PUFFIN';

-- query 20
SELECT id FROM ${case_db}.rwm18_hour6 ORDER BY id;

-- query 21
-- @skip_result_check=true
CREATE TABLE ${case_db}.rwm18_date_month (id BIGINT, ts DATE)
WITH (partitioning = ARRAY['month(ts)'], format_version = 3);

-- query 22
-- @skip_result_check=true
INSERT INTO ${case_db}.rwm18_date_month VALUES
  ROW(1, DATE '2020-06-01'),
  ROW(2, DATE '2020-06-02'),
  ROW(3, DATE '2020-07-01'),
  ROW(4, DATE '2020-07-02'),
  ROW(5, DATE '2020-08-01');

-- query 23
-- @skip_result_check=true
-- @result_contains=TableDelete
-- @result_contains=rows:bigint
-- @result_contains=constraint on [
EXPLAIN DELETE FROM ${case_db}.rwm18_date_month WHERE ts >= DATE '2020-06-01' AND ts < DATE '2020-07-01';

-- query 24
-- @skip_result_check=true
-- @expect_update_count=2
-- @be_log_not_contains=NOVAROCKS_PARTICIPATION_ESTABLISH_APPLIED
-- @be_log_not_contains=NOVAROCKS_TASK_CREATE_APPLIED
DELETE FROM ${case_db}.rwm18_date_month WHERE ts >= DATE '2020-06-01' AND ts < DATE '2020-07-01';

-- query 25
SELECT s.operation,
       CAST(element_at(s.summary, 'deleted-records') AS BIGINT) AS physical_rows,
       CAST(element_at(s.summary, 'deleted-data-files') AS BIGINT) > 0 AS removed_data_files,
       element_at(s.summary, 'added-delete-files') IS NULL AS no_new_delete_files
FROM ${case_db}."rwm18_date_month$snapshots" AS s
JOIN ${case_db}."rwm18_date_month$refs" AS r ON s.snapshot_id = r.snapshot_id
WHERE r.name = 'main';

-- query 26
-- @skip_result_check=true
-- @result_contains=RowLevelDelete
-- @result_contains=DeletionVector
-- @result_not_contains=TableDelete
EXPLAIN DELETE FROM ${case_db}.rwm18_date_month WHERE ts >= DATE '2020-07-02' AND ts < DATE '2020-08-01';

-- query 27
-- @skip_result_check=true
-- @expect_update_count=1
-- @be_log_contains=NOVAROCKS_PARTICIPATION_ESTABLISH_APPLIED
-- @be_log_contains=NOVAROCKS_TASK_CREATE_APPLIED
DELETE FROM ${case_db}.rwm18_date_month WHERE ts >= DATE '2020-07-02' AND ts < DATE '2020-08-01';

-- query 28
SELECT CAST(element_at(s.summary, 'added-position-deletes') AS BIGINT) > 0 AS added_positions
FROM ${case_db}."rwm18_date_month$snapshots" AS s
JOIN ${case_db}."rwm18_date_month$refs" AS r ON s.snapshot_id = r.snapshot_id
WHERE r.name = 'main';

-- query 29
SELECT count(*) > 0 AS has_delete_file FROM ${case_db}."rwm18_date_month$files"
WHERE content = 1 AND file_format = 'PUFFIN';

-- query 30
SELECT id FROM ${case_db}.rwm18_date_month ORDER BY id;

-- query 31
-- @skip_result_check=true
CREATE CATALOG "c3_ice_${uuid0}" USING iceberg WITH ("iceberg.catalog.type" = 'rest', "uri" = '${iceberg_rest_uri}', "warehouse" = '${iceberg_rest_warehouse}', "credential.object-store-metadata.consumer-role" = 'frontend', "credential.object-store-metadata.mode" = '${iceberg_object_store_credential_mode}', "credential.object-store-metadata.name" = '${iceberg_object_store_metadata_credential_name}', "credential.object-store-metadata.generation" = '${iceberg_object_store_credential_generation}', "credential.object-store-data.consumer-role" = 'backend', "credential.object-store-data.mode" = '${iceberg_object_store_credential_mode}', "credential.object-store-data.name" = '${iceberg_object_store_data_credential_name}', "credential.object-store-data.generation" = '${iceberg_object_store_credential_generation}', "aws.s3.endpoint" = '${oss_endpoint}', "aws.s3.region" = '${oss_region}', "aws.s3.enable_path_style_access" = '${oss_path_style}');
CREATE SCHEMA c3_ice_${uuid0}.c3_nano_${uuid0};

-- query 32
-- @skip_result_check=true
-- @result_contains=RWM18_YEAR9_FIXTURE_OK
shell: set -eu
receipt="${NOVAROCKS_WORKSPACE_ROOT:-.}/logs/rwm18/c3-year9-${uuid0}"
mkdir -p "$receipt"
program="$(mktemp "$receipt/seed-XXXXXX.scala")"
trap 'rm -f "$program"' EXIT
cat "${NOVAROCKS_WORKSPACE_ROOT:-.}/tests/sql/fixtures/iru5-commit-interop/SparkCommitInterop.scala" > "$program"
cat >> "$program" <<'SPARK_SCALA'
Iru5SparkCommitInterop.checked("RWM18_YEAR9_FIXTURE_OK") {
  import java.time.{OffsetDateTime, ZoneOffset}
  import org.apache.iceberg.{PartitionKey, PartitionSpec, Schema}
  import org.apache.iceberg.catalog.TableIdentifier
  import org.apache.iceberg.data.{IcebergGenerics, InternalRecordWrapper}
  import org.apache.iceberg.types.Types

  val namespace = "c3_nano_${uuid0}"
  val name = "rwm18_zoned_year9"
  val scope = spark.conf.get("spark.novarocks.fixture.scopeRoot")
  val run = spark.conf.get("spark.novarocks.fixture.runId")
  require(namespace.startsWith(run + "_") && namespace.length <= 63 &&
    namespace.substring(run.length + 1).matches("[a-z][a-z0-9]*"),
    "The SQL runner must bind the seed to its owned REST namespace")
  require(IcebergBuild.version() == "1.11.0", "This seed requires pinned Iceberg 1.11.0")
  val location = s"$scope/rest/$namespace/$name"
  val identifier = TableIdentifier.of(namespace, name)
  val catalog = Spark3Util.loadIcebergCatalog(spark, "ice_rest")
  require(!catalog.tableExists(identifier), "The seed table already exists in this fixture scope")
  val nanos = Types.TimestampNanoType.withZone()
  val schema = new Schema(
    Types.NestedField.optional(1, "id", Types.LongType.get()),
    Types.NestedField.optional(2, "ts", nanos))
  val spec = PartitionSpec.builderFor(schema).year("ts").build()
  val properties = Map("format-version" -> "3", "write.metadata.metrics.default" -> "full")
  val t = catalog.createTable(identifier, schema, spec, location, properties.asJava)
  require(t.location() == location && t.schema().findType("ts") == nanos &&
    t.spec().fields().size() == 1 && t.spec().fields().get(0).transform().toString == "year",
    "The REST-created table must expose the original zoned nanosecond year partition")

  val requested = Vector(
    1L -> "2024-01-01T08:00:00.000000000+08:00",
    2L -> "2024-12-31T23:59:59.999999999Z",
    3L -> "2025-01-01T00:00:00.000000000Z",
    4L -> "2025-01-01T00:00:00.000000001Z",
    5L -> "2026-01-01T00:00:00.000000000Z").map { case (id, ts) =>
    id -> OffsetDateTime.parse(ts)
  }
  val factory = new GenericAppenderFactory(t.schema(), t.spec())
    .set("write.metadata.metrics.default", "full")
  val key = new PartitionKey(t.spec(), t.schema())
  val wrapper = new InternalRecordWrapper(t.schema().asStruct())
  val append = t.newAppend()
  for ((year, rows) <- requested.groupBy(_._2.withOffsetSameInstant(ZoneOffset.UTC).getYear).toSeq.sortBy(_._1)) {
    val records = rows.map { case (id, ts) =>
      val record = GenericRecord.create(t.schema())
      record.setField("id", Long.box(id))
      record.setField("ts", ts)
      key.partition(wrapper.wrap(record))
      require(key.get(0, classOf[java.lang.Integer]).intValue() == year - 1970,
        "Iceberg's actual partition transform must agree with the UTC year")
      record
    }
    val partition = key.copy()
    val output = t.io().newOutputFile(s"$location/data/rwm18-year-$year.parquet")
    val writer = factory.newDataWriter(EncryptedFiles.plainAsEncryptedOutput(output), FileFormat.PARQUET, partition)
    try records.foreach(record => writer.write(record))
    finally writer.close()
    append.appendFile(writer.toDataFile())
  }
  append.commit()
  t.refresh()
  val snapshot = t.currentSnapshot()
  require(snapshot != null && snapshot.operation() == "append")
  Iru5SparkCommitInterop.assertManifestLengths(t, snapshot)
  val files = snapshot.addedDataFiles(t.io()).asScala.map { file =>
    (file.partition().get(0, classOf[java.lang.Integer]).intValue(), file.recordCount())
  }.toVector.sorted
  require(files == Vector((54, 2L), (55, 2L), (56, 1L)), s"Unexpected year files: $files")

  // The generic Java reader preserves nanoseconds. Spark SQL 3.5 has no
  // matching timestamp carrier for this Iceberg schema.
  val reader = IcebergGenerics.read(t).useSnapshot(snapshot.snapshotId()).build()
  val actual = try reader.asScala.map { record =>
    record.getField("id").asInstanceOf[java.lang.Long].longValue() ->
      record.getField("ts").asInstanceOf[OffsetDateTime].toInstant
  }.toVector.sortBy(_._1) finally reader.close()
  require(actual == requested.map { case (id, ts) => id -> ts.toInstant },
    s"The actual Parquet values lost a nanosecond or used the wrong timezone: $actual")
  println("RWM18_YEAR9_SNAPSHOT " + snapshot.snapshotId() + " partitions=" + files)
}
SPARK_SCALA
if ! "${NOVAROCKS_WORKSPACE_ROOT:-.}/docker/iceberg-rest/spark-shell.sh" "$program" >"$receipt/seed.log" 2>&1; then
  cat "$receipt/seed.log"
  exit 1
fi
grep -Fx 'RWM18_YEAR9_FIXTURE_OK' "$receipt/seed.log"

-- query 33
-- @skip_result_check=true
-- @result_contains=TableDelete
-- @result_contains=rows:bigint
-- @result_contains=constraint on [
EXPLAIN DELETE FROM c3_ice_${uuid0}.c3_nano_${uuid0}.rwm18_zoned_year9 WHERE ts >= TIMESTAMP '2024-01-01 08:00:00.000000000 Asia/Shanghai' AND ts < TIMESTAMP '2025-01-01 00:00:00.000000000 UTC';

-- query 34
-- @skip_result_check=true
-- @expect_update_count=2
-- @be_log_not_contains=NOVAROCKS_PARTICIPATION_ESTABLISH_APPLIED
-- @be_log_not_contains=NOVAROCKS_TASK_CREATE_APPLIED
DELETE FROM c3_ice_${uuid0}.c3_nano_${uuid0}.rwm18_zoned_year9 WHERE ts >= TIMESTAMP '2024-01-01 08:00:00.000000000 Asia/Shanghai' AND ts < TIMESTAMP '2025-01-01 00:00:00.000000000 UTC';

-- query 35
SELECT s.operation,
       CAST(element_at(s.summary, 'deleted-records') AS BIGINT) AS physical_rows,
       CAST(element_at(s.summary, 'deleted-data-files') AS BIGINT) > 0 AS removed_data_files,
       element_at(s.summary, 'added-delete-files') IS NULL AS no_new_delete_files
FROM c3_ice_${uuid0}.c3_nano_${uuid0}."rwm18_zoned_year9$snapshots" AS s
JOIN c3_ice_${uuid0}.c3_nano_${uuid0}."rwm18_zoned_year9$refs" AS r ON s.snapshot_id = r.snapshot_id
WHERE r.name = 'main';

-- query 36
-- @skip_result_check=true
-- @result_contains=RowLevelDelete
-- @result_contains=DeletionVector
-- @result_not_contains=TableDelete
EXPLAIN DELETE FROM c3_ice_${uuid0}.c3_nano_${uuid0}.rwm18_zoned_year9 WHERE ts >= TIMESTAMP '2025-01-01 00:00:00.000000001 UTC' AND ts < TIMESTAMP '2026-01-01 00:00:00.000000000 UTC';

-- query 37
-- @skip_result_check=true
-- @expect_update_count=1
-- @be_log_contains=NOVAROCKS_PARTICIPATION_ESTABLISH_APPLIED
-- @be_log_contains=NOVAROCKS_TASK_CREATE_APPLIED
DELETE FROM c3_ice_${uuid0}.c3_nano_${uuid0}.rwm18_zoned_year9 WHERE ts >= TIMESTAMP '2025-01-01 00:00:00.000000001 UTC' AND ts < TIMESTAMP '2026-01-01 00:00:00.000000000 UTC';

-- query 38
SELECT CAST(element_at(s.summary, 'added-position-deletes') AS BIGINT) > 0 AS added_positions
FROM c3_ice_${uuid0}.c3_nano_${uuid0}."rwm18_zoned_year9$snapshots" AS s
JOIN c3_ice_${uuid0}.c3_nano_${uuid0}."rwm18_zoned_year9$refs" AS r ON s.snapshot_id = r.snapshot_id
WHERE r.name = 'main';

-- query 39
SELECT count(*) > 0 AS has_delete_file FROM c3_ice_${uuid0}.c3_nano_${uuid0}."rwm18_zoned_year9$files"
WHERE content = 1 AND file_format = 'PUFFIN';

-- query 40
SELECT id FROM c3_ice_${uuid0}.c3_nano_${uuid0}.rwm18_zoned_year9 ORDER BY id;

-- query 41
-- @skip_result_check=true
CREATE TABLE ${case_db}.rwm18_integer_truncate (id BIGINT, p INTEGER)
WITH (partitioning = ARRAY['truncate(p, 10)'], format_version = 3);

-- query 42
-- @skip_result_check=true
INSERT INTO ${case_db}.rwm18_integer_truncate VALUES
  ROW(1, 9),
  ROW(2, 10),
  ROW(3, 19),
  ROW(4, 20),
  ROW(5, 21),
  ROW(6, 30);

-- query 43
-- @skip_result_check=true
-- @result_contains=TableDelete
-- @result_contains=rows:bigint
-- @result_contains=constraint on [
EXPLAIN DELETE FROM ${case_db}.rwm18_integer_truncate WHERE p >= 10 AND p < 20;

-- query 44
-- @skip_result_check=true
-- @expect_update_count=2
-- @be_log_not_contains=NOVAROCKS_PARTICIPATION_ESTABLISH_APPLIED
-- @be_log_not_contains=NOVAROCKS_TASK_CREATE_APPLIED
DELETE FROM ${case_db}.rwm18_integer_truncate WHERE p >= 10 AND p < 20;

-- query 45
SELECT s.operation,
       CAST(element_at(s.summary, 'deleted-records') AS BIGINT) AS physical_rows,
       CAST(element_at(s.summary, 'deleted-data-files') AS BIGINT) > 0 AS removed_data_files,
       element_at(s.summary, 'added-delete-files') IS NULL AS no_new_delete_files
FROM ${case_db}."rwm18_integer_truncate$snapshots" AS s
JOIN ${case_db}."rwm18_integer_truncate$refs" AS r ON s.snapshot_id = r.snapshot_id
WHERE r.name = 'main';

-- query 46
-- @skip_result_check=true
-- @result_contains=RowLevelDelete
-- @result_contains=DeletionVector
-- @result_not_contains=TableDelete
EXPLAIN DELETE FROM ${case_db}.rwm18_integer_truncate WHERE p >= 21 AND p < 30;

-- query 47
-- @skip_result_check=true
-- @expect_update_count=1
-- @be_log_contains=NOVAROCKS_PARTICIPATION_ESTABLISH_APPLIED
-- @be_log_contains=NOVAROCKS_TASK_CREATE_APPLIED
DELETE FROM ${case_db}.rwm18_integer_truncate WHERE p >= 21 AND p < 30;

-- query 48
SELECT CAST(element_at(s.summary, 'added-position-deletes') AS BIGINT) > 0 AS added_positions
FROM ${case_db}."rwm18_integer_truncate$snapshots" AS s
JOIN ${case_db}."rwm18_integer_truncate$refs" AS r ON s.snapshot_id = r.snapshot_id
WHERE r.name = 'main';

-- query 49
SELECT count(*) > 0 AS has_delete_file FROM ${case_db}."rwm18_integer_truncate$files"
WHERE content = 1 AND file_format = 'PUFFIN';

-- query 50
SELECT id FROM ${case_db}.rwm18_integer_truncate ORDER BY id;

-- query 51
-- @skip_result_check=true
CREATE TABLE ${case_db}.rwm18_bucket (id BIGINT, p INTEGER)
WITH (partitioning = ARRAY['bucket(p, 16)'], format_version = 3);

-- query 52
-- @skip_result_check=true
INSERT INTO ${case_db}.rwm18_bucket VALUES
  ROW(1, NULL),
  ROW(2, 10),
  ROW(3, 20);

-- query 53
-- @skip_result_check=true
-- @result_contains=TableDelete
-- @result_contains=rows:bigint
-- @result_contains=constraint on [
EXPLAIN DELETE FROM ${case_db}.rwm18_bucket WHERE p IS NULL;

-- query 54
-- @skip_result_check=true
-- @expect_update_count=1
-- @be_log_not_contains=NOVAROCKS_PARTICIPATION_ESTABLISH_APPLIED
-- @be_log_not_contains=NOVAROCKS_TASK_CREATE_APPLIED
DELETE FROM ${case_db}.rwm18_bucket WHERE p IS NULL;

-- query 55
SELECT s.operation,
       CAST(element_at(s.summary, 'deleted-records') AS BIGINT) AS physical_rows,
       CAST(element_at(s.summary, 'deleted-data-files') AS BIGINT) > 0 AS removed_data_files,
       element_at(s.summary, 'added-delete-files') IS NULL AS no_new_delete_files
FROM ${case_db}."rwm18_bucket$snapshots" AS s
JOIN ${case_db}."rwm18_bucket$refs" AS r ON s.snapshot_id = r.snapshot_id
WHERE r.name = 'main';

-- query 56
-- @skip_result_check=true
-- @result_contains=RowLevelDelete
-- @result_contains=DeletionVector
-- @result_not_contains=TableDelete
EXPLAIN DELETE FROM ${case_db}.rwm18_bucket WHERE p IS NOT NULL;

-- query 57
-- @skip_result_check=true
-- @result_contains=RowLevelDelete
-- @result_contains=DeletionVector
-- @result_not_contains=TableDelete
EXPLAIN DELETE FROM ${case_db}.rwm18_bucket WHERE p = 10;

-- query 58
-- @skip_result_check=true
-- @expect_update_count=1
-- @be_log_contains=NOVAROCKS_PARTICIPATION_ESTABLISH_APPLIED
-- @be_log_contains=NOVAROCKS_TASK_CREATE_APPLIED
DELETE FROM ${case_db}.rwm18_bucket WHERE p = 10;

-- query 59
SELECT CAST(element_at(s.summary, 'added-position-deletes') AS BIGINT) > 0 AS added_positions
FROM ${case_db}."rwm18_bucket$snapshots" AS s
JOIN ${case_db}."rwm18_bucket$refs" AS r ON s.snapshot_id = r.snapshot_id
WHERE r.name = 'main';

-- query 60
SELECT count(*) > 0 AS has_delete_file FROM ${case_db}."rwm18_bucket$files"
WHERE content = 1 AND file_format = 'PUFFIN';

-- query 61
SELECT id FROM ${case_db}.rwm18_bucket ORDER BY id;

-- query 62
-- @cleanup=true
-- @skip_result_check=true
DROP TABLE IF EXISTS ${case_db}.rwm18_day6;
DROP TABLE IF EXISTS ${case_db}.rwm18_hour6;
DROP TABLE IF EXISTS ${case_db}.rwm18_date_month;
DROP SCHEMA IF EXISTS c3_ice_${uuid0}.c3_nano_${uuid0} CASCADE;
DROP CATALOG IF EXISTS c3_ice_${uuid0};
DROP TABLE IF EXISTS ${case_db}.rwm18_integer_truncate;
DROP TABLE IF EXISTS ${case_db}.rwm18_bucket;
