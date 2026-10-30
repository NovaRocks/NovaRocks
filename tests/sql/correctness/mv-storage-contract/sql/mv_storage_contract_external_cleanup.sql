-- Licensed to the Apache Software Foundation (ASF) under one
-- or more contributor license agreements.  See the NOTICE file
-- distributed with this work for additional information
-- regarding copyright ownership.  The ASF licenses this file
-- to you under the Apache License, Version 2.0.
--
-- @sequential=true
-- @tags=mv,iceberg,rest,minio,storage-contract,external-cleanup

-- query 1
-- @skip_result_check=true
CREATE EXTERNAL CATALOG mvclean_${uuid0}
PROPERTIES (
  "type" = "iceberg",
  "iceberg.catalog.type" = "rest",
  "uri" = "${iceberg_rest_uri}",
  "warehouse" = "${iceberg_rest_warehouse}",
  "credential.object-store-metadata.consumer-role" = "frontend",
  "credential.object-store-metadata.mode" = "${iceberg_object_store_credential_mode}",
  "credential.object-store-metadata.name" = "${iceberg_object_store_metadata_credential_name}",
  "credential.object-store-metadata.generation" = "${iceberg_object_store_credential_generation}",
  "credential.object-store-data.consumer-role" = "backend",
  "credential.object-store-data.mode" = "${iceberg_object_store_credential_mode}",
  "credential.object-store-data.name" = "${iceberg_object_store_data_credential_name}",
  "credential.object-store-data.generation" = "${iceberg_object_store_credential_generation}",
  "aws.s3.endpoint" = "${oss_endpoint}",
  "aws.s3.region" = "${oss_region}",
  "aws.s3.enable_path_style_access" = "${oss_path_style}"
);

-- query 2
-- @skip_result_check=true
CREATE DATABASE mvclean_${uuid0}.ns_${uuid0};

-- query 3
-- @skip_result_check=true
CREATE TABLE mvclean_${uuid0}.ns_${uuid0}.fact (k STRING, v BIGINT)
TBLPROPERTIES ("format-version" = "3", "write.row-lineage" = "true");

-- query 4
-- @skip_result_check=true
INSERT INTO mvclean_${uuid0}.ns_${uuid0}.fact VALUES ('east', 10);

-- query 5
-- @skip_result_check=true
SET CATALOG mvclean_${uuid0};
USE ns_${uuid0};
CREATE MATERIALIZED VIEW cleanup_mv
PARTITION BY bucket(k, 4)
DISTRIBUTED BY HASH(k) BUCKETS 1 REFRESH DEFERRED MANUAL
PROPERTIES ('storage_engine' = 'iceberg') AS SELECT k, v FROM fact;
REFRESH MATERIALIZED VIEW cleanup_mv WITH SYNC MODE;
INSERT INTO fact VALUES ('west', 20);
REFRESH MATERIALIZED VIEW cleanup_mv WITH SYNC MODE;
ALTER MATERIALIZED VIEW cleanup_mv REPARTITION BY (truncate(k, 2));

-- query 6
-- Every action operates on this exact table, with no shared-warehouse sweep.
-- Both listing modes must actually remove their own orphan candidate. Spark
-- shell's process status alone is insufficient, so require a completion marker.
-- @result_contains=MV_EXTERNAL_CLEANUP_OK
shell: set -eu
tmp_scala="$(mktemp "${TMPDIR:-/tmp}/novarocks-mv-cleanup-XXXXXX.scala")"
trap 'rm -f "$tmp_scala"' EXIT
cat > "$tmp_scala" <<'SPARK_SCALA'
try {
import org.apache.iceberg.spark.Spark3Util
import org.apache.iceberg.spark.actions.SparkActions
import java.sql.Timestamp
import scala.collection.JavaConverters._
import spark.implicits._
val table = Spark3Util.loadIcebergTable(spark, "ice_rest.ns_${uuid0}.cleanup_mv")
spark.sparkContext.hadoopConfiguration.set("fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
require(table.snapshots().asScala.size == 3, "fixture did not create three historical publications")
for (fileList <- Seq(false, true)) {
  val path = table.location() + "/data/t09-orphan-${uuid0}-" + fileList + ".bin"
  val output = table.io().newOutputFile(path).create()
  try { output.write(1) } finally { output.close() }
  require(table.io().newInputFile(path).exists(), "test orphan was not written")
  val action = SparkActions.get(spark).deleteOrphanFiles(table).olderThan(System.currentTimeMillis() + 1000L)
  if (fileList) {
    val files = Seq((path, new Timestamp(0L))).toDF("file_path", "last_modified")
    action.compareToFileList(files)
  }
  val removed = action.execute().orphanFileLocations().asScala.toSet
  require(removed.contains(path), "orphan action did not report the exact test candidate")
  require(!table.io().newInputFile(path).exists(), "orphan action left its candidate")
}
table.expireSnapshots().expireOlderThan(System.currentTimeMillis() + 1000L).retainLast(1).commit()
table.refresh()
require(table.snapshots().asScala.size == 1, "external expiration did not retain exactly current")
println("MV_EXTERNAL_CLEANUP_OK")
} catch { case t: Throwable => t.printStackTrace(); System.exit(1) }
SPARK_SCALA
spark_out="$("${NOVAROCKS_WORKSPACE_ROOT:-.}/docker/iceberg-rest/spark-shell.sh" "$tmp_scala" 2>&1)"
printf '%s\n' "$spark_out"
printf '%s\n' "$spark_out" | grep -F MV_EXTERNAL_CLEANUP_OK

-- query 7
-- The retained P's old dependencies are still in current properties; no old
-- metadata log or document sidecar is needed. Counts remain cumulative at REST.
-- @skip_result_check=true
-- @mv_rest_document_graph=ns_${uuid0}.cleanup_mv,publications=1,slot-changes=0,table-commits=4
-- @result_rows_where=Name=cleanup_mv
-- @result_rows_count=1
-- @result_rows_expect=Manageability=MANAGEABLE
SHOW MATERIALIZED VIEWS;

-- query 8
-- @skip_result_check=true
-- @mv_rest_document_graph=ns_${uuid0}.cleanup_mv,publications=2,slot-changes=0,table-commits=5
REFRESH MATERIALIZED VIEW cleanup_mv WITH SYNC MODE;

-- query 9
SELECT k, v FROM cleanup_mv ORDER BY k;

-- query 10
-- This second MV is disposable after an external data rewrite. Separate
-- publications guarantee two candidate files; rewrite-all must really commit.
-- @skip_result_check=true
CREATE MATERIALIZED VIEW rewrite_mv
DISTRIBUTED BY HASH(k) BUCKETS 1 REFRESH DEFERRED MANUAL
PROPERTIES ('storage_engine' = 'iceberg') AS SELECT k, v FROM fact;
REFRESH MATERIALIZED VIEW rewrite_mv WITH SYNC MODE;
INSERT INTO fact VALUES ('north', 30);
REFRESH MATERIALIZED VIEW rewrite_mv WITH SYNC MODE;

-- query 11
-- @result_contains=MV_EXTERNAL_REWRITE_NO_RECORD
shell: set -eu
tmp_scala="$(mktemp "${TMPDIR:-/tmp}/novarocks-mv-rewrite-XXXXXX.scala")"
trap 'rm -f "$tmp_scala"' EXIT
cat > "$tmp_scala" <<'SPARK_SCALA'
try {
import org.apache.iceberg.spark.Spark3Util
val table = Spark3Util.loadIcebergTable(spark, "ice_rest.ns_${uuid0}.rewrite_mv")
val before = table.currentSnapshot().snapshotId()
val report = spark.sql("CALL ice_rest.system.rewrite_data_files(table => 'ns_${uuid0}.rewrite_mv', options => map('rewrite-all', 'true'))").collect()
require(report.nonEmpty && report.head.getAs[java.lang.Number]("rewritten_data_files_count").longValue() > 0, "external rewrite did not rewrite files")
table.refresh()
require(table.currentSnapshot().snapshotId() != before, "external rewrite did not advance main")
require(!table.currentSnapshot().summary().containsKey("novarocks.doc.v2.record.novarocks.mv~publication"), "external rewrite unexpectedly preserved a NovaRocks publication")
println("MV_EXTERNAL_REWRITE_NO_RECORD")
} catch { case t: Throwable => t.printStackTrace(); System.exit(1) }
SPARK_SCALA
spark_out="$("${NOVAROCKS_WORKSPACE_ROOT:-.}/docker/iceberg-rest/spark-shell.sh" "$tmp_scala" 2>&1)"
printf '%s\n' "$spark_out"
printf '%s\n' "$spark_out" | grep -F MV_EXTERNAL_REWRITE_NO_RECORD

-- query 12
-- @expect_error=external
REFRESH MATERIALIZED VIEW rewrite_mv WITH SYNC MODE;

-- query 13
-- @cleanup=true
-- @skip_result_check=true
SET CATALOG mvclean_${uuid0};
USE ns_${uuid0};
DROP MATERIALIZED VIEW IF EXISTS rewrite_mv;
DROP MATERIALIZED VIEW IF EXISTS cleanup_mv;
DROP TABLE mvclean_${uuid0}.ns_${uuid0}.fact FORCE;
DROP DATABASE mvclean_${uuid0}.ns_${uuid0};
DROP CATALOG mvclean_${uuid0};
