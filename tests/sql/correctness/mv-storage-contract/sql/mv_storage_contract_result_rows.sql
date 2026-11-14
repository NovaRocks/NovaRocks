-- Licensed to the Apache Software Foundation (ASF) under one
-- or more contributor license agreements.  See the NOTICE file
-- distributed with this work for additional information
-- regarding copyright ownership.  The ASF licenses this file
-- to you under the Apache License, Version 2.0.
--
-- @sequential=true
-- @tags=mv,iceberg,rest,minio,storage-contract,logical-result-rows,deletion-vector

-- query 1
-- @skip_result_check=true
CREATE EXTERNAL CATALOG mvrows_${uuid0}
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
CREATE DATABASE mvrows_${uuid0}.ns_${uuid0};

-- query 3
-- @skip_result_check=true
CREATE TABLE mvrows_${uuid0}.ns_${uuid0}.fact (k STRING, v BIGINT)
TBLPROPERTIES ("format-version" = "3", "write.row-lineage" = "true");

-- query 4
-- @skip_result_check=true
INSERT INTO mvrows_${uuid0}.ns_${uuid0}.fact VALUES ('east', 10), ('west', 20);

-- query 5
-- @skip_result_check=true
USE mvrows_${uuid0}.ns_${uuid0};
CREATE MATERIALIZED VIEW rows_mv
DISTRIBUTED BY HASH(k) BUCKETS 1
REFRESH DEFERRED MANUAL
PROPERTIES ('storage_engine' = 'iceberg')
AS SELECT k, v FROM fact;
REFRESH MATERIALIZED VIEW rows_mv WITH SYNC MODE;

-- query 6
-- @skip_result_check=true
DELETE FROM fact WHERE k = 'east';

-- query 7
-- The MV delta deletes one logical row. SHOW and P must report the one
-- visible row for either legal file layout. Query 21 checks the actual files.
-- @skip_result_check=true
-- @mv_rest_document_graph=ns_${uuid0}.rows_mv,publications=2,slot-changes=0,logical-rows=1
REFRESH MATERIALIZED VIEW rows_mv WITH SYNC MODE;

-- query 8
-- @skip_result_check=true
-- @result_rows_where=name=rows_mv
-- @result_rows_count=1
-- @result_rows_expect=last_refresh_rows=1
SELECT * FROM system.runtime.materialized_view_states WHERE catalog_name = 'mvrows_${uuid0}';

-- query 9
SELECT k, v FROM rows_mv ORDER BY k;

-- query 10
-- @skip_result_check=true
INSERT INTO mvrows_${uuid0}.ns_${uuid0}.fact VALUES ('north', 30);

-- query 11
-- This refresh accepts one new row, while the complete target contains two.
-- @skip_result_check=true
-- @mv_rest_document_graph=ns_${uuid0}.rows_mv,publications=3,slot-changes=0,logical-rows=2
REFRESH MATERIALIZED VIEW rows_mv WITH SYNC MODE;

-- query 12
-- @skip_result_check=true
-- @result_rows_where=name=rows_mv
-- @result_rows_count=1
-- @result_rows_expect=last_refresh_rows=2
SELECT * FROM system.runtime.materialized_view_states WHERE catalog_name = 'mvrows_${uuid0}';

-- query 13
SELECT k, v FROM rows_mv ORDER BY k;

-- query 14
-- An unchanged source produces a metadata-only publication. Its zero accepted
-- rows must preserve the complete target's logical row count and physical files.
-- @skip_result_check=true
-- @mv_rest_document_graph=ns_${uuid0}.rows_mv,publications=4,slot-changes=0,logical-rows=2,metadata-only-last=true
REFRESH MATERIALIZED VIEW rows_mv WITH SYNC MODE;

-- query 15
-- @skip_result_check=true
-- @result_rows_where=name=rows_mv
-- @result_rows_count=1
-- @result_rows_expect=last_refresh_rows=2
SELECT * FROM system.runtime.materialized_view_states WHERE catalog_name = 'mvrows_${uuid0}';

-- query 16
SELECT k, v FROM rows_mv ORDER BY k;

-- query 17
-- @skip_result_check=true
DELETE FROM fact WHERE k IN ('west', 'north');

-- query 18
-- Deleting both remaining rows accepts two changes, but leaves zero target rows.
-- @skip_result_check=true
-- @mv_rest_document_graph=ns_${uuid0}.rows_mv,publications=5,slot-changes=0,logical-rows=0
REFRESH MATERIALIZED VIEW rows_mv WITH SYNC MODE;

-- query 19
-- @skip_result_check=true
-- @result_rows_where=name=rows_mv
-- @result_rows_count=1
-- @result_rows_expect=last_refresh_rows=0
SELECT * FROM system.runtime.materialized_view_states WHERE catalog_name = 'mvrows_${uuid0}';

-- query 20
SELECT COUNT(*) AS row_count FROM rows_mv;

-- query 21
-- @result_contains=MV_RESULT_ROWS_FILE_CENSUS_OK
shell: set -eu
tmp_scala="$(mktemp "${TMPDIR:-/tmp}/novarocks-mv-result-rows-XXXXXX.scala")"
trap 'rm -f "$tmp_scala"' EXIT
cat > "$tmp_scala" <<'SPARK_SCALA'
try {
  import org.apache.iceberg.Snapshot
  import org.apache.iceberg.spark.Spark3Util
  import scala.jdk.CollectionConverters._

  case class Blob(path: String, offset: Option[Long], size: Option[Long],
                  reference: Option[String], format: String, count: Long)
  case class DataFile(count: Long, deletes: Set[Blob])
  val table = Spark3Util.loadIcebergTable(spark, "ice_rest.ns_${uuid0}.rows_mv")
  def parent(snapshot: Snapshot): Snapshot =
    Option(snapshot.parentId()).flatMap(id => Option(table.snapshot(id.longValue())))
      .getOrElse(throw new IllegalStateException("Missing parent MV snapshot"))
  def files(snapshot: Snapshot): Map[String, DataFile] = {
    val scan = table.newScan().useSnapshot(snapshot.snapshotId()).planFiles()
    val entries = try scan.asScala.map { task =>
      val deletes = task.deletes().asScala.map { entry =>
        Blob(entry.location(), Option(entry.contentOffset()).map(_.longValue()),
          Option(entry.contentSizeInBytes()).map(_.longValue()),
          Option(entry.referencedDataFile()), entry.format().name(), entry.recordCount())
      }.toSet
      task.file().location() -> DataFile(task.file().recordCount(), deletes)
    }.toVector finally scan.close()
    require(entries.map(_._1).distinct.size == entries.size, "Duplicate live MV data file")
    entries.toMap
  }

  val empty = table.currentSnapshot()
  val noOp = parent(empty)
  val appended = parent(noOp)
  val removed = parent(appended)
  val first = files(parent(removed))
  val afterDelete = files(removed)
  require(first.values.map(_.count).sum == 2 && first.values.forall(_.deletes.isEmpty),
    "First refresh must have two physical rows and no delete files")
  val deletion = if (first.size == 1) {
    val path = first.keys.head
    require(afterDelete.keySet == first.keySet, "Partial delete changed the original data file")
    val file = afterDelete(path)
    require(file.count == 2 && file.deletes.size == 1, "Partial delete must retain two physical rows and one DV")
    val dv = file.deletes.head
    require(dv.format == "PUFFIN" && dv.count == 1 && dv.reference.contains(path) &&
      dv.offset.exists(_ >= 0) && dv.size.exists(_ > 0), "Expected the exact one-row Puffin DV")
    "partial_dv"
  } else {
    require(first.size == 2 && first.values.forall(_.count == 1) &&
      afterDelete.size == 1 && afterDelete.forall { case (path, file) => first.get(path).contains(file) },
      "Whole-file delete must remove one original singleton and preserve its peer without a DV")
    "whole_file"
  }

  val afterAppend = files(appended)
  val added = afterAppend.filterNot { case (path, _) => afterDelete.contains(path) }
  require(afterDelete.forall { case (path, file) => afterAppend.get(path).contains(file) } &&
    added.size == 1 && added.values.forall(file => file.count == 1 && file.deletes.isEmpty),
    "Append must preserve the previous files and add exactly one row")
  require(files(noOp) == afterAppend, "Metadata-only refresh changed the live files")
  require(files(empty).isEmpty, "Final delete retained live data or attached delete files")
  println(s"MV_RESULT_ROWS_FILE_CENSUS_OK branch=$deletion")
} catch { case t: Throwable => t.printStackTrace(); System.exit(1) }
SPARK_SCALA
"${NOVAROCKS_WORKSPACE_ROOT:-.}/docker/iceberg-rest/spark-shell.sh" "$tmp_scala"

-- query 22
-- @cleanup=true
-- @skip_result_check=true
USE mvrows_${uuid0}.ns_${uuid0};
DROP MATERIALIZED VIEW IF EXISTS rows_mv;
DROP TABLE mvrows_${uuid0}.ns_${uuid0}.fact FORCE;
DROP DATABASE mvrows_${uuid0}.ns_${uuid0};
DROP CATALOG mvrows_${uuid0};
