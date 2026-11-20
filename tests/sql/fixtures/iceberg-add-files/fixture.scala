// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

import org.apache.hadoop.fs.{FileUtil, Path}
import org.apache.iceberg.spark.Spark3Util
import org.apache.spark.sql.SparkSession

object IcebergAddFilesFixture {
  val restMapping = """[{"field-id":1,"names":["id","old_id"]},{"field-id":2,"names":["note","old_note"]}]"""

  def hadoopPath(uri: String): Path = new Path(uri.replaceFirst("^s3://", "s3a://"))

  def put(spark: SparkSession, uri: String, bytes: Array[Byte]): Unit = {
    val path = hadoopPath(uri)
    val fs = path.getFileSystem(spark.sparkContext.hadoopConfiguration)
    fs.mkdirs(path.getParent)
    val output = fs.create(path, false)
    try output.write(bytes) finally output.close()
  }

  def copy(spark: SparkSession, original: String, destination: String): Unit = {
    val conf = spark.sparkContext.hadoopConfiguration
    val source = hadoopPath(original)
    val target = hadoopPath(destination)
    val targetFs = target.getFileSystem(conf)
    targetFs.mkdirs(target.getParent)
    require(FileUtil.copy(source.getFileSystem(conf), source, targetFs, target, false, conf),
      s"Could not copy fixture to $destination")
  }

  def oneFile(spark: SparkSession, table: String): String = {
    val files = spark.table(table + ".files").select("file_path").collect()
    require(files.length == 1, s"Expected one fixture data file for $table, found " + files.length)
    files.head.getString(0)
  }

  def prepareRest(spark: SparkSession, schema: String, token: String, root: String): Unit = {
    spark.sql(s"CREATE NAMESPACE IF NOT EXISTS ice_rest.$schema")
    val source = s"ice_rest.$schema.rwm17c_source_$token"
    try {
      spark.sql(s"CREATE TABLE $source (id BIGINT, note STRING) USING iceberg " +
        "TBLPROPERTIES ('format-version'='2', 'write.distribution-mode'='none')")
      spark.sql(s"INSERT INTO $source SELECT /*+ COALESCE(1) */ id,note FROM VALUES " +
        "(CAST(11 AS BIGINT),'alpha'),(CAST(22 AS BIGINT),'beta') source(id,note)")
      val original = oneFile(spark, source)
      Seq("flat/top.data", "single/_one.parquet", "include/top.parquet", "include/nested/more.parquet",
        "skip/top.parquet", "skip/nested/more.parquet", "fail/top.parquet", "fail/nested/more.parquet",
        "mixed/z/nested.parquet", "v1/top.parquet", "incompatible/top.parquet", "missing-column/top.parquet"
      ).foreach(path => copy(spark, original, s"$root/$path"))
      spark.sql(s"TRUNCATE TABLE $source")
      spark.sql(s"INSERT INTO $source SELECT CAST(NULL AS BIGINT), 'null-value'")
      copy(spark, oneFile(spark, source), s"$root/nullable/top.parquet")
    } finally spark.sql(s"DROP TABLE IF EXISTS $source PURGE")

    val invalid = "not Parquet".getBytes(java.nio.charset.StandardCharsets.UTF_8)
    Seq("flat/_SUCCESS", "include/_temporary/hidden.bin", "include/.dot", "include/nested/_temporary/hidden",
      "empty/_SUCCESS", "bad/top.parquet", "mixed/a.bad").foreach(path => put(spark, s"$root/$path", invalid))

    spark.conf.set("spark.sql.parquet.fieldId.write.enabled", "false")
    spark.sql("SELECT CAST(33 AS BIGINT) AS id, 'gamma' AS note")
      .coalesce(1).write.parquet(hadoopPath(s"$root/no-id").toString)
    spark.sql(s"CREATE TABLE ice_rest.$schema.rwm17c_main_$token (id BIGINT, note STRING) USING iceberg " +
      s"TBLPROPERTIES ('format-version'='2','schema.name-mapping.default'='$restMapping')")
    println("RWM17C_REST_SOURCE_READY")
  }

  def observeRest(spark: SparkSession, schema: String, token: String): Unit = {
    val table = Spark3Util.loadIcebergTable(spark, s"ice_rest.$schema.rwm17c_main_$token")
    require(table.properties().get("schema.name-mapping.default") == restMapping)
    val empty = Spark3Util.loadIcebergTable(spark, s"ice_rest.$schema.rwm17c_empty_$token")
    val snapshots = empty.snapshots().iterator()
    require(snapshots.hasNext)
    val append = snapshots.next()
    require(!snapshots.hasNext && append.operation() == "append")
    require(append.summary().get("total-data-files") == "0" &&
      append.summary().get("total-records") == "0")
    println("RWM17C_REST_MAPPING_AND_EMPTY_APPEND_VERIFIED")
  }
}
