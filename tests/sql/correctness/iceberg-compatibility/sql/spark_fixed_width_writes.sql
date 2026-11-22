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
-- Spark/Java author and read the exact fixed schema and bytes. NovaRocks only
-- consumes existing source columns; no STRUCT-constructor capability is assumed.

-- query 1
-- @result_contains=FIXED_WIDTH_CREATED_OK
shell: set -eu
interop_root="${NOVAROCKS_WORKSPACE_ROOT:-.}/logs/sql-iceberg-fixed-width/${uuid0}"
mkdir -p "$interop_root"
umask 077
spark_diag_dir="$(mktemp -d "$interop_root/created-XXXXXX")"
tmp_scala="$spark_diag_dir/input.scala"
cat > "$interop_root/fixed_width_mor_oracle.scala" <<'SCALA'
object FixedWidthMorOracle {
  def filesAt(table: Table, snapshotId: Long): Vector[(String,Long,Vector[(String,Long,String)])] = {
    val tasks=table.newScan().useSnapshot(snapshotId).planFiles()
    try tasks.asScala.map(t => (t.file().path().toString,t.file().recordCount(),
      t.deletes().asScala.map(d => (d.path().toString,d.recordCount(),d.referencedDataFile())).toVector)).toVector
    finally tasks.close()
  }
  def verify(table: Table): Unit = {
    val initialId=table.properties().get("rwm7.fixed.initial-snapshot").toLong
    val source=table.properties().get("rwm7.fixed.initial-data-file")
    val initial=Option(table.snapshot(initialId))
    val children=table.snapshots().asScala.filter(s => s.parentId()!=null && s.parentId().longValue()==initialId).toVector
    val audit=table.snapshots().asScala.toVector.sortBy(_.sequenceNumber()).map(s =>
      (s.snapshotId(),s.parentId(),s.summary(),
       s.addedDataFiles(table.io()).asScala.map(f => (f.path().toString,f.recordCount())).toVector,
       s.removedDataFiles(table.io()).asScala.map(f => (f.path().toString,f.recordCount())).toVector,
       s.addedDeleteFiles(table.io()).asScala.map(d =>
         (d.path().toString,d.recordCount(),d.referencedDataFile(),d.format(),d.content())).toVector,
       s.removedDeleteFiles(table.io()).asScala.map(d =>
         (d.path().toString,d.recordCount(),d.referencedDataFile())).toVector))
    println("FIXED_WIDTH_MOR_SNAPSHOT_AUDIT="+(initialId,source,children.map(_.snapshotId()),audit))
    require(initial.isDefined,"Remembered single-file initial snapshot is absent")
    require(children.size==1,"Expected one direct child of the single-file initial snapshot")
    val partial=children.head
    val originalFiles=filesAt(table,initialId)
    val partialFiles=filesAt(table,partial.snapshotId())
    val added=partial.addedDataFiles(table.io()).asScala.toVector
    val removed=partial.removedDataFiles(table.io()).asScala.toVector
    val deletes=partial.addedDeleteFiles(table.io()).asScala.toVector
    println("FIXED_WIDTH_MOR_PARTIAL_FILE_AUDIT="+(originalFiles,partialFiles))
    val fullChildren=table.snapshots().asScala.filter(s => s.parentId()!=null && s.parentId().longValue()==partial.snapshotId()).toVector
    println("FIXED_WIDTH_MOR_FULL_CHILD_AUDIT="+(partial.snapshotId(),fullChildren.map(_.snapshotId())))
    require(fullChildren.size==1,"Expected one full-coverage UPDATE child of the partial snapshot")
    val full=fullChildren.head
    val fullAdded=full.addedDataFiles(table.io()).asScala.toVector
    val fullRemoved=full.removedDataFiles(table.io()).asScala.toVector
    val fullAddedDeletes=full.addedDeleteFiles(table.io()).asScala.toVector
    val fullRemovedDeletes=full.removedDeleteFiles(table.io()).asScala.toVector
    println("FIXED_WIDTH_MOR_FULL_COVERAGE_AUDIT="+(full.snapshotId(),
      fullAdded.map(f => (f.path().toString,f.recordCount())),
      fullRemoved.map(f => (f.path().toString,f.recordCount())),
      fullAddedDeletes.map(d => (d.path().toString,d.recordCount(),d.referencedDataFile())),
      fullRemovedDeletes.map(d => (d.path().toString,d.recordCount(),d.referencedDataFile()))))
    require(table.properties().get("write.update.mode")=="copy-on-write","Fixture lost the conflicting COW property")
    require(originalFiles.size==1 && originalFiles.head._1==source && originalFiles.head._2==2L && originalFiles.head._3.isEmpty,
      "Initial snapshot must contain exactly one two-row data file without deletes")
    require(partialFiles.exists(f => f._1==source && f._2==2L),"Partial UPDATE must retain the unmatched row's source file")
    require(removed.isEmpty,"Partial UPDATE must not rewrite/remove the two-row source file")
    require(added.nonEmpty && added.map(_.recordCount()).sum==1L,"Partial UPDATE must add exactly the one matched replacement row")
    require(deletes.size==1 && deletes.head.referencedDataFile()==source && deletes.head.recordCount()==1L &&
      deletes.head.format()==FileFormat.PUFFIN && deletes.head.content()==FileContent.POSITION_DELETES,
      "Partial UPDATE must add a one-position DV for the retained source file despite the COW property")
    require(partialFiles.find(_._1==source).get._3==Vector((deletes.head.path().toString,1L,source)),
      "Partial snapshot scan must apply that exact DV to the original source file")
    require(partialFiles.size==2,"Partial snapshot must contain the source and one replacement data file")
    require(fullAdded.nonEmpty && fullAdded.map(_.recordCount()).sum==2L,
      "Full-coverage UPDATE must add exactly two replacement rows")
    require(fullRemoved.map(_.path().toString).sorted==partialFiles.map(_._1).sorted,
      "Full-coverage UPDATE must retire exactly both partial-snapshot data files")
    require(fullAddedDeletes.isEmpty,"Full coverage must remove data files instead of adding DVs")
    require(fullRemovedDeletes.map(_.path().toString).sorted==deletes.map(_.path().toString).sorted,
      "Full coverage must retire exactly the partial snapshot's exclusive old DV")
  }
}
SCALA
cat "${NOVAROCKS_WORKSPACE_ROOT:-.}/tests/sql/fixtures/iceberg-variant/generate.scala" > "$tmp_scala"
cat >> "$tmp_scala" <<'SCALA'
try {
  val session = org.apache.spark.sql.SparkSession.active
  VariantFixture.initialize(session,"${case_db}","spark_fixed_width_${uuid0}")
  val fixedSchema=new Schema(
    Types.NestedField.required(1,"id",Types.IntegerType.get()),
    Types.NestedField.required(2,"raw",Types.FixedType.ofLength(16)),
    Types.NestedField.optional(3,"st",Types.StructType.of(Types.NestedField.required(5,"f",Types.FixedType.ofLength(4)))),
    Types.NestedField.required(4,"n",Types.IntegerType.get()))
  val fixedTable=VariantFixture.create(fixedSchema,properties=MAP(ARRAY["write.update.mode"->"copy-on-write"], ARRAY["write.row-lineage"->"true"]))
  def fixedBytes(hex: String): Array[Byte] = hex.grouped(2).map(pair => Integer.parseInt(pair,16).toByte).toArray
  val nested=GenericRecord.create(fixedSchema.findType("st").asStructType())
  nested.setField("f",fixedBytes("01020304"))
  val originalFile=VariantFixture.write(fixedTable,Seq(
    Seq[Any](Int.box(1),fixedBytes("00112233445566778899AABBCCDDEEFF"),nested,Int.box(10)),
    Seq[Any](Int.box(2),fixedBytes("FFEEDDCCBBAA99887766554433221100"),null,Int.box(20))))
  val initialSnapshot=fixedTable.currentSnapshot().snapshotId()
  val initialTasks=fixedTable.newScan().useSnapshot(initialSnapshot).planFiles()
  val initialFiles=try initialTasks.asScala.map(t => (t.file().path().toString,t.file().recordCount(),t.deletes().size())).toVector finally initialTasks.close()
  val membership=session.sql("SELECT id,_file FROM ice_rest.${case_db}.spark_fixed_width_${uuid0} ORDER BY id").collect().toVector.map(r => (r.getInt(0),r.getString(1)))
  println("FIXED_WIDTH_INITIAL_FILE_AUDIT="+(initialSnapshot,initialFiles,membership))
  require(initialFiles==Vector((originalFile.path().toString,2L,0)),"Fixed fixture must have exactly one two-row file without deletes")
  require(membership==Vector((1,originalFile.path().toString),(2,originalFile.path().toString)),"Both initial row identities must share the declared source file")
  fixedTable.updateProperties().set("rwm7.fixed.initial-snapshot",initialSnapshot.toString)
    .set("rwm7.fixed.initial-data-file",originalFile.path().toString).commit()
  require(VariantFixture.load().schema().asStruct()==fixedSchema.asStruct(),"Spark changed the fixed declaration")
  val initial=session.sql("SELECT id,hex(raw),hex(st.f),n FROM ice_rest.${case_db}.spark_fixed_width_${uuid0} ORDER BY id").collect().toVector
  require(initial.size==2 && initial(0).getString(1)=="00112233445566778899AABBCCDDEEFF" && initial(0).getString(2)=="01020304" && initial(1).getString(1)=="FFEEDDCCBBAA99887766554433221100" && initial(1).isNullAt(2),"Spark fixture payload differs")
  // A separate ordinary Binary source supplies nested values without requiring
  // NovaRocks to construct a STRUCT or change its declared NULL promises.
  VariantFixture.initialize(session,"${case_db}","spark_fixed_source_${uuid0}")
  val sourceSchema=new Schema(
    Types.NestedField.required(1,"id",Types.IntegerType.get()),
    Types.NestedField.required(2,"raw",Types.BinaryType.get()),
    Types.NestedField.optional(3,"st",Types.StructType.of(Types.NestedField.optional(5,"f",Types.BinaryType.get()))),
    Types.NestedField.required(4,"n",Types.IntegerType.get()))
  VariantFixture.create(sourceSchema,properties=Map("write.row-lineage"->"true"))
  session.sql("INSERT INTO ice_rest.${case_db}.spark_fixed_source_${uuid0} VALUES (1,unhex('00112233445566778899AABBCCDDEEFF'),named_struct('f',CAST(NULL AS BINARY)),11),(3,unhex('000102030405060708090A0B0C0D0E0F'),named_struct('f',unhex('AABBCCDD')),30),(4,unhex('F0F1F2F3F4F5F6F7F8F9FAFBFCFDFEFF'),named_struct('f',unhex('05060708')),40)")
  require(VariantFixture.load().schema().asStruct()==sourceSchema.asStruct(),"Spark changed the Binary source declaration")
  val invalid=session.sql("SELECT st IS NOT NULL,st.f IS NULL FROM ice_rest.${case_db}.spark_fixed_source_${uuid0} WHERE id=1").head()
  require(invalid.getBoolean(0) && invalid.getBoolean(1),"Nested-requiredness source must have an active parent and a NULL child")
  println("FIXED_WIDTH_CREATED_OK")
} catch { case failure:Throwable => failure.printStackTrace(); System.exit(1) }
SCALA
spark_status=0
"${NOVAROCKS_WORKSPACE_ROOT:-.}/docker/iceberg-rest/spark-shell.sh" "$tmp_scala" \
  > "$spark_diag_dir/stdout" 2> "$spark_diag_dir/stderr" || spark_status=$?
printf '%s\n' "$spark_status" > "$spark_diag_dir/exit-status"
if [ "$spark_status" -ne 0 ]; then
  printf 'Spark created failed (exit=%s); input, stdout, stderr and exit retained at %s\n' "$spark_status" "$spark_diag_dir" >&2
  exit "$spark_status"
fi
if ! grep -Fx 'FIXED_WIDTH_CREATED_OK' "$spark_diag_dir/stdout"; then
  printf 'Spark created sentinel missing; input, stdout, stderr and exit retained at %s\n' "$spark_diag_dir" >&2
  exit 1
fi
printf 'Spark created evidence retained at %s\n' "$spark_diag_dir"

-- query 2
SELECT id,hex(CAST(raw AS VARBINARY)) AS raw_hex,st IS NULL AS nested_is_null,n+1 AS next_n,n
FROM iceberg_compat_${suite_uuid0}.${case_db}.spark_fixed_width_${uuid0} ORDER BY id;

-- query 3
-- The first update is provably partial-file: id=2 shares the pinned source file.
UPDATE iceberg_compat_${suite_uuid0}.${case_db}.spark_fixed_width_${uuid0}
SET raw=raw,st=st WHERE id=1;

-- query 4
-- Full coverage may legitimately retire data files instead of adding new DVs.
UPDATE iceberg_compat_${suite_uuid0}.${case_db}.spark_fixed_width_${uuid0}
SET n=n+1;

-- query 5
SELECT id,hex(CAST(raw AS VARBINARY)) AS raw_hex,st IS NULL AS nested_is_null,n
FROM iceberg_compat_${suite_uuid0}.${case_db}.spark_fixed_width_${uuid0} ORDER BY id;

-- query 6
-- @result_contains=FIXED_WIDTH_ORIGINAL_UPDATES_OK
shell: set -eu
interop_root="${NOVAROCKS_WORKSPACE_ROOT:-.}/logs/sql-iceberg-fixed-width/${uuid0}"
mkdir -p "$interop_root"
umask 077
spark_diag_dir="$(mktemp -d "$interop_root/original_updates-XXXXXX")"
tmp_scala="$spark_diag_dir/input.scala"
cat "${NOVAROCKS_WORKSPACE_ROOT:-.}/tests/sql/fixtures/iceberg-variant/generate.scala" > "$tmp_scala"
cat "$interop_root/fixed_width_mor_oracle.scala" >> "$tmp_scala"
cat >> "$tmp_scala" <<'SCALA'
try {
  val session = org.apache.spark.sql.SparkSession.active
  VariantFixture.initialize(session,"${case_db}","spark_fixed_width_${uuid0}",readOnly=true)
  val table=VariantFixture.load()
  val expectedSchema=new Schema(
    Types.NestedField.required(1,"id",Types.IntegerType.get()),
    Types.NestedField.required(2,"raw",Types.FixedType.ofLength(16)),
    Types.NestedField.optional(3,"st",Types.StructType.of(Types.NestedField.required(5,"f",Types.FixedType.ofLength(4)))),
    Types.NestedField.required(4,"n",Types.IntegerType.get()))
  require(table.schema().asStruct()==expectedSchema.asStruct(),"Write changed the exact fixed declaration or requiredness")
  val rows=session.sql("SELECT id,hex(raw),hex(st.f),n FROM ice_rest.${case_db}.spark_fixed_width_${uuid0} ORDER BY id").collect().toVector
  val actual=rows.map(r => (r.getInt(0),r.getString(1),if(r.isNullAt(2)) None else Some(r.getString(2)),r.getInt(3)))
  val expected:Vector[(Int,String,Option[String],Int)]=Vector((1,"00112233445566778899AABBCCDDEEFF",Some("01020304"),11),(2,"FFEEDDCCBBAA99887766554433221100",None,21))
  require(actual==expected,"Spark fixed-width row oracle differs: "+actual)
  FixedWidthMorOracle.verify(table)

  println("FIXED_WIDTH_ORIGINAL_UPDATES_OK")
} catch { case failure:Throwable => failure.printStackTrace(); System.exit(1) }
SCALA
spark_status=0
"${NOVAROCKS_WORKSPACE_ROOT:-.}/docker/iceberg-rest/spark-shell.sh" "$tmp_scala" \
  > "$spark_diag_dir/stdout" 2> "$spark_diag_dir/stderr" || spark_status=$?
printf '%s\n' "$spark_status" > "$spark_diag_dir/exit-status"
if [ "$spark_status" -ne 0 ]; then
  printf 'Spark original_updates failed (exit=%s); input, stdout, stderr and exit retained at %s\n' "$spark_status" "$spark_diag_dir" >&2
  exit "$spark_status"
fi
if ! grep -Fx 'FIXED_WIDTH_ORIGINAL_UPDATES_OK' "$spark_diag_dir/stdout"; then
  printf 'Spark original_updates sentinel missing; input, stdout, stderr and exit retained at %s\n' "$spark_diag_dir" >&2
  exit 1
fi
printf 'Spark original_updates evidence retained at %s\n' "$spark_diag_dir"

-- query 7
-- A NULL parent hides any unreferenced child bytes from width validation.
UPDATE iceberg_compat_${suite_uuid0}.${case_db}.spark_fixed_width_${uuid0}
SET st=st WHERE id=2;

-- query 8
-- @result_contains=FIXED_WIDTH_NULL_PARENT_OK
shell: set -eu
interop_root="${NOVAROCKS_WORKSPACE_ROOT:-.}/logs/sql-iceberg-fixed-width/${uuid0}"
mkdir -p "$interop_root"
umask 077
spark_diag_dir="$(mktemp -d "$interop_root/null_parent-XXXXXX")"
tmp_scala="$spark_diag_dir/input.scala"
cat "${NOVAROCKS_WORKSPACE_ROOT:-.}/tests/sql/fixtures/iceberg-variant/generate.scala" > "$tmp_scala"
cat "$interop_root/fixed_width_mor_oracle.scala" >> "$tmp_scala"
cat >> "$tmp_scala" <<'SCALA'
try {
  val session = org.apache.spark.sql.SparkSession.active
  VariantFixture.initialize(session,"${case_db}","spark_fixed_width_${uuid0}",readOnly=true)
  val table=VariantFixture.load()
  val expectedSchema=new Schema(
    Types.NestedField.required(1,"id",Types.IntegerType.get()),
    Types.NestedField.required(2,"raw",Types.FixedType.ofLength(16)),
    Types.NestedField.optional(3,"st",Types.StructType.of(Types.NestedField.required(5,"f",Types.FixedType.ofLength(4)))),
    Types.NestedField.required(4,"n",Types.IntegerType.get()))
  require(table.schema().asStruct()==expectedSchema.asStruct(),"Write changed the exact fixed declaration or requiredness")
  val rows=session.sql("SELECT id,hex(raw),hex(st.f),n FROM ice_rest.${case_db}.spark_fixed_width_${uuid0} ORDER BY id").collect().toVector
  val actual=rows.map(r => (r.getInt(0),r.getString(1),if(r.isNullAt(2)) None else Some(r.getString(2)),r.getInt(3)))
  val expected:Vector[(Int,String,Option[String],Int)]=Vector((1,"00112233445566778899AABBCCDDEEFF",Some("01020304"),11),(2,"FFEEDDCCBBAA99887766554433221100",None,21))
  require(actual==expected,"Spark fixed-width row oracle differs: "+actual)
  FixedWidthMorOracle.verify(table)

  println("FIXED_WIDTH_NULL_PARENT_OK")
} catch { case failure:Throwable => failure.printStackTrace(); System.exit(1) }
SCALA
spark_status=0
"${NOVAROCKS_WORKSPACE_ROOT:-.}/docker/iceberg-rest/spark-shell.sh" "$tmp_scala" \
  > "$spark_diag_dir/stdout" 2> "$spark_diag_dir/stderr" || spark_status=$?
printf '%s\n' "$spark_status" > "$spark_diag_dir/exit-status"
if [ "$spark_status" -ne 0 ]; then
  printf 'Spark null_parent failed (exit=%s); input, stdout, stderr and exit retained at %s\n' "$spark_status" "$spark_diag_dir" >&2
  exit "$spark_status"
fi
if ! grep -Fx 'FIXED_WIDTH_NULL_PARENT_OK' "$spark_diag_dir/stdout"; then
  printf 'Spark null_parent sentinel missing; input, stdout, stderr and exit retained at %s\n' "$spark_diag_dir" >&2
  exit 1
fi
printf 'Spark null_parent evidence retained at %s\n' "$spark_diag_dir"

-- query 9
INSERT INTO iceberg_compat_${suite_uuid0}.${case_db}.spark_fixed_width_${uuid0} (id,raw,st,n)
SELECT id,raw,st,n FROM iceberg_compat_${suite_uuid0}.${case_db}.spark_fixed_source_${uuid0} WHERE id=3;

-- query 10
-- @result_contains=FIXED_WIDTH_INSERT_OK
shell: set -eu
interop_root="${NOVAROCKS_WORKSPACE_ROOT:-.}/logs/sql-iceberg-fixed-width/${uuid0}"
mkdir -p "$interop_root"
umask 077
spark_diag_dir="$(mktemp -d "$interop_root/insert-XXXXXX")"
tmp_scala="$spark_diag_dir/input.scala"
cat "${NOVAROCKS_WORKSPACE_ROOT:-.}/tests/sql/fixtures/iceberg-variant/generate.scala" > "$tmp_scala"
cat "$interop_root/fixed_width_mor_oracle.scala" >> "$tmp_scala"
cat >> "$tmp_scala" <<'SCALA'
try {
  val session = org.apache.spark.sql.SparkSession.active
  VariantFixture.initialize(session,"${case_db}","spark_fixed_width_${uuid0}",readOnly=true)
  val table=VariantFixture.load()
  val expectedSchema=new Schema(
    Types.NestedField.required(1,"id",Types.IntegerType.get()),
    Types.NestedField.required(2,"raw",Types.FixedType.ofLength(16)),
    Types.NestedField.optional(3,"st",Types.StructType.of(Types.NestedField.required(5,"f",Types.FixedType.ofLength(4)))),
    Types.NestedField.required(4,"n",Types.IntegerType.get()))
  require(table.schema().asStruct()==expectedSchema.asStruct(),"Write changed the exact fixed declaration or requiredness")
  val rows=session.sql("SELECT id,hex(raw),hex(st.f),n FROM ice_rest.${case_db}.spark_fixed_width_${uuid0} ORDER BY id").collect().toVector
  val actual=rows.map(r => (r.getInt(0),r.getString(1),if(r.isNullAt(2)) None else Some(r.getString(2)),r.getInt(3)))
  val expected:Vector[(Int,String,Option[String],Int)]=Vector((1,"00112233445566778899AABBCCDDEEFF",Some("01020304"),11),(2,"FFEEDDCCBBAA99887766554433221100",None,21),(3,"000102030405060708090A0B0C0D0E0F",Some("AABBCCDD"),30))
  require(actual==expected,"Spark fixed-width row oracle differs: "+actual)
  FixedWidthMorOracle.verify(table)

  println("FIXED_WIDTH_INSERT_OK")
} catch { case failure:Throwable => failure.printStackTrace(); System.exit(1) }
SCALA
spark_status=0
"${NOVAROCKS_WORKSPACE_ROOT:-.}/docker/iceberg-rest/spark-shell.sh" "$tmp_scala" \
  > "$spark_diag_dir/stdout" 2> "$spark_diag_dir/stderr" || spark_status=$?
printf '%s\n' "$spark_status" > "$spark_diag_dir/exit-status"
if [ "$spark_status" -ne 0 ]; then
  printf 'Spark insert failed (exit=%s); input, stdout, stderr and exit retained at %s\n' "$spark_status" "$spark_diag_dir" >&2
  exit "$spark_status"
fi
if ! grep -Fx 'FIXED_WIDTH_INSERT_OK' "$spark_diag_dir/stdout"; then
  printf 'Spark insert sentinel missing; input, stdout, stderr and exit retained at %s\n' "$spark_diag_dir" >&2
  exit 1
fi
printf 'Spark insert evidence retained at %s\n' "$spark_diag_dir"

-- query 11
MERGE INTO iceberg_compat_${suite_uuid0}.${case_db}.spark_fixed_width_${uuid0} AS t
USING (SELECT id,raw,st,n FROM iceberg_compat_${suite_uuid0}.${case_db}.spark_fixed_source_${uuid0} WHERE id=4) AS s
ON t.id=s.id
WHEN NOT MATCHED THEN INSERT (id,raw,st,n) VALUES (s.id,s.raw,s.st,s.n);

-- query 12
-- @result_contains=FIXED_WIDTH_MERGE_OK
shell: set -eu
interop_root="${NOVAROCKS_WORKSPACE_ROOT:-.}/logs/sql-iceberg-fixed-width/${uuid0}"
mkdir -p "$interop_root"
umask 077
spark_diag_dir="$(mktemp -d "$interop_root/merge-XXXXXX")"
tmp_scala="$spark_diag_dir/input.scala"
cat "${NOVAROCKS_WORKSPACE_ROOT:-.}/tests/sql/fixtures/iceberg-variant/generate.scala" > "$tmp_scala"
cat "$interop_root/fixed_width_mor_oracle.scala" >> "$tmp_scala"
cat >> "$tmp_scala" <<'SCALA'
try {
  val session = org.apache.spark.sql.SparkSession.active
  VariantFixture.initialize(session,"${case_db}","spark_fixed_width_${uuid0}",readOnly=true)
  val table=VariantFixture.load()
  val expectedSchema=new Schema(
    Types.NestedField.required(1,"id",Types.IntegerType.get()),
    Types.NestedField.required(2,"raw",Types.FixedType.ofLength(16)),
    Types.NestedField.optional(3,"st",Types.StructType.of(Types.NestedField.required(5,"f",Types.FixedType.ofLength(4)))),
    Types.NestedField.required(4,"n",Types.IntegerType.get()))
  require(table.schema().asStruct()==expectedSchema.asStruct(),"Write changed the exact fixed declaration or requiredness")
  val rows=session.sql("SELECT id,hex(raw),hex(st.f),n FROM ice_rest.${case_db}.spark_fixed_width_${uuid0} ORDER BY id").collect().toVector
  val actual=rows.map(r => (r.getInt(0),r.getString(1),if(r.isNullAt(2)) None else Some(r.getString(2)),r.getInt(3)))
  val expected:Vector[(Int,String,Option[String],Int)]=Vector((1,"00112233445566778899AABBCCDDEEFF",Some("01020304"),11),(2,"FFEEDDCCBBAA99887766554433221100",None,21),(3,"000102030405060708090A0B0C0D0E0F",Some("AABBCCDD"),30),(4,"F0F1F2F3F4F5F6F7F8F9FAFBFCFDFEFF",Some("05060708"),40))
  require(actual==expected,"Spark fixed-width row oracle differs: "+actual)
  FixedWidthMorOracle.verify(table)
  VariantFixture.remember(table)
  println("FIXED_WIDTH_MERGE_OK")
} catch { case failure:Throwable => failure.printStackTrace(); System.exit(1) }
SCALA
spark_status=0
"${NOVAROCKS_WORKSPACE_ROOT:-.}/docker/iceberg-rest/spark-shell.sh" "$tmp_scala" \
  > "$spark_diag_dir/stdout" 2> "$spark_diag_dir/stderr" || spark_status=$?
printf '%s\n' "$spark_status" > "$spark_diag_dir/exit-status"
if [ "$spark_status" -ne 0 ]; then
  printf 'Spark merge failed (exit=%s); input, stdout, stderr and exit retained at %s\n' "$spark_status" "$spark_diag_dir" >&2
  exit "$spark_status"
fi
if ! grep -Fx 'FIXED_WIDTH_MERGE_OK' "$spark_diag_dir/stdout"; then
  printf 'Spark merge sentinel missing; input, stdout, stderr and exit retained at %s\n' "$spark_diag_dir" >&2
  exit 1
fi
printf 'Spark merge evidence retained at %s\n' "$spark_diag_dir"

-- query 13
-- @expect_error_tier=target
-- @expect_sql_code=query.execution.invalid_data
-- @expect_sql_phase=Execution
-- @expect_error=[query.execution.invalid_data] Data failure (reason 4:1)
UPDATE iceberg_compat_${suite_uuid0}.${case_db}.spark_fixed_width_${uuid0}
SET raw=X'0011' WHERE id=1;

-- query 14
-- @result_contains=FIXED_WIDTH_WIDTH_REJECTED_OK
shell: set -eu
interop_root="${NOVAROCKS_WORKSPACE_ROOT:-.}/logs/sql-iceberg-fixed-width/${uuid0}"
mkdir -p "$interop_root"
umask 077
spark_diag_dir="$(mktemp -d "$interop_root/width_rejected-XXXXXX")"
tmp_scala="$spark_diag_dir/input.scala"
cat "${NOVAROCKS_WORKSPACE_ROOT:-.}/tests/sql/fixtures/iceberg-variant/generate.scala" > "$tmp_scala"
cat "$interop_root/fixed_width_mor_oracle.scala" >> "$tmp_scala"
cat >> "$tmp_scala" <<'SCALA'
try {
  val session = org.apache.spark.sql.SparkSession.active
  VariantFixture.initialize(session,"${case_db}","spark_fixed_width_${uuid0}",readOnly=true)
  val table=VariantFixture.load()
  val expectedSchema=new Schema(
    Types.NestedField.required(1,"id",Types.IntegerType.get()),
    Types.NestedField.required(2,"raw",Types.FixedType.ofLength(16)),
    Types.NestedField.optional(3,"st",Types.StructType.of(Types.NestedField.required(5,"f",Types.FixedType.ofLength(4)))),
    Types.NestedField.required(4,"n",Types.IntegerType.get()))
  require(table.schema().asStruct()==expectedSchema.asStruct(),"Write changed the exact fixed declaration or requiredness")
  val rows=session.sql("SELECT id,hex(raw),hex(st.f),n FROM ice_rest.${case_db}.spark_fixed_width_${uuid0} ORDER BY id").collect().toVector
  val actual=rows.map(r => (r.getInt(0),r.getString(1),if(r.isNullAt(2)) None else Some(r.getString(2)),r.getInt(3)))
  val expected:Vector[(Int,String,Option[String],Int)]=Vector((1,"00112233445566778899AABBCCDDEEFF",Some("01020304"),11),(2,"FFEEDDCCBBAA99887766554433221100",None,21),(3,"000102030405060708090A0B0C0D0E0F",Some("AABBCCDD"),30),(4,"F0F1F2F3F4F5F6F7F8F9FAFBFCFDFEFF",Some("05060708"),40))
  require(actual==expected,"Spark fixed-width row oracle differs: "+actual)
  FixedWidthMorOracle.verify(table)
  VariantFixture.unchanged()
  println("FIXED_WIDTH_WIDTH_REJECTED_OK")
} catch { case failure:Throwable => failure.printStackTrace(); System.exit(1) }
SCALA
spark_status=0
"${NOVAROCKS_WORKSPACE_ROOT:-.}/docker/iceberg-rest/spark-shell.sh" "$tmp_scala" \
  > "$spark_diag_dir/stdout" 2> "$spark_diag_dir/stderr" || spark_status=$?
printf '%s\n' "$spark_status" > "$spark_diag_dir/exit-status"
if [ "$spark_status" -ne 0 ]; then
  printf 'Spark width_rejected failed (exit=%s); input, stdout, stderr and exit retained at %s\n' "$spark_status" "$spark_diag_dir" >&2
  exit "$spark_status"
fi
if ! grep -Fx 'FIXED_WIDTH_WIDTH_REJECTED_OK' "$spark_diag_dir/stdout"; then
  printf 'Spark width_rejected sentinel missing; input, stdout, stderr and exit retained at %s\n' "$spark_diag_dir" >&2
  exit 1
fi
printf 'Spark width_rejected evidence retained at %s\n' "$spark_diag_dir"

-- query 15
-- @expect_error_tier=target
-- @expect_sql_code=query.execution.invalid_data
-- @expect_sql_phase=Execution
-- @expect_error=[query.execution.invalid_data] Data failure (reason 7:498)
MERGE INTO iceberg_compat_${suite_uuid0}.${case_db}.spark_fixed_width_${uuid0} AS t
USING iceberg_compat_${suite_uuid0}.${case_db}.spark_fixed_source_${uuid0} AS s
ON t.id=s.id AND t.id=1
WHEN MATCHED THEN UPDATE SET st=s.st;

-- query 16
-- @result_contains=FIXED_WIDTH_REQUIRED_REJECTED_OK
shell: set -eu
interop_root="${NOVAROCKS_WORKSPACE_ROOT:-.}/logs/sql-iceberg-fixed-width/${uuid0}"
mkdir -p "$interop_root"
umask 077
spark_diag_dir="$(mktemp -d "$interop_root/required_rejected-XXXXXX")"
tmp_scala="$spark_diag_dir/input.scala"
cat "${NOVAROCKS_WORKSPACE_ROOT:-.}/tests/sql/fixtures/iceberg-variant/generate.scala" > "$tmp_scala"
cat "$interop_root/fixed_width_mor_oracle.scala" >> "$tmp_scala"
cat >> "$tmp_scala" <<'SCALA'
try {
  val session = org.apache.spark.sql.SparkSession.active
  VariantFixture.initialize(session,"${case_db}","spark_fixed_width_${uuid0}",readOnly=true)
  val table=VariantFixture.load()
  val expectedSchema=new Schema(
    Types.NestedField.required(1,"id",Types.IntegerType.get()),
    Types.NestedField.required(2,"raw",Types.FixedType.ofLength(16)),
    Types.NestedField.optional(3,"st",Types.StructType.of(Types.NestedField.required(5,"f",Types.FixedType.ofLength(4)))),
    Types.NestedField.required(4,"n",Types.IntegerType.get()))
  require(table.schema().asStruct()==expectedSchema.asStruct(),"Write changed the exact fixed declaration or requiredness")
  val rows=session.sql("SELECT id,hex(raw),hex(st.f),n FROM ice_rest.${case_db}.spark_fixed_width_${uuid0} ORDER BY id").collect().toVector
  val actual=rows.map(r => (r.getInt(0),r.getString(1),if(r.isNullAt(2)) None else Some(r.getString(2)),r.getInt(3)))
  val expected:Vector[(Int,String,Option[String],Int)]=Vector((1,"00112233445566778899AABBCCDDEEFF",Some("01020304"),11),(2,"FFEEDDCCBBAA99887766554433221100",None,21),(3,"000102030405060708090A0B0C0D0E0F",Some("AABBCCDD"),30),(4,"F0F1F2F3F4F5F6F7F8F9FAFBFCFDFEFF",Some("05060708"),40))
  require(actual==expected,"Spark fixed-width row oracle differs: "+actual)
  FixedWidthMorOracle.verify(table)
  VariantFixture.unchanged()
  println("FIXED_WIDTH_REQUIRED_REJECTED_OK")
} catch { case failure:Throwable => failure.printStackTrace(); System.exit(1) }
SCALA
spark_status=0
"${NOVAROCKS_WORKSPACE_ROOT:-.}/docker/iceberg-rest/spark-shell.sh" "$tmp_scala" \
  > "$spark_diag_dir/stdout" 2> "$spark_diag_dir/stderr" || spark_status=$?
printf '%s\n' "$spark_status" > "$spark_diag_dir/exit-status"
if [ "$spark_status" -ne 0 ]; then
  printf 'Spark required_rejected failed (exit=%s); input, stdout, stderr and exit retained at %s\n' "$spark_status" "$spark_diag_dir" >&2
  exit "$spark_status"
fi
if ! grep -Fx 'FIXED_WIDTH_REQUIRED_REJECTED_OK' "$spark_diag_dir/stdout"; then
  printf 'Spark required_rejected sentinel missing; input, stdout, stderr and exit retained at %s\n' "$spark_diag_dir" >&2
  exit 1
fi
printf 'Spark required_rejected evidence retained at %s\n' "$spark_diag_dir"

-- query 17
SELECT id,hex(CAST(raw AS VARBINARY)) AS raw_hex,st IS NULL AS nested_is_null,n
FROM iceberg_compat_${suite_uuid0}.${case_db}.spark_fixed_width_${uuid0} ORDER BY id;

-- query 18
-- @cleanup=true
-- @skip_result_check=true
DROP TABLE IF EXISTS iceberg_compat_${suite_uuid0}.${case_db}.spark_fixed_width_${uuid0} ;
DROP TABLE IF EXISTS iceberg_compat_${suite_uuid0}.${case_db}.spark_fixed_source_${uuid0} ;
