// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

import org.apache.avro.{Schema => AvroSchema}
import org.apache.avro.file.{CodecFactory, DataFileStream, DataFileWriter}
import org.apache.avro.generic.{GenericData, GenericDatumReader, GenericDatumWriter, GenericRecord => AvroRecord}

object DeleteApplicabilityAvro {
  import DeleteApplicabilityFixture._
  case class Container(schema: AvroSchema, metadata: Vector[(String, Array[Byte])], rows: Vector[AvroRecord])
  def copy(r: AvroRecord): AvroRecord = GenericData.get().deepCopy(r.getSchema, r)
  def readAvro(io: FileIO, path: String): Container = {
    val reader = new DataFileStream[AvroRecord](io.newInputFile(path).newStream(), new GenericDatumReader[AvroRecord]())
    try Container(reader.getSchema, reader.getMetaKeys.asScala.toVector
      .filterNot(_.startsWith("avro.")).map(k => (k, reader.getMeta(k))),
      reader.iterator().asScala.map(copy).toVector)
    finally reader.close()
  }
  def writeAvro(io: FileIO, path: String, c: Container): Unit = {
    val writer = new DataFileWriter[AvroRecord](new GenericDatumWriter[AvroRecord](c.schema))
    c.metadata.foreach { case (k, v) => writer.setMeta(k, v) }
    writer.setCodec(CodecFactory.nullCodec())
    writer.create(c.schema, io.newOutputFile(path).create())
    try c.rows.foreach(writer.append) finally writer.close()
  }
  def file(r: AvroRecord): AvroRecord = r.get("data_file").asInstanceOf[AvroRecord]
  def patch(r: AvroRecord)(f: AvroRecord => Unit): AvroRecord = { val c = copy(r); f(c); c }
  def rawEntry(r: AvroRecord): JsonNode = mapper.readTree(r.toString)
  def unique(t: Table, name: String, suffix: String): String =
    t.location() + "/metadata/g02-" + name + "-" + java.util.UUID.randomUUID().toString + suffix
}
