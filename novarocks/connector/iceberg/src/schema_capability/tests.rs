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

//! Correctness contract maintained by the Iceberg provider. The original
//! SDK writer/reader is the independent oracle for the SDK capability edge.
//! All files are owned by a per-test memory FileIO and disappear with it.

use super::*;
use crate::iceberg::io::FileIO;
use crate::iceberg::spec::{
    DataContentType, DataFileBuilder, DataFileFormat, ListType, Manifest, ManifestWriterBuilder,
    NestedField, PartitionSpec, PrimitiveType, Struct, StructType, Type,
};
use std::sync::Arc;

fn schema(nesting: usize, arrays: bool) -> Schema {
    let mut ty = Type::Primitive(PrimitiveType::Int);
    for level in (0..nesting).rev() {
        let id = i32::try_from(level + 2).unwrap();
        ty = if arrays {
            Type::List(ListType::new(Arc::new(NestedField::list_element(
                id, ty, false,
            ))))
        } else {
            Type::Struct(StructType::new(vec![Arc::new(NestedField::optional(
                id,
                format!("n{level}"),
                ty,
            ))]))
        };
    }
    Schema::builder()
        .with_fields(vec![Arc::new(NestedField::optional(1, "deep", ty))])
        .build()
        .unwrap()
}

#[test]
fn ddl_admission_agrees_with_original_sdk_schema_deserializer() {
    for (depth, arrays, admitted) in [(41, false, true), (42, false, false), (63, true, true)] {
        let schema = schema(depth, arrays);
        let json = serde_json::to_vec(&schema).unwrap();
        assert_eq!(serde_json::from_slice::<Schema>(&json).is_ok(), admitted);
        match admit_schema(&schema) {
            Ok(()) => assert!(admitted),
            Err(error) => {
                assert!(!admitted);
                assert_eq!(error.kind(), ConnectorErrorKind::Unsupported);
                assert!(error.message().contains("iceberg-rust 0.11"));
                assert!(error.message().contains("official iceberg-rust release"));
                assert!(error.message().len() <= 512);
            }
        }
    }
}

#[tokio::test]
async fn manifest_preflight_agrees_with_sdk_writer_reader_at_struct_boundary() {
    for (depth, arrays, admitted) in [(41, false, true), (42, false, false), (63, true, true)] {
        let schema = Arc::new(schema(depth, arrays));
        let io = FileIO::new_with_memory();
        let output = io.new_output("memory:///schema-boundary.avro").unwrap();
        let mut writer = ManifestWriterBuilder::new(
            output,
            Some(7),
            Arc::clone(&schema),
            PartitionSpec::unpartition_spec(),
        )
        .build_v2_data();
        let file = DataFileBuilder::default()
            .content(DataContentType::Data)
            .file_path("memory:///data.parquet".to_owned())
            .file_format(DataFileFormat::Parquet)
            .partition(Struct::empty())
            .record_count(1)
            .file_size_in_bytes(1)
            .build()
            .unwrap();
        writer.add_file(file, 0).unwrap();
        let descriptor = writer.write_manifest_file().await.unwrap();
        let bytes = io
            .new_input(&descriptor.manifest_path)
            .unwrap()
            .read()
            .await
            .unwrap();
        // Keep the oracle independent: call the original SDK directly even
        // for the case that NovaRocks refuses before handing it the bytes.
        let sdk = Manifest::parse_avro(&bytes);
        assert_eq!(sdk.is_ok(), admitted, "depth {depth}, arrays {arrays}");
        let actual = crate::manifest::decode_manifest(&bytes, &descriptor);
        match actual {
            Ok(decoded) => {
                assert!(admitted);
                assert_eq!(decoded.metadata().schema.as_ref(), schema.as_ref());
                assert_eq!(decoded.entries().len(), 1);
            }
            Err(error) => {
                assert!(!admitted);
                assert_eq!(error.kind(), crate::iceberg::ErrorKind::FeatureUnsupported);
                assert!(
                    error
                        .message()
                        .contains("Iceberg schema JSON exceeds 127 container levels")
                );
                assert_eq!(
                    sdk.unwrap_err().kind(),
                    crate::iceberg::ErrorKind::DataInvalid
                );
            }
        }
    }
}
