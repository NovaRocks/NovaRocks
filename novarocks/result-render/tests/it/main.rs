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

use std::sync::Arc;

use arrow::array::builder::FixedSizeBinaryBuilder;
use arrow::array::{Array, ArrayRef, BinaryArray, ListArray, StructArray};
use arrow::buffer::{OffsetBuffer, ScalarBuffer};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use novarocks_result_contract::{
    ClientPresentationFamily, ClientRenderSchema, NamedRenderField, NativeRenderType, RenderColumn,
    RenderField, RenderPresentation, RootProfileV1, TrinoJsonFacts,
};
use novarocks_result_render::{
    ArrowTrinoJsonEncoder, BoundedClientRowEncoder, RenderErrorKind, RenderTurnStatus,
};

fn field(native_type: NativeRenderType, nullable: bool) -> RenderField {
    RenderField {
        native_type,
        nullable,
        presentation: RenderPresentation::TrinoValue,
    }
}

fn schema(fields: Vec<RenderField>) -> Arc<ClientRenderSchema> {
    let count = fields.len();
    Arc::new(
        ClientRenderSchema::try_new(
            ClientPresentationFamily::TrinoJson(TrinoJsonFacts::default()),
            fields
                .into_iter()
                .enumerate()
                .map(|(ordinal, field)| RenderColumn {
                    source_ordinal: ordinal as u32,
                    source_slot: None,
                    name: ordinal.to_string(),
                    field,
                })
                .collect(),
            count,
        )
        .unwrap(),
    )
}

#[test]
fn uuid_uses_canonical_text_in_top_level_and_nested_values() {
    let bytes = [
        0xf2, 0x4f, 0x9b, 0x64, 0x81, 0xfa, 0x49, 0xd1, 0xb7, 0x4e, 0x8c, 0x09, 0xa6, 0xe3, 0x1c,
        0x56,
    ];
    let mut fixed = FixedSizeBinaryBuilder::new(16);
    fixed.append_value(bytes).unwrap();
    fixed.append_null();
    let fixed: ArrayRef = Arc::new(fixed.finish());
    let binary: ArrayRef = Arc::new(BinaryArray::from(vec![Some(bytes.as_slice()), None]));
    let list = ListArray::new(
        Arc::new(Field::new("item", DataType::FixedSizeBinary(16), true)),
        OffsetBuffer::new(ScalarBuffer::from(vec![0, 2, 2])),
        fixed.clone(),
        None,
    );
    let list_field = Arc::new(Field::new("items", list.data_type().clone(), false));
    let row: ArrayRef = Arc::new(StructArray::new(
        vec![list_field].into(),
        vec![Arc::new(list)],
        None,
    ));
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("uuid", DataType::FixedSizeBinary(16), true),
            Field::new("raw", DataType::Binary, true),
            Field::new("nested", row.data_type().clone(), false),
        ])),
        vec![fixed, binary, row],
    )
    .unwrap();
    let uuid = field(NativeRenderType::Uuid, true);
    let schema = schema(vec![
        uuid.clone(),
        field(NativeRenderType::Binary, true),
        field(
            NativeRenderType::Struct(vec![NamedRenderField {
                name: "items".into(),
                field: field(NativeRenderType::List(Box::new(uuid)), false),
            }]),
            false,
        ),
    ]);
    let mut encoder = ArrowTrinoJsonEncoder::try_new(schema, batch).unwrap();
    let mut out = [0; 64];
    let mut encoded = Vec::new();
    let mut completed = false;
    for _ in 0..32 {
        let turn = encoder.step(&mut out).unwrap();
        assert!(turn.emitted_bytes + turn.examined_bytes <= RootProfileV1::EMIT_BYTES_PER_TURN);
        encoded.extend_from_slice(&out[..turn.emitted_bytes]);
        if turn.status == RenderTurnStatus::InputComplete {
            completed = true;
            break;
        }
    }
    assert!(completed);
    let expected: [&[u8]; 2] = [
        br#"["f24f9b64-81fa-49d1-b74e-8c09a6e31c56","8k+bZIH6SdG3TowJpuMcVg==",[["f24f9b64-81fa-49d1-b74e-8c09a6e31c56",null]]]"#,
        br#"[null,null,[[]]]"#,
    ];
    let mut actual = encoded.as_slice();
    for payload in expected {
        let len = u32::from_le_bytes(actual[..4].try_into().unwrap()) as usize;
        assert_eq!(len, payload.len());
        assert_eq!(&actual[4..4 + len], payload);
        actual = &actual[4 + len..];
    }
    assert!(actual.is_empty());
}

#[test]
fn uuid_refuses_wrong_width_before_row_encoding() {
    let mut fixed = FixedSizeBinaryBuilder::new(15);
    fixed.append_value([0; 15]).unwrap();
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new(
            "uuid",
            DataType::FixedSizeBinary(15),
            false,
        )])),
        vec![Arc::new(fixed.finish())],
    )
    .unwrap();
    let schema = schema(vec![field(NativeRenderType::Uuid, false)]);
    let error = ArrowTrinoJsonEncoder::try_new(schema, batch).err().unwrap();
    assert_eq!(error.kind, RenderErrorKind::UnsupportedCarrier);
    assert_eq!(error.output_ordinal, Some(0));
}
