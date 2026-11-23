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

use novarocks_proto_codec::{FieldPath, root_result};
use novarocks_proto_models::result as wire;
use novarocks_result_contract::{
    ClientPresentationFamily, ClientRenderSchema, NativeRenderType, RenderColumn, RenderField,
    RenderPresentation, TrinoJsonFacts,
};
use novarocks_task_codec::resource_preflight::check_client_render_schema;
use prost::Message;

#[test]
fn root_result_uuid_is_a_parameterless_leaf_through_preflight_and_prost() {
    let uuid = RenderField {
        native_type: NativeRenderType::Uuid,
        presentation: RenderPresentation::TrinoValue,
        nullable: true,
    };
    let schema = ClientRenderSchema::try_new(
        ClientPresentationFamily::TrinoJson(TrinoJsonFacts::default()),
        vec![
            RenderColumn {
                source_ordinal: 0,
                source_slot: Some(10),
                name: "uuid".into(),
                field: uuid.clone(),
            },
            RenderColumn {
                source_ordinal: 1,
                source_slot: Some(11),
                name: "list".into(),
                field: RenderField {
                    native_type: NativeRenderType::List(Box::new(uuid)),
                    presentation: RenderPresentation::TrinoValue,
                    nullable: false,
                },
            },
        ],
        2,
    )
    .unwrap();
    let encoded = root_result::encode_client_schema(&schema).encode_to_vec();
    check_client_render_schema(&encoded).unwrap();
    let mut decoded = wire::ClientRenderSchema::decode(encoded.as_slice()).unwrap();
    assert_eq!(
        root_result::decode_client_schema(&decoded, 2, FieldPath::root("schema")).unwrap(),
        schema,
    );

    let leaf = decoded
        .field_nodes
        .iter_mut()
        .filter_map(|node| node.native_type.as_mut())
        .find(|ty| ty.kind == wire::RenderTypeKind::Uuid as i32)
        .unwrap();
    leaf.bits = 128;
    assert!(root_result::decode_client_schema(&decoded, 2, FieldPath::root("schema")).is_err());
}
