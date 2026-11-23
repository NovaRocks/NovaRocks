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

use arrow::datatypes::{DataType as D, Field};
use novarocks_physical_plan::{
    FragmentId, NodeId, OutputPort, ResultField, ResultPort, ResultValueDomain, ValueId, ValueType,
};
use novarocks_result_contract::{NativeRenderType as R, RenderPresentation, TrinoJsonFacts};
use novarocks_sql::compiler::{
    ClientRenderSchemaError, RenderFamilyRequest, client_render_schema_from_port,
    validate_public_result_logical_identities,
};
use novarocks_type_contract::{
    ValueKind, ValueLayout, ValueLogicalType, ValueTypeRef, ValueTypeStorageLifetime,
};

fn port(ty: ValueType) -> ResultPort {
    ResultPort {
        scalar_schema: None,
        fragment: FragmentId::new(1),
        output: OutputPort {
            node: NodeId::new(1),
            columns: Box::new([ValueId::new(1)]),
        },
        fields: Box::new([ResultField {
            name: "original".into(),
            alias: None,
            value: ValueId::new(1),
            domain: ResultValueDomain::Plain,
            ty,
        }]),
    }
}

#[test]
fn root_render_type_uses_nominal_uuid_and_keeps_binary_distinct() {
    let facts = TrinoJsonFacts::default();
    let family = RenderFamilyRequest::TrinoJson(&facts);
    for (ty, expected) in [
        (
            ValueType::try_with_logical_type(D::FixedSizeBinary(16), true, ValueLogicalType::Uuid)
                .unwrap(),
            R::Uuid,
        ),
        (ValueType::new(D::Binary, true), R::Binary),
        (ValueType::new(D::Utf8, true), R::String),
    ] {
        let port = port(ty);
        validate_public_result_logical_identities(Some(&port)).unwrap();
        let schema = client_render_schema_from_port(Some(&port), family).unwrap();
        assert_eq!(schema.columns()[0].field.native_type, expected);
    }
    let wide = port(
        ValueType::try_with_logical_type(D::FixedSizeBinary(16), true, ValueLogicalType::LargeInt)
            .unwrap(),
    );
    assert_eq!(
        client_render_schema_from_port(Some(&wide), family),
        Err(ClientRenderSchemaError::UnsupportedClientType {
            column: "original".into(),
            native_type: R::LargeInt,
        }),
    );
    let fixed = port(ValueType::new(D::FixedSizeBinary(16), true));
    assert!(client_render_schema_from_port(Some(&fixed), family).is_err());
}

#[test]
fn root_render_type_preserves_nested_uuid_and_variant_declarations() {
    let facts = TrinoJsonFacts::default();
    for (storage, nominal, expected) in [
        (D::FixedSizeBinary(16), ValueLogicalType::Uuid, R::Uuid),
        (D::LargeBinary, ValueLogicalType::Variant, R::Variant),
    ] {
        let hold = ValueTypeStorageLifetime::pure();
        let child = ValueType::try_with_logical_type(storage.clone(), true, nominal).unwrap();
        let semantic = ValueTypeRef::try_new(
            ValueKind::List(child.value_type().clone()),
            true,
            hold.clone(),
        )
        .unwrap();
        let layout = D::List(Arc::new(Field::new("item", storage, true)));
        let ty = ValueType::try_from_value(semantic, ValueLayout::try_new(&layout, hold).unwrap())
            .unwrap();
        let port = port(ty);
        let schema =
            client_render_schema_from_port(Some(&port), RenderFamilyRequest::TrinoJson(&facts))
                .unwrap();
        let R::List(child) = &schema.columns()[0].field.native_type else {
            panic!("expected a list render field");
        };
        assert_eq!(child.native_type, expected);
        assert_eq!(child.presentation, RenderPresentation::TrinoValue);
    }
}
