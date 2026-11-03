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

use super::*;
use arrow::datatypes::{DataType, Field};
use novarocks_proto_models::physical_package_v2 as raw;
use novarocks_type_contract::ArrowTypeProfile;
use std::sync::Arc;

struct Control;
impl PureCompileControl for Control {
    fn checkpoint(&self, _: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        assert!(units <= 256);
        Ok(())
    }
}
fn projection() -> TypeProjectionLimits {
    TypeProjectionLimits {
        max_definitions: 20000,
        max_expanded_nodes: 1_000_000,
        max_string_bytes: 1024 * 1024,
    }
}
fn receiving() -> PackageTypeProjectionLimits {
    PackageTypeProjectionLimits {
        max_definitions: 20000,
        max_expanded_nodes: 1_000_000,
        max_string_bytes: 1024 * 1024,
        max_allocation_requests: 1_000_000,
        max_allocation_request_bytes: 512 * 1024 * 1024,
        max_coexisting_source_and_request_bytes: 1024 * 1024 * 1024,
        max_work: usize::MAX / 4,
    }
}
fn maps(logical_depth: usize) -> DataType {
    let mut value = DataType::Int64;
    for _ in 1..logical_depth {
        value = DataType::Map(
            Arc::new(Field::new(
                "entries",
                DataType::Struct(
                    vec![
                        Arc::new(Field::new("key", DataType::Int32, false)),
                        Arc::new(Field::new("value", value, false)),
                    ]
                    .into(),
                ),
                false,
            )),
            false,
        );
    }
    value
}
fn receive(package: &raw::FragmentPackage) -> Result<DecodedTypeTable, TypeCodecError> {
    let mut work = CompileCheckpoints::try_new(&Control, CompilePhase::Decode)?;
    let result = decode_package_type_table_observed(
        package,
        128 * 1024 * 1024,
        receiving(),
        &mut |_| Ok(()),
        &mut work,
    );
    if !matches!(result, Err(TypeCodecError::Control(_))) {
        work.finish()?;
    }
    result
}

#[test]
fn execution_map_64_roundtrips_through_real_value_and_schema_occurrences() {
    let value = FunctionValueType::new(maps(64), false);
    let field = Arc::new(Field::new("root", value.data_type().clone(), false));
    let values = [(7, &value)];
    let fields = [BorrowedFieldTypeRoot {
        id: 19,
        field: &field,
        value_type_id: Some(7),
    }];
    let mut work = CompileCheckpoints::try_new(&Control, CompilePhase::Encode).unwrap();
    let types = encode_borrowed_type_table_associated_sources_pure_in(
        &values,
        &fields,
        projection(),
        &mut work,
    )
    .unwrap()
    .into_wire();
    work.finish().unwrap();
    let package = raw::FragmentPackage {
        types: Some(types),
        source_fields: Some(wire::SourceFieldTable {
            shape: Some(wire::TypeTable::default()),
            field_facts: vec![],
        }),
        schemas: vec![raw::SchemaDefinition {
            id: 0,
            field_ids: vec![19],
        }],
        ..Default::default()
    };
    let decoded = receive(&package).unwrap();
    assert_eq!(decoded.value_type(7), Some(&value));
    assert_eq!(decoded.field_execution_type(19), Some(&value));
}

#[test]
fn actual_unbound_physical_root_cannot_borrow_a_value_profiles_larger_carrier_depth() {
    let value = FunctionValueType::new(maps(64), false);
    let mut table = encode_type_table(&[(7, value)], projection(), &Control).unwrap();
    let carrier = table.value_types[0].carrier_type_id;
    table.fields.push(wire::FieldDefinition {
        id: u32::MAX,
        name: "source".into(),
        nullable: false,
        carrier_type_id: carrier,
        logical_type_id: None,
        value_binding_id: None,
    });
    let package = raw::FragmentPackage {
        types: Some(table),
        schemas: vec![raw::SchemaDefinition {
            id: 0,
            field_ids: vec![u32::MAX],
        }],
        ..Default::default()
    };
    assert!(matches!(
        receive(&package),
        Err(TypeCodecError::ValueType(ValueTypeError::TooDeep))
    ));
}

#[test]
fn canonical_profile_does_not_turn_a_generic_struct_190_into_a_logical_value() {
    let mut value = DataType::Int64;
    for _ in 1..190 {
        value = DataType::Struct(vec![Arc::new(Field::new("child", value, false))].into());
    }
    assert!(
        novarocks_type_contract::validate_arrow_layout_structure_with_profile_observed::<
            ValueTypeError,
        >(&value, ArrowTypeProfile::Value, |_| Ok(()))
        .is_err()
    );
    assert!(
        novarocks_type_contract::validate_arrow_layout_structure_with_profile_observed::<
            ValueTypeError,
        >(&value, ArrowTypeProfile::CanonicalValue, |_| Ok(()))
        .is_err()
    );
}

#[test]
fn legacy_pure_dto_decoder_retains_its_explicit_physical64_policy() {
    let value = FunctionValueType::new(maps(64), false);
    let table = encode_type_table(&[(7, value)], projection(), &Control).unwrap();
    assert!(matches!(
        decode_type_table_pure(&table, projection(), &Control),
        Err(TypeCodecError::ValueType(ValueTypeError::TooDeep))
    ));
}
