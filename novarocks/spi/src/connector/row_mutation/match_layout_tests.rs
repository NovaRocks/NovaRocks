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

use super::typed_execution_fields_tests::value;
use super::*;
use novarocks_type_contract::FunctionValueType;

fn owner() -> ConnectorProviderBindingKey {
    let instance_id = super::super::ConnectorInstanceId::parse("iceberg").unwrap();
    ConnectorProviderBindingKey {
        instance_id,
        incarnation: super::super::ProviderBindingEpoch::from_bytes([7; 16]),
    }
}

fn token(ordinal: u32) -> ConnectorWriteFieldToken {
    let mut bytes = [0; 32];
    bytes[..4].copy_from_slice(&ordinal.to_be_bytes());
    ConnectorWriteFieldToken::from_bytes(bytes)
}

fn identity() -> ConnectorMutationSourceField {
    let physical = FunctionValueType::new(DataType::Int64, false);
    ConnectorMutationSourceField::try_new_typed(
        token(0),
        Field::new("id", DataType::Int64, false),
        0,
        physical,
    )
    .unwrap()
}

fn contract(
    after: Vec<ConnectorMutationTargetField>,
    check: impl FnMut() -> Result<(), ConnectorError>,
) -> Result<ConnectorMutationMatchContract, ConnectorError> {
    let owner = owner();
    let table =
        ConnectorTableHandle::try_new(owner.instance_id.clone(), Bytes::from_static(b"table"))
            .unwrap();
    let base = ConnectorWriteBaseVersion::try_new(Bytes::from_static(b"base")).unwrap();
    let last = u32::try_from(after.len()).unwrap() + 1;
    ConnectorMutationMatchContract::try_new_observed(
        owner,
        table,
        base,
        vec![identity()],
        vec![],
        after,
        ConnectorMutationEffectField::try_new(
            token(last),
            Field::new("effect", DataType::Int8, false),
            last,
        )
        .unwrap(),
        check,
    )
}

fn target(
    name: impl Into<String>,
    ordinal: u32,
    value: FunctionValueType,
) -> ConnectorMutationTargetField {
    let field = Field::new(name.into(), value.data_type().clone(), value.nullable());
    ConnectorMutationTargetField::try_new_typed(token(ordinal), field, ordinal, value).unwrap()
}

#[test]
fn wide_match_layout_validates_actual_fields_and_preserves_roles() {
    let value = FunctionValueType::new(DataType::Int64, false);
    let wide = (1..=4097)
        .map(|ordinal| target(format!("column_{ordinal}"), ordinal, value.clone()))
        .collect::<Vec<_>>();
    let matched = contract(wide.clone(), || Ok(())).unwrap();
    assert_eq!(matched.after_fields().len(), 4097);
    assert_eq!(matched.effect_field().target_ordinal(), 4098);
    let owner = owner();
    let mut schema = vec![identity().field().clone()];
    schema.extend(wide.iter().map(|field| field.field().clone()));
    let schema = Arc::new(Schema::new(schema));
    let table = matched.table().clone();
    let base = matched.base_version().clone();
    let operation = ConnectorWriteOperationId::from_bytes([9; 16]);
    let prepared = ConnectorRowMutationPreparation::try_new(
        owner,
        operation,
        table.clone(),
        table,
        schema,
        ConnectorWriteTargetRef::main(),
        ConnectorRowMutationIntent::Update,
        base,
        matched,
        ConnectorRowMutationStrategy::MergeOnRead,
        None,
        Bytes::from_static(b"payload"),
    )
    .unwrap();
    assert_eq!(prepared.match_source_schema().fields().len(), 4098);
    assert_eq!(
        prepared.match_contract().after_fields()[4096]
            .field()
            .name(),
        "column_4097"
    );
    let mut wrong_order = wide.clone();
    wrong_order.swap(123, 124);
    assert!(contract(wrong_order, || Ok(())).is_err());
    let mut duplicate_name = wide;
    duplicate_name[1] = target("column_1", 2, value);
    assert!(contract(duplicate_name, || Ok(())).is_err());
}

#[test]
fn match_layout_observer_refusal_keeps_the_original_failure() {
    let mut count = 0;
    let failure = contract(
        vec![target(
            "value",
            1,
            FunctionValueType::new(DataType::Int64, false),
        )],
        || {
            count += 1;
            assert!(count <= 3);
            if count == 3 {
                return Err(ConnectorError::observed_cancellation(
                    "original match layout stopped",
                ));
            }
            Ok(())
        },
    )
    .unwrap_err();
    assert_eq!(count, 3);
    assert_eq!(failure.kind(), ConnectorErrorKind::Cancelled);
}
