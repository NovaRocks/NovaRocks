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
use novarocks_type_contract::{DecimalOverflowPolicy, FunctionValueType, ValueLogicalType};

fn source(value_type: FunctionValueType) -> TypedExpr {
    TypedExpr {
        kind: ExprKind::ColumnRef {
            column_id: crate::column_id::ColumnId(42),
            qualifier: Some("fixture".into()),
            column: "index".into(),
        },
        value_type,
    }
}

#[test]
fn implicit_signed_cast_nullability_preserves_complete_source_and_policy() {
    let signed = [
        DataType::Int8,
        DataType::Int16,
        DataType::Int32,
        DataType::Int64,
    ];
    for (source_width, carrier) in signed.iter().enumerate() {
        for (target_width, target) in signed.iter().enumerate() {
            for nullable in [false, true] {
                for policy in [
                    DecimalOverflowPolicy::OutputNull,
                    DecimalOverflowPolicy::ReportError,
                ] {
                    let input = source(FunctionValueType::new(carrier.clone(), nullable));
                    let output = cast_null_preserving_target_type(input.clone(), target, policy);
                    eprintln!(
                        "signed constructor source={:?} target={target:?} policy={policy:?} actual={:?}",
                        input.value_type, output.value_type
                    );
                    assert_eq!(
                        output.value_type,
                        FunctionValueType::new(
                            target.clone(),
                            nullable || target_width < source_width
                        )
                    );
                    if source_width == target_width {
                        assert_eq!(format!("{output:?}"), format!("{input:?}"));
                    } else {
                        let ExprKind::Cast {
                            expr,
                            target: actual_target,
                            decimal_overflow_policy,
                        } = output.kind
                        else {
                            panic!("runtime cast");
                        };
                        assert_eq!(format!("{expr:?}"), format!("{input:?}"));
                        assert_eq!(actual_target, *target);
                        assert_eq!(decimal_overflow_policy, policy);
                    }
                }
            }
        }
    }
}

#[test]
fn same_carrier_keeps_nominal_and_nested_type_identity() {
    let nested = DataType::List(Arc::new(
        arrow::datatypes::Field::new("original_item", DataType::Int32, false)
            .with_metadata([("provider.field_id".into(), "41".into())].into()),
    ));
    let types = [
        FunctionValueType::new(nested, false),
        FunctionValueType::try_with_logical_type(
            DataType::FixedSizeBinary(16),
            false,
            ValueLogicalType::LargeInt,
        )
        .unwrap(),
    ];
    for ty in types {
        let input = source(ty);
        let output = cast_null_preserving_target_type(
            input.clone(),
            &input.value_type.data_type,
            DecimalOverflowPolicy::ReportError,
        );
        assert_eq!(format!("{output:?}"), format!("{input:?}"));
    }
}

#[test]
fn non_signed_constructor_keeps_existing_nullability_and_target_metadata() {
    let nested = DataType::List(Arc::new(
        arrow::datatypes::Field::new("original_item", DataType::Int16, true)
            .with_metadata([("provider.field_id".into(), "41".into())].into()),
    ));
    for (carrier, target) in [
        (DataType::Boolean, DataType::Int8),
        (DataType::Float64, DataType::Int32),
        (DataType::UInt64, DataType::Int32),
        (
            DataType::Date32,
            DataType::Timestamp(TimeUnit::Microsecond, None),
        ),
        (
            DataType::List(Arc::new(arrow::datatypes::Field::new(
                "source",
                DataType::Int32,
                true,
            ))),
            nested,
        ),
    ] {
        for nullable in [false, true] {
            let output = cast_null_preserving_target_type(
                source(FunctionValueType::new(carrier.clone(), nullable)),
                &target,
                DecimalOverflowPolicy::OutputNull,
            );
            assert_eq!(
                output.value_type,
                FunctionValueType::new(target.clone(), nullable)
            );
        }
    }
}
