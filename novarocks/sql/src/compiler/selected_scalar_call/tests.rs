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
use arrow::datatypes::TimeUnit;

#[test]
fn comparison_conversions_keep_each_side_nullability_and_late_output_null_policy() {
    let functions = crate::functions::builtin_sql_function_catalog();
    let control = crate::compiler::SqlCompileControl::unbounded();
    let timestamp = DataType::Timestamp(TimeUnit::Microsecond, None);
    for (left, right, target) in [
        (DataType::Date32, timestamp.clone(), timestamp),
        (
            DataType::Decimal128(5, 2),
            DataType::Decimal128(12, 4),
            DataType::Decimal128(12, 4),
        ),
        (
            DataType::Int32,
            DataType::Decimal128(12, 4),
            DataType::Decimal128(14, 4),
        ),
    ] {
        for (carrier, nullable) in [(left, false), (right, true)] {
            let source = FunctionValueType::new(carrier, nullable);
            let target = FunctionValueType::new(target.clone(), nullable);
            let conversion = plan_conversion(
                functions,
                &source,
                &target,
                DecimalOverflowPolicy::OutputNull,
                None,
                &control,
            )
            .unwrap();
            match conversion {
                PlannedConversion::Identity => assert_eq!(source, target),
                PlannedConversion::Carrier(step) => {
                    assert_ne!(source, target);
                    assert_eq!(step.target(), &target);
                    assert_eq!(step.target().nullable(), source.nullable());
                    assert_eq!(
                        step.decimal_overflow_policy(),
                        DecimalOverflowPolicy::OutputNull
                    );
                }
                PlannedConversion::ValueConversion { .. } => {
                    panic!("physical comparison requires only carrier conversion")
                }
            }
        }
    }
}

#[test]
fn explicit_array_modes_survive_identity_and_reject_foreign_type_boundaries() {
    use arrow::datatypes::Field;
    use novarocks_type_contract::ArrayCastFailureMode;
    let control = crate::compiler::SqlCompileControl::unbounded();
    let catalog = crate::functions::builtin_sql_function_catalog();
    let ty = FunctionValueType::new(
        DataType::List(Arc::new(Field::new("item", DataType::Int32, true))),
        true,
    );
    assert!(matches!(
        plan_conversion(
            catalog,
            &ty,
            &ty,
            DecimalOverflowPolicy::OutputNull,
            None,
            &control
        )
        .unwrap(),
        PlannedConversion::Identity
    ));
    for mode in [
        ArrayCastFailureMode::ReportError,
        ArrayCastFailureMode::OutputNull,
    ] {
        let PlannedConversion::Carrier(step) = plan_conversion(
            catalog,
            &ty,
            &ty,
            DecimalOverflowPolicy::OutputNull,
            Some(mode),
            &control,
        )
        .unwrap() else {
            panic!("explicit boundary must not become identity")
        };
        assert_eq!(step.target(), &ty);
        assert_eq!(step.array_failure_mode(), Some(mode));
        assert!(matches!(
            plan_conversion(
                catalog,
                &ty,
                &FunctionValueType::new(DataType::Int32, true),
                DecimalOverflowPolicy::OutputNull,
                Some(mode),
                &control
            ),
            Err(FunctionBindingError::InvalidBinding(_))
        ));
    }
}
