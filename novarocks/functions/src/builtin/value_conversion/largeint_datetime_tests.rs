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

//! Exact existing LARGEINT_SIGNED + numeric calendar composition.
use super::*;
use arrow_schema::TimeUnit;
use novarocks_type_contract::{
    CompileControlError, ValueConstructionFailure, ValueTypeStorageHold, ValueTypeStorageLifetime,
};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
fn source(nullable: bool) -> FunctionValueType {
    FunctionValueType::try_with_logical_type(
        DataType::FixedSizeBinary(16),
        nullable,
        ValueLogicalType::LargeInt,
    )
    .unwrap()
}
fn target(nullable: bool) -> FunctionValueType {
    FunctionValueType::new(DataType::Timestamp(TimeUnit::Microsecond, None), nullable)
}
#[test]
fn largeint_datetime_intermediate_is_only_exact_nullable_i64_and_existing_frozen_family() {
    for nullable in [false, true] {
        let source = source(nullable);
        let target = target(true);
        let intermediate = conversion_intermediate_type(&source, &target)
            .unwrap()
            .unwrap();
        assert_eq!(intermediate, FunctionValueType::new(DataType::Int64, true));
        let selected =
            resolve_value_conversion(&source, &intermediate, crate::binding_test_control())
                .unwrap();
        assert_eq!(selected.overload.as_str(), LARGEINT_SIGNED);
        assert_eq!(
            selected.argument_types.as_ref(),
            &[FunctionArgumentType::Value(source.clone())]
        );
        assert_eq!(
            selected.result_type,
            FunctionResultType::Scalar(intermediate.clone())
        );
        // No invented direct LARGEINT->Timestamp implementation is installed.
        assert!(resolve_value_conversion(&source, &target, crate::binding_test_control()).is_err());
        assert!(is_default_scalar_carrier_cast_overload(&selected.overload));
        crate::PreparedCastRecipe::try_new(
            crate::CastOperation::Carrier,
            &intermediate,
            &target,
            novarocks_type_contract::DecimalOverflowPolicy::ReportError,
            true,
            crate::binding_test_control(),
        )
        .unwrap();
    }
    for target in [
        target(false),
        FunctionValueType::new(DataType::Date32, true),
        FunctionValueType::new(DataType::Timestamp(TimeUnit::Second, None), true),
        FunctionValueType::new(DataType::Timestamp(TimeUnit::Millisecond, None), true),
        FunctionValueType::new(DataType::Timestamp(TimeUnit::Nanosecond, None), true),
        FunctionValueType::new(
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            true,
        ),
    ] {
        assert!(
            conversion_intermediate_type(&source(false), &target).is_err(),
            "{target:?}"
        );
    }
    let timestamp = target(true);
    let uuid = FunctionValueType::try_with_logical_type(
        DataType::FixedSizeBinary(16),
        true,
        ValueLogicalType::Uuid,
    )
    .unwrap();
    assert!(conversion_intermediate_type(&uuid, &timestamp).is_err());
    let bytes = FunctionValueType::new(DataType::FixedSizeBinary(16), true);
    assert_eq!(
        conversion_intermediate_type(&bytes, &timestamp).unwrap(),
        None,
        "same physical domain is not nominal numeric authority"
    );
    assert!(
        crate::PreparedCastRecipe::try_new(
            crate::CastOperation::Carrier,
            &bytes,
            &timestamp,
            novarocks_type_contract::DecimalOverflowPolicy::ReportError,
            true,
            crate::binding_test_control()
        )
        .is_err()
    );
    assert_eq!(
        conversion_intermediate_type(&FunctionValueType::new(DataType::Int64, true), &timestamp)
            .unwrap(),
        None,
        "existing direct physical calendar path remains direct"
    );
}
struct Creator(Arc<AtomicUsize>);
impl ValueTypeStorageHold for Creator {}
impl Drop for Creator {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}
#[test]
fn largeint_datetime_intermediate_retains_actual_caller_creator_after_constructor_exits() {
    use super::super::scalar_construction::ScalarConstruction;
    let exits = Arc::new(AtomicUsize::new(0));
    let intermediate = {
        let storage = ValueTypeStorageLifetime::held(Arc::new(Creator(exits.clone())));
        let mut creation = ScalarConstruction::<std::convert::Infallible>::new(storage, None);
        conversion_intermediate_type_with_construction(&source(false), &target(true), &mut creation)
            .unwrap()
            .unwrap()
    };
    assert_eq!(exits.load(Ordering::SeqCst), 0);
    assert!(
        !intermediate
            .typed_binding()
            .layout()
            .storage_lifetime()
            .is_pure()
    );
    let alias = intermediate.clone();
    drop(intermediate);
    assert_eq!(exits.load(Ordering::SeqCst), 0);
    drop(alias);
    assert_eq!(exits.load(Ordering::SeqCst), 1);
}
#[test]
fn largeint_datetime_intermediate_every_actual_constructor_refusal_is_preserved_without_tail() {
    use super::super::scalar_construction::{OwnerFailure, ScalarConstruction};
    let source = source(false);
    let target = target(true);
    let mut observed = Vec::new();
    {
        let mut observer = |request| {
            observed.push(request);
            Ok::<_, CompileControlError>(())
        };
        let mut creation =
            ScalarConstruction::new(ValueTypeStorageLifetime::pure(), Some(&mut observer));
        conversion_intermediate_type_with_construction(&source, &target, &mut creation)
            .unwrap()
            .unwrap();
    }
    assert!(
        !observed.is_empty(),
        "the new value is created through the actual constructor author"
    );
    for at in 0..observed.len() {
        for cause in [
            CompileControlError::Cancelled,
            CompileControlError::DeadlineExceeded,
            CompileControlError::ResourceExhausted,
        ] {
            let mut calls = 0;
            let result = {
                let mut observer = |_| {
                    let ordinal = calls;
                    calls += 1;
                    if ordinal == at { Err(cause) } else { Ok(()) }
                };
                let mut creation =
                    ScalarConstruction::new(ValueTypeStorageLifetime::pure(), Some(&mut observer));
                conversion_intermediate_type_with_construction(&source, &target, &mut creation)
            };
            assert!(
                matches!(result,Err(OwnerFailure::Construction(ValueConstructionFailure::Observer(actual))) if actual==cause),
                "{result:?}"
            );
            assert_eq!(calls, at + 1);
        }
    }
}
