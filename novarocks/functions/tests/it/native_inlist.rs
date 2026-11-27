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

use arrow_array::*;
use arrow_buffer::i256;
use arrow_schema::{DataType, Field};
use novarocks_failure_contract::{FailureClass, FailureProjection};
use novarocks_functions::native_inlist::{InError, MembershipComparison};
use novarocks_functions::opaque_memory::OpaqueAllocationHost;
use novarocks_functions::{
    AggregateStateAllocator, ComparisonOperator, ComparisonPrepareError, EvaluatedArgument,
    KernelEvaluationControl, KernelFailure, PreparedComparisonRecipe,
};
use novarocks_type_contract::{
    CompileControlError, CompilePhase, FunctionValueType, PureCompileControl, ValueLogicalType,
};
use std::alloc::Layout;
use std::ptr::NonNull;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

struct Active;

impl PureCompileControl for Active {
    fn checkpoint(&self, _: CompilePhase, _: u32) -> Result<(), CompileControlError> {
        Ok(())
    }
}
impl KernelEvaluationControl for Active {
    fn checkpoint(&self, _: u32) -> Result<(), KernelFailure> {
        Ok(())
    }

    fn wait(&self, _: Duration) -> Result<(), KernelFailure> {
        panic!("IN does not wait")
    }
}

fn prepare(input: &FunctionValueType, candidate: &FunctionValueType) -> MembershipComparison {
    MembershipComparison::try_new(input, candidate, &Active).unwrap()
}

fn assert_matches_equal(input: ArrayRef, candidate: ArrayRef, logical: ValueLogicalType) {
    let ty =
        FunctionValueType::try_with_logical_type(input.data_type().clone(), true, logical).unwrap();
    let membership = prepare(&ty, &ty);
    let equality = if matches!(ty.data_type(), DataType::Float32 | DataType::Float64) {
        PreparedComparisonRecipe::try_new_ieee_float_equality(&ty, &ty, &Active).unwrap()
    } else {
        PreparedComparisonRecipe::try_new(ComparisonOperator::Eq, &ty, &ty, &Active).unwrap()
    };
    for (candidate, scalar) in [(candidate.slice(0, 1), true), (candidate, false)] {
        let actual = membership
            .membership_equalities_observed(&input, &candidate, None, &Active, &mut |_| {
                Ok::<_, ()>(())
            })
            .unwrap();
        let right = if scalar {
            EvaluatedArgument::Scalar(&candidate)
        } else {
            EvaluatedArgument::Column(&candidate)
        };
        let expected = (0..input.len()).map(|row| {
            equality
                .compare_rows(
                    EvaluatedArgument::Column(&input),
                    row,
                    row,
                    right,
                    row,
                    row,
                    &Active,
                )
                .unwrap()
        });
        assert_eq!(
            actual.iter().collect::<Vec<_>>(),
            expected.collect::<Vec<_>>(),
            "{logical:?} {:?}, scalar={scalar}",
            input.data_type(),
        );
    }
}

#[test]
fn in_matches_equal_for_flat_domains_and_scalar_candidates() {
    macro_rules! check {
        ($array:ident, $a:expr, $b:expr, $c:expr) => {
            assert_matches_equal(
                Arc::new($array::from(vec![Some($a), Some($b), None])),
                Arc::new($array::from(vec![Some($a), Some($c), Some($b)])),
                ValueLogicalType::Physical,
            )
        };
    }
    check!(BooleanArray, true, false, true);
    check!(Int8Array, -1, 7, 9);
    check!(Int16Array, -1, 7, 9);
    check!(Int32Array, -1, 7, 9);
    check!(Int64Array, -1, 7, 9);
    check!(UInt8Array, 1, 2, u8::MAX);
    check!(UInt16Array, 1, 2, u16::MAX);
    check!(UInt32Array, 1, 2, u32::MAX);
    check!(UInt64Array, 1, 2, u64::MAX);
    check!(Float32Array, f32::NAN, 0.0, -0.0);
    check!(Float64Array, f64::NAN, -0.0, 0.0);
    check!(Decimal32Array, -100, 1, 19);
    check!(Decimal64Array, -100, 1, 19);
    check!(Decimal128Array, -100, 1, 19);
    check!(
        Decimal256Array,
        i256::from_i128(-100),
        i256::ONE,
        i256::from_i128(19)
    );
    check!(Date32Array, -1, 0, 1);
    check!(Date64Array, -86_400_000, 0, 86_400_000);
    check!(Time32SecondArray, 0, 30, 90);
    check!(Time64MicrosecondArray, 0, 30, 90);
    check!(TimestampMicrosecondArray, -1, 0, 1);
    check!(StringArray, "1.0", "1.00", "1.0");
    check!(LargeStringArray, "a", "é", "e");
    assert_matches_equal(
        Arc::new(BinaryArray::from(vec![
            Some(&b"abc"[..]),
            Some(&b"bcd"[..]),
            None,
        ])),
        Arc::new(BinaryArray::from(vec![
            Some(&b"abc"[..]),
            Some(&b"dce"[..]),
            None,
        ])),
        ValueLogicalType::Physical,
    );
    assert_matches_equal(
        Arc::new(NullArray::new(3)),
        Arc::new(NullArray::new(3)),
        ValueLogicalType::Physical,
    );
    let minus = (-1i128).to_be_bytes();
    let zero = 0i128.to_be_bytes();
    let one = 1i128.to_be_bytes();
    for logical in [
        ValueLogicalType::Physical,
        ValueLogicalType::LargeInt,
        ValueLogicalType::Uuid,
    ] {
        assert_matches_equal(
            Arc::new(
                FixedSizeBinaryArray::try_from_sparse_iter_with_size(
                    [Some(minus), Some(zero), None].into_iter(),
                    16,
                )
                .unwrap(),
            ),
            Arc::new(
                FixedSizeBinaryArray::try_from_sparse_iter_with_size(
                    [Some(minus), Some(one), Some(zero)].into_iter(),
                    16,
                )
                .unwrap(),
            ),
            logical,
        );
    }
}

#[test]
fn in_comparison_accepts_nullable_candidate_beside_required_source() {
    let required = FunctionValueType::new(DataType::Int64, false);
    let nullable = FunctionValueType::new(DataType::Int64, true);
    let recipe = MembershipComparison::try_new(&required, &nullable, &Active).unwrap();
    let input: ArrayRef = Arc::new(Int64Array::from(vec![7, 7]));
    let candidate: ArrayRef = Arc::new(Int64Array::from(vec![None, Some(7)]));
    let equalities = recipe
        .membership_equalities_observed(&input, &candidate, None, &Active, &mut |_| Ok::<_, ()>(()))
        .unwrap();
    assert_eq!(equalities.iter().collect::<Vec<_>>(), [None, Some(true)]);
}

#[test]
fn in_admission_checks_the_comparison_domain_and_exact_operand_carriers() {
    let utf8 = FunctionValueType::new(DataType::Utf8, true);
    let json = FunctionValueType::try_with_logical_type(
        DataType::LargeBinary,
        true,
        ValueLogicalType::Json,
    )
    .unwrap();
    let unsupported_list = FunctionValueType::new(
        DataType::List(Arc::new(Field::new("item", DataType::UInt64, true))),
        true,
    );
    let boolean = FunctionValueType::new(DataType::Boolean, true);
    for (input, candidate, failure) in [
        (&utf8, &json, ComparisonPrepareError::TypeMismatch),
        (&utf8, &boolean, ComparisonPrepareError::TypeMismatch),
        (
            &unsupported_list,
            &unsupported_list,
            ComparisonPrepareError::Unsupported,
        ),
    ] {
        assert_eq!(
            MembershipComparison::try_new(input, candidate, &Active).unwrap_err(),
            failure
        );
    }
    let input: ArrayRef = Arc::new(BooleanArray::from(vec![true]));
    let foreign: ArrayRef = Arc::new(UInt8Array::from(vec![1]));
    let actual = prepare(&boolean, &boolean).membership_equalities_observed(
        &input,
        &foreign,
        None,
        &Active,
        &mut |_| Ok::<_, ()>(()),
    );
    assert!(matches!(
        actual,
        Err(InError::Operation(error)) if error.failure_fact().class() == FailureClass::Invariant
    ));
}

fn json_recipe() -> MembershipComparison {
    let json = FunctionValueType::try_with_logical_type(
        DataType::LargeBinary,
        true,
        ValueLogicalType::Json,
    )
    .unwrap();
    prepare(&json, &json)
}

struct JsonWork;
impl novarocks_functions::json_value::JsonWork for JsonWork {
    type Reservation = ();

    fn step(&mut self) -> Result<(), KernelFailure> {
        Ok(())
    }

    fn reserve(&mut self, _: usize) -> Result<Self::Reservation, KernelFailure> {
        Ok(())
    }

    fn reserve_output(&mut self, _: usize) -> Result<(), KernelFailure> {
        Ok(())
    }
}

#[test]
fn in_json_uses_canonical_keys_while_varchar_compares_bytes() {
    let string = FunctionValueType::new(DataType::Utf8, false);
    let evaluate = |recipe: &MembershipComparison, left: ArrayRef, right: ArrayRef| {
        recipe
            .membership_equalities_observed(&left, &right, None, &Active, &mut |_| Ok::<_, ()>(()))
            .unwrap()
            .iter()
            .next()
            .unwrap()
    };
    let json = |text| -> ArrayRef {
        let bytes = novarocks_functions::json_value::parse(text, &mut JsonWork).unwrap();
        Arc::new(LargeBinaryArray::from(vec![bytes.as_slice()]))
    };
    let text = |text| -> ArrayRef { Arc::new(StringArray::from(vec![text])) };
    let keys = json_recipe();
    let strings = prepare(&string, &string);
    for (left, right, equal_json) in [
        ("1.0", "1.00", true),
        (r#"{"b":1,"a":2}"#, r#"{"a":2,"b":1}"#, true),
        ("1.5e-7", "0.00000015", true),
        ("-0.0", "0.0", true),
    ] {
        assert_eq!(evaluate(&keys, json(left), json(right)), Some(equal_json));
        assert_eq!(evaluate(&strings, text(left), text(right)), Some(false));
    }
}

#[test]
fn in_json_rejects_damaged_values_even_beside_sql_null() {
    let recipe = json_recipe();
    let bad: ArrayRef = Arc::new(LargeBinaryArray::from(vec![b"bad".as_slice()]));
    let null: ArrayRef = Arc::new(LargeBinaryArray::from(vec![None::<&[u8]>]));
    for (left, right) in [(&bad, &bad), (&bad, &null), (&null, &bad)] {
        let result = recipe
            .membership_equalities_observed(left, right, None, &Active, &mut |_| Ok::<_, ()>(()));
        assert!(matches!(
            result,
            Err(InError::Operation(error)) if error.failure_fact().class() == FailureClass::Invariant
        ));
    }
}

struct Budget {
    capacity: usize,
    held: AtomicUsize,
}
impl OpaqueAllocationHost for Budget {
    fn reserve_opaque(&self, bytes: usize) -> Result<(), KernelFailure> {
        self.held
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |held| {
                held.checked_add(bytes)
                    .filter(|next| *next <= self.capacity)
            })
            .map(|_| ())
            .map_err(|_| KernelFailure::ResourceExhausted)
    }
    fn release_opaque(&self, bytes: usize) {
        assert!(self.held.fetch_sub(bytes, Ordering::SeqCst) >= bytes);
    }
}
impl AggregateStateAllocator for Budget {
    fn opaque_allocation_host(&self) -> Option<&dyn OpaqueAllocationHost> {
        Some(self)
    }
    fn allocate(&self, _: Layout) -> Result<NonNull<u8>, KernelFailure> {
        panic!("ordinary comparison does not allocate through the block host")
    }
    unsafe fn release(&self, _: NonNull<u8>, _: Layout) {
        panic!("ordinary comparison does not own a block")
    }
}

#[test]
fn in_equalities_keep_the_frame_budget_until_the_array_is_destroyed() {
    let ty = FunctionValueType::new(DataType::UInt64, true);
    let input: ArrayRef = Arc::new(UInt64Array::from(vec![Some(7), None]));
    let recipe = prepare(&ty, &ty);
    let compare = |host: Arc<Budget>| {
        recipe.membership_equalities_observed(&input, &input, Some(host), &Active, &mut |_| {
            Ok::<_, ()>(())
        })
    };
    let host = Arc::new(Budget {
        capacity: 128,
        held: AtomicUsize::new(0),
    });
    let values = compare(Arc::clone(&host)).unwrap();
    assert_eq!(values.iter().collect::<Vec<_>>(), [Some(true), None]);
    assert_eq!(host.held.load(Ordering::SeqCst), 128);
    drop(values);
    assert_eq!(host.held.load(Ordering::SeqCst), 0);

    let small = Arc::new(Budget {
        capacity: 127,
        held: AtomicUsize::new(0),
    });
    assert!(matches!(
        compare(Arc::clone(&small)),
        Err(InError::Operation(KernelFailure::ResourceExhausted))
    ));
    assert_eq!(small.held.load(Ordering::SeqCst), 0);
}
