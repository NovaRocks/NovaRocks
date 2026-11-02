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
use arrow::array::{ArrayRef, DictionaryArray, Float32Array, Int64Array, StringArray};
use arrow::datatypes::{Field, Int8Type};
use novarocks_functions::{ConstantPolicy, ConstantPool};
use novarocks_type_contract::{NR_LOGICAL_TYPE_KEY, ValueLogicalType};
use std::sync::{Arc, Mutex};

struct Control {
    events: Mutex<Vec<(CompilePhase, u32)>>,
    refuse: Option<usize>,
    cause: CompileControlError,
}
impl Control {
    fn good() -> Self {
        Self::at(None, CompileControlError::Cancelled)
    }
    fn at(refuse: Option<usize>, cause: CompileControlError) -> Self {
        Self {
            events: Mutex::new(Vec::new()),
            refuse,
            cause,
        }
    }
    fn trace(&self) -> Vec<(CompilePhase, u32)> {
        self.events.lock().unwrap().clone()
    }
}
impl PureCompileControl for Control {
    fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        assert!(units <= 256);
        let mut events = self.events.lock().unwrap();
        let index = events.len();
        events.push((phase, units));
        if self.refuse == Some(index) {
            Err(self.cause.clone())
        } else {
            Ok(())
        }
    }
}
fn policy() -> ConstantPolicy {
    ConstantPolicy {
        max_rows: 1024,
        max_array_nodes: 4096,
        max_logical_elements: 1_000_000,
        max_retained_buffer_bytes: 16_777_216,
        max_type_depth: 64,
        max_type_nodes: 4096,
        max_dictionary_depth: 16,
        max_metadata_bytes: 1_048_576,
        max_library_validation_work: 67_108_864,
        max_library_validation_bytes: 67_108_864,
    }
}
fn cv(array: ArrayRef, ty: FunctionValueType, field: Arc<Field>, ordinal: u32) -> ConstantValue {
    ConstantPool::try_new(
        field,
        ty,
        array.to_data(),
        policy(),
        CompilePhase::Validate,
        &Control::good(),
    )
    .unwrap()
    .value(ordinal)
    .unwrap()
}
fn integer(values: Vec<i64>, ordinal: u32) -> ConstantValue {
    let ty = FunctionValueType::new(DataType::Int64, false);
    cv(
        Arc::new(Int64Array::from(values)),
        ty.clone(),
        Arc::new(ty.try_to_field("literal").unwrap()),
        ordinal,
    )
}
fn insert(
    arena: &mut ScalarArena,
    value: ConstantValue,
    control: &Control,
) -> Result<ScalarId, SqlCompileError> {
    let ty = value.value_type().clone();
    arena.intern_observed(ScalarNode::Constant(value), ty, control)
}
fn assert_cause(error: SqlCompileError, cause: CompileControlError) {
    assert!(matches!(
        (error, cause),
        (SqlCompileError::Cancelled, CompileControlError::Cancelled)
            | (
                SqlCompileError::DeadlineExceeded,
                CompileControlError::DeadlineExceeded
            )
            | (
                SqlCompileError::ResourceExhausted,
                CompileControlError::ResourceExhausted
            )
    ));
}

#[test]
fn selected_value_identity_ignores_pool_rows_and_keeps_original_backing() {
    let selected = integer(vec![99, 7, 21], 1);
    let mut arena = ScalarArena::with_constant_policy(policy());
    let first = insert(&mut arena, selected.clone(), &Control::good()).unwrap();
    assert_eq!(
        insert(&mut arena, integer(vec![7], 0), &Control::good()).unwrap(),
        first
    );
    assert_ne!(
        insert(&mut arena, integer(vec![99, 8], 1), &Control::good()).unwrap(),
        first
    );
    let ScalarNode::Constant(retained) = &arena.nodes[first.0 as usize] else {
        panic!("constant expected")
    };
    assert_eq!(retained.ordinal(), 1);
    assert!(Arc::ptr_eq(
        retained.pool().field_ref(),
        selected.pool().field_ref()
    ));
    assert!(Arc::ptr_eq(
        retained.pool().array(),
        selected.pool().array()
    ));
}

#[test]
fn dictionary_renumbering_and_unused_entries_are_not_constant_identity() {
    let dictionary = |keys: Vec<i8>, strings: Vec<&str>, ordinal| {
        let array: ArrayRef = Arc::new(
            DictionaryArray::<Int8Type>::try_new(
                arrow::array::Int8Array::from(keys),
                Arc::new(StringArray::from(strings)),
            )
            .unwrap(),
        );
        let ty = FunctionValueType::new(array.data_type().clone(), false);
        cv(
            array.clone(),
            ty.clone(),
            Arc::new(Field::new("literal", array.data_type().clone(), false)),
            ordinal,
        )
    };
    let a = dictionary(vec![0, 1], vec!["unused", "kept"], 1);
    let b = dictionary(vec![0], vec!["kept", "other-unused"], 0);
    let mut arena = ScalarArena::with_constant_policy(policy());
    let first = insert(&mut arena, a, &Control::good()).unwrap();
    assert_eq!(insert(&mut arena, b, &Control::good()).unwrap(), first);
}

#[test]
fn float32_nan_payload_and_signed_zero_are_exact_materialized_values() {
    let mut arena = ScalarArena::with_constant_policy(policy());
    let value = |bits| {
        let ty = FunctionValueType::new(DataType::Float32, false);
        cv(
            Arc::new(Float32Array::from(vec![f32::from_bits(bits)])),
            ty.clone(),
            Arc::new(ty.try_to_field("literal").unwrap()),
            0,
        )
    };
    let mut ids = Vec::new();
    for bits in [0x7f800001, 0x7f800002, 0, 0x80000000] {
        let id = insert(&mut arena, value(bits), &Control::good()).unwrap();
        assert!(!ids.contains(&id));
        ids.push(id);
        assert_eq!(
            insert(&mut arena, value(bits), &Control::good()).unwrap(),
            id
        );
    }
}

#[test]
fn root_source_metadata_is_independent_and_authored_logical_domains_remain_identity() {
    let mut arena = ScalarArena::with_constant_policy(policy());
    let value = |logical, metadata: &str| {
        let ty =
            FunctionValueType::try_from_legacy_declared_projection(DataType::Utf8, false, logical)
                .expect("valid temporary legacy value projection");
        let field = Arc::new(
            ty.try_to_field("literal").unwrap().with_metadata(
                [("provider.fact".into(), metadata.into())]
                    .into_iter()
                    .chain(
                        (logical == ValueLogicalType::Json)
                            .then(|| (NR_LOGICAL_TYPE_KEY.into(), "json".into())),
                    )
                    .collect::<std::collections::HashMap<String, String>>(),
            ),
        );
        cv(Arc::new(StringArray::from(vec!["x"])), ty, field, 0)
    };
    let first = insert(
        &mut arena,
        value(ValueLogicalType::Physical, "a"),
        &Control::good(),
    )
    .unwrap();
    assert_eq!(
        insert(
            &mut arena,
            value(ValueLogicalType::Physical, "b"),
            &Control::good()
        )
        .unwrap(),
        first
    );
    assert_ne!(
        insert(
            &mut arena,
            value(ValueLogicalType::Json, "a"),
            &Control::good()
        )
        .unwrap(),
        first
    );
}

#[test]
fn forged_carrier_logical_and_nullable_sources_refuse_before_publication() {
    let value = integer(vec![7], 0);
    for ty in [
        FunctionValueType::new(DataType::Int32, false),
        FunctionValueType::new(DataType::Int64, true),
    ] {
        let mut arena = ScalarArena::with_constant_policy(policy());
        assert!(matches!(
            arena.intern_observed(ScalarNode::Constant(value.clone()), ty, &Control::good()),
            Err(SqlCompileError::Compilation(_))
        ));
        assert!(arena.nodes.is_empty());
        assert!(arena.intern.is_empty());
    }
    let ty = FunctionValueType::try_from_legacy_declared_projection(
        DataType::Utf8,
        false,
        ValueLogicalType::Json,
    )
    .expect("valid temporary legacy value projection");
    let value = cv(
        Arc::new(StringArray::from(vec!["x"])),
        ty.clone(),
        Arc::new(ty.try_to_field("literal").unwrap()),
        0,
    );
    let mut arena = ScalarArena::with_constant_policy(policy());
    assert!(
        arena
            .intern_observed(
                ScalarNode::Constant(value),
                FunctionValueType::new(DataType::Utf8, false),
                &Control::good()
            )
            .is_err()
    );
    assert!(arena.nodes.is_empty());
}

#[test]
fn candidate_complete_value_quantum_preserves_every_original_refusal_prefix_without_publication() {
    let fields = (0..320)
        .map(|i| Arc::new(Field::new(format!("value_{i:04}"), DataType::Int64, false)))
        .collect::<Vec<_>>();
    let array: ArrayRef = Arc::new(arrow::array::StructArray::from(
        fields
            .iter()
            .map(|field| {
                (
                    field.clone(),
                    Arc::new(Int64Array::from(vec![7, 7])) as ArrayRef,
                )
            })
            .collect::<Vec<_>>(),
    ));
    let ty = FunctionValueType::new(array.data_type().clone(), false);
    let field = Arc::new(Field::new("literal", array.data_type().clone(), false));
    let a = cv(array.clone(), ty.clone(), field.clone(), 0);
    let b = cv(array, ty, field, 1);
    let make_arena = || {
        let mut arena = ScalarArena::with_constant_policy(policy());
        insert(&mut arena, a.clone(), &Control::good()).unwrap();
        arena
    };
    let mut arena = make_arena();
    let baseline = Control::good();
    insert(&mut arena, b.clone(), &baseline).unwrap();
    let trace = baseline.trace();
    assert!(trace.iter().any(|(_, n)| *n == 256));
    for cause in [
        CompileControlError::Cancelled,
        CompileControlError::DeadlineExceeded,
        CompileControlError::ResourceExhausted,
    ] {
        for index in 0..trace.len() {
            let control = Control::at(Some(index), cause.clone());
            let mut arena = make_arena();
            assert_cause(
                insert(&mut arena, b.clone(), &control).unwrap_err(),
                cause.clone(),
            );
            assert_eq!(control.trace(), trace[..=index]);
            assert_eq!(arena.nodes.len(), 1);
        }
    }
}

#[test]
fn ordinary_source_refusal_observes_its_tail_and_preserves_primary_tail_control() {
    let value = integer(vec![7], 0);
    let bad = FunctionValueType::new(DataType::Int32, false);
    let mut arena = ScalarArena::with_constant_policy(policy());
    let baseline = Control::good();
    assert!(
        arena
            .intern_observed(ScalarNode::Constant(value.clone()), bad.clone(), &baseline)
            .is_err()
    );
    let trace = baseline.trace();
    assert!(trace.len() > 1);
    for cause in [
        CompileControlError::Cancelled,
        CompileControlError::DeadlineExceeded,
        CompileControlError::ResourceExhausted,
    ] {
        for index in 0..trace.len() {
            let control = Control::at(Some(index), cause.clone());
            let mut arena = ScalarArena::with_constant_policy(policy());
            assert_cause(
                arena
                    .intern_observed(ScalarNode::Constant(value.clone()), bad.clone(), &control)
                    .unwrap_err(),
                cause.clone(),
            );
            assert_eq!(control.trace(), trace[..=index]);
            assert!(arena.nodes.is_empty());
        }
    }
}

#[test]
fn first_matching_constant_does_not_visit_later_collision_payloads() {
    let value = |text: &str| {
        let ty = FunctionValueType::new(DataType::Utf8, false);
        cv(
            Arc::new(StringArray::from(vec![text])),
            ty.clone(),
            Arc::new(ty.try_to_field("literal").unwrap()),
            0,
        )
    };
    let mut short = ScalarArena::with_constant_policy(policy());
    let mut crowded = ScalarArena::with_constant_policy(policy());
    let first = insert(&mut short, value("hit"), &Control::good()).unwrap();
    insert(&mut crowded, value("hit"), &Control::good()).unwrap();
    insert(&mut crowded, value(&"z".repeat(320_000)), &Control::good()).unwrap();
    let a = Control::good();
    let b = Control::good();
    assert_eq!(insert(&mut short, value("hit"), &a).unwrap(), first);
    assert_eq!(insert(&mut crowded, value("hit"), &b).unwrap(), first);
    assert_eq!(a.trace(), b.trace());
    assert_eq!(crowded.nodes.len(), 2);
}
