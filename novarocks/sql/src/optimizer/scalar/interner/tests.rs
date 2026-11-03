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
use crate::optimizer::scalar::ColumnId;
use arrow::datatypes::Field;
use std::sync::{Arc, Mutex};

#[derive(Clone, Copy)]
enum Refusal {
    Never,
    Entry,
    Positive,
}
struct Control {
    events: Mutex<Vec<(CompilePhase, u32)>>,
    refusal: Refusal,
    error: CompileControlError,
}
impl Control {
    fn good() -> Self {
        Self::new(Refusal::Never, CompileControlError::Cancelled)
    }
    fn new(refusal: Refusal, error: CompileControlError) -> Self {
        Self {
            events: Mutex::new(Vec::new()),
            refusal,
            error,
        }
    }
}
impl PureCompileControl for Control {
    fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        self.events.lock().unwrap().push((phase, units));
        match self.refusal {
            Refusal::Entry if units == 0 => Err(self.error.clone()),
            Refusal::Positive if units > 0 => Err(self.error.clone()),
            _ => Ok(()),
        }
    }
}
fn ty() -> FunctionValueType {
    FunctionValueType::new(DataType::Int64, false)
}
fn text(s: &str) -> ScalarNode {
    ScalarNode::Literal(HashableLiteral(LiteralValue::String(s.into())))
}
fn forced(
    arena: &mut ScalarArena,
    node: ScalarNode,
    ty: FunctionValueType,
    control: &Control,
) -> Result<ScalarId, SqlCompileError> {
    intern_inner(arena, node, ty, control, Some(9))
}
fn publication(arena: &ScalarArena) -> (usize, usize, usize, Vec<(u64, Vec<ScalarId>)>) {
    let mut buckets = arena
        .intern
        .iter()
        .map(|(k, v)| (*k, v.clone()))
        .collect::<Vec<_>>();
    buckets.sort_by_key(|(key, _)| *key);
    (
        arena.nodes.len(),
        arena.value_types.len(),
        arena.function_volatility.len(),
        buckets,
    )
}

#[test]
fn forced_collisions_compare_exact_values_and_stop_at_first_hit() {
    let mut alone = ScalarArena::new();
    let mut crowded = ScalarArena::new();
    let string_type = FunctionValueType::new(DataType::Utf8, false);
    let first = forced(
        &mut alone,
        text("hit"),
        string_type.clone(),
        &Control::good(),
    )
    .unwrap();
    assert_eq!(
        forced(
            &mut crowded,
            text("hit"),
            string_type.clone(),
            &Control::good()
        )
        .unwrap(),
        first
    );
    let other = forced(
        &mut crowded,
        text(&"x".repeat(600_000)),
        string_type.clone(),
        &Control::good(),
    )
    .unwrap();
    assert_ne!(first, other);
    let baseline = Control::good();
    let actual = Control::good();
    assert_eq!(
        forced(&mut alone, text("hit"), string_type.clone(), &baseline).unwrap(),
        first
    );
    assert_eq!(
        forced(&mut crowded, text("hit"), string_type, &actual).unwrap(),
        first
    );
    assert_eq!(
        *baseline.events.lock().unwrap(),
        *actual.events.lock().unwrap(),
        "the later collision candidate must not be visited"
    );
    assert_eq!(crowded.nodes.len(), 2);
}

#[test]
fn commutative_normalization_and_float_bit_identity_are_preserved() {
    let control = Control::good();
    let mut arena = ScalarArena::new();
    let left = arena
        .intern_observed(
            ScalarNode::ColumnRef(crate::column_id::ColumnId(1)),
            ty(),
            &control,
        )
        .unwrap();
    let right = arena
        .intern_observed(
            ScalarNode::ColumnRef(crate::column_id::ColumnId(2)),
            ty(),
            &control,
        )
        .unwrap();
    let binary = |left, right| ScalarNode::BinaryOp {
        op: crate::common::BinOp::Eq,
        left,
        right,
        decimal_overflow_policy: novarocks_type_contract::DecimalOverflowPolicy::OutputNull,
    };
    let a = arena
        .intern_observed(
            binary(left, right),
            FunctionValueType::new(DataType::Boolean, false),
            &control,
        )
        .unwrap();
    let b = arena
        .intern_observed(
            binary(right, left),
            FunctionValueType::new(DataType::Boolean, false),
            &control,
        )
        .unwrap();
    assert_eq!(a, b);
    let float_ty = FunctionValueType::new(DataType::Float64, false);
    let literal =
        |bits| ScalarNode::Literal(HashableLiteral(LiteralValue::Float(f64::from_bits(bits))));
    let nan = forced(
        &mut arena,
        literal(0x7ff8_0000_0000_0042),
        float_ty.clone(),
        &control,
    )
    .unwrap();
    assert_eq!(
        forced(
            &mut arena,
            literal(0x7ff8_0000_0000_0042),
            float_ty.clone(),
            &control
        )
        .unwrap(),
        nan
    );
    assert_ne!(
        forced(
            &mut arena,
            literal(0x7ff8_0000_0000_0043),
            float_ty.clone(),
            &control
        )
        .unwrap(),
        nan
    );
    let zero = forced(&mut arena, literal(0), float_ty.clone(), &control).unwrap();
    assert_ne!(
        forced(&mut arena, literal(1u64 << 63), float_ty, &control).unwrap(),
        zero
    );
}

#[test]
fn full_types_metadata_and_nullability_are_exact_even_in_the_same_bucket() {
    let control = Control::good();
    let mut arena = ScalarArena::new();
    let column = || ScalarNode::ColumnRef(crate::column_id::ColumnId(1));
    let field = |value: &str| {
        Arc::new(Field::new("item", DataType::Utf8, false).with_metadata(
            std::collections::HashMap::from([("provider".into(), value.into())]),
        ))
    };
    let first_ty = FunctionValueType::new(DataType::List(field("first")), false);
    let second_ty = FunctionValueType::new(
        DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
        false,
    );
    assert_eq!(
        forced(
            &mut arena,
            column(),
            FunctionValueType::new(DataType::List(field("second")), false),
            &control
        )
        .unwrap(),
        forced(&mut arena, column(), first_ty.clone(), &control).unwrap(),
        "source attributes do not author value identity"
    );
    let first = forced(&mut arena, column(), first_ty.clone(), &control).unwrap();
    assert_eq!(
        forced(&mut arena, column(), first_ty.clone(), &control).unwrap(),
        first
    );
    assert_ne!(
        forced(&mut arena, column(), second_ty, &control).unwrap(),
        first
    );
    let mut nullable = first_ty;
    nullable = nullable.with_nullable(true).unwrap();
    assert_ne!(
        forced(&mut arena, column(), nullable, &control).unwrap(),
        first
    );
    let physical = forced(
        &mut arena,
        column(),
        FunctionValueType::new(DataType::Utf8, false),
        &control,
    )
    .unwrap();
    let mut json = FunctionValueType::new(DataType::Utf8, false);
    json = novarocks_type_contract::FunctionValueType::try_with_logical_type(
        json.data_type().clone(),
        json.nullable(),
        novarocks_type_contract::ValueLogicalType::Json,
    )
    .unwrap();
    assert_ne!(
        forced(&mut arena, column(), json, &control).unwrap(),
        physical
    );
}

#[test]
fn ordered_children_and_display_and_frame_facts_participate_in_exact_interning() {
    let control = Control::good();
    let mut arena = ScalarArena::new();
    let a = ScalarId(0);
    let b = ScalarId(1);
    let list = |list| ScalarNode::InList {
        child: a,
        list,
        negated: false,
    };
    let first = forced(&mut arena, list(vec![a, b]), ty(), &control).unwrap();
    assert_ne!(
        forced(&mut arena, list(vec![b, a]), ty(), &control).unwrap(),
        first
    );
    let binding = super::super::test_function_binding(
        &arena,
        "window_fixture",
        &[],
        DataType::Int64,
        false,
        super::super::FunctionVolatility::Immutable,
    );
    let window = |display: &str, frame| ScalarNode::WindowCall {
        name: "window_fixture".into(),
        args: vec![],
        distinct: false,
        binding: binding.clone(),
        function_order_by: vec![SortKey {
            expr: a,
            asc: true,
            nulls_first: false,
            display: Some(ColumnDisplay::new(None, display.into())),
        }],
        aggregate_binding: None,
        partition_by: vec![a],
        order_by: vec![],
        window_frame: Some(WindowFrame {
            frame_type: crate::common::WindowFrameType::Rows,
            start: WindowBound::Preceding(frame),
            end: WindowBound::CurrentRow,
        }),
        ignore_nulls: false,
    };
    let w = forced(&mut arena, window("a", 1), ty(), &control).unwrap();
    assert_ne!(
        forced(&mut arena, window("renamed", 1), ty(), &control).unwrap(),
        w
    );
    assert_ne!(
        forced(&mut arena, window("a", 2), ty(), &control).unwrap(),
        w
    );
}

#[test]
fn exact_selected_binding_facts_participate_even_with_identical_call_spelling() {
    let control = Control::good();
    let mut arena = ScalarArena::new();
    let binding = super::super::test_function_binding(
        &arena,
        "same_name",
        &[],
        DataType::Int64,
        false,
        super::super::FunctionVolatility::Immutable,
    );
    let node = |binding| ScalarNode::FunctionCall {
        name: "same_name".into(),
        args: vec![],
        distinct: false,
        binding,
        volatility: super::super::FunctionVolatility::Immutable,
    };
    let first = forced(&mut arena, node(binding.clone()), ty(), &control).unwrap();
    assert_eq!(
        forced(&mut arena, node(binding.clone()), ty(), &control).unwrap(),
        first
    );
    let mut different = binding.resolved().clone();
    different.logical_argument_count = 1;
    assert_ne!(
        forced(
            &mut arena,
            node(crate::binding::SqlFunctionBinding::new(
                different,
                binding.decimal_overflow_policy()
            )),
            ty(),
            &control
        )
        .unwrap(),
        first
    );
    let mut different = binding.resolved().clone();
    different.semantics.failure_behavior =
        novarocks_functions::FunctionFailureBehavior::ReturnsNull;
    assert_ne!(
        forced(
            &mut arena,
            node(crate::binding::SqlFunctionBinding::new(
                different,
                binding.decimal_overflow_policy()
            )),
            ty(),
            &control
        )
        .unwrap(),
        first
    );
    let mut different = binding.resolved().clone();
    different.selected.result_type = novarocks_functions::FunctionResultType::Scalar(
        FunctionValueType::new(DataType::Int32, false),
    );
    assert_ne!(
        forced(
            &mut arena,
            node(crate::binding::SqlFunctionBinding::new(
                different,
                binding.decimal_overflow_policy()
            )),
            ty(),
            &control
        )
        .unwrap(),
        first
    );
}

#[test]
fn forced_collisions_preserve_scalar_aggregate_and_window_call_policies() {
    use novarocks_type_contract::DecimalOverflowPolicy::{OutputNull, ReportError};
    let control = Control::good();
    let mut arena = ScalarArena::new();
    let argument = arena
        .intern_observed(
            ScalarNode::Literal(HashableLiteral(LiteralValue::Int(1))),
            ty(),
            &control,
        )
        .unwrap();
    let scalar = super::super::test_function_binding(
        &arena,
        "call",
        &[argument],
        DataType::Int64,
        false,
        super::super::FunctionVolatility::Immutable,
    );
    let aggregate = crate::functions::test_resolved_aggregate("sum", &[DataType::Int64], false);
    for kind in 0..3 {
        let selected = if kind == 0 { &scalar } else { &aggregate };
        let node = |policy| {
            let binding =
                crate::binding::SqlFunctionBinding::new(selected.resolved().clone(), policy);
            match kind {
                0 => ScalarNode::FunctionCall {
                    name: "call".into(),
                    args: vec![argument],
                    distinct: false,
                    binding,
                    volatility: super::super::FunctionVolatility::Immutable,
                },
                1 => ScalarNode::AggregateCall {
                    name: "sum".into(),
                    args: vec![argument],
                    distinct: false,
                    order_by: vec![],
                    resolved: binding,
                },
                _ => ScalarNode::WindowCall {
                    name: "sum".into(),
                    args: vec![argument],
                    distinct: false,
                    aggregate_binding: Some(binding.clone()),
                    binding,
                    function_order_by: vec![],
                    partition_by: vec![],
                    order_by: vec![],
                    window_frame: None,
                    ignore_nulls: false,
                },
            }
        };
        let nullable = FunctionValueType::new(DataType::Int64, true);
        let null = forced(&mut arena, node(OutputNull), nullable.clone(), &control).unwrap();
        let error = forced(&mut arena, node(ReportError), nullable.clone(), &control).unwrap();
        assert_ne!(
            null, error,
            "different policies must survive one forced collision bucket"
        );
        assert_eq!(
            forced(&mut arena, node(OutputNull), nullable.clone(), &control).unwrap(),
            null
        );
        assert_eq!(
            forced(&mut arena, node(ReportError), nullable.clone(), &control).unwrap(),
            error
        );
        if kind == 2 {
            let mut primary_only = node(OutputNull);
            let ScalarNode::WindowCall { binding, .. } = &mut primary_only else {
                unreachable!()
            };
            *binding =
                crate::binding::SqlFunctionBinding::new(binding.resolved().clone(), ReportError);
            let primary =
                forced(&mut arena, primary_only.clone(), nullable.clone(), &control).unwrap();
            let mut aggregate_only = node(OutputNull);
            let ScalarNode::WindowCall {
                aggregate_binding, ..
            } = &mut aggregate_only
            else {
                unreachable!()
            };
            let original = aggregate_binding.as_ref().unwrap();
            *aggregate_binding = Some(crate::binding::SqlFunctionBinding::new(
                original.resolved().clone(),
                ReportError,
            ));
            let aggregate_id =
                forced(&mut arena, aggregate_only, nullable.clone(), &control).unwrap();
            assert_ne!(primary, null);
            assert_ne!(aggregate_id, null);
            assert_ne!(
                primary, aggregate_id,
                "each window binding is a separate identity channel"
            );
            assert_ne!(primary, error);
            assert_ne!(aggregate_id, error);
            assert_eq!(
                forced(&mut arena, primary_only, nullable, &control).unwrap(),
                primary
            );
        }
    }
}

#[test]
fn entry_and_tail_refusal_publish_no_node_or_index_for_all_control_categories() {
    for error in [
        CompileControlError::Cancelled,
        CompileControlError::DeadlineExceeded,
        CompileControlError::ResourceExhausted,
    ] {
        for refusal in [Refusal::Entry, Refusal::Positive] {
            let mut arena = ScalarArena::new();
            arena
                .intern_observed(
                    ScalarNode::ColumnRef(crate::column_id::ColumnId(1)),
                    ty(),
                    &Control::good(),
                )
                .unwrap();
            let before = publication(&arena);
            let control = Control::new(refusal, error.clone());
            assert_eq!(
                arena.intern_observed(
                    ScalarNode::ColumnRef(crate::column_id::ColumnId(1)),
                    ty(),
                    &control
                ),
                Err(error.clone().into())
            );
            assert_eq!(publication(&arena), before);
            let events = control.events.lock().unwrap();
            assert_eq!(events[0], (CompilePhase::Validate, 0));
            if matches!(refusal, Refusal::Positive) {
                assert_eq!(events.len(), 2);
                assert!(events[1].1 > 0 && events[1].1 < 256);
            }
        }
    }
}

#[test]
fn actual_long_bytes_and_child_fanout_observe_256_and_keep_primary_control() {
    for error in [
        CompileControlError::Cancelled,
        CompileControlError::DeadlineExceeded,
        CompileControlError::ResourceExhausted,
    ] {
        for node in [
            text(&"x".repeat(600_000)),
            ScalarNode::InList {
                child: ScalarId(0),
                list: vec![ScalarId(0); 600],
                negated: false,
            },
        ] {
            let mut arena = ScalarArena::new();
            let before = publication(&arena);
            let control = Control::new(Refusal::Positive, error.clone());
            assert_eq!(
                arena.intern_observed(node, ty(), &control),
                Err(error.clone().into())
            );
            assert_eq!(publication(&arena), before);
            assert_eq!(
                *control.events.lock().unwrap(),
                vec![(CompilePhase::Validate, 0), (CompilePhase::Validate, 256)],
                "the actual loop fails at its first full work quantum with no replacement exit"
            );
        }
    }
}

fn map64_carrier(mut leaf: DataType) -> DataType {
    for _ in 0..63 {
        leaf = DataType::Map(
            Arc::new(Field::new(
                "entries",
                DataType::Struct(
                    vec![
                        Arc::new(Field::new("key", DataType::Int32, false)),
                        Arc::new(Field::new("value", leaf, false)),
                    ]
                    .into(),
                ),
                false,
            )),
            false,
        );
    }
    leaf
}

#[test]
fn typed_map64_interning_and_cast_target_collisions_keep_logical_depth() {
    let value = FunctionValueType::new(map64_carrier(DataType::Int64), false);
    assert_eq!(value.value_type().depth(), 64);
    assert!(
        novarocks_type_contract::arrow_data_type_fingerprint_observed::<
            novarocks_type_contract::ValueTypeError,
        >(value.data_type(), &mut || Ok(()))
        .is_err(),
        "raw physical policy stays separate"
    );
    let mut arena = ScalarArena::new();
    let first = arena
        .intern_observed(
            ScalarNode::ColumnRef(ColumnId(7)),
            value.clone(),
            &Control::good(),
        )
        .unwrap();
    assert_eq!(
        first,
        arena
            .intern_observed(
                ScalarNode::ColumnRef(ColumnId(7)),
                value.clone(),
                &Control::good()
            )
            .unwrap()
    );
    let cast = || ScalarNode::Cast {
        child: first,
        target: value.data_type().clone(),
        decimal_overflow_policy: novarocks_type_contract::DecimalOverflowPolicy::OutputNull,
    };
    let cast_id = arena
        .intern_observed(cast(), value.clone(), &Control::good())
        .unwrap();
    assert_eq!(
        cast_id,
        arena
            .intern_observed(cast(), value.clone(), &Control::good())
            .unwrap()
    );
    let before = publication(&arena);
    assert!(matches!(
        arena.intern_observed(
            cast(),
            value,
            &Control::new(Refusal::Positive, CompileControlError::Cancelled)
        ),
        Err(SqlCompileError::Cancelled)
    ));
    assert_eq!(
        before,
        publication(&arena),
        "cancelled typed traversal must not publish an arena node"
    );
}
