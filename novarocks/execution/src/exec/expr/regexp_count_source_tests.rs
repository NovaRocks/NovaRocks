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
use super::function::FunctionKind;
use super::{ExprArena, ExprId, ExprNode};
use crate::exec::chunk::{Chunk, ChunkSchema};
use arrow::array::{Array, ArrayRef, Int64Array, RecordBatch, RecordBatchOptions, StringArray};
use arrow::datatypes::{DataType, Schema};
use novarocks_functions::FunctionValueType;
use novarocks_type_contract::RegexpCountPatternSource as Source;
use std::sync::Arc;

fn input() -> Chunk {
    let schema = Arc::new(Schema::empty());
    let batch = RecordBatch::try_new_with_options(
        schema.clone(),
        vec![],
        &RecordBatchOptions::new().with_row_count(Some(1)),
    )
    .unwrap();
    Chunk::new_with_chunk_schema(
        batch,
        ChunkSchema::try_ref_from_schema_and_slot_ids(&schema, &[]).unwrap(),
    )
}
fn fixture(pattern: Option<&str>, complete: bool) -> (ExprArena, ExprId) {
    let mut arena = ExprArena::default();
    let mut push = |rows: Vec<Option<&str>>| {
        let ty = FunctionValueType::new(DataType::Utf8, rows.iter().any(Option::is_none));
        let array: ArrayRef = Arc::new(StringArray::from(rows));
        let pool = novarocks_functions::ConstantPool::try_new(
            Arc::new(arrow::datatypes::Field::new(
                "pattern source",
                DataType::Utf8,
                ty.nullable(),
            )),
            ty.clone(),
            array.to_data(),
            super::pure_differential::constant_policy(),
            novarocks_type_contract::CompilePhase::FunctionSpecialization,
            &super::pure_differential::HarnessControl,
        )
        .unwrap();
        let selected = pool.value(1).unwrap();
        assert_eq!(selected.ordinal(), 1);
        if complete {
            arena.push_value_typed(ExprNode::Constant(selected), ty)
        } else {
            arena.push_typed(ExprNode::Constant(selected), DataType::Utf8)
        }
    };
    let text = push(vec![Some("poison"), Some("aaa")]);
    let pattern = push(vec![Some("a"), pattern]);
    let node = ExprNode::FunctionCall {
        kind: FunctionKind::String("regexp_count"),
        args: vec![text, pattern],
    };
    let root = if complete {
        arena.push_value_typed(node, FunctionValueType::new(DataType::Int64, true))
    } else {
        arena.push_typed(node, DataType::Int64)
    };
    (arena, root)
}
fn value(arena: &ExprArena, root: ExprId) -> Option<i64> {
    let output = arena.eval(root, &input()).unwrap();
    let output = output.as_any().downcast_ref::<Int64Array>().unwrap();
    (!output.is_null(0)).then(|| output.value(0))
}
#[test]
fn regexp_count_explicit_receipt_preserves_raw_pool_and_exact_selected_ordinal() {
    for complete in [false, true] {
        let (mut arena, root) = fixture(Some("[0-9"), complete);
        assert_eq!(
            value(&arena, root),
            None,
            "raw checked pool remains Dynamic"
        );
        arena
            .set_regexp_count_pattern_source(root, Source::NativeV1Utf8LiteralWhenPresent)
            .unwrap();
        assert!(
            arena
                .eval(root, &input())
                .unwrap_err()
                .starts_with("Invalid regex expression: [0-9.")
        );
        assert!(
            arena
                .set_regexp_count_pattern_source(root, Source::Dynamic)
                .is_err()
        );
        let (mut dynamic, root) = fixture(Some("[0-9"), complete);
        dynamic
            .set_regexp_count_pattern_source(root, Source::Dynamic)
            .unwrap();
        assert_eq!(value(&dynamic, root), None);
        let (mut valid, root) = fixture(Some("a"), complete);
        valid
            .set_regexp_count_pattern_source(root, Source::NativeV1Utf8LiteralWhenPresent)
            .unwrap();
        assert_eq!(value(&valid, root), Some(3));
        let (mut null, root) = fixture(None, complete);
        null.set_regexp_count_pattern_source(root, Source::NativeV1Utf8LiteralWhenPresent)
            .unwrap();
        assert_eq!(
            value(&null, root),
            None,
            "selected NULL masks the literal policy"
        );
    }
}
#[test]
fn regexp_count_receipt_survives_full_and_legacy_freeze_thaw_without_inventing_absent_fact() {
    for complete in [false, true] {
        for source in [
            None,
            Some(Source::NativeV1Utf8LiteralWhenPresent),
            Some(Source::Dynamic),
        ] {
            let (mut arena, root) = fixture(Some("[0-9"), complete);
            if let Some(source) = source {
                arena.set_regexp_count_pattern_source(root, source).unwrap();
            }
            let frozen = arena.into_immutable().unwrap();
            assert_eq!(frozen.nodes()[root.0].regexp_count_pattern_source(), source);
            let restored = ExprArena::from_immutable(&frozen).unwrap();
            assert_eq!(restored.regexp_count_pattern_source(root), source);
            if source == Some(Source::NativeV1Utf8LiteralWhenPresent) {
                assert!(
                    restored
                        .eval(root, &input())
                        .unwrap_err()
                        .starts_with("Invalid regex expression:")
                );
            } else {
                assert_eq!(value(&restored, root), None);
            }
        }
    }
}
#[test]
fn regexp_count_receipt_rejects_foreign_call_dynamic_literal_claim_and_wrong_carrier() {
    let (mut arena, root) = fixture(Some("a"), true);
    let foreign = arena.push_typed(
        ExprNode::FunctionCall {
            kind: FunctionKind::String("lower"),
            args: vec![root],
        },
        DataType::Utf8,
    );
    assert!(
        arena
            .set_regexp_count_pattern_source(foreign, Source::Dynamic)
            .is_err()
    );
    let slot = arena.push_value_typed(
        ExprNode::SlotId(novarocks_types::SlotId::new(1)),
        FunctionValueType::new(DataType::Utf8, true),
    );
    let dynamic = arena.push_typed(
        ExprNode::FunctionCall {
            kind: FunctionKind::String("regexp_count"),
            args: vec![slot, slot],
        },
        DataType::Int64,
    );
    assert!(
        arena
            .set_regexp_count_pattern_source(dynamic, Source::NativeV1Utf8LiteralWhenPresent)
            .is_err()
    );
    arena
        .set_regexp_count_pattern_source(dynamic, Source::Dynamic)
        .unwrap();
    let arity = arena.push_typed(
        ExprNode::FunctionCall {
            kind: FunctionKind::String("regexp_count"),
            args: vec![slot],
        },
        DataType::Int64,
    );
    assert!(
        arena
            .set_regexp_count_pattern_source(arity, Source::Dynamic)
            .is_err()
    );
    let wrong = arena.push_typed(
        ExprNode::Literal(super::LiteralValue::Int64(1)),
        DataType::Int64,
    );
    let malformed = arena.push_typed(
        ExprNode::FunctionCall {
            kind: FunctionKind::String("regexp_count"),
            args: vec![wrong, wrong],
        },
        DataType::Int64,
    );
    assert!(
        arena
            .set_regexp_count_pattern_source(malformed, Source::Dynamic)
            .is_err()
    );
    use novarocks_local_program::{
        ImmutableExpressions, ProgramExprId, StaticExprKind, StaticExprNode, StaticFunctionKind,
    };
    let foreign = StaticExprNode::new(
        StaticExprKind::FunctionCall {
            kind: StaticFunctionKind::String("lower"),
            args: vec![],
        },
        DataType::Utf8,
        None,
    );
    assert!(
        foreign
            .with_regexp_count_pattern_source(Source::Dynamic)
            .is_err()
    );
    let child = StaticExprNode::new(
        StaticExprKind::Literal(novarocks_local_program::StaticLiteral::Int64(1)),
        DataType::Int64,
        None,
    );
    let call = StaticExprNode::new(
        StaticExprKind::FunctionCall {
            kind: StaticFunctionKind::String("regexp_count"),
            args: vec![ProgramExprId::new(0), ProgramExprId::new(0)],
        },
        DataType::Int64,
        None,
    )
    .with_regexp_count_pattern_source(Source::Dynamic)
    .unwrap();
    assert!(
        ImmutableExpressions::try_new(vec![child, call], false, Default::default(), None).is_err()
    );
}

#[test]
fn regexp_count_receipt_rejects_actual_json_pool_even_when_complete_physical_type_shadows_it() {
    use novarocks_local_program::{
        ImmutableExpressions, ProgramExprId, StaticExprKind, StaticExprNode, StaticFunctionKind,
        StaticLiteral,
    };
    let json = FunctionValueType::try_with_logical_type(
        DataType::Utf8,
        false,
        novarocks_type_contract::ValueLogicalType::Json,
    )
    .unwrap();
    let physical = FunctionValueType::new(DataType::Utf8, false);
    let array: ArrayRef = Arc::new(StringArray::from(vec!["null", "\"[\""]));
    let pool = novarocks_functions::ConstantPool::try_new(
        Arc::new(json.try_to_field("checked json pattern").unwrap()),
        json,
        array.to_data(),
        super::pure_differential::constant_policy(),
        novarocks_type_contract::CompilePhase::FunctionSpecialization,
        &super::pure_differential::HarnessControl,
    )
    .unwrap();
    let value = pool.value(1).unwrap();
    assert_eq!(value.ordinal(), 1);
    for complete in [false, true] {
        for source in [Source::Dynamic, Source::NativeV1Utf8LiteralWhenPresent] {
            let mut arena = ExprArena::default();
            let text = arena.push_value_typed(
                ExprNode::Literal(super::LiteralValue::Utf8("aaa".into())),
                physical.clone(),
            );
            let pattern = if complete {
                arena.push_value_typed(ExprNode::Constant(value.clone()), physical.clone())
            } else {
                arena.push_typed(ExprNode::Constant(value.clone()), DataType::Utf8)
            };
            let root = arena.push_typed(
                ExprNode::FunctionCall {
                    kind: FunctionKind::String("regexp_count"),
                    args: vec![text, pattern],
                },
                DataType::Int64,
            );
            assert!(arena.set_regexp_count_pattern_source(root, source).is_err());
            assert_eq!(arena.regexp_count_pattern_source(root), None);

            // Restore the genuine complete Json declaration before the saved
            // graph test; no mismatched-type acceptance is part of this oracle.
            if complete {
                arena
                    .set_value_type(pattern, value.value_type().clone())
                    .unwrap();
            }
            // An unreceipted Json graph may round trip, but restoring it must
            // never establish Physical Utf8 pattern-source authority.
            let frozen = arena.into_immutable().unwrap();
            let mut restored = ExprArena::from_immutable(&frozen).unwrap();
            assert_eq!(restored.regexp_count_pattern_source(root), None);
            assert!(
                restored
                    .set_regexp_count_pattern_source(root, source)
                    .is_err()
            );

            // Independently authored saved call metadata must be refused by
            // graph admission before it can reach a thawed runtime arena.
            let pattern = if complete {
                StaticExprNode::new_value_typed(
                    StaticExprKind::Constant(value.clone()),
                    physical.clone(),
                )
            } else {
                StaticExprNode::new(
                    StaticExprKind::Constant(value.clone()),
                    DataType::Utf8,
                    None,
                )
            };
            let call = StaticExprNode::new(
                StaticExprKind::FunctionCall {
                    kind: StaticFunctionKind::String("regexp_count"),
                    args: vec![ProgramExprId::new(0), ProgramExprId::new(1)],
                },
                DataType::Int64,
                None,
            )
            .with_regexp_count_pattern_source(source)
            .unwrap();
            assert!(
                ImmutableExpressions::try_new(
                    vec![
                        StaticExprNode::new_value_typed(
                            StaticExprKind::Literal(StaticLiteral::Utf8("aaa".into())),
                            physical.clone(),
                        ),
                        pattern,
                        call,
                    ],
                    false,
                    Default::default(),
                    None,
                )
                .is_err()
            );
        }
    }
}

#[test]
fn regexp_count_freeze_rechecks_declared_domain_after_an_existing_receipt() {
    let json = FunctionValueType::try_with_logical_type(
        DataType::Utf8,
        false,
        novarocks_type_contract::ValueLogicalType::Json,
    )
    .unwrap();
    for complete in [false, true] {
        for source in [Source::Dynamic, Source::NativeV1Utf8LiteralWhenPresent] {
            let (mut arena, root) = fixture(Some("a"), complete);
            arena.set_regexp_count_pattern_source(root, source).unwrap();
            let Some(ExprNode::FunctionCall { args, .. }) = arena.node(root) else {
                panic!("actual regexp_count fixture call");
            };
            let pattern = args[1];
            arena.set_value_type(pattern, json.clone()).unwrap();
            // The original checked pool is still Physical Utf8. The changed
            // complete declaration must independently invalidate the receipt.
            assert!(arena.into_immutable().is_err());
        }
    }
}

#[test]
fn regexp_count_receipt_rejects_actual_checked_carrier_hidden_by_declared_utf8() {
    let value = super::pure_differential::constant(
        FunctionValueType::new(DataType::Int64, false),
        Arc::new(Int64Array::from(vec![42])) as ArrayRef,
    );
    for complete in [false, true] {
        for source in [Source::Dynamic, Source::NativeV1Utf8LiteralWhenPresent] {
            let mut arena = ExprArena::default();
            let text = arena.push_typed(
                ExprNode::Literal(super::LiteralValue::Utf8("aaa".into())),
                DataType::Utf8,
            );
            let pattern = if complete {
                arena.push_value_typed(
                    ExprNode::Constant(value.clone()),
                    FunctionValueType::new(DataType::Utf8, false),
                )
            } else {
                arena.push_typed(ExprNode::Constant(value.clone()), DataType::Utf8)
            };
            let root = arena.push_typed(
                ExprNode::FunctionCall {
                    kind: FunctionKind::String("regexp_count"),
                    args: vec![text, pattern],
                },
                DataType::Int64,
            );
            // Receipt admission itself must check the pool's actual carrier,
            // before generic expression evaluation can reject the mismatch.
            assert!(arena.set_regexp_count_pattern_source(root, source).is_err());
            assert_eq!(arena.regexp_count_pattern_source(root), None);
        }
    }
}

#[test]
fn regexp_count_receipt_rejects_independent_nominal_field_schema_and_rechecks_before_freeze() {
    use crate::exec::chunk::ChunkFieldSchema;
    let json = FunctionValueType::try_with_logical_type(
        DataType::Utf8,
        false,
        novarocks_type_contract::ValueLogicalType::Json,
    )
    .unwrap();
    let physical = FunctionValueType::new(DataType::Utf8, false);
    for complete in [false, true] {
        for checked_constant in [false, true] {
            for source in [Source::Dynamic, Source::NativeV1Utf8LiteralWhenPresent] {
                let (mut arena, root) = if checked_constant {
                    fixture(Some("a"), complete)
                } else {
                    let mut arena = ExprArena::default();
                    let text = arena.push_typed(
                        ExprNode::Literal(super::LiteralValue::Utf8("aaa".into())),
                        DataType::Utf8,
                    );
                    let node = ExprNode::Literal(super::LiteralValue::Utf8("a".into()));
                    let pattern = if complete {
                        arena.push_value_typed(node, physical.clone())
                    } else {
                        arena.push_typed(node, DataType::Utf8)
                    };
                    let root = arena.push_typed(
                        ExprNode::FunctionCall {
                            kind: FunctionKind::String("regexp_count"),
                            args: vec![text, pattern],
                        },
                        DataType::Int64,
                    );
                    (arena, root)
                };
                let Some(ExprNode::FunctionCall { args, .. }) = arena.node(root) else {
                    panic!("actual regexp_count fixture call");
                };
                let pattern = args[1];
                arena.set_field_schema(
                    pattern,
                    ChunkFieldSchema::from_value_type(json.value_type().clone()),
                );
                // Both the carrier and checked pool remain Physical Utf8. The
                // independent nominal field source cannot be hidden by either.
                assert!(arena.set_regexp_count_pattern_source(root, source).is_err());
                assert_eq!(arena.regexp_count_pattern_source(root), None);
                arena.set_field_schema(
                    pattern,
                    ChunkFieldSchema::from_value_type(physical.value_type().clone()),
                );
                arena.set_regexp_count_pattern_source(root, source).unwrap();
                assert_eq!(value(&arena, root), Some(3));
                arena.set_field_schema(
                    pattern,
                    ChunkFieldSchema::from_value_type(json.value_type().clone()),
                );
                // A stale receipt must fail at use and before freeze can discard a
                // legacy field source shadowed by the complete Physical binding.
                assert!(arena.eval(root, &input()).is_err());
                assert!(arena.into_immutable().is_err());
            }
        }
    }
}

#[test]
fn regexp_count_nominal_legacy_field_schema_survives_unreceipted_thaw_and_blocks_saved_receipts() {
    use crate::exec::chunk::ChunkFieldSchema;
    use novarocks_local_program::{
        ImmutableExpressions, ProgramExprId, StaticExprKind, StaticExprNode, StaticFunctionKind,
        StaticLiteral,
    };
    let json = FunctionValueType::try_with_logical_type(
        DataType::Utf8,
        false,
        novarocks_type_contract::ValueLogicalType::Json,
    )
    .unwrap();
    for source in [Source::Dynamic, Source::NativeV1Utf8LiteralWhenPresent] {
        let (mut arena, root) = fixture(Some("a"), false);
        let Some(ExprNode::FunctionCall { args, .. }) = arena.node(root) else {
            panic!("actual regexp_count fixture call");
        };
        let pattern = args[1];
        arena.set_field_schema(
            pattern,
            ChunkFieldSchema::from_value_type(json.value_type().clone()),
        );
        assert!(arena.value_type(pattern).is_none());
        let frozen = arena.into_immutable().unwrap();
        let mut restored = ExprArena::from_immutable(&frozen).unwrap();
        assert_eq!(
            restored.field_schema(pattern).unwrap().logical_type(),
            Some(novarocks_type_contract::ValueLogicalType::Json)
        );
        assert!(
            restored
                .set_regexp_count_pattern_source(root, source)
                .is_err()
        );
        let call = StaticExprNode::new(
            StaticExprKind::FunctionCall {
                kind: StaticFunctionKind::String("regexp_count"),
                args: vec![ProgramExprId::new(0), ProgramExprId::new(1)],
            },
            DataType::Int64,
            None,
        )
        .with_regexp_count_pattern_source(source)
        .unwrap();
        let graph = ImmutableExpressions::try_new(
            vec![
                StaticExprNode::new(
                    StaticExprKind::Literal(StaticLiteral::Utf8("aaa".into())),
                    DataType::Utf8,
                    None,
                ),
                StaticExprNode::new(
                    StaticExprKind::Literal(StaticLiteral::Utf8("a".into())),
                    DataType::Utf8,
                    Some(json.value_type().clone()),
                ),
                call,
            ],
            false,
            Default::default(),
            None,
        );
        assert!(graph.is_err());
    }
}
