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

//! Actual installed whole-call differential checks against the shared v1 shell.
use super::generate::InputProfile;
use super::*;
use arrow::array::{
    BooleanArray, Date32Array, Decimal256Array, ListArray, MapArray, StringArray, StructArray,
    UInt32Array,
};
use arrow::buffer::{NullBuffer, OffsetBuffer};
use arrow::datatypes::{Field, TimeUnit};
use arrow_buffer::i256;
use novarocks_functions::{ScalarInvocationActivation, ScalarInvocationFailure};

fn result() -> FunctionValueType {
    FunctionValueType::try_with_logical_type(DataType::Utf8, true, ValueLogicalType::Json).unwrap()
}
fn compare(arguments: Vec<(FunctionValueType, ArrayRef)>) {
    for policy in [
        DecimalOverflowPolicy::OutputNull,
        DecimalOverflowPolicy::ReportError,
    ] {
        let mut spec = ScalarDiffSpec::new("json_object")
            .decimal_overflow(policy)
            .expect_result_type(result())
            .sparse_selections(5, 0xC161);
        for (ty, array) in &arguments {
            spec = spec.typed_column(ty.clone(), array.clone());
        }
        let summary = assert_scalar_matches_v1(spec);
        assert_eq!(summary.result_type, result());
    }
}
fn typed(array: ArrayRef) -> (FunctionValueType, ArrayRef) {
    (
        FunctionValueType::new(array.data_type().clone(), true),
        array,
    )
}

#[test]
fn c16_p1_json_object_differential_all_original_leaf_carriers_and_unsupported_values() {
    let types = [
        DataType::Null,
        DataType::Boolean,
        DataType::Int8,
        DataType::Int16,
        DataType::Int32,
        DataType::Int64,
        DataType::UInt8,
        DataType::UInt16,
        DataType::UInt32,
        DataType::UInt64,
        DataType::Float32,
        DataType::Float64,
        DataType::Utf8,
        DataType::LargeUtf8,
        DataType::Binary,
        DataType::LargeBinary,
        DataType::Timestamp(TimeUnit::Second, None),
        DataType::Timestamp(TimeUnit::Millisecond, None),
        DataType::Timestamp(TimeUnit::Microsecond, None),
        DataType::Timestamp(TimeUnit::Nanosecond, None),
        DataType::Decimal128(38, 9),
        DataType::Decimal256(76, -4),
    ];
    let mut generator = InputGenerator::new(0xC161);
    for ty in types {
        let ty = FunctionValueType::new(ty, true);
        let values = generator.column(&ty, 32, &InputProfile::default());
        let keys: ArrayRef = Arc::new(StringArray::from(
            (0..32)
                .map(|i| (i % 7 != 0).then_some("\0µkey"))
                .collect::<Vec<_>>(),
        ));
        compare(vec![typed(keys), (ty.clone(), values.clone())]);
        compare(vec![
            (ty, values),
            typed(Arc::new(arrow::array::Int64Array::from(vec![Some(1); 32]))),
        ]);
    }
    let largeint = FunctionValueType::try_with_logical_type(
        DataType::FixedSizeBinary(16),
        true,
        ValueLogicalType::LargeInt,
    )
    .unwrap();
    let values = generator.column(&largeint, 32, &InputProfile::default());
    compare(vec![
        typed(Arc::new(StringArray::from(vec!["key"; 32]))),
        (largeint, values),
    ]);
    compare(vec![
        typed(Arc::new(StringArray::from(vec![
            Some("day"),
            None,
            Some("day"),
            Some("day"),
        ]))),
        typed(Arc::new(Date32Array::from(vec![
            Some(0),
            None,
            Some(-719162),
            Some(i32::MIN),
        ]))),
    ]);
}

#[test]
fn c16_p1_json_object_differential_nested_debug_decimal_and_complex_key_data() {
    let decimals: ArrayRef = Arc::new(
        Decimal256Array::from(vec![Some(i256::MIN), None, Some(i256::MAX)])
            .with_precision_and_scale(76, 0)
            .unwrap(),
    );
    let list: ArrayRef = Arc::new(ListArray::new(
        Arc::new(Field::new("item", decimals.data_type().clone(), true)),
        OffsetBuffer::new(vec![0, 2, 3, 3].into()),
        decimals,
        Some(NullBuffer::from(vec![true, true, false])),
    ));
    let entries = StructArray::new(
        vec![
            Arc::new(Field::new("key", DataType::Utf8, false)),
            Arc::new(Field::new("value", DataType::Boolean, true)),
        ]
        .into(),
        vec![
            Arc::new(StringArray::from(vec!["µ", "b"])),
            Arc::new(BooleanArray::from(vec![Some(true), None])),
        ],
        None,
    );
    let map: ArrayRef = Arc::new(MapArray::new(
        Arc::new(Field::new("entries", entries.data_type().clone(), false)),
        OffsetBuffer::new(vec![0, 2, 2, 2].into()),
        entries,
        Some(NullBuffer::from(vec![true, false, true])),
        false,
    ));
    let structure: ArrayRef = Arc::new(StructArray::new(
        vec![
            Arc::new(Field::new("list", list.data_type().clone(), true)),
            Arc::new(Field::new("map", map.data_type().clone(), true)),
        ]
        .into(),
        vec![list.clone(), map.clone()],
        None,
    ));
    for value in [list.clone(), map, structure] {
        compare(vec![
            typed(Arc::new(StringArray::from(vec!["key"; 3]))),
            typed(value),
        ]);
    }
    compare(vec![
        typed(list),
        typed(Arc::new(StringArray::from(vec!["value"; 3]))),
    ]);
}

#[test]
fn c16_p1_json_object_differential_pair_order_odd_tail_parser_fallback_and_constants() {
    let keys: ArrayRef = Arc::new(StringArray::from(vec![
        Some("b"),
        Some("a"),
        None,
        Some("µ"),
        Some(""),
    ]));
    let values: ArrayRef = Arc::new(StringArray::from(vec![
        "{\"z\":2,\"a\":1}",
        "ordinary text",
        "[1,null]",
        "null",
        "{\"$serde_json::private::RawValue\":\"[2,1]\"}",
    ]));
    let odd: ArrayRef = Arc::new(StringArray::from(vec!["odd"; 5]));
    compare(vec![typed(keys), typed(values), typed(odd)]);
    for policy in [
        DecimalOverflowPolicy::OutputNull,
        DecimalOverflowPolicy::ReportError,
    ] {
        assert_scalar_matches_v1(
            ScalarDiffSpec::new("json_object")
                .decimal_overflow(policy)
                .expect_result_type(result())
                .constant_array(Arc::new(StringArray::from(vec!["b"])))
                .constant_array(Arc::new(StringArray::from(vec!["{\"z\":2,\"a\":1}"])))
                .constant_array(Arc::new(StringArray::from(vec!["odd"])))
                .constant_rows(0),
        );
    }
    compare(vec![
        typed(Arc::new(StringArray::from(vec![None::<&str>; 5]))),
        typed(Arc::new(UInt32Array::from(vec![1, 2, 3, 4, 5]))),
    ]);
}

#[derive(Default)]
struct FrameControl;
impl KernelEvaluationControl for FrameControl {
    fn checkpoint(&self, units: u32) -> Result<(), KernelFailure> {
        assert!(units <= 256);
        Ok(())
    }
    fn wait(&self, _: std::time::Duration) -> Result<(), KernelFailure> {
        panic!("JSON_OBJECT never waits")
    }
}

#[test]
fn c16_p1_json_object_real_sql_package_local_compiler_and_frame_keep_whole_call_data() {
    use super::super::compiled_program::CompiledExpressionInstance;
    use super::super::filter_conjunction_actual_sql_compiler_tests::compiler_results;
    use super::super::numeric_unary_ordered_sql_source_tests::installed_builtin_owner_catalogue;
    use super::super::numeric_unary_original_nonnull_sql_baseline_tests::sql_source_with_declared_fields;
    use novarocks_local_program::{ProgramExpressionRootSite, ProgramNodeKind, StaticExprKind};
    use novarocks_physical_plan::{ExprKind, PhysicalCallDefinition};
    use novarocks_sql::compiler::SqlPhysicalEmissionMode;
    for mode in [
        SqlPhysicalEmissionMode::OriginalNativeV1,
        SqlPhysicalEmissionMode::ExactComputedWithOriginalDeclaration,
    ] {
        for scenario in 0..5 {
            let complex_key = matches!(scenario, 1..=3);
            let mut fields = vec![
                Field::new("k", DataType::Utf8, true),
                Field::new("v", DataType::Utf8, true),
                Field::new("n", DataType::Int64, true),
            ];
            if scenario >= 3 {
                fields.push(Field::new("id", DataType::Int32, true));
            }
            let sql = match scenario {
                0 => "SELECT json_object(k, v) AS j FROM t",
                1 => "SELECT json_object([n], v) AS j FROM t",
                2 => "SELECT json_object(NULL, json_object([n], v)) AS j FROM t",
                3 | 4 => "SELECT json_object([[id, n]][IF(id = 2, NULL, 1)], v) AS j FROM t",
                _ => unreachable!(),
            };
            let source = sql_source_with_declared_fields(sql, &fields, mode);
            let mut authored = 0;
            for fragment in source.plan().fragments().values() {
                for (id, expression) in fragment.expressions().iter() {
                    let ExprKind::FunctionCall { function, args } = &expression.kind else {
                        continue;
                    };
                    if function.function_id.as_str() != "builtin.scalar/json_object/v1" {
                        continue;
                    }
                    assert_eq!(expression.ty, result());
                    assert_eq!(function.result_type, expression.ty);
                    assert_eq!(args.len(), 2);
                    let request = fragment
                        .call_requests()
                        .get(PhysicalCallDefinition::Expression(*id))
                        .unwrap();
                    assert_eq!(request.logical_argument_count, 2);
                    assert_eq!(request.arguments.len(), 2);
                    authored += 1;
                }
            }
            assert_eq!(authored, if scenario == 2 { 2 } else { 1 });

            if scenario >= 3 {
                if let Ok(directory) = std::env::var("C16_SQL_SOURCE_OUT") {
                    std::fs::write(
                        std::path::Path::new(&directory)
                            .join(format!("nested-index-{mode:?}-{scenario}.txt")),
                        format!("{:#?}\n", source.plan()),
                    )
                    .unwrap();
                }
                let mut accesses = 0;
                for fragment in source.plan().fragments().values() {
                    for (_, expression) in fragment.expressions().iter() {
                        let ExprKind::FunctionCall { function, args } = &expression.kind else {
                            continue;
                        };
                        if function.function_id.as_str() != "builtin.scalar/__array_element_at/v1" {
                            continue;
                        }
                        accesses += 1;
                        assert_eq!(args.len(), 2);
                        assert_eq!(
                            expression.ty.data_type,
                            DataType::List(Arc::new(Field::new("item", DataType::Int64, true)))
                        );
                        assert!(expression.ty.nullable);
                        let index = fragment.expressions().get(args[1]).unwrap();
                        assert_eq!(index.ty.data_type, DataType::Int32);
                        assert!(index.ty.nullable);
                        let ExprKind::Cast {
                            expr: if_id,
                            target,
                            ..
                        } = &index.kind
                        else {
                            panic!("SQL subscript's actual index cast");
                        };
                        assert_eq!(target, &DataType::Int32);
                        let conditional = fragment.expressions().get(*if_id).unwrap();
                        assert_eq!(conditional.ty.data_type, DataType::Int64);
                        assert!(conditional.ty.nullable);
                        let ExprKind::FunctionCall { function, args } = &conditional.kind else {
                            panic!("primitive SQL IF");
                        };
                        assert_eq!(function.function_id.as_str(), "builtin.scalar/if/v1");
                        assert_eq!(args.len(), 3);
                        for (arg, value) in args[1..].iter().zip([None, Some(1)]) {
                            let branch = fragment.expressions().get(*arg).unwrap();
                            assert_eq!(branch.ty.data_type, DataType::Int64);
                            let actual = match &branch.kind {
                                ExprKind::Literal(
                                    novarocks_physical_plan::LiteralValue::Int64(v),
                                ) => Some(*v),
                                ExprKind::Constant(reference) => {
                                    let pool =
                                        &source.plan().constants().entries()[&reference.pool];
                                    assert_eq!(pool.value_type(), &branch.ty);
                                    let values = pool
                                        .array()
                                        .as_any()
                                        .downcast_ref::<arrow::array::Int64Array>()
                                        .unwrap();
                                    let row = usize::try_from(reference.ordinal).unwrap();
                                    assert_eq!(branch.ty.nullable, value.is_none());
                                    (!values.is_null(row)).then(|| values.value(row))
                                }
                                other => panic!("actual SQL integer source {other:?}"),
                            };
                            assert_eq!(actual, value);
                        }
                    }
                }
                assert_eq!(accesses, 1, "real nested List subscript is present");
            }

            let programs = compiler_results(&source, &installed_builtin_owner_catalogue());
            let mut producer = None;
            for program in programs.into_values() {
                let program = Arc::new(program.expect("actual complete JSON SQL package compiles"));
                let snapshot = program
                    .checked()
                    .channels()
                    .expressions()
                    .resolved_calls()
                    .snapshot();
                for (site, use_id) in snapshot.bindings() {
                    let calls = program.checked().channels().expressions().resolved_calls();
                    let Some(call) =
                        calls
                            .calls()
                            .get(&novarocks_local_program::ProgramCallSite::Expression(
                                novarocks_local_program::ProgramUseRef {
                                    arena: site.arena(),
                                    use_id: *use_id,
                                },
                            ))
                    else {
                        continue;
                    };
                    if call.call_contract().function_id().as_str()
                        != "builtin.scalar/json_object/v1"
                    {
                        continue;
                    }
                    assert_eq!(call.call_contract().logical_argument_count(), 2);
                    assert_eq!(
                        call.call_contract().effects().argument_control,
                        novarocks_type_contract::ArgumentControl::Eager
                    );
                    assert_eq!(
                        call.call_contract().effects().null_behavior,
                        novarocks_type_contract::FunctionNullBehavior::CalledOnNull
                    );
                    assert_eq!(
                        snapshot.flows()[&site.arena()].uses()[use_id]
                            .arguments
                            .len(),
                        2
                    );
                    let flow = &snapshot.flows()[&site.arena()];
                    let arena = &snapshot.roots().arenas()[&site.arena()];
                    let StaticExprKind::BoundCall { args, .. } =
                        arena.node(flow.uses()[use_id].definition).unwrap().kind()
                    else {
                        panic!("real JSON producer");
                    };
                    let key_slot = if complex_key || scenario == 4 {
                        None
                    } else {
                        let StaticExprKind::SlotId(slot) = arena.node(args[0]).unwrap().kind()
                        else {
                            panic!("real key slot");
                        };
                        Some(*slot)
                    };
                    let value = if scenario == 2 {
                        let StaticExprKind::BoundCall { args: inner, .. } =
                            arena.node(args[1]).unwrap().kind()
                        else {
                            panic!("required original inner call");
                        };
                        inner[1]
                    } else {
                        args[1]
                    };
                    let StaticExprKind::SlotId(value_slot) = arena.node(value).unwrap().kind()
                    else {
                        panic!("real value slot");
                    };
                    assert!(
                        producer
                            .replace((program.clone(), *site, key_slot, *value_slot))
                            .is_none()
                    );
                }
            }
            let (program, site, key_slot, value_slot) =
                producer.expect("actual installed JSONObject producer root");
            let ProgramExpressionRootSite::Node { node, .. } = site else {
                panic!("project root");
            };
            let ProgramNodeKind::Project { input, .. } =
                program.graph().nodes()[node.index()].kind()
            else {
                panic!("original project producer");
            };
            let schema = program.graph().nodes()[input.index()]
                .output_layout()
                .schema()
                .clone();
            let slots = program.graph().nodes()[input.index()]
                .output_layout()
                .slots();
            let columns = schema
                .fields()
                .iter()
                .zip(slots)
                .map(|(field, slot)| match field.data_type() {
                    DataType::Int32 => {
                        Arc::new(arrow::array::Int32Array::from(vec![1, 2, 3])) as ArrayRef
                    }
                    DataType::Int64 => Arc::new(arrow::array::Int64Array::from(if scenario >= 3 {
                        vec![Some(42), Some(-7), None]
                    } else {
                        vec![Some(1), None, Some(3)]
                    })) as ArrayRef,
                    DataType::Utf8 => Arc::new(StringArray::from(if key_slot == Some(*slot) {
                        vec![Some("a"), None, Some("b")]
                    } else {
                        assert_eq!(*slot, value_slot, "the real ordered value channel");
                        vec![Some("{\"z\":2,\"a\":1}"), None, Some("ordinary text")]
                    })) as ArrayRef,
                    other => panic!("exact authored input {other:?}"),
                })
                .collect();
            let batch = RecordBatch::try_new(schema, columns).unwrap();
            let tracker =
                crate::runtime::mem_tracker::MemTracker::new_root("json-object-actual-frame");
            tracker.install_limit_once(8 * 1024 * 1024).unwrap();
            let host = crate::exec::operators::compiled_aggregate::expression_allocation_host(
                tracker.clone(),
            );
            let mut frame = CompiledExpressionInstance::try_new_with_allocator(
                program.clone(),
                site,
                &FrameControl,
                Some(host),
            )
            .unwrap();
            let rows: &[usize] = if scenario == 4 { &[1] } else { &[0, 2] };
            let selection = Selection::try_sparse(3, rows).unwrap();
            let output = frame.evaluate_evaluation(
                &batch,
                selection,
                ScalarInvocationActivation::Activated,
                &FrameControl,
            );
            if complex_key {
                let Err(ScalarInvocationFailure::Data(data)) = output else {
                    panic!("whole-call complex-key Data");
                };
                assert_eq!(
                    data.message(),
                    "json_object key does not support complex type"
                );
                assert_eq!(data.source_row(0), Some(0));
                assert_eq!(data.source_row(1), Some(2));
                assert!(matches!(
                    frame.evaluate_evaluation(
                        &batch,
                        selection,
                        ScalarInvocationActivation::Activated,
                        &FrameControl
                    ),
                    Err(ScalarInvocationFailure::Kernel(
                        KernelFailure::InstanceFailed
                    ))
                ));
                drop(frame);
                assert!(tracker.current() > 0, "Data retains actual host funding");
                drop(data);
            } else {
                let output = output.unwrap();
                assert!(output.errors().is_empty());
                let text = output
                    .values()
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap();
                if scenario == 4 {
                    assert_eq!(text.len(), 1);
                    assert!(
                        text.is_null(0),
                        "a NULL nested index yields a successful NULL key"
                    );
                } else {
                    assert_eq!(text.value(0), "{\"a\": {\"a\": 1, \"z\": 2}}");
                    assert_eq!(text.value(1), "{\"b\": \"ordinary text\"}");
                }
                let alias = output.values().slice(if scenario == 4 { 0 } else { 1 }, 1);
                drop(output);
                drop(frame);
                assert!(
                    tracker.current() > 0,
                    "independent Arrow alias retains actual host funding"
                );
                drop(alias);
            }
            assert_eq!(tracker.current(), 0);
            for activation in [
                ScalarInvocationActivation::Activated,
                ScalarInvocationActivation::ValidateOnly,
            ] {
                let host = crate::exec::operators::compiled_aggregate::expression_allocation_host(
                    tracker.clone(),
                );
                let mut empty = CompiledExpressionInstance::try_new_with_allocator(
                    program.clone(),
                    site,
                    &FrameControl,
                    Some(host),
                )
                .unwrap();
                let output = empty
                    .evaluate_evaluation(
                        &batch,
                        Selection::try_sparse(3, &[]).unwrap(),
                        activation,
                        &FrameControl,
                    )
                    .unwrap();
                assert!(output.values().is_empty());
                assert!(output.errors().is_empty());
                drop(output);
                drop(empty);
                assert_eq!(tracker.current(), 0);
            }
        }
    }
}

#[test]
fn c16_p1_json_object_differential_unsupported_null_carriers_and_suppressed_nested_failure() {
    use arrow::array::new_null_array;
    let inner = Arc::new(Field::new("child", DataType::Int64, true));
    for data_type in [
        DataType::Date64,
        DataType::Time32(TimeUnit::Second),
        DataType::Time64(TimeUnit::Nanosecond),
        DataType::Duration(TimeUnit::Second),
        DataType::FixedSizeBinary(3),
        DataType::LargeList(inner.clone()),
        DataType::FixedSizeList(inner, 2),
        DataType::Dictionary(Box::new(DataType::Int8), Box::new(DataType::Utf8)),
        DataType::Utf8View,
        DataType::BinaryView,
        DataType::Decimal32(9, 2),
        DataType::Decimal64(18, 2),
    ] {
        let nulls = new_null_array(&data_type, 3);
        compare(vec![
            typed(Arc::new(StringArray::from(vec!["v"; 3]))),
            typed(nulls.clone()),
        ]);
        compare(vec![
            typed(nulls),
            typed(Arc::new(StringArray::from(vec!["v"; 3]))),
        ]);
    }
    let value: ArrayRef = Arc::new(StructArray::new(
        vec![Arc::new(Field::new("u", DataType::UInt32, true))].into(),
        vec![Arc::new(UInt32Array::from(vec![1, 2, 3]))],
        Some(NullBuffer::from(vec![false, true, true])),
    ));
    // The first NULL parent skips its hidden unsupported child. The middle
    // non-NULL parent is never decoded when its paired key is NULL.
    compare(vec![
        typed(Arc::new(StringArray::from(vec![Some("v"), None, None]))),
        typed(value.clone()),
    ]);
    compare(vec![
        typed(Arc::new(StringArray::from(vec![
            Some("v"),
            Some("v"),
            None,
        ]))),
        typed(value),
    ]);
}
