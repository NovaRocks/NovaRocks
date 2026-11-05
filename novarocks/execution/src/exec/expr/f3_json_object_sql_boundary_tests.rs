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

//! Retained JSON_OBJECT SQL/package/Frame boundary properties for F3.
//!
//! SQL owns binding and type facts; LocalCompiler and Frame consume the actual
//! frozen packages. Expected strings below follow the explicit input pairs:
//! JSON text is parsed and its object keys are ordered, while ordinary text is
//! quoted. No legacy evaluator or differential harness supplies expected values.

use std::sync::Arc;

use arrow::array::{Array, ArrayRef, StringArray};
use arrow::datatypes::{DataType, Field};
use arrow::record_batch::RecordBatch;
use novarocks_functions::{
    FunctionArgumentType, KernelEvaluationControl, KernelFailure, ScalarInvocationActivation,
    Selection,
};
use novarocks_type_contract::{FunctionValueType, ValueLogicalType};
use serde_json::{Value, json};

use crate::exec::expr::sql_test_source::sql_source_with_declared_fields;
use crate::runtime::task::ExecutionFailureCause;

fn result() -> FunctionValueType {
    FunctionValueType::try_with_logical_type(DataType::Utf8, true, ValueLogicalType::Json).unwrap()
}

// Only the declared input schema is needed by this SQL metadata test. The
// original five-row fixture's values are not evaluated to author these facts.
fn declared_fields() -> Vec<Field> {
    [
        ("id", DataType::Int32),
        ("k", DataType::Utf8),
        ("v", DataType::Utf8),
        ("n", DataType::Int64),
        ("flag", DataType::Boolean),
        ("day", DataType::Date32),
        ("amount", DataType::Decimal128(18, 4)),
    ]
    .into_iter()
    .map(|(name, data_type)| Field::new(name, data_type, true))
    .collect()
}

fn type_metadata(ty: &FunctionValueType) -> Value {
    let field = match ty.data_type() {
        DataType::List(field) => {
            json!({"name":field.name(), "nullable":field.is_nullable(), "data_type":format!("{:?}",field.data_type()), "metadata":field.metadata()})
        }
        _ => Value::Null,
    };
    json!({"physical_type":format!("{:?}",ty.data_type()),"nullable":ty.nullable(),"logical_type":format!("{:?}",ty.logical_type()),"field":field})
}
const ORIGINAL_SQL: &[(usize, &str)] = &[
    (
        4,
        r#"SELECT id, json_object(k, v, 'n', n, 'b', flag, 'day', day, 'amount', amount, 'dup', v, 'dup', n) AS object_value
FROM t ORDER BY id;"#,
    ),
    (
        5,
        r#"SELECT id, json_object(k) AS odd_tail FROM t ORDER BY id;"#,
    ),
    (
        6,
        r#"SELECT id, json_object(k, [id, n]) AS debug_value FROM t ORDER BY id;"#,
    ),
    (
        7,
        r#"SELECT id, json_object(k, v, 'n', n) AS selected_value FROM t WHERE id IN (1, 3, 5) ORDER BY id;"#,
    ),
    (
        8,
        r#"SELECT id, json_object(k, v) AS empty_value FROM t WHERE id < 0 ORDER BY id;"#,
    ),
    (
        9,
        r#"SELECT json_object(IF(id = 2, NULL, [id, n]), n) AS complex_key FROM t WHERE id = 1;"#,
    ),
    (
        10,
        r#"SELECT id FROM (SELECT id, json_object(IF(id = 2, NULL, [id, n]), n) AS unused_error FROM t) s ORDER BY id;"#,
    ),
    (
        11,
        r#"SELECT id FROM (SELECT id, json_object(IF(id = 2, NULL, [id, n]), n) AS unused_error FROM t) s WHERE id = 2 ORDER BY id;"#,
    ),
];

fn ancestors(
    plan: &novarocks_physical_plan::PhysicalPlan,
    start: (
        novarocks_physical_plan::FragmentId,
        novarocks_physical_plan::NodeId,
    ),
) -> std::collections::BTreeSet<(
    novarocks_physical_plan::FragmentId,
    novarocks_physical_plan::NodeId,
)> {
    use novarocks_physical_plan::NodeKind;
    let mut seen = std::collections::BTreeSet::new();
    let mut pending = vec![start];
    while let Some((fragment, node)) = pending.pop() {
        if !seen.insert((fragment, node)) {
            continue;
        }
        let current = &plan.fragments()[&fragment].nodes()[&node];
        pending.extend(current.inputs.iter().map(|id| (fragment, *id)));
        if let NodeKind::ExchangeSource { edge, .. } = &current.kind {
            let source = plan.edges()[edge].source.fragment;
            pending.push((source, plan.fragments()[&source].root()));
        }
    }
    seen
}
#[test]
fn json_object_legacy_native_oracle_original_sql_metadata_and_error_preservation() {
    use novarocks_physical_plan::{ExprKind, NodeKind};

    let fields = declared_fields();
    let mut records = Vec::new();
    for (query, sql) in ORIGINAL_SQL {
        let source = sql_source_with_declared_fields(sql, &fields);
        let plan = source.plan();
        let result = plan.result_port().unwrap();
        let live = ancestors(plan, (result.fragment, result.output.node));
        let mut owners = Vec::new();
        let mut call_metadata = Vec::new();
        for fragment in plan.fragments().values() {
            for (id, expr) in fragment.expressions().iter() {
                let ExprKind::FunctionCall { function, .. } = &expr.kind else {
                    continue;
                };
                if function.function_id.as_str() != "builtin.scalar/json_object/v1" {
                    continue;
                }
                assert_eq!(expr.ty.logical_type(), ValueLogicalType::Json);
                assert_eq!(expr.ty.data_type(), &DataType::Utf8);
                assert_eq!(function.result_type, expr.ty);
                assert!(
                    live.contains(&(fragment.id(), expr.owner)),
                    "original throwing use remains root-reachable"
                );
                let NodeKind::Project { expressions } = &fragment.nodes()[&expr.owner].kind else {
                    panic!("original throwing projection owner")
                };
                assert!(expressions.iter().any(|(root, _)| root == id));
                call_metadata.push(json!({"result_type":type_metadata(&function.result_type),"argument_types":function.argument_types.iter().map(|a| match a { FunctionArgumentType::Value(v) => type_metadata(v), _ => panic!("original value channel") }).collect::<Vec<_>>()}));
                owners.push((fragment.id(), expr.owner));
            }
        }
        assert!(!owners.is_empty(), "original JSON call is not pruned");
        if *query == 11 {
            let filters = live
                .iter()
                .filter(|(f, n)| {
                    matches!(plan.fragments()[f].nodes()[n].kind, NodeKind::Filter { .. })
                })
                .collect::<Vec<_>>();
            assert!(!filters.is_empty());
            assert!(
                filters.iter().any(|site| owners
                    .iter()
                    .all(|owner| ancestors(plan, **site).contains(owner))),
                "outer filter remains above the throwing project"
            );
            for (f, n) in &live {
                if let NodeKind::Scan {
                    relation,
                    residuals,
                    ..
                } = &plan.fragments()[f].nodes()[n].kind
                {
                    assert!(residuals.is_empty());
                    assert!(
                        relation.predicate_guarantees().is_empty(),
                        "no provider predicate hides a pushed filter"
                    );
                }
            }
        }
        let headers = result
            .fields
            .iter()
            .map(|f| f.alias.as_deref().unwrap_or(&f.name))
            .collect::<Vec<_>>();
        let expected: &[&str] = match query {
            4 => &["id", "object_value"],
            5 => &["id", "odd_tail"],
            6 => &["id", "debug_value"],
            7 => &["id", "selected_value"],
            8 => &["id", "empty_value"],
            9 => &["complex_key"],
            10 | 11 => &["id"],
            _ => unreachable!(),
        };
        assert_eq!(headers, expected);
        records.push(json!({"query":query,"sql":sql,"headers":headers,"fields":format!("{:?}",result.fields),"full_plan":format!("{plan:#?}"),"reachable_json_project_count":owners.len(),"call_metadata":call_metadata}));
    }
    if let Ok(path) = std::env::var("C16_ORIGINAL_SQL_OUT") {
        std::fs::write(path, serde_json::to_string_pretty(&json!({"mode":"CompiledPackage","fold":"NoOp original source fixture","queries":records})).unwrap()+"\n").unwrap();
    }
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
    use crate::exec::expr::compiled_program::CompiledExpressionInstance;
    use crate::exec::expr::filter_conjunction_actual_sql_compiler_tests::compiler_results;
    use crate::exec::expr::numeric_unary_ordered_sql_source_tests::installed_builtin_owner_catalogue;
    use novarocks_local_program::{ProgramExpressionRootSite, ProgramNodeKind, StaticExprKind};
    use novarocks_physical_plan::{ExprKind, PhysicalCallDefinition};

    {
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
            let source = sql_source_with_declared_fields(sql, &fields);
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
                            .join(format!("nested-index-compiled-{scenario}.txt")),
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
                            expression.ty.data_type(),
                            &DataType::List(Arc::new(Field::new("item", DataType::Int64, true)))
                        );
                        assert!(expression.ty.nullable());
                        let index = fragment.expressions().get(args[1]).unwrap();
                        assert_eq!(index.ty.data_type(), &DataType::Int32);
                        assert!(index.ty.nullable());
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
                        assert_eq!(conditional.ty.data_type(), &DataType::Int64);
                        assert!(conditional.ty.nullable());
                        let ExprKind::FunctionCall { function, args } = &conditional.kind else {
                            panic!("primitive SQL IF");
                        };
                        assert_eq!(function.function_id.as_str(), "builtin.scalar/if/v1");
                        assert_eq!(args.len(), 3);
                        for (arg, value) in args[1..].iter().zip([None, Some(1)]) {
                            let branch = fragment.expressions().get(*arg).unwrap();
                            assert_eq!(branch.ty.data_type(), &DataType::Int64);
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
                                    assert_eq!(branch.ty.nullable(), value.is_none());
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
            let host = crate::runtime::tracked_state_allocator::expression_allocation_host(
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
                let error = output.unwrap_err();
                let ExecutionFailureCause::ScalarInvocationData(data) = error.cause() else {
                    panic!("whole-call complex-key Data: {error:?}")
                };
                let data = data.clone();
                drop(error);
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
                    Err(error) if matches!(error.cause(), ExecutionFailureCause::Kernel(KernelFailure::InstanceFailed))
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
                let host = crate::runtime::tracked_state_allocator::expression_allocation_host(
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
