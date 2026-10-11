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
use arrow::array::{Int32Array, StringArray, new_empty_array};
use arrow::datatypes::Field;
use generate::InputProfile;
const POLICIES: [DecimalOverflowPolicy; 2] = [
    DecimalOverflowPolicy::OutputNull,
    DecimalOverflowPolicy::ReportError,
];
#[derive(Default)]
struct Collection {
    profiles: usize,
    selections: usize,
    rows: usize,
    failures: Vec<String>,
}
impl Collection {
    fn check(&mut self, label: String, spec: ScalarDiffSpec) {
        self.profiles += 1;
        match run_scalar_differential(&spec) {
            Ok(result) => {
                self.selections += result.selections;
                self.rows += result.rows;
            }
            Err(error) => self.failures.push(format!("{label}: {error:?}")),
        }
    }
    fn finish(self) {
        println!(
            "C12-P3 profiles={} selections={} rows={} disagreements={}",
            self.profiles,
            self.selections,
            self.rows,
            self.failures.len()
        );
        assert!(self.profiles > 0);
        assert!(self.failures.is_empty(), "{}", self.failures.join("\n"));
    }
}
#[test]
fn c12_p3_substring_index_generated_sliced_all_nullable_profiles_match_v1() {
    let mut collection = Collection::default();
    for mask in 0..8 {
        for policy in POLICIES {
            for seed in [0xC12301, 0xC12302, 0xC12303] {
                let mut generator = InputGenerator::new(seed);
                let mut spec = ScalarDiffSpec::new("substring_index")
                    .decimal_overflow(policy)
                    .expect_result_type(FunctionValueType::new(DataType::Utf8, true))
                    .sparse_selections(7, seed);
                for i in 0..3 {
                    let ty = FunctionValueType::new(
                        if i < 2 {
                            DataType::Utf8
                        } else {
                            DataType::Int32
                        },
                        mask & (1 << i) != 0,
                    );
                    let array = generator.column(&ty, 259, &InputProfile::default());
                    spec = spec.typed_column(ty, array.slice(1, 257));
                }
                collection.check(format!("{mask}/{policy:?}/{seed}"), spec);
            }
        }
    }
    assert_eq!(collection.profiles, 48);
    collection.finish();
}
#[test]
fn c12_p3_substring_index_fixed_forward_boundaries_constants_and_empty_match_v1() {
    let cases = [
        (Some("aaaaa"), Some("aa"), Some(1)),
        (Some("aaaaa"), Some("aa"), Some(-1)),
        (Some("aaaaa"), Some("aa"), Some(-2)),
        (Some("aaaaa"), Some("aa"), Some(-3)),
        (Some("ababa"), Some("aba"), Some(-1)),
        (Some("/a//"), Some("/"), Some(-2)),
        (Some("é::你好::\0"), Some("::"), Some(-2)),
        (Some(""), Some("/"), Some(-1)),
        (Some("aaa"), Some("a"), Some(i32::MIN)),
        (Some("aaa"), Some("a"), Some(i32::MAX)),
        (Some("no match"), Some("/"), Some(i32::MIN)),
        (Some("hidden"), Some(""), Some(-1)),
        (Some("hidden"), Some("/"), Some(0)),
        (None, Some("/"), Some(1)),
        (Some("hidden"), None, Some(1)),
        (Some("hidden"), Some("/"), None),
    ];
    let mut collection = Collection::default();
    let large = "x".repeat(1_048_577);
    for policy in POLICIES {
        collection.check(
            format!("fixed/{policy:?}"),
            ScalarDiffSpec::new("substring_index")
                .decimal_overflow(policy)
                .column(Arc::new(StringArray::from(
                    cases.iter().map(|v| v.0).collect::<Vec<_>>(),
                )))
                .column(Arc::new(StringArray::from(
                    cases.iter().map(|v| v.1).collect::<Vec<_>>(),
                )))
                .column(Arc::new(Int32Array::from(
                    cases.iter().map(|v| v.2).collect::<Vec<_>>(),
                )))
                .sparse_selections(7, 0xC12301),
        );
        collection.check(
            format!("large/{policy:?}"),
            ScalarDiffSpec::new("substring_index")
                .decimal_overflow(policy)
                .column(Arc::new(StringArray::from(vec![Some(large.as_str())])))
                .column(Arc::new(StringArray::from(vec![Some("y")])))
                .column(Arc::new(Int32Array::from(vec![Some(i32::MAX)]))),
        );
        for count in [-1, 0, 1, i32::MIN] {
            for rows in [0, 1, 9] {
                collection.check(
                    format!("constant/{policy:?}/{count}/{rows}"),
                    ScalarDiffSpec::new("substring_index")
                        .decimal_overflow(policy)
                        .constant_array(Arc::new(StringArray::from(vec![Some("é::你好::\0")])))
                        .constant_array(Arc::new(StringArray::from(vec![Some("::")])))
                        .constant_array(Arc::new(Int32Array::from(vec![Some(count)])))
                        .constant_rows(rows)
                        .sparse_selections(3, 0xC12303),
                );
            }
        }
        for nullable in [false, true] {
            let mut spec = ScalarDiffSpec::new("substring_index").decimal_overflow(policy);
            for data_type in [DataType::Utf8, DataType::Utf8, DataType::Int32] {
                spec = spec.typed_column(
                    FunctionValueType::new(data_type.clone(), nullable),
                    new_empty_array(&data_type),
                );
            }
            collection.check(format!("empty/{policy:?}/{nullable}"), spec);
        }
    }
    assert_eq!(collection.profiles, 32);
    collection.finish();
}
#[test]
fn c12_p3_substring_index_actual_sql_keeps_ordered_three_child_types_in_both_modes() {
    use crate::exec::expr::numeric_unary_original_nonnull_sql_baseline_tests::sql_source_with_declared_fields;
    use novarocks_physical_plan::{
        ExprKind, FunctionArgumentType, PhysicalCallDefinition, StaticFunctionArgument,
    };
    use novarocks_sql::compiler::SqlPhysicalEmissionMode;
    let types = [DataType::Utf8, DataType::Utf8, DataType::Int32];
    for mode in [
        SqlPhysicalEmissionMode::OriginalNativeV1,
        SqlPhysicalEmissionMode::ExactComputedWithOriginalDeclaration,
    ] {
        let fields = ["txt", "delim", "cnt"]
            .into_iter()
            .zip(&types)
            .map(|(name, ty)| Field::new(name, ty.clone(), true))
            .collect::<Vec<_>>();
        let owner = sql_source_with_declared_fields(
            "SELECT substring_index(txt,delim,cnt) FROM t",
            &fields,
            mode,
        );
        let mut count = 0;
        for fragment in owner.plan().fragments().values() {
            for (id, node) in fragment.expressions().iter() {
                let ExprKind::FunctionCall { function, args } = &node.kind else {
                    continue;
                };
                if function.function_id.as_str() != "builtin.scalar/substring_index/v1" {
                    continue;
                }
                let request = fragment
                    .call_requests()
                    .get(PhysicalCallDefinition::Expression(*id))
                    .unwrap();
                assert_eq!(args.len(), 3);
                assert_eq!(request.logical_argument_count, 3);
                // Opaque source payloads establish ordered type correspondence;
                // distinct argument values are observed by compiled slots and Native.
                for (i, arg) in args.iter().enumerate() {
                    let StaticFunctionArgument::Value {
                        value_type,
                        constant,
                    } = &request.arguments[i]
                    else {
                        panic!("actual value source")
                    };
                    let FunctionArgumentType::Value(selected) = &function.argument_types[i] else {
                        panic!("actual selected source")
                    };
                    assert!(constant.is_none());
                    assert_eq!(value_type, selected);
                    assert_eq!(selected, &FunctionValueType::new(types[i].clone(), true));
                    let child = fragment.expressions().get(*arg).unwrap();
                    assert_eq!(&child.ty, selected);
                    assert!(!matches!(child.kind, ExprKind::Literal(_)));
                }
                assert_eq!(node.ty, FunctionValueType::new(DataType::Utf8, true));
                count += 1;
                println!("C12-P3 retained {mode:?} definition={id:?} children={args:?}");
            }
        }
        assert!(count > 0);
    }
}
fn compiled_slot_program(
    arrays: &[ArrayRef],
    constant_count: Option<i32>,
    text_child: bool,
) -> Arc<novarocks_local_program::LocalProgram> {
    use crate::exec::expr::compiled_program::guarded_tests as host;
    use novarocks_functions::{
        EngineFunctionCatalogBuilder, FunctionArgument, FunctionId, FunctionKind,
        InstalledPureKernel, PureImplementationDeclaration, PureImplementationId, PureKernelAbi,
    };
    use novarocks_physical_plan::{
        ExprKind, FragmentBuilder, FragmentId, FragmentSink, LiteralValue, NodeId,
        PipelineDopDomain, ResultField, ResultPort, ValueDef, ValueId, ValueOrigin,
    };
    use novarocks_type_contract::{ControlShape, SemanticParameters};
    use std::collections::BTreeMap;
    let name = "substring_index";
    let actual = builtin_engine_function_catalog();
    let mut catalog = EngineFunctionCatalogBuilder::new();
    let mut installed = vec![];
    for function_name in [name, "concat"] {
        let definition = actual
            .definition(function_name, FunctionKind::Scalar)
            .unwrap();
        catalog.register(definition.clone()).unwrap();
        installed.push(InstalledPureKernel {
            function: FunctionId::try_new(format!("builtin.scalar/{function_name}/v1")).unwrap(),
            kind: FunctionKind::Scalar,
            implementation: PureImplementationDeclaration {
                overload: definition.binding_declaration().unwrap().overloads()[0]
                    .identity
                    .clone(),
                implementation: PureImplementationId::try_new(format!(
                    "builtin.scalar/{function_name}/selected-v1"
                ))
                .unwrap(),
                abi: PureKernelAbi::ScalarV1,
            },
            aggregate_state_format: None,
        });
    }
    let functions = catalog.seal_pure(installed).unwrap();
    let fragment_id = FragmentId::new(196);
    let output = NodeId::new(0);
    let input = NodeId::new(1);
    let source = NodeId::new(2);
    let mut builder = FragmentBuilder::new(fragment_id);
    builder
        .add_values(source, Box::from([Box::default()]), Box::default())
        .unwrap();
    let mut projections = vec![];
    let mut values = vec![];
    let mut args = vec![];
    let mut requests = vec![];
    for (i, array) in arrays.iter().enumerate() {
        let ty = FunctionValueType::new(array.data_type().clone(), true);
        let value = ValueId::new(i as u32 + 10);
        let literal = builder
            .add_expression(input, ty.clone(), ExprKind::Literal(LiteralValue::Null))
            .unwrap();
        builder
            .insert_value(ValueDef {
                id: value,
                ty: ty.clone(),
                origin: ValueOrigin::Expr {
                    node: input,
                    expr: literal,
                },
            })
            .unwrap();
        projections.push((literal, value));
        values.push(value);
        if i != 2 || constant_count.is_none() {
            args.push(
                builder
                    .add_expression(output, ty.clone(), ExprKind::Value(value))
                    .unwrap(),
            );
        }
        requests.push(FunctionArgument::Value {
            value_type: ty,
            constant: None,
        });
    }
    builder
        .add_project(
            input,
            source,
            projections.into_boxed_slice(),
            values.into_boxed_slice(),
        )
        .unwrap();
    let mut authors = BTreeMap::new();
    if let Some(count) = constant_count {
        let ty = FunctionValueType::new(DataType::Int32, true);
        let value = novarocks_functions::ConstantValue::from_i32(
            Arc::new(ty.try_to_field("count").unwrap()),
            ty.clone(),
            count,
            novarocks_functions::ConstantPolicy {
                max_rows: 16,
                max_array_nodes: 128,
                max_logical_elements: 1024,
                max_retained_buffer_bytes: 1 << 20,
                max_type_depth: 64,
                max_type_nodes: 4096,
                max_dictionary_depth: 64,
                max_metadata_bytes: 1 << 20,
                max_library_validation_work: 1 << 20,
                max_library_validation_bytes: 1 << 20,
            },
            CompilePhase::FunctionSpecialization,
            &HarnessControl,
        )
        .unwrap();
        // The original request exporter allocates exactly this sole constant
        // backing as its first pool. Use its checked Int32 address, rather
        // than declaring an Int64 literal to be an Int32 value.
        args.push(
            builder
                .add_expression(
                    output,
                    ty.clone(),
                    ExprKind::Constant(novarocks_physical_plan::ConstantReference {
                        pool: novarocks_physical_plan::ConstantPoolId::new(0),
                        ordinal: value.ordinal(),
                    }),
                )
                .unwrap(),
        );
        requests[2] = FunctionArgument::Value {
            value_type: ty,
            constant: Some(value),
        };
    }
    if text_child {
        let child_author = host::author(
            &functions,
            "concat",
            vec![requests[0].clone(); 2],
            ControlShape::Eager,
        );
        args[0] = host::call(&mut builder, &mut authors, child_author, vec![args[0]; 2]);
    }
    let author = host::author(&functions, name, requests, ControlShape::Eager);
    let result_ty = author.result();
    let call = host::call(&mut builder, &mut authors, author, args);
    let value = builder
        .add_value(
            result_ty.clone(),
            ValueOrigin::Expr {
                node: output,
                expr: call,
            },
        )
        .unwrap();
    builder
        .add_project(
            output,
            input,
            Box::from([(call, value)]),
            Box::from([value]),
        )
        .unwrap();
    let fragment = builder
        .finish_definition(
            output,
            FragmentSink::Result,
            PipelineDopDomain {
                min: 1,
                max: 1,
                requires_power_of_two: false,
            },
        )
        .unwrap();
    let result = ResultPort {
        scalar_schema: None,
        fragment: fragment_id,
        output: fragment.nodes()[&output].output.clone(),
        fields: Box::from([ResultField {
            domain: crate::test_result_domain::result_value_domain(&result_ty),
            name: name.into(),
            alias: None,
            value,
            ty: result_ty,
        }]),
    };
    host::compile_checked_fragment_with_parameters(
        &functions,
        fragment,
        &authors,
        result,
        SemanticParameters::try_new([]).unwrap(),
    )
}

fn compiled_input(
    program: &novarocks_local_program::LocalProgram,
    arrays: &[ArrayRef],
) -> (
    RecordBatch,
    novarocks_local_program::ProgramExpressionRootSite,
) {
    use novarocks_local_program::{
        ProgramExpressionRootSite, ProgramNodeExpressionRole, ProgramNodeKind,
    };
    let root_id = program.graph().root();
    let node = program
        .graph()
        .nodes()
        .iter()
        .find(|n| n.local_id() == Some(root_id))
        .unwrap();
    let ProgramNodeKind::Project { input, .. } = node.kind() else {
        panic!("actual Project root")
    };
    let input = program
        .graph()
        .nodes()
        .iter()
        .find(|n| n.local_id() == Some(*input))
        .unwrap();
    (
        RecordBatch::try_new(input.output_layout().schema().clone(), arrays.to_vec()).unwrap(),
        ProgramExpressionRootSite::Node {
            node: root_id,
            role: ProgramNodeExpressionRole::ProjectOutput { expression: 0 },
        },
    )
}
#[test]
fn c12_p3_substring_index_actual_compiled_slots_constants_strict_null_and_explicit_empty_match_v1()
{
    use crate::exec::expr::compiled_program::CompiledExpressionInstance;
    let mut observations = 0;
    for text in [
        vec![Some("aaaaa"), None, Some("é::你好::\0"), Some("/edge/")],
        vec![None; 4],
    ] {
        let arrays = vec![
            Arc::new(StringArray::from(text)) as ArrayRef,
            Arc::new(StringArray::from(vec![
                Some("aa"),
                Some("/"),
                Some("::"),
                Some("/"),
            ])) as ArrayRef,
            Arc::new(Int32Array::from(vec![Some(-1), Some(0), Some(-2), Some(1)])) as ArrayRef,
        ];
        for constant_count in [None, Some(-1), Some(0)] {
            let program = compiled_slot_program(&arrays, constant_count, false);
            let (batch, root) = compiled_input(&program, &arrays);
            // The real checked compiler helper uses ReportError. Other policy
            // observations belong to the direct differential fixtures above.
            let mut spec = ScalarDiffSpec::new("substring_index")
                .decimal_overflow(DecimalOverflowPolicy::ReportError);
            for (i, array) in arrays.iter().enumerate() {
                spec = if i == 2 && constant_count.is_some() {
                    spec.constant_array(Arc::new(Int32Array::from(vec![constant_count])))
                } else {
                    spec.column(array.clone())
                };
            }
            let bound = resolve_like_sql(
                builtin_engine_function_catalog(),
                "substring_index",
                CatalogKind::Scalar,
                &spec.arguments,
            )
            .unwrap();
            let FunctionResultType::Scalar(ty) = &bound.selected.result_type else {
                panic!("scalar result")
            };
            let legacy = ScalarCall {
                spec: &spec,
                legacy_kind: scalar_legacy_implementation("substring_index").unwrap(),
                result_type: ty,
                rows: 4,
            };
            for rows in [None, Some(&[0, 2, 3][..]), Some(&[][..])] {
                let selection = rows.map_or_else(
                    || Selection::all(4),
                    |rows| Selection::try_sparse(4, rows).unwrap(),
                );
                let expected = legacy.evaluate_legacy_inner(rows).unwrap();
                let mut instance =
                    CompiledExpressionInstance::try_new(program.clone(), root, &HarnessControl)
                        .unwrap();
                let actual = instance
                    .evaluate(&batch, selection, &HarnessControl)
                    .unwrap();
                assert_eq!(actual.selection(), selection);
                assert_eq!(actual.values().data_type(), &ty.data_type);
                assert!(actual.errors().is_empty());
                assert_eq!(actual.values().len(), expected.len());
                for i in 0..expected.len() {
                    compare_value(&expected, i, actual.values(), i, FloatComparison::Exact)
                        .unwrap();
                }
                observations += 1;
            }
        }
    }
    assert_eq!(observations, 18);
    println!(
        "C12-P3 actual compiled ReportError observations={observations}; deterministic positive-carrier empty masks included"
    );
}
#[derive(Default)]
struct ChildControl {
    trace: std::sync::Mutex<Vec<u32>>,
    stop: Option<usize>,
}
impl KernelEvaluationControl for ChildControl {
    fn checkpoint(&self, units: u32) -> Result<(), KernelFailure> {
        let mut trace = self.trace.lock().unwrap();
        let at = trace.len();
        if let Some(stop) = self.stop {
            assert!(at <= stop, "callback after child cause");
        }
        trace.push(units);
        if self.stop == Some(at) {
            Err(KernelFailure::Cancelled)
        } else {
            Ok(())
        }
    }
    fn wait(&self, _: Duration) -> Result<(), KernelFailure> {
        panic!("pure child never waits")
    }
}
#[test]
fn c12_p3_substring_index_real_required_text_child_control_failure_survives_strict_null_and_latches()
 {
    use crate::exec::expr::compiled_program::CompiledExpressionInstance;
    let wide = "x".repeat(1025);
    let arrays = [
        Arc::new(StringArray::from(vec![Some(wide.as_str())])) as ArrayRef,
        Arc::new(StringArray::from(vec![None::<&str>])) as ArrayRef,
        Arc::new(Int32Array::from(vec![1])) as ArrayRef,
    ];
    let mut child_trace = None;
    for text_child in [false, true] {
        let program = compiled_slot_program(&arrays, None, text_child);
        let (batch, root) = compiled_input(&program, &arrays);
        let good = ChildControl::default();
        let mut instance =
            CompiledExpressionInstance::try_new(program.clone(), root, &HarnessControl).unwrap();
        let output = instance.evaluate(&batch, Selection::all(1), &good).unwrap();
        assert!(output.errors().is_empty());
        assert!(output.values().is_null(0));
        let trace = good.trace.lock().unwrap().clone();
        if text_child {
            assert!(trace.contains(&256));
            child_trace = Some((program, batch, root, trace));
        } else {
            assert!(!trace.contains(&256));
        }
    }
    let (program, batch, root, trace) = child_trace.unwrap();
    let at = trace.iter().position(|u| *u == 256).unwrap();
    let control = ChildControl {
        trace: std::sync::Mutex::new(vec![]),
        stop: Some(at),
    };
    let mut instance = CompiledExpressionInstance::try_new(program, root, &HarnessControl).unwrap();
    assert_eq!(
        instance
            .evaluate(&batch, Selection::all(1), &control)
            .unwrap_err(),
        KernelFailure::Cancelled
    );
    assert_eq!(*control.trace.lock().unwrap(), trace[..=at]);
    let after = ChildControl::default();
    assert_eq!(
        instance
            .evaluate(&batch, Selection::all(1), &after)
            .unwrap_err(),
        KernelFailure::InstanceFailed
    );
    assert!(after.trace.lock().unwrap().is_empty());
    println!(
        "C12-P3 actual concat child quantum refusal preserved through strict-null parent; failed replay has zero callbacks"
    );
}
