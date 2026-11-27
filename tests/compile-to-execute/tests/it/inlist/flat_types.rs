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
use arrow::array::{
    ArrayRef, BinaryArray, BooleanArray, Date32Array, FixedSizeBinaryArray, Float64Array,
    UInt64Array,
};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use novarocks_expression::testkit::{
    CountingAllocator, HarnessControl, PassThroughCallHost, add_catalogue_predicate,
    constant_policy, declare_call_controls, finish_unit, fixture_scope,
};
use novarocks_expression::{
    CompiledExprSet, CompiledExpressionInstance, EvaluationEnvironment, EvaluationInput,
    ScopeLayouts, compile_expr_set_pure,
};
use novarocks_expression_contract::{
    BoundExprSetBuilder, ExprKind, ExprPort, RootOrdinal, RootScope, UnitLimits, ValueId,
};
use novarocks_functions::{
    KernelEvaluationControl, KernelFailure, ScalarInvocationActivation, ScalarInvocationScope,
    Selection,
};
use novarocks_physical_plan::ConstantPools;
use novarocks_sql_testkit::{installed_builtin_owner_catalogue, sql_source_with_declared_fields};
use novarocks_test_compile_to_execute::compiler_results::compiler_results_with_declared_read_fields;
use novarocks_test_compile_to_execute::programs::compiled_roots;
use novarocks_type_contract::{
    CompileCheckpoints, CompilePhase, ControlShape, EvaluationDemand, FunctionArgumentType,
    FunctionValueType, OperatorFunction, SemanticParameters, ValueLogicalType,
};
use std::{collections::BTreeMap, sync::Arc, time::Duration};

struct Control;
impl KernelEvaluationControl for Control {
    fn checkpoint(&self, units: u32) -> Result<(), KernelFailure> {
        assert!(units <= 256);
        Ok(())
    }
    fn wait(&self, _: Duration) -> Result<(), KernelFailure> {
        panic!("IN does not wait")
    }
}

fn evaluate(sql: &str, array: &ArrayRef, expected: &[Option<bool>]) {
    let fields = [Field::new("k", array.data_type().clone(), true)];
    let source = sql_source_with_declared_fields(sql, &fields);
    let programs = compiler_results_with_declared_read_fields(
        &source,
        &fields,
        &installed_builtin_owner_catalogue(),
    );
    let mut found = None;
    for program in programs.into_values() {
        let program = program.unwrap();
        for (site, unit, root) in compiled_roots(&program) {
            let uses = unit.control_flow().uses();
            let invocation = &uses[&root.use_id];
            let positive = invocation.control == ControlShape::CandidateGroup;
            let negative = unit.calls().get(&root.use_id).is_some_and(|call| {
                call.call_contract().function_id().as_str() == OperatorFunction::BooleanNot.function_id_text()
                    && matches!(invocation.arguments.as_ref(), [child] if uses[child].control == ControlShape::CandidateGroup)
            });
            if positive || negative {
                assert!(found.replace((Arc::clone(unit), site.root)).is_none());
            }
        }
    }
    let (unit, root) = found.expect("actual SQL membership root");
    evaluate_unit(&unit, root, vec![Arc::clone(array)], expected);
}

fn evaluate_unit(
    unit: &Arc<CompiledExprSet>,
    root: RootOrdinal,
    columns: Vec<ArrayRef>,
    expected: &[Option<bool>],
) {
    let schema = unit
        .scope(unit.root(root).unwrap().scope)
        .unwrap()
        .schema()
        .clone();
    let batch = RecordBatch::try_new(schema, columns).unwrap();
    let allocator = Arc::new(CountingAllocator::new(usize::MAX));
    let mut frame = CompiledExpressionInstance::try_new_with_allocator(
        Arc::clone(unit),
        root,
        &Control,
        Some(allocator.clone()),
    )
    .unwrap();
    let selection = Selection::all(batch.num_rows());
    let output = frame
        .evaluate(
            EvaluationInput::Batch {
                batch: &batch,
                source: None,
            },
            ScalarInvocationScope::new(selection, selection),
            ScalarInvocationActivation::Activated,
            &Control,
            &mut PassThroughCallHost,
        )
        .unwrap();
    assert!(output.errors().is_empty());
    let actual = output
        .values()
        .as_any()
        .downcast_ref::<BooleanArray>()
        .unwrap();
    assert_eq!(actual.iter().collect::<Vec<_>>(), expected);
}

#[test]
fn wide_in_actual_sql_roundtrip_with_dynamic_probe_hits_and_misses() {
    use arrow::array::Int64Array;
    use novarocks_physical_plan::StaticFunctionArgument;
    use novarocks_test_compile_to_execute::programs::producer_root;

    let fields = [Field::new("k", DataType::Int64, true)];
    let start = i64::from(i32::MAX) + 1;
    for candidate_count in [4095, 4096] {
        let end = start + candidate_count;
        let candidates = (start..end)
            .map(|value| value.to_string())
            .collect::<Vec<_>>()
            .join(",");
        let sql = format!("SELECT k IN ({candidates}) AS matches FROM fixture.source");
        let source = sql_source_with_declared_fields(&sql, &fields);
        let mut actual_groups = 0;
        let programs =
            crate::fixtures::compile_sql_package_roundtrip(&source, &fields, |package| {
                for node in package.fragment().nodes().values() {
                    let Some(unit) = node.expressions.as_ref() else {
                        continue;
                    };
                    for call in unit.definitions() {
                        let ExprKind::FunctionCall { function, args } = &call.kind else {
                            continue;
                        };
                        if function.function_id.as_str()
                            != OperatorFunction::InList.function_id_text()
                        {
                            continue;
                        }
                        actual_groups += 1;
                        assert_eq!(args.len(), candidate_count as usize + 1);
                        assert!(
                            unit.definition(args[0])
                                .unwrap()
                                .kind
                                .input_value()
                                .is_some()
                        );
                        assert!(matches!(
                            function.argument_types.as_ref(),
                            [
                                FunctionArgumentType::Value(_),
                                FunctionArgumentType::CandidateGroup { .. },
                            ]
                        ));
                        let request = &unit.requests()[&call.id];
                        assert_eq!(request.logical_argument_count, 2);
                        let [
                            StaticFunctionArgument::Value { constant: None, .. },
                            StaticFunctionArgument::CandidateGroup { members, .. },
                        ] = request.arguments.as_ref()
                        else {
                            panic!(
                                "decoded IN must keep the actual probe and ordered candidate group"
                            );
                        };
                        assert_eq!(members.len(), candidate_count as usize);
                        for (ordinal, (&source, member)) in
                            args[1..].iter().zip(members).enumerate()
                        {
                            let reference =
                                member.constant.expect("wide IN source is an exact literal");
                            let definition = unit.definition(source).unwrap();
                            assert_eq!(definition.kind, ExprKind::Constant(reference));
                            assert_eq!(definition.ty, member.value_type);
                            let value = package.constants().entries()[&reference.pool]
                                .value(reference.ordinal)
                                .unwrap();
                            assert_eq!(value.try_i64().unwrap(), Some(start + ordinal as i64));
                        }
                        assert!(unit.control_flow().uses().values().any(|invocation| {
                            invocation.definition == call.id
                                && invocation.control == ControlShape::CandidateGroup
                                && invocation.arguments.len() == args.len()
                        }));
                    }
                }
            });
        assert_eq!(actual_groups, 1);
        let (program, root) = producer_root(&programs);
        evaluate_unit(
            program.unit(root.unit).unwrap(),
            root.root,
            vec![Arc::new(Int64Array::from(vec![
                Some(start),
                Some(end - 1),
                Some(end),
                None,
            ]))],
            &[Some(true), Some(true), Some(false), None],
        );
    }
}

#[test]
fn compiled_flat_in_and_not_in_propagate_null_membership() {
    let cases = [
        (
            Arc::new(BooleanArray::from(vec![Some(true), Some(false), None])) as ArrayRef,
            "TRUE, CAST(NULL AS BOOLEAN)",
        ),
        (
            Arc::new(Float64Array::from(vec![Some(1.5), Some(2.5), None])) as ArrayRef,
            "CAST(1.5 AS DOUBLE), CAST(NULL AS DOUBLE)",
        ),
        (
            Arc::new(Date32Array::from(vec![Some(0), Some(1), None])) as ArrayRef,
            "DATE '1970-01-01', CAST(NULL AS DATE)",
        ),
        (
            Arc::new(BinaryArray::from(vec![
                Some(&b"a"[..]),
                Some(&b"b"[..]),
                None,
            ])) as ArrayRef,
            "X'61', CAST(NULL AS VARBINARY)",
        ),
    ];
    for (values, candidates) in cases {
        evaluate(
            &format!("SELECT k IN ({candidates}) FROM t0"),
            &values,
            &[Some(true), None, None],
        );
        evaluate(
            &format!("SELECT k NOT IN ({candidates}) FROM t0"),
            &values,
            &[Some(false), None, None],
        );
    }
}

#[test]
fn compiled_flat_not_in_distinguishes_absent_from_null_source() {
    let values: ArrayRef = Arc::new(Float64Array::from(vec![Some(1.5), Some(2.5), None]));
    evaluate(
        "SELECT k NOT IN (CAST(1.5 AS DOUBLE), CAST(3.5 AS DOUBLE)) FROM t0",
        &values,
        &[Some(false), Some(true), None],
    );
}

#[test]
fn compiled_in_preserves_unsigned_and_nominal_uuid_port_types() {
    // Exercise explicitly authored expression ports independently of SQL
    // connector admission, which has no UInt64 scan output.
    let functions = installed_builtin_owner_catalogue();
    let parameters = SemanticParameters::default();
    let policy = constant_policy();
    let columns = [
        [Some(u64::MAX), Some(u64::MAX), None, Some(u64::MAX)],
        [Some(u64::MAX), Some(8), Some(u64::MAX), None],
        [None, Some(9), Some(8), Some(9)],
    ];
    let cases = [
        (
            FunctionValueType::new(DataType::UInt64, true),
            columns.map(|values| Arc::new(UInt64Array::from(values.to_vec())) as ArrayRef),
        ),
        (
            FunctionValueType::try_with_logical_type(
                DataType::FixedSizeBinary(16),
                true,
                ValueLogicalType::Uuid,
            )
            .unwrap(),
            columns.map(|values| {
                let bytes = values.map(|value| value.map(|value| u128::from(value).to_be_bytes()));
                Arc::new(
                    FixedSizeBinaryArray::try_from_sparse_iter_with_size(bytes.into_iter(), 16)
                        .unwrap(),
                ) as ArrayRef
            }),
        ),
    ];
    for (ty, arrays) in cases {
        let mut constants = ConstantPools::empty();
        let values = [0, 1, 2].map(ValueId::new);
        let port = ExprPort {
            values: values.map(|value| (value, ty.clone())).into(),
        };
        let schema = Arc::new(Schema::new(
            ["source", "first", "second"].map(|name| Arc::new(ty.try_to_field(name).unwrap())),
        ));
        let scope = fixture_scope(&port, schema);
        let mut builder = BoundExprSetBuilder::new(UnitLimits {
            expression_semantic_depth: 8,
        });
        let [source, first, second] = values.map(|value| {
            builder
                .add(None, ty.clone(), ExprKind::input(value))
                .unwrap()
        });
        let membership = add_catalogue_predicate(
            &mut builder,
            &mut constants,
            OperatorFunction::InList,
            vec![source, first, second],
        )
        .unwrap();
        let root = builder
            .root(membership, EvaluationDemand::Value, RootScope::Port(0))
            .unwrap();
        let mut work =
            CompileCheckpoints::try_new(&HarnessControl, CompilePhase::Validate).unwrap();
        declare_call_controls(&mut builder, &functions, &constants, &parameters, &mut work);
        let sealed = builder.seal(vec![port], &mut work).unwrap();
        let unit = finish_unit(
            &functions,
            sealed,
            &BTreeMap::new(),
            &constants,
            &parameters,
            &mut work,
        );
        work.finish().unwrap();

        let mut work =
            CompileCheckpoints::try_new(&HarnessControl, CompilePhase::LowerProgram).unwrap();
        let compiled = compile_expr_set_pure(
            &unit,
            EvaluationEnvironment {
                constants: &constants,
                parameters: &parameters,
            },
            ScopeLayouts {
                ports: &[scope],
                all_ports: None,
            },
            policy,
            &functions,
            &mut work,
        )
        .unwrap();
        work.finish().unwrap();

        let use_id = compiled.root(root).unwrap().use_id;
        let call = &compiled.calls()[&use_id];
        assert!(matches!(
            call.prepared(),
            novarocks_functions::PreparedPureKernel::Membership(_)
        ));
        assert_eq!(
            call.call_contract().selected().argument_types.as_ref(),
            &[
                FunctionArgumentType::Value(ty.clone()),
                FunctionArgumentType::CandidateGroup { element_type: ty },
            ]
        );
        let invocation = &compiled.control_flow().uses()[&use_id];
        let ordinals: Vec<_> = invocation
            .arguments
            .iter()
            .map(|&argument| compiled.input(argument).unwrap().ordinal)
            .collect();
        assert_eq!(ordinals, [0, 1, 2]);
        evaluate_unit(
            &compiled,
            root,
            arrays.into(),
            &[Some(true), Some(false), None, None],
        );
    }
}
