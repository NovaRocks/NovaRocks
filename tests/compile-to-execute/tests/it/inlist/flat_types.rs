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
    CountingAllocator, HarnessControl, PassThroughCallHost, finish_unit, fixture_scope,
};
use novarocks_expression::{
    CompiledExprSet, CompiledExpressionInstance, EvaluationEnvironment, EvaluationInput,
    ScopeLayouts, StaticExprKind, compile_expr_set_pure,
};
use novarocks_expression_contract::{
    BoundExprSetBuilder, ExprKind, ExprPort, RootOrdinal, RootScope, UnitLimits, ValueId,
};
use novarocks_functions::{
    KernelEvaluationControl, KernelFailure, ScalarInvocationActivation, ScalarInvocationScope,
    Selection,
};
use novarocks_sql_testkit::{installed_builtin_owner_catalogue, sql_source_with_declared_fields};
use novarocks_test_compile_to_execute::compiler_results::compiler_results_with_declared_read_fields;
use novarocks_test_compile_to_execute::programs::compiled_roots;
use novarocks_type_contract::{
    CompileCheckpoints, CompilePhase, EvaluationDemand, FunctionValueType, ValueLogicalType,
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
            if matches!(
                unit.definitions().node(root.definition).unwrap().kind(),
                StaticExprKind::PreparedInList { .. },
            ) {
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
fn compiled_in_preserves_unsigned_sql_fields_and_exact_nominal_uuid() {
    let functions = installed_builtin_owner_catalogue();
    let sql = "SELECT k IN (c1,c2) FROM t0";
    let column_names = ["k", "c1", "c2"];
    let unsigned_fields = column_names.map(|name| Field::new(name, DataType::UInt64, true));
    let source = sql_source_with_declared_fields(sql, &unsigned_fields);
    let plan = source.plan();
    let environment = EvaluationEnvironment {
        constants: plan.constants(),
        parameters: plan.parameters(),
    };
    let policy = plan.statement_constant_policy();

    // UInt64 can be authored from a declared SQL column, but has no current
    // connector comparison carrier. Lower its original SQL unit directly.
    let mut unsigned_root = None;
    for fragment in plan.fragments().values() {
        for node in fragment.nodes().values() {
            let Some(unit) = fragment.unit(node.id) else {
                continue;
            };
            for (index, root) in unit.roots().iter().enumerate() {
                if matches!(
                    unit.definition(root.definition).unwrap().kind,
                    ExprKind::InList { .. }
                ) {
                    let ordinal = RootOrdinal::new(u32::try_from(index).unwrap());
                    assert!(unsigned_root.replace((unit, ordinal)).is_none());
                }
            }
        }
    }
    let (unsigned_unit, unsigned_root) = unsigned_root.expect("original typed SQL IN root");

    // The SQL fixture admits only physical column declarations. Author the
    // nominal UUID port explicitly before running the same expression compiler.
    let uuid = FunctionValueType::try_with_logical_type(
        DataType::FixedSizeBinary(16),
        true,
        ValueLogicalType::Uuid,
    )
    .unwrap();
    let values = [0, 1, 2].map(ValueId::new);
    let port = ExprPort {
        values: values.map(|value| (value, uuid.clone())).into(),
    };
    let mut builder = BoundExprSetBuilder::new(UnitLimits {
        expression_semantic_depth: 8,
    });
    let [source, first, second] = values.map(|value| {
        builder
            .add(None, uuid.clone(), ExprKind::Value(value))
            .unwrap()
    });
    let membership = builder
        .add(
            None,
            FunctionValueType::new(DataType::Boolean, true),
            ExprKind::InList {
                expr: source,
                list: [first, second].into(),
                negated: false,
            },
        )
        .unwrap();
    let uuid_root = builder
        .root(membership, EvaluationDemand::Value, RootScope::Port(0))
        .unwrap();
    let mut work = CompileCheckpoints::try_new(&HarnessControl, CompilePhase::Validate).unwrap();
    let sealed = builder.seal(vec![port], &mut work).unwrap();
    let uuid_unit = finish_unit(
        &functions,
        sealed,
        &BTreeMap::new(),
        environment.constants,
        environment.parameters,
        policy,
        &mut work,
    );
    work.finish().unwrap();

    let columns = [
        [Some(u64::MAX), Some(u64::MAX), None, Some(u64::MAX)],
        [Some(u64::MAX), Some(8), Some(u64::MAX), None],
        [None, Some(9), Some(8), Some(9)],
    ];
    let test_cases = [
        (
            unsigned_unit,
            unsigned_root,
            columns.map(|values| Arc::new(UInt64Array::from(values.to_vec())) as ArrayRef),
            FunctionValueType::new(DataType::UInt64, true),
        ),
        (
            &uuid_unit,
            uuid_root,
            columns.map(|values| {
                Arc::new(
                    FixedSizeBinaryArray::try_from_sparse_iter_with_size(
                        values
                            .into_iter()
                            .map(|value| value.map(|value| u128::from(value).to_be_bytes())),
                        16,
                    )
                    .unwrap(),
                ) as ArrayRef
            }),
            uuid,
        ),
    ];
    for (unit, root, arrays, expected_type) in test_cases {
        let [port] = unit.ports() else {
            panic!("membership has one original input port");
        };
        let schema = Arc::new(Schema::new(
            port.values
                .iter()
                .enumerate()
                .map(|(ordinal, (_, ty))| ty.try_to_field(format!("input_{ordinal}")).unwrap())
                .collect::<Vec<_>>(),
        ));
        let scope = fixture_scope(port, schema);
        let all_ports = unit
            .roots()
            .iter()
            .any(|root| root.scope == RootScope::AllPorts)
            .then_some(&scope);
        let mut work =
            CompileCheckpoints::try_new(&HarnessControl, CompilePhase::LowerProgram).unwrap();
        let evaluated = compile_expr_set_pure(
            unit,
            environment,
            ScopeLayouts {
                ports: std::slice::from_ref(&scope),
                all_ports,
            },
            policy,
            &functions,
            &mut work,
        )
        .unwrap();
        work.finish().unwrap();
        let use_id = evaluated.root(root).unwrap().use_id;
        let recipe = evaluated.recipes().native_inlist(use_id).unwrap();
        assert_eq!(recipe.source_type(), &expected_type);
        assert_eq!(
            recipe.candidate_types(),
            &[expected_type.clone(), expected_type]
        );

        // Follow the exact input binding of each authored expression use.
        let invocation = &evaluated.control_flow().uses()[&use_id];
        assert_eq!(invocation.arguments.len(), arrays.len());
        let mut input = [None, None, None];
        for (&argument, array) in invocation.arguments.iter().zip(arrays) {
            let binding = evaluated.input(argument).unwrap();
            assert!(input[binding.ordinal as usize].replace(array).is_none());
        }
        evaluate_unit(
            &evaluated,
            root,
            input.into_iter().map(Option::unwrap).collect(),
            &[Some(true), Some(false), None, None],
        );
    }
}
