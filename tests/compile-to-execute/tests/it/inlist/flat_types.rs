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
    CountingAllocator, HarnessControl, PassThroughCallHost, constant_policy, finish_unit,
    fixture_scope,
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
use novarocks_physical_plan::ConstantPools;
use novarocks_sql_testkit::{installed_builtin_owner_catalogue, sql_source_with_declared_fields};
use novarocks_test_compile_to_execute::compiler_results::compiler_results_with_declared_read_fields;
use novarocks_test_compile_to_execute::programs::compiled_roots;
use novarocks_type_contract::{
    CompileCheckpoints, CompilePhase, EvaluationDemand, FunctionValueType, SemanticParameters,
    ValueLogicalType,
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
fn compiled_in_preserves_unsigned_and_nominal_uuid_port_types() {
    // Exercise explicitly authored expression ports independently of SQL
    // connector admission, which has no UInt64 scan output.
    let functions = installed_builtin_owner_catalogue();
    let constants = ConstantPools::empty();
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
                .add(None, ty.clone(), ExprKind::Value(value))
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
        let root = builder
            .root(membership, EvaluationDemand::Value, RootScope::Port(0))
            .unwrap();
        let mut work =
            CompileCheckpoints::try_new(&HarnessControl, CompilePhase::Validate).unwrap();
        let sealed = builder.seal(vec![port], &mut work).unwrap();
        let unit = finish_unit(
            &functions,
            sealed,
            &BTreeMap::new(),
            &constants,
            &parameters,
            policy,
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
        let recipe = compiled.recipes().native_inlist(use_id).unwrap();
        assert_eq!(recipe.source_type(), &ty);
        assert_eq!(recipe.candidate_types(), &[ty.clone(), ty]);
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
