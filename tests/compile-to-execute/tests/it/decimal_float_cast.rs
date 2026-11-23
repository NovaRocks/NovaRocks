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
//! Actual non-NULL Connector SQL source -> owned transaction -> LocalCompiler -> Frame.
//! SQL-authored decimal casts and their exact evaluator results.
use arrow::array::{Array, ArrayRef, Decimal128Array, Float32Array};
use arrow::datatypes::DataType;
use novarocks_expression::CompiledExpressionInstance;
use novarocks_expression::EvaluationInput;
use novarocks_expression::testkit::PassThroughCallHost;
use novarocks_expression_contract::ExprKind;
use novarocks_functions::{
    KernelEvaluationControl, KernelFailure, ScalarInvocationActivation, ScalarInvocationScope,
    Selection, testkit::shared_installed_builtin_owner_catalogue,
};
use novarocks_sql_testkit::sql_source;
use novarocks_test_compile_to_execute::programs::{
    batch, producer_root, programs, programs_with_catalogue,
};
use novarocks_type_contract::{CompileControlError, CompilePhase, PureCompileControl};
use std::{sync::Arc, time::Duration};

struct Control;
impl PureCompileControl for Control {
    fn checkpoint(&self, _: CompilePhase, n: u32) -> Result<(), CompileControlError> {
        assert!(n <= 256);
        Ok(())
    }
}
impl KernelEvaluationControl for Control {
    fn checkpoint(&self, n: u32) -> Result<(), KernelFailure> {
        assert!(n <= 256);
        Ok(())
    }
    fn wait(&self, _: Duration) -> Result<(), KernelFailure> {
        panic!("Decimal Float32 SQL cast never waits")
    }
}

// The exact rational values of coefficients [0, 4, -4, 1] at each
// declared scale have these nearest-even binary32 representations.
fn expected_bits(scale: i8) -> [Option<u32>; 4] {
    match scale {
        0 => [
            Some(0),
            Some(0x40800000),
            Some(0xc0800000),
            Some(0x3f800000),
        ],
        2 => [
            Some(0),
            Some(0x3d23d70a),
            Some(0xbd23d70a),
            Some(0x3c23d70a),
        ],
        _ => panic!("SQL corpus needs an explicit independent IEEE table"),
    }
}

fn assert_bits(array: &ArrayRef, expected: &[Option<u32>]) {
    assert_eq!(array.data_type(), &DataType::Float32);
    let values = array.as_any().downcast_ref::<Float32Array>().unwrap();
    assert_eq!(values.len(), expected.len());
    let actual = (0..values.len())
        .map(|row| (!values.is_null(row)).then(|| values.value(row).to_bits()))
        .collect::<Vec<_>>();
    assert_eq!(actual, expected);
}

fn same_source_case(scale: i8) {
    let dtype = DataType::Decimal128(38, scale);
    let sql = "SELECT CAST(k AS REAL) AS original_cast FROM fixture";
    // ColumnDef and Arrow Field are authored false by the existing real connector seam.
    let source = sql_source(sql, dtype.clone());
    let mut casts = 0;
    for fragment in source.plan().fragments().values() {
        for physical in fragment.nodes().values() {
            let Some(unit) = fragment.unit(physical.id) else {
                continue;
            };
            for node in unit.definitions() {
                if let ExprKind::Cast { expr, target, .. } = &node.kind {
                    let operand = unit.definition(*expr).unwrap();
                    if *operand.ty.data_type() == dtype && target == &DataType::Float32 {
                        casts += 1;
                        assert!(!operand.ty.nullable());
                        assert!(!node.ty.nullable());
                        assert_eq!(&node.ty.data_type().clone(), target);
                    }
                }
            }
        }
    }
    assert!(
        casts > 0,
        "actual SQL must retain the real DECIMAL-to-REAL cast"
    );
    let programs = programs(&source);
    let (program, root) = producer_root(&programs);
    let values = Arc::new(
        Decimal128Array::from(vec![0_i128, 4, -4, 1])
            .with_precision_and_scale(38, scale)
            .unwrap(),
    );
    let data = batch(&program, root, values);
    let mut frame = CompiledExpressionInstance::try_new(
        program.unit(root.unit).unwrap().clone(),
        root.root,
        &Control,
    )
    .unwrap();
    let selected = frame
        .evaluate(
            EvaluationInput::Batch {
                batch: &data,
                source: None,
            },
            ScalarInvocationScope::new(Selection::all(4), Selection::all(4)),
            ScalarInvocationActivation::Activated,
            &Control,
            &mut PassThroughCallHost,
        )
        .unwrap();
    assert!(selected.errors().is_empty());
    let expected = expected_bits(scale);
    assert_eq!(selected.selection(), Selection::all(4));
    assert_bits(selected.values(), &expected);
    let rows = [0, 1, 3];
    let sparse = frame
        .evaluate(
            EvaluationInput::Batch {
                batch: &data,
                source: None,
            },
            {
                let selection = Selection::try_sparse(4, &rows).unwrap();
                ScalarInvocationScope::new(selection, selection)
            },
            ScalarInvocationActivation::Activated,
            &Control,
            &mut PassThroughCallHost,
        )
        .unwrap();
    assert_eq!(sparse.selection(), Selection::try_sparse(4, &rows).unwrap());
    assert!(sparse.errors().is_empty());
    assert_bits(sparse.values(), &rows.map(|row| expected[row]));
    let empty_selection = Selection::try_sparse(4, &[]).unwrap();
    let empty = frame
        .evaluate(
            EvaluationInput::Batch {
                batch: &data,
                source: None,
            },
            ScalarInvocationScope::new(empty_selection, empty_selection),
            ScalarInvocationActivation::Activated,
            &Control,
            &mut PassThroughCallHost,
        )
        .unwrap();
    assert_eq!(empty.selection(), empty_selection);
    assert!(empty.errors().is_empty());
    assert_bits(empty.values(), &[]);
}

#[test]
fn decimal_real_sql_explicit_cast_preserves_nonnull_scan_and_fractional_values() {
    for scale in [0, 2] {
        same_source_case(scale);
    }
}

#[test]
fn decimal_real_sql_explicit_cast_uses_standard_precision_boundary_and_unary_frame() {
    use arrow::datatypes::Schema;
    use arrow::record_batch::{RecordBatch, RecordBatchOptions};
    use novarocks_local_program::ProgramNodeKind;
    use std::sync::Arc;
    // Use the largest supported DECIMAL precision in the original SQL literal.
    let literal = "DECIMAL '40000000000000000000000000000000000000'";
    let functions = shared_installed_builtin_owner_catalogue();
    for negative in [false, true] {
        let sql = format!(
            "SELECT CAST({}{literal} AS REAL) AS boundary_literal",
            if negative { "-" } else { "" }
        );
        let source = sql_source(&sql, DataType::Int8);
        let mut actual_decimal = 0;
        for fragment in source.plan().fragments().values() {
            for node in fragment.nodes().values() {
                assert!(!matches!(
                    node.kind,
                    novarocks_physical_plan::NodeKind::Scan { .. }
                ));
            }
            for expression in fragment
                .nodes()
                .values()
                .filter_map(|node| fragment.unit(node.id))
                .flat_map(|unit| unit.definitions())
            {
                if expression.ty.data_type() == &DataType::Decimal128(38, 0) {
                    actual_decimal += 1;
                }
            }
        }
        assert!(
            actual_decimal > 0,
            "the actual SQL literal author must retain DECIMAL(38,0)"
        );
        let lowered = programs_with_catalogue(&source, &functions);
        // Borrow actual compiled root bindings. Project is preferred when the
        // original optimizer retained it; a dynamic Values cell has the same
        // documented empty one-row port. No alternate graph is constructed.
        let mut selected = None;
        for program in lowered.values() {
            for node in program.graph().nodes() {
                if let ProgramNodeKind::Project { exprs, .. } = node.kind() {
                    for &site in exprs {
                        assert!(selected.is_none());
                        selected = Some((program.clone(), node.local_id().unwrap(), site));
                    }
                }
            }
        }
        if selected.is_none() {
            for program in lowered.values() {
                for node in program.graph().nodes() {
                    if let ProgramNodeKind::Values { values } = node.kind() {
                        for cell in values.dynamic_cells() {
                            assert!(selected.is_none());
                            selected =
                                Some((program.clone(), node.local_id().unwrap(), cell.definition));
                        }
                    }
                }
            }
        }
        let (program, node, root) =
            selected.expect("actual SQL literal Cast must retain a runtime projection/cell");
        let empty_one_row = |schema: Arc<Schema>| {
            assert!(
                schema.fields().is_empty(),
                "dynamic literal cell reads the exact empty port"
            );
            RecordBatch::try_new_with_options(
                schema,
                Vec::new(),
                &RecordBatchOptions::new().with_row_count(Some(1)),
            )
            .unwrap()
        };
        let port = match program.graph().nodes()[node.index()].kind() {
            ProgramNodeKind::Project { input, .. } => {
                let parent = &program.graph().nodes()[input.index()];
                match parent.kind() {
                    ProgramNodeKind::Values { values } => match values.batch() {
                        Some(batch) => batch.clone(),
                        None => empty_one_row(parent.output_layout().schema().clone()),
                    },
                    _ => empty_one_row(parent.output_layout().schema().clone()),
                }
            }
            ProgramNodeKind::Values { .. } => {
                let unit = program.unit(root.unit).unwrap();
                empty_one_row(
                    unit.scope(unit.root(root.root).unwrap().scope)
                        .unwrap()
                        .schema()
                        .clone(),
                )
            }
            _ => unreachable!(),
        };
        let mut frame = CompiledExpressionInstance::try_new(
            program.unit(root.unit).unwrap().clone(),
            root.root,
            &Control,
        )
        .unwrap();
        let output = frame
            .evaluate(
                EvaluationInput::Batch {
                    batch: &port,
                    source: None,
                },
                ScalarInvocationScope::new(Selection::all(1), Selection::all(1)),
                ScalarInvocationActivation::Activated,
                &Control,
                &mut PassThroughCallHost,
            )
            .unwrap();
        assert!(output.errors().is_empty());
        let value = output
            .values()
            .as_any()
            .downcast_ref::<Float32Array>()
            .unwrap();
        assert_eq!(value.null_count(), 0);
        assert_eq!(
            value.value(0).to_bits(),
            if negative { 0xfdf0bdc2 } else { 0x7df0bdc2 }
        );
        assert_eq!(
            frame
                .evaluate(
                    EvaluationInput::Batch {
                        batch: &port,
                        source: None
                    },
                    {
                        let selection = Selection::try_sparse(1, &[]).unwrap();
                        ScalarInvocationScope::new(selection, selection)
                    },
                    ScalarInvocationActivation::Activated,
                    &Control,
                    &mut PassThroughCallHost,
                )
                .unwrap()
                .values()
                .len(),
            0
        );
    }
}
