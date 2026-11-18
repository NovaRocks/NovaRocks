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
//! The original declaration is a public obligation, not the internal Cast admission.
use novarocks_expression::CompiledExpressionInstance;
use super::numeric_unary_owned_transaction_tests::{batch, producer_root, programs};
use super::sql_test_source::sql_source;
use arrow::array::{Array, ArrayRef, Decimal128Array, Decimal256Array, Float32Array};
use arrow::datatypes::DataType;
use novarocks_expression::EvaluationInput;
use novarocks_expression::testkit::PassThroughCallHost;
use novarocks_expression_contract::ExprKind;
use novarocks_functions::{
    KernelEvaluationControl, KernelFailure, ScalarInvocationActivation, ScalarInvocationScope,
    Selection,
};
use novarocks_query_application::api::QueryExecutionKind;
use novarocks_query_application::preparation::{CompletedPhysicalPlanCandidate, OutputContract};
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

// Pure carrier construction, with no conversion or evaluator call.
fn input(wide: bool, precision: u8, scale: i8, raw: Vec<Option<i128>>) -> ArrayRef {
    if wide {
        Arc::new(
            Decimal256Array::from(
                raw.into_iter()
                    .map(|value| value.map(arrow_buffer::i256::from_i128))
                    .collect::<Vec<_>>(),
            )
            .with_precision_and_scale(precision, scale)
            .unwrap(),
        )
    } else {
        Arc::new(
            Decimal128Array::from(raw)
                .with_precision_and_scale(precision, scale)
                .unwrap(),
        )
    }
}

// Corpus coefficients are [0, 4, -4, 1]. Their decimal values are r*10^-s.
// IEEE-754 round-to-nearest-even gives 1e38 -> 0x7e967699;
// |4e38| exceeds binary32's finite range. The preserved Decimal128 author
// projects its non-finite results to SQL NULL, whereas Decimal256 retains
// infinities. Neither NULL below is an i128 intermediate scaling overflow.
// The independent exact-rational midpoint worksheet lives with this draft.
fn expected_bits(wide: bool, scale: i8) -> [Option<u32>; 4] {
    match (wide, scale) {
        (false, 0) | (true, 0) => [
            Some(0x00000000),
            Some(0x40800000),
            Some(0xc0800000),
            Some(0x3f800000),
        ],
        (false, -38) => [Some(0x00000000), None, None, Some(0x7e967699)],
        (true, -38) => [
            Some(0x00000000),
            Some(0x7f800000),
            Some(0xff800000),
            Some(0x7e967699),
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

fn same_source_case(wide: bool, scale: i8, expected_null_values: bool) {
    let dtype = if wide {
        DataType::Decimal256(76, scale)
    } else {
        DataType::Decimal128(38, scale)
    };
    let sql = "SELECT CAST(k AS FLOAT) AS original_cast FROM fixture";
    // ColumnDef and Arrow Field are authored false by the existing real connector seam.
    let before = sql_source(sql, dtype.clone());
    let source = sql_source(sql, dtype.clone());
    let original = source
        .checked_original_result_declaration_observed(&Control)
        .unwrap()
        .unwrap();
    let computed = source.plan().result_port().unwrap();
    assert_eq!(
        original.fields(),
        before.plan().result_port().unwrap().fields.as_ref()
    );
    assert_eq!(original.fields().len(), 1);
    assert_eq!(
        original.fields()[0].ty.data_type().clone(),
        DataType::Float32
    );
    // Explicit SQL CAST is originally nullable=true, even from a non-NULL column.
    // Do not manufacture a false public obligation for this actual source shape.
    assert!(original.fields()[0].ty.nullable());
    assert!(computed.fields[0].ty.nullable());
    assert_eq!(computed.output, original.original_port().output);
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
                        assert!(node.ty.nullable());
                        assert_eq!(&node.ty.data_type().clone(), target);
                    }
                }
            }
        }
    }
    assert!(
        casts > 0,
        "actual SQL must retain the real Decimal-to-FLOAT Cast"
    );
    let candidate =
        CompletedPhysicalPlanCandidate::for_sql_program(source.clone(), &Control).unwrap();
    let output =
        OutputContract::from_completed_candidate(QueryExecutionKind::Read, &candidate).unwrap();
    assert_eq!(output.fields()[0].name(), "original_cast");
    assert_eq!(output.fields()[0].data_type(), &DataType::Float32);
    assert!(output.fields()[0].nullable());
    let programs = programs(&source);
    let (program, root) = producer_root(&programs);
    let values = input(
        wide,
        if wide { 76 } else { 38 },
        scale,
        vec![Some(0), Some(4), Some(-4), Some(1)],
    );
    let data = batch(&program, root, values.clone());
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
    let expected = expected_bits(wide, scale);
    assert_eq!(selected.selection(), Selection::all(4));
    assert_bits(selected.values(), &expected);
    let floats = selected
        .values()
        .as_any()
        .downcast_ref::<Float32Array>()
        .unwrap();
    assert_eq!(floats.value(0).to_bits(), 0f32.to_bits());
    if expected_null_values {
        assert!(floats.is_null(1));
        assert!(floats.is_null(2));
    } else if wide && scale == -38 {
        assert_eq!(floats.value(1), f32::INFINITY);
        assert_eq!(floats.value(2), f32::NEG_INFINITY);
        assert_eq!(floats.null_count(), 0);
    }
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
fn decimal_float32_sql_explicit_cast_nonnull_decimal128_negative_scale_preserves_original_public_true()
 {
    same_source_case(false, -38, true);
}
#[test]
fn decimal_float32_sql_explicit_cast_nonnegative_decimal128_and_decimal256_preserve_original_projection()
 {
    same_source_case(false, 0, false);
    // Wide Decimal has no genuine ConnectorValueType assignment. Its entire
    // Float32 recipe/Frame domain stays in the existing direct compiler oracles.
}

#[test]
fn decimal_float32_sql_explicit_cast_actual_decimal256_literal_and_unary_frame_preserve_infinity() {
    use arrow::datatypes::Schema;
    use arrow::record_batch::{RecordBatch, RecordBatchOptions};
    use novarocks_local_program::ProgramNodeKind;
    use std::sync::Arc;
    // The point makes this a genuine decimal token; >38 digits is authored
    // Decimal256 by the original analyzer, not a retagged scan or guessed CV.
    let literal = "4000000000000000000000000000000000000000.0";
    for negative in [false, true] {
        let sql = format!(
            "SELECT CAST({}{literal} AS FLOAT) AS wide_literal",
            if negative { "-" } else { "" }
        );
        let source = sql_source(&sql, DataType::Int8);
        let mut actual_wide = 0;
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
                if matches!(expression.ty.data_type(), DataType::Decimal256(p, s) if *p > 38 && *s == 1)
                {
                    actual_wide += 1;
                }
            }
        }
        assert!(
            actual_wide > 0,
            "the actual SQL literal author must retain Decimal256"
        );
        let original = source.original_public_result_declaration().unwrap();
        assert_eq!(
            original.fields()[0].ty.data_type().clone(),
            DataType::Float32
        );
        assert!(original.fields()[0].ty.nullable());
        let lowered = programs(&source);
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
            value.value(0),
            if negative {
                f32::NEG_INFINITY
            } else {
                f32::INFINITY
            }
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
