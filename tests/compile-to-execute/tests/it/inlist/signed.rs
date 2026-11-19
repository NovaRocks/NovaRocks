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

//! Signed IN source/ordered lifecycle probes. UNRUN until serialized root tests.
//! Formal host MEM grants and the Utf8 Variant environment remain separate gates.
use arrow::array::{Array, ArrayRef, BooleanArray, Int8Array, Int16Array, Int32Array, Int64Array};
use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;
use novarocks_expression::CompiledExpressionInstance;
use novarocks_expression::testkit::{CallbackControl, PassThroughCallHost, callback_causes};
use novarocks_expression::{EvaluationInput, ExprEvaluationFailure, StaticExprKind};
use novarocks_functions::{
    KernelEvaluationControl, KernelFailure, ScalarInvocationActivation, ScalarInvocationScope,
    Selection,
};
use novarocks_local_program::{LocalProgram, ProgramRootRef};
use novarocks_sql_testkit::{installed_builtin_owner_catalogue, signed_inlist_source};
use novarocks_test_compile_to_execute::programs::compiled_signed_inlist_root;

use novarocks_type_contract::ControlShape;
use std::{sync::Arc, time::Duration};
struct Control;
impl KernelEvaluationControl for Control {
    fn checkpoint(&self, n: u32) -> Result<(), KernelFailure> {
        assert!(n <= 256);
        Ok(())
    }
    fn wait(&self, _: Duration) -> Result<(), KernelFailure> {
        panic!("IN never waits")
    }
}
fn batch(program: &LocalProgram, site: ProgramRootRef, input: ArrayRef) -> RecordBatch {
    let unit = program.unit(site.unit).unwrap();
    let root = unit.root(site.root).unwrap();
    let schema = unit.scope(root.scope).unwrap().schema().clone();
    assert_eq!(schema.fields().len(), 1);
    assert_eq!(schema.field(0).data_type(), input.data_type());
    RecordBatch::try_new(schema, vec![input]).unwrap()
}
fn values(output: &novarocks_functions::SelectedValues<'_>) -> Vec<Option<bool>> {
    output
        .values()
        .as_any()
        .downcast_ref::<BooleanArray>()
        .unwrap()
        .iter()
        .collect()
}
#[test]
fn inlist_signed_actual_all_widths_value_truthonly_null_sparse_sliced_empty() {
    for (ty, input) in [
        (
            DataType::Int8,
            Arc::new(
                Int8Array::from(vec![Some(99), Some(1), Some(3), None, Some(2), Some(98)])
                    .slice(1, 4),
            ) as ArrayRef,
        ),
        (
            DataType::Int16,
            Arc::new(
                Int16Array::from(vec![Some(99), Some(1), Some(3), None, Some(2), Some(98)])
                    .slice(1, 4),
            ) as ArrayRef,
        ),
        (
            DataType::Int32,
            Arc::new(
                Int32Array::from(vec![Some(99), Some(1), Some(3), None, Some(2), Some(98)])
                    .slice(1, 4),
            ) as ArrayRef,
        ),
        (
            DataType::Int64,
            Arc::new(
                Int64Array::from(vec![Some(99), Some(1), Some(3), None, Some(2), Some(98)])
                    .slice(1, 4),
            ) as ArrayRef,
        ),
    ] {
        let name = match ty {
            DataType::Int8 => "TINYINT",
            DataType::Int16 => "SMALLINT",
            DataType::Int32 => "INT",
            DataType::Int64 => "BIGINT",
            _ => unreachable!(),
        };
        let positive = format!(
            "SELECT k IN (CAST(1 AS {name}),CAST(2 AS {name}),CAST(NULL AS {name})) FROM t0"
        );
        let negative = format!(
            "SELECT k NOT IN (CAST(1 AS {name}),CAST(2 AS {name}),CAST(NULL AS {name})) FROM t0"
        );
        let positive_truth = format!(
            "SELECT k FROM t0 WHERE k IN (CAST(1 AS {name}),CAST(2 AS {name}),CAST(NULL AS {name}))"
        );
        let negative_truth = format!(
            "SELECT k FROM t0 WHERE k NOT IN (CAST(1 AS {name}),CAST(2 AS {name}),CAST(NULL AS {name}))"
        );
        for (sql, expected) in [
            (positive.as_str(), vec![Some(true), None, None, Some(true)]),
            (
                negative.as_str(),
                vec![Some(false), None, None, Some(false)],
            ),
            (
                positive_truth.as_str(),
                vec![Some(true), Some(false), Some(false), Some(true)],
            ),
            (negative_truth.as_str(), vec![Some(false); 4]),
        ] {
            let (program, site) = compiled_signed_inlist_root(
                &signed_inlist_source(sql, ty.clone()),
                &installed_builtin_owner_catalogue(),
            );
            let unit = program.unit(site.unit).unwrap();
            let use_id = unit.root(site.root).unwrap().use_id;
            let recipe = unit.recipes().native_inlist(use_id).unwrap();
            assert_eq!(recipe.source_type().data_type().clone(), ty);
            assert!(
                recipe
                    .candidate_types()
                    .iter()
                    .all(|candidate| *candidate.data_type() == ty)
            );
            let data = batch(&program, site, input.clone());
            for rows in [vec![0, 1, 2, 3], vec![1, 3], vec![]] {
                let selection = Selection::try_sparse(4, &rows).unwrap();
                let mut frame = CompiledExpressionInstance::try_new(
                    program.unit(site.unit).unwrap().clone(),
                    site.root,
                    &Control,
                )
                .unwrap();
                let out = frame
                    .evaluate(
                        EvaluationInput::Batch {
                            batch: &data,
                            source: None,
                        },
                        ScalarInvocationScope::new(selection, selection),
                        ScalarInvocationActivation::Activated,
                        &Control,
                        &mut PassThroughCallHost,
                    )
                    .unwrap();
                assert!(out.errors().is_empty());
                assert_eq!(
                    values(&out),
                    rows.iter().map(|r| expected[*r]).collect::<Vec<_>>()
                );
            }
        }
    }
}
// The additive inverse of i64::MIN is outside the signed 64-bit carrier.
// This exact native subtraction diagnostic is the public row-error contract.
const NEGATE_MIN_ERROR: &str =
    "Arithmetic overflow: Overflow happened on: 0 - -9223372036854775808";
#[test]
fn inlist_signed_actual_match_never_masks_later_data_error_and_sparse_isolates_it() {
    for sql in [
        "SELECT k IN (k,-k) FROM t0",
        "SELECT k NOT IN (k,-k) FROM t0",
    ] {
        let negated = sql.contains("NOT IN");
        let (program, site) = compiled_signed_inlist_root(
            &signed_inlist_source(sql, DataType::Int64),
            &installed_builtin_owner_catalogue(),
        );
        let data = batch(
            &program,
            site,
            Arc::new(Int64Array::from(vec![i64::MIN, 7, 0])),
        );
        let mut frame = CompiledExpressionInstance::try_new(
            program.unit(site.unit).unwrap().clone(),
            site.root,
            &Control,
        )
        .unwrap();
        let out = frame
            .evaluate(
                EvaluationInput::Batch {
                    batch: &data,
                    source: None,
                },
                ScalarInvocationScope::new(Selection::all(3), Selection::all(3)),
                ScalarInvocationActivation::Activated,
                &Control,
                &mut PassThroughCallHost,
            )
            .unwrap();
        assert_eq!(values(&out), vec![None, Some(!negated), Some(!negated)]);
        assert_eq!(out.errors().len(), 1);
        assert_eq!(out.errors()[0].selected_ordinal(), 0);
        assert_eq!(out.errors()[0].message(), NEGATE_MIN_ERROR);
        assert_eq!(
            out.errors()[0].reason(),
            novarocks_functions::DataFailureReason::NumericOutOfRange
        );
        let rows = [1, 2];
        let selection = Selection::try_sparse(3, &rows).unwrap();
        let mut frame = CompiledExpressionInstance::try_new(
            program.unit(site.unit).unwrap().clone(),
            site.root,
            &Control,
        )
        .unwrap();
        let out = frame
            .evaluate(
                EvaluationInput::Batch {
                    batch: &data,
                    source: None,
                },
                ScalarInvocationScope::new(selection, selection),
                ScalarInvocationActivation::Activated,
                &Control,
                &mut PassThroughCallHost,
            )
            .unwrap();
        assert!(out.errors().is_empty());
        assert_eq!(values(&out), vec![Some(!negated); 2]);
    }
}
fn causes() -> [KernelFailure; 7] {
    callback_causes([
        "in-origin-invalid",
        "in-origin-internal",
        "in-origin-operational",
    ])
}
#[test]
fn inlist_signed_actual_match_or_null_candidate_keeps_required_case_child_error_and_control() {
    // A prior match, NULL candidate, or runtime NULL source cannot suppress
    // the later required CASE expression. All are actual single-column SQL.
    for (source, prefix) in [
        ("k", "k"),
        ("k", "CAST(NULL AS BIGINT)"),
        (
            "(CASE WHEN k < 0 THEN CAST(NULL AS BIGINT) ELSE k END)",
            "k",
        ),
    ] {
        for operator in ["IN", "NOT IN"] {
            let negated = operator == "NOT IN";
            let sql = format!(
                "SELECT {source} {operator} ({prefix},CASE WHEN k < 0 THEN -k ELSE k END) FROM t0"
            );
            let (program, site) = compiled_signed_inlist_root(
                &signed_inlist_source(&sql, DataType::Int64),
                &installed_builtin_owner_catalogue(),
            );
            let unit = program.unit(site.unit).unwrap();
            let flow = unit.control_flow();
            let membership = &flow.uses()[&unit.root(site.root).unwrap().use_id];
            assert_eq!(membership.control, ControlShape::Membership { negated });
            assert_eq!(membership.arguments.len(), 3);
            let case_use = &flow.uses()[&membership.arguments[2]];
            assert_eq!(
                case_use.control,
                ControlShape::Case {
                    simple: false,
                    arms: 1,
                    has_else: true
                }
            );
            assert_eq!(
                case_use.context.demand,
                novarocks_type_contract::EvaluationDemand::Value
            );
            assert_eq!(
                flow.regions()[&case_use.context.region].guard.unwrap().kind,
                novarocks_type_contract::GuardKind::MembershipAfterSource { ordinal: 1 }
            );
            let definition = unit.definitions().node(membership.definition).unwrap();
            let StaticExprKind::PreparedInList {
                values: definitions,
                ..
            } = definition.kind()
            else {
                panic!("actual ordered membership")
            };
            assert_eq!(definitions.len(), 2);
            for (ordinal, definition) in definitions.iter().enumerate() {
                assert_eq!(
                    flow.uses()[&membership.arguments[ordinal + 1]].definition,
                    *definition
                );
            }
            let data = batch(
                &program,
                site,
                Arc::new(Int64Array::from(vec![
                    Some(i64::MIN),
                    Some(7),
                    Some(0),
                    None,
                ])),
            );
            let expected = [None, Some(!negated), Some(!negated), None];
            for rows in [vec![0, 1, 2, 3], vec![0, 2], vec![1, 2, 3], vec![]] {
                let selection = Selection::try_sparse(4, &rows).unwrap();
                let mut frame = CompiledExpressionInstance::try_new(
                    program.unit(site.unit).unwrap().clone(),
                    site.root,
                    &Control,
                )
                .unwrap();
                let output = frame
                    .evaluate(
                        EvaluationInput::Batch {
                            batch: &data,
                            source: None,
                        },
                        ScalarInvocationScope::new(selection, selection),
                        ScalarInvocationActivation::Activated,
                        &Control,
                        &mut PassThroughCallHost,
                    )
                    .unwrap();
                assert_eq!(output.selection(), selection);
                assert_eq!(
                    values(&output),
                    rows.iter().map(|row| expected[*row]).collect::<Vec<_>>()
                );
                let failed_ordinal = rows.iter().position(|row| *row == 0);
                assert_eq!(output.errors().len(), usize::from(failed_ordinal.is_some()));
                if let Some(ordinal) = failed_ordinal {
                    assert_eq!(output.errors()[0].selected_ordinal(), ordinal);
                    assert_eq!(
                        output.errors()[0].reason(),
                        novarocks_functions::DataFailureReason::NumericOutOfRange
                    );
                    assert_eq!(output.errors()[0].message(), NEGATE_MIN_ERROR);
                }
            }
            // The same actual CASE succeeds for -7 and its selected THEN path
            // remains observable. Refuse every recorded runtime callback and
            // preserve the original cause, exact prefix and failed latch.
            let success = batch(
                &program,
                site,
                Arc::new(Int64Array::from(vec![Some(-7), Some(0), Some(9), None])),
            );
            let record =
                CallbackControl::bounded(KernelFailure::Cancelled, usize::MAX, "IN never waits");
            let mut frame = CompiledExpressionInstance::try_new(
                program.unit(site.unit).unwrap().clone(),
                site.root,
                &Control,
            )
            .unwrap();
            let output = frame
                .evaluate(
                    EvaluationInput::Batch {
                        batch: &success,
                        source: None,
                    },
                    ScalarInvocationScope::new(Selection::all(4), Selection::all(4)),
                    ScalarInvocationActivation::Activated,
                    &record,
                    &mut PassThroughCallHost,
                )
                .unwrap();
            assert!(output.errors().is_empty());
            let trace = record.trace();
            assert!(!trace.is_empty());
            for stop in 1..=trace.len() {
                for cause in causes() {
                    let mut frame = CompiledExpressionInstance::try_new(
                        program.unit(site.unit).unwrap().clone(),
                        site.root,
                        &Control,
                    )
                    .unwrap();
                    let refused = CallbackControl::bounded(cause.clone(), stop, "IN never waits");
                    assert_eq!(
                        frame
                            .evaluate(
                                EvaluationInput::Batch {
                                    batch: &success,
                                    source: None
                                },
                                ScalarInvocationScope::new(Selection::all(4), Selection::all(4)),
                                ScalarInvocationActivation::Activated,
                                &refused,
                                &mut PassThroughCallHost,
                            )
                            .unwrap_err(),
                        ExprEvaluationFailure::Kernel(cause)
                    );
                    assert_eq!(refused.trace(), trace[..stop]);
                    let after = CallbackControl::bounded(
                        KernelFailure::Cancelled,
                        usize::MAX,
                        "IN never waits",
                    );
                    assert_eq!(
                        frame
                            .evaluate(
                                EvaluationInput::Batch {
                                    batch: &success,
                                    source: None
                                },
                                ScalarInvocationScope::new(Selection::all(4), Selection::all(4)),
                                ScalarInvocationActivation::Activated,
                                &after,
                                &mut PassThroughCallHost,
                            )
                            .unwrap_err(),
                        ExprEvaluationFailure::Kernel(KernelFailure::InstanceFailed)
                    );
                    assert!(after.trace().is_empty());
                }
            }
        }
    }
}
