// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

use super::*;
use novarocks_functions::EvaluationContractError;
use std::sync::Mutex;

#[derive(Clone, Copy, Debug)]
enum Cell {
    True,
    False,
    Null,
    Error,
}

fn operand<'a>(selection: Selection<'a>, cells: &[Cell], message: &str) -> SelectedValues<'a> {
    let values = cells
        .iter()
        .map(|cell| match cell {
            Cell::True => Some(true),
            Cell::False => Some(false),
            Cell::Null | Cell::Error => None,
        })
        .collect::<Vec<_>>();
    let errors = cells
        .iter()
        .enumerate()
        .filter(|(_, cell)| matches!(cell, Cell::Error))
        .map(|(ordinal, _)| RowDataError::new(ordinal, message))
        .collect::<Vec<_>>();
    SelectedValues::try_new(
        selection,
        &DataType::Boolean,
        Arc::new(BooleanArray::from(values)),
        errors.into_boxed_slice(),
    )
    .unwrap()
}

struct Control {
    trace: Mutex<Vec<u32>>,
    stop: usize,
    cause: KernelFailure,
    refused: AtomicBool,
}
impl Control {
    fn recording() -> Self {
        Self::stopping(usize::MAX, KernelFailure::Cancelled)
    }
    fn stopping(stop: usize, cause: KernelFailure) -> Self {
        Self {
            trace: Mutex::new(vec![]),
            stop,
            cause,
            refused: AtomicBool::new(false),
        }
    }
}
impl KernelEvaluationControl for Control {
    fn checkpoint(&self, units: u32) -> Result<(), KernelFailure> {
        assert!(
            !self.refused.load(Ordering::Relaxed),
            "no callback after originating refusal"
        );
        let mut trace = self.trace.lock().unwrap();
        trace.push(units);
        if trace.len() == self.stop {
            self.refused.store(true, Ordering::Relaxed);
            Err(self.cause.clone())
        } else {
            Ok(())
        }
    }
    fn wait(&self, _: Duration) -> Result<(), KernelFailure> {
        panic!("Boolean region performs no waits")
    }
}
fn run<T>(
    control: &dyn KernelEvaluationControl,
    body: impl FnOnce(&mut Work<'_, '_>) -> Result<T, KernelFailure>,
) -> Result<T, KernelFailure> {
    let observed = ObservedControl {
        original: control,
        refused: AtomicBool::new(false),
        operation_aborted: AtomicBool::new(false),
    };
    observed.checkpoint(0)?;
    let mut work = Work {
        control: &observed,
        pending: 0,
        scalar_scope: None,
    };
    let result = body(&mut work);
    work.finish(result)
}

fn finish_values(
    selection: Selection<'_>,
    result: (ArrayRef, Box<[RowDataError]>),
) -> SelectedValues<'_> {
    SelectedValues::try_new(selection, &DataType::Boolean, result.0, result.1).unwrap()
}
fn nullable(values: &SelectedValues<'_>) -> Vec<Option<bool>> {
    values
        .values()
        .as_any()
        .downcast_ref::<BooleanArray>()
        .unwrap()
        .iter()
        .collect()
}

// Independent SQL truth table: errors are suppressible only within this pure
// ordered region, and successful NULL differs from an errored NULL placeholder.
fn expected(
    shape: ControlShape,
    demand: EvaluationDemand,
    pair: [Cell; 2],
) -> (Option<bool>, bool) {
    let has = |test: fn(Cell) -> bool| pair.iter().copied().any(test);
    let t = has(|c| matches!(c, Cell::True));
    let f = has(|c| matches!(c, Cell::False));
    let n = has(|c| matches!(c, Cell::Null));
    let e = has(|c| matches!(c, Cell::Error));
    if shape == ControlShape::Conjunction {
        if f || (n && demand == EvaluationDemand::TruthOnly) {
            (Some(false), false)
        } else if e {
            (None, true)
        } else if n {
            (None, false)
        } else {
            (Some(true), false)
        }
    } else if t {
        (Some(true), false)
    } else if e {
        (None, true)
    } else if n {
        (
            (demand == EvaluationDemand::TruthOnly).then_some(false),
            false,
        )
    } else {
        (Some(false), false)
    }
}

#[test]
fn exhaustive_pure_and_or_value_and_truth_only_use_actual_sparse_results() {
    let cells = [Cell::True, Cell::False, Cell::Null, Cell::Error];
    let pairs = cells
        .into_iter()
        .flat_map(|a| cells.into_iter().map(move |b| [a, b]))
        .collect::<Vec<_>>();
    let outer_rows = (0..pairs.len()).map(|i| i * 3 + 2).collect::<Vec<_>>();
    let outer = Selection::try_sparse(64, &outer_rows).unwrap();
    for shape in [ControlShape::Conjunction, ControlShape::Disjunction] {
        for demand in [EvaluationDemand::Value, EvaluationDemand::TruthOnly] {
            let control = Control::recording();
            let result = run(&control, |work| {
                let mut state = BooleanRows::new(pairs.len(), work)?;
                let mut terminal = BTreeMap::new();
                let mut remaining = (0..pairs.len()).collect::<Vec<_>>();
                for argument in [0, 1] {
                    let rows = remaining.iter().map(|&i| outer_rows[i]).collect::<Vec<_>>();
                    let selection = Selection::try_sparse(64, &rows).unwrap();
                    let selected = remaining
                        .iter()
                        .map(|&i| pairs[i][argument])
                        .collect::<Vec<_>>();
                    let output = operand(selection, &selected, "actual pure operand error");
                    remaining = state.consume(
                        &output,
                        &remaining,
                        shape,
                        demand,
                        true,
                        &mut terminal,
                        work,
                    )?;
                }
                state.finish(shape, demand, terminal, work)
            })
            .unwrap();
            let result = finish_values(outer, result);
            let oracle = pairs
                .iter()
                .map(|&pair| expected(shape, demand, pair))
                .collect::<Vec<_>>();
            assert_eq!(
                nullable(&result),
                oracle.iter().map(|p| p.0).collect::<Vec<_>>()
            );
            assert_eq!(
                result
                    .errors()
                    .iter()
                    .map(RowDataError::selected_ordinal)
                    .collect::<Vec<_>>(),
                oracle
                    .iter()
                    .enumerate()
                    .filter_map(|(i, p)| p.1.then_some(i))
                    .collect::<Vec<_>>()
            );
            for error in result.errors() {
                assert_eq!(
                    result.selection().row(error.selected_ordinal()),
                    Some(outer_rows[error.selected_ordinal()])
                );
            }
        }
    }
}

#[test]
fn pure_first_error_is_owned_and_suppressed_only_by_actual_deciding_value() {
    let rows = [2, 7, 11];
    let selection = Selection::try_sparse(12, &rows).unwrap();
    for (shape, later) in [
        (ControlShape::Conjunction, Cell::False),
        (ControlShape::Disjunction, Cell::True),
    ] {
        let result = run(&Control::recording(), |work| {
            let mut state = BooleanRows::new(3, work)?;
            let mut terminal = BTreeMap::new();
            let first = operand(
                selection,
                &[Cell::Error; 3],
                "first error survives its array",
            );
            let weak = Arc::downgrade(first.values());
            let remaining = state.consume(
                &first,
                &[0, 1, 2],
                shape,
                EvaluationDemand::Value,
                true,
                &mut terminal,
                work,
            )?;
            drop(first);
            assert!(
                weak.upgrade().is_none(),
                "state retains diagnostics, not operand arrays"
            );
            let second = operand(selection, &[later, Cell::Error, Cell::Null], "second error");
            state.consume(
                &second,
                &remaining,
                shape,
                EvaluationDemand::Value,
                true,
                &mut terminal,
                work,
            )?;
            state.finish(shape, EvaluationDemand::Value, terminal, work)
        })
        .unwrap();
        let result = finish_values(selection, result);
        assert_eq!(
            nullable(&result),
            vec![Some(shape == ControlShape::Disjunction), None, None]
        );
        assert_eq!(
            result
                .errors()
                .iter()
                .map(RowDataError::selected_ordinal)
                .collect::<Vec<_>>(),
            vec![1, 2]
        );
        assert!(
            result
                .errors()
                .iter()
                .all(|e| e.message() == "first error survives its array")
        );
        assert_eq!(result.selection().row(1), Some(7));
        assert_eq!(result.selection().row(2), Some(11));
    }
}

#[test]
fn successful_null_and_error_distinguish_truth_demand_and_error_placeholder() {
    for demand in [EvaluationDemand::Value, EvaluationDemand::TruthOnly] {
        for first_error in [false, true] {
            let mut consumed = 0;
            let result = run(&Control::recording(), |work| {
                let mut state = BooleanRows::new(1, work)?;
                let mut terminal = BTreeMap::new();
                let mut remaining = vec![0];
                for cell in if first_error {
                    [Cell::Error, Cell::Null]
                } else {
                    [Cell::Null, Cell::Error]
                } {
                    if remaining.is_empty() {
                        break;
                    }
                    consumed += 1;
                    let output = operand(Selection::all(1), &[cell], "required operand error");
                    remaining = state.consume(
                        &output,
                        &remaining,
                        ControlShape::Conjunction,
                        demand,
                        true,
                        &mut terminal,
                        work,
                    )?;
                }
                state.finish(ControlShape::Conjunction, demand, terminal, work)
            })
            .unwrap();
            let result = finish_values(Selection::all(1), result);
            if demand == EvaluationDemand::TruthOnly {
                assert_eq!(nullable(&result), vec![Some(false)]);
                assert!(result.errors().is_empty());
                assert_eq!(consumed, if first_error { 2 } else { 1 });
            } else {
                assert_eq!(nullable(&result), vec![None]);
                assert_eq!(result.errors().len(), 1);
                assert_eq!(consumed, 2);
            }
        }
    }
    // The public author prohibits a successful Boolean payload behind an error.
    let invalid = SelectedValues::try_new(
        Selection::all(1),
        &DataType::Boolean,
        Arc::new(BooleanArray::from(vec![Some(false)])),
        vec![RowDataError::new(0, "not a NULL")].into_boxed_slice(),
    );
    assert!(matches!(
        invalid,
        Err(EvaluationContractError::InvalidRowErrors)
    ));
    let result = run(&Control::recording(), |work| {
        let mut state = BooleanRows::new(1, work)?;
        let mut terminal = BTreeMap::new();
        let output = operand(
            Selection::all(1),
            &[Cell::Error],
            "error is not a successful NULL",
        );
        assert_eq!(
            state.consume(
                &output,
                &[0],
                ControlShape::Conjunction,
                EvaluationDemand::TruthOnly,
                true,
                &mut terminal,
                work
            )?,
            vec![0]
        );
        state.finish(
            ControlShape::Conjunction,
            EvaluationDemand::TruthOnly,
            terminal,
            work,
        )
    })
    .unwrap();
    assert_eq!(finish_values(Selection::all(1), result).errors().len(), 1);
}

#[test]
fn impure_boundary_promotes_pending_before_later_decider_and_preserves_sparse_parent() {
    let outer_rows = [2, 7];
    let outer = Selection::try_sparse(10, &outer_rows).unwrap();
    let result = run(&Control::recording(), |work| {
        let mut state = BooleanRows::new(2, work)?;
        let mut terminal = BTreeMap::new();
        let output = operand(
            outer,
            &[Cell::Error, Cell::True],
            "must precede observable work",
        );
        let mut remaining = state.consume(
            &output,
            &[0, 1],
            ControlShape::Conjunction,
            EvaluationDemand::Value,
            true,
            &mut terminal,
            work,
        )?;
        assert_eq!(remaining, vec![0, 1]);
        state.boundary(&mut terminal, &mut remaining, work)?;
        assert_eq!(remaining, vec![1]);
        assert_eq!(
            terminal.get(&0).unwrap().message(),
            "must precede observable work"
        );
        let rows = [7];
        let later = operand(
            Selection::try_sparse(10, &rows).unwrap(),
            &[Cell::False],
            "not evaluated for parent 2",
        );
        state.consume(
            &later,
            &remaining,
            ControlShape::Conjunction,
            EvaluationDemand::Value,
            false,
            &mut terminal,
            work,
        )?;
        state.finish(
            ControlShape::Conjunction,
            EvaluationDemand::Value,
            terminal,
            work,
        )
    })
    .unwrap();
    let result = finish_values(outer, result);
    assert_eq!(nullable(&result), vec![None, Some(false)]);
    assert_eq!(result.errors()[0].selected_ordinal(), 0);
    assert_eq!(result.selection().row(0), Some(2));
}

#[test]
fn immediate_impure_error_leaves_region_and_cannot_be_masked_by_later_decider() {
    for shape in [ControlShape::Conjunction, ControlShape::Disjunction] {
        let result = run(&Control::recording(), |work| {
            let mut state = BooleanRows::new(2, work)?;
            let mut terminal = BTreeMap::new();
            let first = operand(
                Selection::all(2),
                &[
                    Cell::Error,
                    if shape == ControlShape::Conjunction {
                        Cell::True
                    } else {
                        Cell::False
                    },
                ],
                "impure required error",
            );
            let remaining = state.consume(
                &first,
                &[0, 1],
                shape,
                EvaluationDemand::Value,
                false,
                &mut terminal,
                work,
            )?;
            assert_eq!(remaining, vec![1]);
            let rows = [1];
            let later = operand(
                Selection::try_sparse(2, &rows).unwrap(),
                &[if shape == ControlShape::Conjunction {
                    Cell::False
                } else {
                    Cell::True
                }],
                "unused",
            );
            state.consume(
                &later,
                &remaining,
                shape,
                EvaluationDemand::Value,
                true,
                &mut terminal,
                work,
            )?;
            state.finish(shape, EvaluationDemand::Value, terminal, work)
        })
        .unwrap();
        let result = finish_values(Selection::all(2), result);
        assert_eq!(
            nullable(&result),
            vec![None, Some(shape == ControlShape::Disjunction)]
        );
        assert_eq!(result.errors()[0].message(), "impure required error");
    }
}

#[test]
fn empty_boolean_region_returns_identity_and_retains_no_operand() {
    for shape in [ControlShape::Conjunction, ControlShape::Disjunction] {
        for demand in [EvaluationDemand::Value, EvaluationDemand::TruthOnly] {
            let result = run(&Control::recording(), |work| {
                BooleanRows::new(0, work)?.finish(shape, demand, BTreeMap::new(), work)
            })
            .unwrap();
            let result = finish_values(Selection::all(0), result);
            assert!(result.values().is_empty());
            assert!(result.errors().is_empty());
        }
    }
}

fn all_causes() -> [KernelFailure; 7] {
    [
        KernelFailure::Cancelled,
        KernelFailure::DeadlineExceeded,
        KernelFailure::ResourceExhausted,
        invalid("originating Boolean control invalid"),
        internal("originating Boolean control internal"),
        KernelFailure::Operational(KernelDiagnostic::new(
            "originating Boolean control operational",
        )),
        KernelFailure::InstanceFailed,
    ]
}
fn wide_region(
    control: &dyn KernelEvaluationControl,
) -> Result<(ArrayRef, Box<[RowDataError]>), KernelFailure> {
    let cells = vec![Cell::Error; 320];
    let output = operand(Selection::all(320), &cells, "wide actual operand error");
    run(control, |work| {
        let mut state = BooleanRows::new(320, work)?;
        let mut terminal = BTreeMap::new();
        let ordinals = (0..320).collect::<Vec<_>>();
        let mut remaining = state.consume(
            &output,
            &ordinals,
            ControlShape::Conjunction,
            EvaluationDemand::Value,
            true,
            &mut terminal,
            work,
        )?;
        state.boundary(&mut terminal, &mut remaining, work)?;
        assert!(remaining.is_empty());
        state.finish(
            ControlShape::Conjunction,
            EvaluationDemand::Value,
            terminal,
            work,
        )
    })
}
#[test]
fn every_actual_boolean_callback_preserves_all_seven_causes_without_retry() {
    let recorder = Control::recording();
    let (values, errors) = wide_region(&recorder).unwrap();
    assert_eq!(values.len(), 320);
    assert_eq!(errors.len(), 320);
    let trace = recorder.trace.lock().unwrap().clone();
    assert_eq!(trace[0], 0);
    assert!(trace.contains(&256));
    for index in 1..=trace.len() {
        for cause in all_causes() {
            let control = Control::stopping(index, cause.clone());
            let result = wide_region(&control);
            assert!(
                matches!(result,Err(actual) if actual==cause),
                "callback {index}"
            );
            assert_eq!(*control.trace.lock().unwrap(), trace[..index]);
        }
    }
}

#[test]
fn non_boolean_operand_refusal_observes_ordinary_tail_and_keeps_primary_control() {
    let input = SelectedValues::try_new(
        Selection::all(1),
        &DataType::Int64,
        Arc::new(arrow::array::Int64Array::from(vec![7])),
        Box::default(),
    )
    .unwrap();
    let evaluate = |control: &dyn KernelEvaluationControl| {
        run(control, |work| {
            let mut state = BooleanRows::new(1, work)?;
            state.consume(
                &input,
                &[0],
                ControlShape::Conjunction,
                EvaluationDemand::Value,
                true,
                &mut BTreeMap::new(),
                work,
            )
        })
    };
    let recorder = Control::recording();
    assert!(matches!(
        evaluate(&recorder),
        Err(KernelFailure::InvalidProgram(_))
    ));
    assert_eq!(*recorder.trace.lock().unwrap(), vec![0, 1]);
    for cause in all_causes() {
        let control = Control::stopping(2, cause.clone());
        assert!(matches!(evaluate(&control),Err(actual) if actual==cause));
        assert_eq!(*control.trace.lock().unwrap(), vec![0, 1]);
    }
}
