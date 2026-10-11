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
use super::analytic_input_stages::{
    self as stages, AnalyticInputSource, AnalyticInputStage, AnalyticInputWork,
    AnalyticStageFailure,
};
use super::{ArrayRef, Chunk, ChunkSchemaRef};
use crate::exec::chunk::{ChunkSchema, ChunkSlotSchema};
use arrow::array::{Array, BinaryArray, Int32Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field};
use novarocks_functions::{
    EvaluationCheckpoints, KernelControlObservation, KernelDiagnostic, KernelEvaluationControl,
    KernelFailure, MAX_UNOBSERVED_KERNEL_WORK,
};
use novarocks_types::SlotId;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering},
};
use std::time::Duration;

fn schema(dtype: DataType) -> ChunkSchemaRef {
    let mut metadata = std::collections::HashMap::new();
    metadata.insert("PARQUET:field_id".into(), "71".into());
    Arc::new(
        ChunkSchema::try_new(vec![ChunkSlotSchema::new_with_field(
            SlotId::new(7),
            Field::new("stage_value", dtype, true).with_metadata(metadata),
            None,
            Some(71),
        )])
        .expect("actual chunk schema"),
    )
}
fn chunk(array: ArrayRef) -> Chunk {
    Chunk::try_new_with_columns(schema(array.data_type().clone()), vec![array])
        .expect("actual input chunk")
}
fn ints(values: Vec<Option<i32>>) -> Chunk {
    chunk(Arc::new(Int32Array::from(values)))
}

struct Control {
    trace: Mutex<Vec<u32>>,
    refusal: Option<(usize, KernelFailure)>,
}
impl Control {
    fn new(refusal: Option<(usize, KernelFailure)>) -> Self {
        Self {
            trace: Mutex::new(Vec::new()),
            refusal,
        }
    }
    fn trace(&self) -> Vec<u32> {
        self.trace.lock().unwrap().clone()
    }
}
impl KernelEvaluationControl for Control {
    fn checkpoint(&self, units: u32) -> Result<(), KernelFailure> {
        assert!(units <= MAX_UNOBSERVED_KERNEL_WORK);
        let mut trace = self.trace.lock().unwrap();
        let ordinal = trace.len();
        if let Some((stop, _)) = &self.refusal {
            assert!(ordinal <= *stop, "callback after first refusal");
        }
        trace.push(units);
        match &self.refusal {
            Some((stop, cause)) if ordinal == *stop => Err(cause.clone()),
            _ => Ok(()),
        }
    }
    fn wait(&self, _: Duration) -> Result<(), KernelFailure> {
        panic!("analytic input stages do not wait")
    }
}
#[derive(Debug)]
struct LifetimeWitness(Arc<AtomicUsize>);
impl Drop for LifetimeWitness {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}
// This test witness proves payload/scope lifetime only. It is NOT a memory grant.
struct Work<'a> {
    control: EvaluationCheckpoints<'a>,
    drops: Arc<AtomicUsize>,
    admitted: Vec<AnalyticInputStage>,
}
impl<'a> Work<'a> {
    fn new(control: &'a dyn KernelEvaluationControl, drops: Arc<AtomicUsize>) -> Self {
        Self {
            control: EvaluationCheckpoints::new(control),
            drops,
            admitted: Vec::new(),
        }
    }
}
impl AnalyticInputWork for Work<'_> {
    type Failure = KernelFailure;
    type Scope = LifetimeWitness;
    fn admit(&mut self, source: AnalyticInputSource<'_>) -> Result<LifetimeWitness, KernelFailure> {
        self.control.flush()?;
        let stage = source.stage();
        match source {
            AnalyticInputSource::Gather { chunks } => assert!(!chunks.is_empty()),
            AnalyticInputSource::Regroup { chunk, keys } => {
                for key in keys {
                    assert_eq!(key.len(), chunk.len());
                }
            }
            AnalyticInputSource::OutputSplit {
                output,
                columns,
                chunks,
            } => {
                assert_eq!(columns.len(), output.slots().len());
                assert_eq!(
                    columns[0].len(),
                    chunks.iter().map(Chunk::len).sum::<usize>()
                );
            }
        }
        self.admitted.push(stage);
        Ok(LifetimeWitness(Arc::clone(&self.drops)))
    }
    fn step(&mut self) -> Result<(), KernelFailure> {
        self.control.step()
    }
    fn boundary(&mut self) -> Result<(), KernelFailure> {
        self.control.flush()
    }
}
fn causes() -> Vec<KernelFailure> {
    vec![
        KernelFailure::Cancelled,
        KernelFailure::DeadlineExceeded,
        KernelFailure::ResourceExhausted,
        KernelFailure::InvalidProgram(KernelDiagnostic::new("stage exact invalid program")),
        KernelFailure::Internal(KernelDiagnostic::new("stage exact internal")),
        KernelFailure::Operational(KernelDiagnostic::new("stage exact operational")),
        KernelFailure::InstanceFailed,
    ]
}
fn run(
    stage: AnalyticInputStage,
    bad: bool,
    work: &mut Work<'_>,
) -> Result<(), AnalyticStageFailure<LifetimeWitness, KernelFailure>> {
    let source = ints(vec![Some(4), None, Some(2), Some(3)]);
    match stage {
        AnalyticInputStage::Gather => {
            let second = if bad {
                chunk(Arc::new(Int64Array::from(vec![5, 6])))
            } else {
                ints(vec![Some(5), Some(6)])
            };
            let result = stages::gather(&[source, second], work)?;
            assert_eq!(result.stage, stage);
            drop(result);
        }
        AnalyticInputStage::Regroup => {
            let keys: ArrayRef = if bad {
                Arc::new(BinaryArray::from(vec![
                    b"d".as_slice(),
                    b"a".as_slice(),
                    b"b".as_slice(),
                    b"c".as_slice(),
                ]))
            } else {
                Arc::new(Int32Array::from(vec![2, 1, 1, 0]))
            };
            let result = stages::regroup(&source, &[keys], work)?;
            assert_eq!(result.stage, stage);
            drop(result);
        }
        AnalyticInputStage::OutputSplit => {
            let output = if bad {
                schema(DataType::Int64)
            } else {
                source.chunk_schema_ref()
            };
            let result = stages::split(output, source.batch.columns(), &[source.clone()], work)?;
            assert_eq!(result.stage, stage);
            drop(result);
        }
    }
    Ok(())
}

#[test]
fn analytic_input_stage_seven_causes_at_every_real_callback_no_footer() {
    for stage in [
        AnalyticInputStage::Gather,
        AnalyticInputStage::Regroup,
        AnalyticInputStage::OutputSplit,
    ] {
        let control = Control::new(None);
        let observed = KernelControlObservation::new(&control);
        let drops = Arc::new(AtomicUsize::new(0));
        let mut work = Work::new(&observed, Arc::clone(&drops));
        assert!(run(stage, false, &mut work).is_ok());
        let trace = control.trace();
        assert!(!trace.is_empty());
        assert_eq!(drops.load(Ordering::SeqCst), 1);
        for cause in causes() {
            for refusal in 0..trace.len() {
                let control = Control::new(Some((refusal, cause.clone())));
                let observed = KernelControlObservation::new(&control);
                let drops = Arc::new(AtomicUsize::new(0));
                let mut work = Work::new(&observed, Arc::clone(&drops));
                match run(stage, false, &mut work) {
                    Err(AnalyticStageFailure::Control {
                        stage: actual,
                        cause: actual_cause,
                    }) => {
                        assert_eq!(actual, stage);
                        assert_eq!(actual_cause, cause);
                    }
                    other => panic!("expected exact stage control, got {other:?}"),
                }
                assert_eq!(control.trace(), trace[..=refusal]);
                assert_eq!(drops.load(Ordering::SeqCst), usize::from(refusal != 0));
                assert_eq!(work.boundary(), Err(cause.clone()));
                assert_eq!(
                    control.trace(),
                    trace[..=refusal],
                    "failed scope cannot replay callbacks"
                );
            }
        }
    }
}

fn original_error(stage: AnalyticInputStage) -> String {
    let source = ints(vec![Some(4), None, Some(2), Some(3)]);
    match stage {
        AnalyticInputStage::Gather => super::concat_original_analytic_input(&[
            source,
            chunk(Arc::new(Int64Array::from(vec![5, 6]))),
        ])
        .err()
        .expect("original concat Data"),
        AnalyticInputStage::Regroup => super::reorder_chunk_by_partition_keys(
            &source,
            &[Arc::new(BinaryArray::from(vec![
                b"d".as_slice(),
                b"a".as_slice(),
                b"b".as_slice(),
                b"c".as_slice(),
            ]))],
        )
        .err()
        .expect("original unsupported key Data"),
        AnalyticInputStage::OutputSplit => super::split_analytic_output_chunks(
            schema(DataType::Int64),
            source.batch.columns(),
            &[source.clone()],
        )
        .err()
        .expect("original split Data"),
    }
}
#[test]
fn analytic_input_stage_original_data_full_text_and_scope_no_failure_footer() {
    for stage in [
        AnalyticInputStage::Gather,
        AnalyticInputStage::Regroup,
        AnalyticInputStage::OutputSplit,
    ] {
        let expected = original_error(stage);
        let control = Control::new(None);
        let observed = KernelControlObservation::new(&control);
        let drops = Arc::new(AtomicUsize::new(0));
        let mut work = Work::new(&observed, Arc::clone(&drops));
        let failure = run(stage, true, &mut work)
            .err()
            .expect("original stage Data");
        match &failure {
            AnalyticStageFailure::OriginalData {
                stage: actual,
                message,
                ..
            } => {
                assert_eq!(*actual, stage);
                assert_eq!(message, &expected);
            }
            other => panic!("wrong channel {other:?}"),
        }
        assert_eq!(
            drops.load(Ordering::SeqCst),
            0,
            "full diagnostic retains original scope"
        );
        let trace = control.trace();
        drop(failure);
        assert_eq!(drops.load(Ordering::SeqCst), 1);
        // A refusal scheduled at the next callback must stay unobserved after Data.
        let control = Control::new(Some((trace.len(), KernelFailure::Cancelled)));
        let observed = KernelControlObservation::new(&control);
        let mut work = Work::new(&observed, Arc::new(AtomicUsize::new(0)));
        assert!(
            matches!(run(stage,true,&mut work),Err(AnalyticStageFailure::OriginalData { message,.. }) if message==expected)
        );
        assert_eq!(control.trace(), trace);
        for cause in causes() {
            for refusal in 0..trace.len() {
                let control = Control::new(Some((refusal, cause.clone())));
                let observed = KernelControlObservation::new(&control);
                let mut work = Work::new(&observed, Arc::new(AtomicUsize::new(0)));
                assert!(
                    matches!(run(stage,true,&mut work),Err(AnalyticStageFailure::Control { cause:actual,.. }) if actual==cause)
                );
                assert_eq!(control.trace(), trace[..=refusal]);
            }
        }
    }
}

#[test]
fn analytic_input_stage_default_values_metadata_null_stability_and_split_boundaries() {
    let first = ints(vec![Some(10), None]);
    let empty = ints(vec![]);
    let second = ints(vec![Some(20), Some(30)]);
    let input = vec![first, empty, second];
    let gathered = super::concat_original_analytic_input(&input).expect("original gather");
    assert_eq!(gathered.len(), 4);
    assert_eq!(gathered.chunk_schema_ref(), input[0].chunk_schema_ref());
    let keys: ArrayRef = Arc::new(Int32Array::from(vec![Some(2), Some(1), Some(1), None]));
    let reordered =
        super::reorder_chunk_by_partition_keys(&gathered, &[keys]).expect("original regroup");
    let actual = reordered
        .batch
        .column(0)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    assert_eq!(
        actual.iter().collect::<Vec<_>>(),
        vec![Some(30), None, Some(20), Some(10)]
    );
    assert_eq!(reordered.chunk_schema_ref(), gathered.chunk_schema_ref());
    let split = super::split_analytic_output_chunks(
        reordered.chunk_schema_ref(),
        reordered.batch.columns(),
        &input,
    )
    .expect("original split");
    assert_eq!(split.iter().map(Chunk::len).collect::<Vec<_>>(), vec![2, 2]);
    assert_eq!(
        split[0].batch.column(0).to_data(),
        reordered.batch.column(0).slice(0, 2).to_data()
    );
    assert_eq!(
        split[1].batch.column(0).to_data(),
        reordered.batch.column(0).slice(2, 2).to_data()
    );
    let original: ArrayRef = Arc::new(StringArray::from(vec![
        Some("prefix"),
        Some("é\0"),
        None,
        Some("last"),
    ]));
    let sliced = chunk(original.slice(1, 3));
    let single = super::concat_original_analytic_input(std::slice::from_ref(&sliced)).unwrap();
    assert!(Arc::ptr_eq(single.batch.column(0), sliced.batch.column(0)));
    let unchanged = super::reorder_chunk_by_partition_keys(&single, &[]).unwrap();
    assert!(Arc::ptr_eq(
        unchanged.batch.column(0),
        single.batch.column(0)
    ));
}

#[test]
fn analytic_input_stage_result_scope_lasts_until_real_result_drop() {
    let control = Control::new(None);
    let observed = KernelControlObservation::new(&control);
    let drops = Arc::new(AtomicUsize::new(0));
    let mut work = Work::new(&observed, Arc::clone(&drops));
    let source = ints(vec![Some(1)]);
    let result = stages::gather(std::slice::from_ref(&source), &mut work)
        .ok()
        .expect("stage success");
    assert_eq!(result.stage, AnalyticInputStage::Gather);
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    drop(result);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
}

#[test]
fn analytic_input_stage_original_null_and_float_key_order_preserved() {
    let keys: ArrayRef = Arc::new(arrow::array::Float64Array::from(vec![
        None,
        Some(f64::NAN),
        Some(1.0),
        Some(f64::NAN),
    ]));
    assert_eq!(
        super::compare_rows_on_partition_keys(std::slice::from_ref(&keys), 0, 1),
        Ok(std::cmp::Ordering::Less)
    );
    assert_eq!(
        super::compare_rows_on_partition_keys(std::slice::from_ref(&keys), 1, 2),
        Ok(std::cmp::Ordering::Greater)
    );
    assert_eq!(
        super::compare_rows_on_partition_keys(std::slice::from_ref(&keys), 1, 3),
        Ok(std::cmp::Ordering::Equal)
    );
}

#[test]
fn analytic_input_stage_many_null_keys_bounded_owned_work_observation() {
    let control = Control::new(None);
    let observed = KernelControlObservation::new(&control);
    let mut work = Work::new(&observed, Arc::new(AtomicUsize::new(0)));
    let source = ints(vec![Some(1), Some(2)]);
    let keys = vec![Arc::new(Int32Array::from(vec![None, None])) as ArrayRef; 600];
    assert!(stages::regroup(&source, &keys, &mut work).is_ok());
    assert!(control.trace().contains(&MAX_UNOBSERVED_KERNEL_WORK));
    assert!(
        control
            .trace()
            .iter()
            .all(|units| *units <= MAX_UNOBSERVED_KERNEL_WORK)
    );
}

#[test]
fn analytic_input_stage_original_empty_gather_panic_remains() {
    assert!(std::panic::catch_unwind(|| super::concat_original_analytic_input(&[])).is_err());
    assert!(
        super::split_analytic_output_chunks(schema(DataType::Int32), &[], &[])
            .unwrap()
            .is_empty()
    );
}

#[path = "analytic_copy_operation_port_tests.rs"]
mod copy_operation_ports;

#[path = "analytic_stage_metadata_escape_tests.rs"]
mod metadata_escape_tests;

#[path = "analytic_chunk_constructor_port_tests.rs"]
mod chunk_constructor_ports;
