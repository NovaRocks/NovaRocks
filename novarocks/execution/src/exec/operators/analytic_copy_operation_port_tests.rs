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

//! Actual original stage-loop copy occurrences. This test adapter observes
//! operations and control; it is explicitly NOT a host allocation grant.
use super::stages::AnalyticCopyFailure;
use super::*;
use arrow::array::{RecordBatch, RecordBatchOptions, UInt32Array};

#[derive(Clone, Debug, PartialEq, Eq)]
enum Occurrence {
    Concat { column: usize, sources: usize },
    Take { column: usize, raw: Vec<u32> },
    Slice { offset: usize, len: usize },
}
struct Ports<'a> {
    original: Work<'a>,
    occurrences: Vec<Occurrence>,
    observe_operations: bool,
}
impl<'a> Ports<'a> {
    fn new(control: &'a dyn KernelEvaluationControl, observe_operations: bool) -> Self {
        Self {
            original: Work::new(control, Arc::new(AtomicUsize::new(0))),
            occurrences: Vec::new(),
            observe_operations,
        }
    }
    fn before_operation(&mut self) -> Result<(), KernelFailure> {
        if self.observe_operations {
            self.original.boundary()?;
        }
        Ok(())
    }
}
fn project(
    error: AnalyticCopyFailure<std::convert::Infallible>,
) -> AnalyticCopyFailure<KernelFailure> {
    match error {
        AnalyticCopyFailure::OriginalArrow { source, .. } => {
            AnalyticCopyFailure::original_arrow(source)
        }
        AnalyticCopyFailure::Control(never) => match never {},
    }
}
impl AnalyticInputWork for Ports<'_> {
    type Failure = KernelFailure;
    type Scope = LifetimeWitness;
    fn admit(&mut self, source: AnalyticInputSource<'_>) -> Result<Self::Scope, Self::Failure> {
        self.original.admit(source)
    }
    fn step(&mut self) -> Result<(), Self::Failure> {
        self.original.step()
    }
    fn boundary(&mut self) -> Result<(), Self::Failure> {
        self.original.boundary()
    }
    fn concat_column(
        &mut self,
        _: &mut Self::Scope,
        batches: &[&RecordBatch],
        sources: &[Chunk],
        ordinal: usize,
    ) -> Result<ArrayRef, AnalyticCopyFailure<Self::Failure>> {
        self.before_operation()
            .map_err(AnalyticCopyFailure::Control)?;
        assert_eq!(batches.len(), sources.len());
        for (batch, source) in batches.iter().zip(sources) {
            assert!(Arc::ptr_eq(
                batch.column(ordinal),
                source.record_batch().column(ordinal)
            ));
        }
        self.occurrences.push(Occurrence::Concat {
            column: ordinal,
            sources: sources.len(),
        });
        stages::UnscopedWork
            .concat_column(&mut (), batches, sources, ordinal)
            .map_err(project)
    }
    fn take_column(
        &mut self,
        _: &mut Self::Scope,
        source: &Chunk,
        ordinal: usize,
        indices: &Arc<UInt32Array>,
    ) -> Result<ArrayRef, AnalyticCopyFailure<Self::Failure>> {
        self.before_operation()
            .map_err(AnalyticCopyFailure::Control)?;
        self.occurrences.push(Occurrence::Take {
            column: ordinal,
            raw: indices.values().to_vec(),
        });
        stages::UnscopedWork
            .take_column(&mut (), source, ordinal, indices)
            .map_err(project)
    }
    fn slice_column(
        &mut self,
        _: &mut Self::Scope,
        source: &ArrayRef,
        offset: usize,
        len: usize,
    ) -> Result<ArrayRef, Self::Failure> {
        self.before_operation()?;
        self.occurrences.push(Occurrence::Slice { offset, len });
        match stages::UnscopedWork.slice_column(&mut (), source, offset, len) {
            Ok(value) => Ok(value),
            Err(never) => match never {},
        }
    }
}

#[test]
fn analytic_copy_operation_port_default_original_batches_and_original_boundaries() {
    let inputs = vec![ints(vec![Some(7), None]), ints(vec![Some(4), Some(3)])];
    let control = Control::new(None);
    let observed = KernelControlObservation::new(&control);
    let mut work = Ports::new(&observed, false);
    let result = stages::gather(&inputs, &mut work).unwrap();
    let expected = arrow::compute::concat_batches(
        &inputs[0].schema(),
        inputs.iter().map(|input| input.record_batch()),
    )
    .unwrap();
    assert_eq!(result.value.record_batch(), &expected);
    assert_eq!(
        work.occurrences,
        vec![Occurrence::Concat {
            column: 0,
            sources: 2
        }]
    );
    let key: ArrayRef = Arc::new(Int32Array::from(vec![2, 0, 1, 0]));
    let ordered = stages::regroup(&result.value, &[key], None, &mut work).unwrap();
    assert_eq!(
        work.occurrences[1],
        Occurrence::Take {
            column: 0,
            raw: vec![1, 3, 2, 0]
        }
    );
    let output = stages::split(
        ordered.value.chunk_schema_ref(),
        ordered.value.columns(),
        &inputs,
        &mut work,
    )
    .unwrap();
    assert_eq!(
        work.occurrences[2..],
        [
            Occurrence::Slice { offset: 0, len: 2 },
            Occurrence::Slice { offset: 2, len: 2 }
        ]
    );
    assert_eq!(output.value.len(), 2);
    let joined = arrow::compute::concat_batches(
        &ordered.value.schema(),
        output.value.iter().map(|chunk| chunk.record_batch()),
    )
    .unwrap();
    assert_eq!(&joined, ordered.value.record_batch());
    assert_eq!(
        result.value.chunk_schema_ref(),
        inputs[0].chunk_schema_ref()
    );
}

#[test]
fn analytic_copy_operation_port_zero_columns_single_source_and_identity_skip_copy() {
    let empty_schema = Arc::new(ChunkSchema::empty());
    let source = |rows| {
        Chunk::try_new_with_chunk_schema(
            RecordBatch::try_new_with_options(
                empty_schema.arrow_schema_ref(),
                Vec::new(),
                &RecordBatchOptions::new().with_row_count(Some(rows)),
            )
            .unwrap(),
            Arc::clone(&empty_schema),
        )
        .unwrap()
    };
    let control = Control::new(None);
    let observed = KernelControlObservation::new(&control);
    let mut work = Ports::new(&observed, false);
    // Work's split-only source proof indexes a real column; this test only
    // uses its exact Gather/Regroup entry where a zero-column batch is valid.
    let result = stages::gather(&[source(2), source(3)], &mut work).unwrap();
    assert_eq!(result.value.len(), 5);
    assert!(work.occurrences.is_empty());
    let single = ints(vec![Some(1), Some(2)]);
    let one = stages::gather(std::slice::from_ref(&single), &mut work).unwrap();
    assert!(Arc::ptr_eq(
        one.value.record_batch().column(0),
        single.record_batch().column(0)
    ));
    let key = Arc::clone(single.record_batch().column(0));
    let identity = stages::regroup(&single, &[key], None, &mut work).unwrap();
    assert!(Arc::ptr_eq(
        identity.value.record_batch().column(0),
        single.record_batch().column(0)
    ));
    assert!(work.occurrences.is_empty());
}

#[test]
fn analytic_copy_operation_port_same_default_control_trace_and_data_formatter() {
    for stage in [
        AnalyticInputStage::Gather,
        AnalyticInputStage::Regroup,
        AnalyticInputStage::OutputSplit,
    ] {
        let source = ints(vec![Some(4), None, Some(2), Some(3)]);
        let evaluate = |work: &mut Ports<'_>| -> Result<(), AnalyticStageFailure<LifetimeWitness, KernelFailure>> {
            match stage {
                AnalyticInputStage::Gather => { drop(stages::gather(&[source.clone(), chunk(Arc::new(Int64Array::from(vec![1, 2])))], work)?); }
                AnalyticInputStage::Regroup => {
                    let key: ArrayRef = Arc::new(BinaryArray::from(vec![b"x".as_slice(), b"z".as_slice(), b"y".as_slice(), b"w".as_slice()]));
                    drop(stages::regroup(&source, &[key], None, work)?);
                }
                AnalyticInputStage::OutputSplit => { drop(stages::split(schema(DataType::Int64), source.columns(), std::slice::from_ref(&source), work)?); }
            }
            Ok(())
        };
        let control = Control::new(None);
        let observed = KernelControlObservation::new(&control);
        let mut work = Ports::new(&observed, false);
        let error = evaluate(&mut work).expect_err("original Data");
        let expected = match stage {
            AnalyticInputStage::Gather => {
                let second = chunk(Arc::new(Int64Array::from(vec![1, 2])));
                let error = arrow::compute::concat_batches(
                    &source.schema(),
                    [source.record_batch(), second.record_batch()],
                )
                .expect_err("original upstream Data");
                format!("concat_batches: {error}")
            }
            AnalyticInputStage::Regroup => "unsupported type for min/max: Binary".to_owned(),
            AnalyticInputStage::OutputSplit => {
                let error =
                    Chunk::try_new_with_columns(schema(DataType::Int64), source.columns().to_vec())
                        .expect_err("original Chunk Data");
                format!("build analytic output batch: {error}")
            }
        };
        match error {
            AnalyticStageFailure::Operation { cause: message, .. } => {
                assert_eq!(message.to_string(), expected)
            }
            other => panic!("wrong channel {other:?}"),
        }
        let trace = control.trace();
        let original_control = Control::new(None);
        let original_observed = KernelControlObservation::new(&original_control);
        let mut original_work = Work::new(&original_observed, Arc::new(AtomicUsize::new(0)));
        assert!(run(stage, true, &mut original_work).is_err());
        assert_eq!(
            trace,
            original_control.trace(),
            "default operation ports add no checkpoint"
        );
    }
}

#[test]
fn analytic_copy_operation_port_all_seven_real_control_refusals_no_footer() {
    let inputs = [ints(vec![Some(2), None]), ints(vec![Some(1), Some(0)])];
    let control = Control::new(None);
    let observed = KernelControlObservation::new(&control);
    let mut work = Ports::new(&observed, true);
    assert!(stages::gather(&inputs, &mut work).is_ok());
    let trace = control.trace();
    for cause in causes() {
        for ordinal in 0..trace.len() {
            let control = Control::new(Some((ordinal, cause.clone())));
            let observed = KernelControlObservation::new(&control);
            let mut work = Ports::new(&observed, true);
            match stages::gather(&inputs, &mut work) {
                Err(AnalyticStageFailure::Control {
                    stage,
                    cause: actual,
                }) => {
                    assert_eq!(stage, AnalyticInputStage::Gather);
                    assert_eq!(actual, cause);
                }
                other => panic!("expected first control {other:?}"),
            }
            assert_eq!(control.trace(), trace[..=ordinal]);
            assert_eq!(work.boundary(), Err(cause.clone()));
            assert_eq!(control.trace(), trace[..=ordinal]);
        }
    }
}
