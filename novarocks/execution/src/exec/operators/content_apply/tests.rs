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
use crate::{
    exec::{
        chunk::typed_fixture,
        hash_table::key_index::{
            BulkBuildGraph, BulkClaim, BulkDomain, BulkInput, CaptureWork, CompleteInput,
            EvaluatedPage, IndexPurpose,
        },
        pipeline::{
            operator::{
                InputConstructionOwner, input_construction::try_runtime_input_construction_factory,
            },
            pipeline::representation_construction_profile,
        },
    },
    runtime::{
        mem_tracker::MemTracker, runtime_state::RuntimeErrorState,
        tracked_state_allocator::expression_allocation_host,
    },
};
use arrow::{
    array::{ArrayRef, Float64Array, Int64Array},
    datatypes::Schema,
};
use novarocks_functions::KernelEvaluationControl;
use std::num::NonZeroU64;

struct Control;

impl KernelEvaluationControl for Control {
    fn checkpoint(&self, _: u32) -> Result<(), KernelFailure> {
        Ok(())
    }

    fn wait(&self, _: std::time::Duration) -> Result<(), KernelFailure> {
        panic!("a local content test must never sleep")
    }
}

fn work_owner() -> (Box<dyn InputConstructionOwner>, Arc<MemTracker>) {
    let tracker = MemTracker::new_root("content apply original test tracker");
    let factory = try_runtime_input_construction_factory(
        representation_construction_profile().unwrap(),
        tracker.clone(),
        Arc::new(RuntimeErrorState::default()),
    )
    .unwrap();
    let layout = typed_fixture::layout(Arc::new(Schema::empty()), &[], &[]);
    let schema = factory.create_compiled_chunk_schema(&layout).unwrap();
    (factory.begin_typed_construction(&schema).unwrap(), tracker)
}

fn ports<'a>(owner: &'a dyn InputConstructionOwner, tracker: Arc<MemTracker>) -> BulkWork<'a> {
    BulkWork {
        owner,
        allocator: AggregateAllocator::new(tracker.clone()),
        host: expression_allocation_host(tracker),
        control: &Control,
    }
}

fn next_turn(units: u32) -> TurnWork {
    TurnWork::new(NonZeroU64::new(1).unwrap(), units)
}

fn build_index(arrays: &[ArrayRef], events: Vec<i8>, work: &BulkWork<'_>) -> ContentApplyIndex {
    let types: Vec<_> = arrays
        .iter()
        .map(|array| FunctionValueType::new(array.data_type().clone(), true))
        .collect();
    let spec = content_spec(&types, work).unwrap();
    let domain = BulkDomain::new(&spec, work.owner, work.control).unwrap();
    let capture = CaptureWork {
        owner: work.owner,
        allocator: work.allocator.clone(),
        host: work.host.clone(),
        control: work.control,
    };
    let mut captures = Vec::new();
    let mut sealed_weights = Vec::new();
    let midpoint = events.len() / 2;
    for range in [0..midpoint, midpoint..events.len()] {
        let mut writer = domain.begin_capture(&capture).unwrap();
        let mut weights = CapturedWeights::new(work.allocator.clone());
        if !range.is_empty() {
            let arrays: Vec<_> = arrays
                .iter()
                .map(|array| array.slice(range.start, range.len()))
                .collect();
            let readable = ReachableRows::all(range.len())
                .materialize(&mut |_| Ok::<_, ExecutionFailure>(()))
                .unwrap();
            let rows: Vec<_> = (0..range.len()).collect();
            let page = writer
                .push_evaluated(
                    EvaluatedPage {
                        arrays: &arrays,
                        bindings: &types,
                        readable: &readable,
                        occurrences: &rows,
                    },
                    &mut ValueConversionContext::default(),
                    &capture,
                )
                .unwrap();
            weights
                .record(page, &Int8Array::from(events[range].to_vec()), work)
                .unwrap();
        }
        captures.push(writer.seal(&capture).unwrap());
        sealed_weights.push(weights);
    }
    let complete = CompleteInput::from_captures(
        BulkInput::new(&domain, captures, &capture).unwrap(),
        IndexPurpose::KeyIds,
        &capture,
    )
    .unwrap();
    let positions = NonZeroUsize::new(2).unwrap();
    let graph = BulkBuildGraph::begin(complete, positions, positions, work).unwrap();
    while let BulkClaim::Step(mut step) = graph.claim(work).unwrap() {
        while !step.advance(17, work).unwrap().is_done() {}
        let mut complete = step.finish().unwrap();
        graph.complete(&mut complete, work).unwrap();
    }
    let ready = graph.take_ready(work).unwrap().unwrap();
    let mut finalizer = WeightFinalizer::new(ready, sealed_weights, work).unwrap();
    while !finalizer.advance(&mut next_turn(2), work).unwrap() {}
    finalizer.finish(work).unwrap()
}

fn probe(index: &ContentApplyIndex, arrays: &[ArrayRef], work: &BulkWork<'_>) -> Vec<BulkMatch> {
    let len = arrays[0].len();
    let readable = ReachableRows::all(len)
        .materialize(&mut |_| Ok::<_, ExecutionFailure>(()))
        .unwrap();
    let rows: Vec<_> = (0..len).collect();
    let input = KeyInput::new_for_spec(
        index.spec(),
        arrays,
        index.spec().types(),
        &readable,
        &rows,
        &mut ValueConversionContext::default(),
        work.owner,
        &|| Ok(()),
    )
    .unwrap();
    let prepared =
        PreparedKeyBatch::from_captured(index.spec(), &input, work.allocator.clone(), &|| Ok(()))
            .unwrap();
    index
        .probe(&prepared, 0..len, work)
        .unwrap()
        .matches()
        .to_vec()
}

fn found(value: BulkMatch) -> BulkKey {
    match value {
        BulkMatch::Found(key) => key,
        other => panic!("expected a content key, found {other:?}"),
    }
}

#[test]
fn net_weights_drive_distinct_location_selection_and_tail_counts() {
    let (owner, tracker) = work_owner();
    let work = ports(owner.as_ref(), tracker);
    let index = build_index(
        &[Arc::new(Int64Array::from(vec![1, 1, 1, 2, 2, 3, 3, 4, 4]))],
        vec![-1, -1, 1, -1, -1, 1, 1, 1, -1],
        &work,
    );
    let matches = probe(
        &index,
        &[Arc::new(Int64Array::from(vec![1, 2, 4, 5]))],
        &work,
    );
    assert_eq!(matches[3], BulkMatch::Miss);
    let one = found(matches[0]);
    let two = found(matches[1]);
    assert!(index.select_position(one, "file-a", 10).unwrap());
    assert!(!index.select_position(one, "file-a", 11).unwrap());
    assert!(!index.select_position(two, "file-a", 10).unwrap());
    assert!(index.select_position(two, "file-b", 10).unwrap());
    assert!(!index.select_position(two, "file-b", 10).unwrap());
    assert!(
        !index
            .select_position(found(matches[2]), "file-a", 12)
            .unwrap()
    );

    let demand = index.export_demand(0..index.len(), &work).unwrap();
    let mut demand_values = demand.arrays()[0]
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .values()
        .to_vec();
    demand_values.sort_unstable();
    assert_eq!(demand_values, [1, 2]);

    let mut tail = ContentApplyTail::new(0..index.len(), &index).unwrap();
    let mut inserted = Vec::new();
    let mut conversions = ValueConversionContext::default();
    let page_rows = NonZeroUsize::new(1).unwrap();
    while tail.counts().is_none() {
        if let Some(page) = tail
            .next_page(
                &index,
                page_rows,
                &mut next_turn(1),
                &mut conversions,
                &work,
                || Ok(false),
            )
            .unwrap()
        {
            let values = page.arrays()[0]
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            assert_eq!(values.len(), 1);
            inserted.push(values.value(0));
        }
    }
    assert_eq!(inserted, [3, 3]);
    let counts = tail.counts().unwrap();
    assert_eq!(
        (
            counts.requested(),
            counts.matched(),
            counts.insufficient_keys()
        ),
        (3, 2, 1)
    );
    assert_ne!(counts.fingerprint(), 0);
}

#[test]
fn content_index_keeps_signed_zero_and_coalesces_nan_and_null() {
    let (owner, tracker) = work_owner();
    let work = ports(owner.as_ref(), tracker);
    let values: ArrayRef = Arc::new(Float64Array::from(vec![
        Some(0.0),
        Some(-0.0),
        Some(f64::from_bits(0x7ff8_0000_0000_0001)),
        Some(f64::from_bits(0xfff8_0000_0000_0002)),
        None,
        None,
    ]));
    let index = build_index(
        std::slice::from_ref(&values),
        vec![1, -1, 1, -1, -1, -1],
        &work,
    );
    let matches = probe(&index, &[values], &work);
    assert_ne!(matches[0], matches[1]);
    assert_eq!(matches[2], matches[3]);
    assert_eq!(matches[4], matches[5]);
    assert_eq!(index.insertion_weight(found(matches[0])).unwrap(), 1);
    assert_eq!(index.insertion_weight(found(matches[2])).unwrap(), 0);
    assert!(index.select_position(found(matches[1]), "file", 0).unwrap());
    assert!(index.select_position(found(matches[4]), "file", 1).unwrap());
    assert!(index.select_position(found(matches[5]), "file", 2).unwrap());
    assert!(!index.select_position(found(matches[5]), "file", 3).unwrap());
}

#[test]
fn empty_original_input_and_empty_tail_unit_report_zero() {
    let (owner, tracker) = work_owner();
    let work = ports(owner.as_ref(), tracker);
    let index = build_index(
        &[Arc::new(Int64Array::from(Vec::<i64>::new()))],
        vec![],
        &work,
    );
    assert_eq!(index.len(), 0);
    let tail = ContentApplyTail::new(0..0, &index).unwrap();
    assert_eq!(tail.counts(), Some(DeleteVerificationCounts::ZERO));
}

#[test]
fn net_weight_overflow_is_a_failure() {
    assert!(add_weight(i64::MAX, 1).is_err());
    assert!(add_weight(i64::MIN, -1).is_err());
}

mod graph;
