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

//! Actual hash exchange over prepared grouped partials, with exact final identity.
use super::*;
use crate::exec::operators::local_exchanger::{LocalExchangePartitionSpec, LocalExchanger};

fn run_optional(
    factory: &CompiledAggregateProcessorFactory,
    inputs: &[Chunk],
    tracker: Arc<MemTracker>,
) -> Option<Chunk> {
    run_optional_dop(factory, inputs, tracker, 2)
}
fn run_optional_dop(
    factory: &CompiledAggregateProcessorFactory,
    inputs: &[Chunk],
    tracker: Arc<MemTracker>,
    dop: i32,
) -> Option<Chunk> {
    let state = RuntimeState::default();
    let mut operator = factory.create(dop, 0);
    operator.set_construction_factory(aggregate_fixture::runtime_construction_factory(
        tracker.clone(),
        factory.error.clone(),
    ));
    operator.set_mem_tracker(tracker);
    operator.prepare().expect("prepare actual aggregate schema");
    let mut processor =
        crate::exec::pipeline::test_driver::TestDriver::new(operator.as_mut(), &state);
    for input in inputs {
        processor.input_without_output(input.clone()).unwrap();
    }
    processor.signal_eos().unwrap();
    let output = processor
        .advance_to_boundary()
        .map(|step| step.output)
        .unwrap();
    assert!(
        processor
            .advance_to_boundary()
            .map(|step| step.output)
            .unwrap()
            .is_none()
    );
    output
}
#[test]
fn ds_hll_grouped_local_after_hash_slots_real_identity_native_and_nulls() {
    use novarocks_functions::AggregateKernelPhase as P;
    for (length, nullable) in [(100_000, false), (321, true), (0, true)] {
        let tracker = MemTracker::new_root("DsHllGroupedAfter");
        {
            let reference = grouped_program(true, nullable);
            let reference_input = grouped_input(&reference, length, nullable);
            let refpartial = factory(&reference, P::Partial);
            let split = length / 2;
            let source_parts = [
                Chunk::new_like(
                    reference_input.record_batch().slice(0, split),
                    &reference_input,
                ),
                Chunk::new_like(
                    reference_input.record_batch().slice(split, length - split),
                    &reference_input,
                ),
            ];
            let references = source_parts
                .iter()
                .filter_map(|chunk| {
                    run_optional(&refpartial, std::slice::from_ref(chunk), tracker.clone())
                })
                .collect::<Vec<_>>();
            let expected =
                run_optional(&factory(&reference, P::Final), &references, tracker.clone())
                    .map(|chunk| estimates(&chunk))
                    .unwrap_or_default();
            let program = grouped_program(false, nullable);
            let source = grouped_input(&program, length, nullable);
            let single = factory(&program, P::Single);
            assert!(single.requires_local_update_stages());
            let source_contract = single.calls[0].contract.clone();
            let output = prepared_schema(&single, tracker.clone());
            let partition_slots = single.local_group_partition_slots();
            assert_eq!(partition_slots, output.slot_ids()[..1]);
            let (partial, final_stage) = single.into_local_update_stages().unwrap();
            assert_eq!(partial.groups, 1);
            assert_eq!(final_stage.groups, 1);
            assert_eq!(
                prepared_schema(&partial, tracker.clone())
                    .arrow_schema_ref()
                    .field(0),
                output.arrow_schema_ref().field(0)
            );
            assert_eq!(
                prepared_schema(&partial, tracker.clone()).slots()[1].data_type(),
                &DataType::Binary
            );
            assert_eq!(prepared_schema(&final_stage, tracker.clone()), output);
            assert_eq!(final_stage.calls[0].first_root, 1);
            assert_eq!(partial.calls[0].first_root, 1);
            assert_eq!(partial.calls[0].roots, 3);
            assert_eq!(partial.calls[0].contract.phase(), P::Partial);
            assert_eq!(final_stage.calls[0].contract.phase(), P::Final);
            assert!(Arc::ptr_eq(
                partial.calls[0].contract.call(),
                source_contract.call()
            ));
            assert!(Arc::ptr_eq(
                final_stage.calls[0].contract.call(),
                source_contract.call()
            ));
            assert!(final_stage.sites.is_empty());
            // The actual exchange consumes the already published group slots;
            // no legacy expression arena participates in routing.
            let exchange = LocalExchanger::new(
                2,
                2,
                LocalExchangePartitionSpec::InputSlotIds(partition_slots),
            );
            let state = RuntimeState::for_local_execution(
                None,
                None,
                None,
                None,
                None,
                Some(tracker.clone()),
                None,
            );
            let queue_tracker = exchange.queue_mem_tracker(&state);
            for driver in 0..2 {
                let offset = if driver == 0 { 0 } else { split };
                let count = if driver == 0 { split } else { length - split };
                if let Some(part) = run_optional(
                    &partial,
                    &[Chunk::new_like(
                        source.record_batch().slice(offset, count),
                        &source,
                    )],
                    tracker.clone(),
                ) {
                    assert_eq!(
                        part.chunk_schema(),
                        prepared_schema(&partial, tracker.clone()).as_ref()
                    );
                    exchange.accept(queue_tracker.as_ref(), part).unwrap();
                }
                assert_eq!(exchange.finish_producer(), driver == 1);
            }
            let mut actual = BTreeMap::new();
            for consumer in 0..2 {
                let mut inputs = Vec::new();
                while let Some(chunk) = exchange.pop_chunk(consumer) {
                    inputs.push(chunk);
                }
                if let Some(out) = run_optional(&final_stage, &inputs, tracker.clone()) {
                    for (key, value) in estimates(&out) {
                        assert!(
                            actual.insert(key, value).is_none(),
                            "each key has exactly one real hash owner"
                        );
                    }
                }
                assert!(exchange.is_done(consumer));
                exchange.close_consumer(consumer);
            }
            assert_eq!(
                actual, expected,
                "actual local stages equal genuine published Partial/Final for the same driver inputs"
            );
        }
        assert_eq!(
            tracker.current(),
            0,
            "states, pending output and exchange queue release actual tracker backing"
        );
    }
}
#[test]
fn ds_hll_grouped_local_after_original_dop_one_raw_unchanged() {
    use novarocks_functions::AggregateKernelPhase as P;
    let tracker = MemTracker::new_root("DsHllGroupedDopOne");
    {
        let program = grouped_program(false, false);
        let single = factory(&program, P::Single);
        let source = grouped_input(&program, 100_000, false);
        let left =
            run_optional_dop(&single, std::slice::from_ref(&source), tracker.clone(), 1).unwrap();
        let right = run(&single, &[source], tracker.clone());
        assert_eq!(estimates(&left), estimates(&right));
        assert_eq!(
            left.chunk_schema(),
            prepared_schema(&single, tracker.clone()).as_ref()
        );
    }
    assert_eq!(tracker.current(), 0);
}

#[test]
fn ds_hll_grouped_local_after_actual_partial_final_cancel_latch_drop() {
    use novarocks_functions::AggregateKernelPhase as P;
    for final_phase in [false, true] {
        let tracker = MemTracker::new_root("DsHllGroupedStop");
        {
            let program = grouped_program(false, true);
            let source = grouped_input(&program, 321, true);
            let (partial, final_stage) = factory(&program, P::Single)
                .into_local_update_stages()
                .unwrap();
            let partial_output = run(&partial, std::slice::from_ref(&source), tracker.clone());
            let phase = if final_phase { &final_stage } else { &partial };
            let first = if final_phase { partial_output } else { source };
            let _state = RuntimeState::default();
            let mut operator = phase.create(2, 0);
            operator.set_construction_factory(aggregate_fixture::runtime_construction_factory(
                tracker.clone(),
                phase.error.clone(),
            ));
            operator.set_mem_tracker(tracker.clone());
            operator.prepare().expect("prepare actual aggregate schema");
            let creator_bytes = tracker.current();
            let mut processor = crate::exec::pipeline::test_driver::TestDriver::with_control(
                operator.as_mut(),
                phase.error.clone(),
            );
            processor.input_without_output(first.clone()).unwrap();
            assert!(tracker.current() > 0);
            let originating =
                crate::runtime::task::ExecutionFailure::from(KernelFailure::ResourceExhausted);
            phase.error.set_error(originating.clone());
            let primary = processor.input_without_output(first.clone()).unwrap_err();
            assert!(primary.same_occurrence(&originating));
            assert_eq!(primary.site(), originating.site());
            // The local driver preserves its already known originating cause.
            assert!(phase.error.error().unwrap().same_occurrence(&originating));
            assert_eq!(tracker.current(), creator_bytes);
            assert_eq!(processor.drive().unwrap_err(), primary);
            assert_eq!(processor.input_without_output(first).unwrap_err(), primary);
            assert_eq!(processor.signal_eos().unwrap_err(), primary);
            assert_eq!(
                processor
                    .advance_to_boundary()
                    .map(|step| step.output)
                    .unwrap_err(),
                primary
            );
        }
        assert_eq!(tracker.current(), 0);
    }
}
