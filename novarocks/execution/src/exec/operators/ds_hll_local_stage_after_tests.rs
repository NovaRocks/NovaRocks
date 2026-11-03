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

//! Actual local lifecycle of one Single call; the original numerical oracle
//! was measured by the unchanged before probes, not invented from the owner.
use super::*;
#[test]
fn ds_hll_local_stage_after_real_factory_100k_and_exact_identity() {
    use novarocks_functions::AggregateKernelPhase as P;
    let tracker = MemTracker::new_root("DsHllLocalStageAfter");
    {
        let program = program(false);
        let source = input(&program);
        let single = factory(&program, P::Single);
        assert!(single.requires_local_update_stages());
        let original = single.calls[0].contract.clone();
        let (partial, final_stage) = single.into_local_update_stages().unwrap();
        assert!(!partial.completes_groups());
        assert!(final_stage.completes_groups());
        assert_eq!(partial.calls[0].contract.phase(), P::Partial);
        assert_eq!(final_stage.calls[0].contract.phase(), P::Final);
        assert!(Arc::ptr_eq(
            partial.calls[0].contract.call(),
            original.call()
        ));
        assert!(Arc::ptr_eq(
            final_stage.calls[0].contract.call(),
            original.call()
        ));
        assert_eq!(
            prepared_schema(&partial, tracker.clone()).slots()[0].data_type(),
            &DataType::Binary
        );
        assert_eq!(
            prepared_schema(&final_stage, tracker.clone()),
            prepared_schema(&factory(&program, P::Single), tracker.clone())
        );
        assert!(
            final_stage.sites.is_empty(),
            "no fabricated expression root"
        );
        let left = run(
            &partial,
            &[Chunk::new_like(
                source.record_batch().slice(0, 50_000),
                &source,
            )],
            tracker.clone(),
        );
        let right = run(
            &partial,
            &[Chunk::new_like(
                source.record_batch().slice(50_000, 50_000),
                &source,
            )],
            tracker.clone(),
        );
        for payload in [&left, &right] {
            let bytes = payload
                .record_batch()
                .column(0)
                .as_any()
                .downcast_ref::<BinaryArray>()
                .unwrap()
                .value(0);
            assert_eq!(bytes[3], 17);
            assert_eq!(bytes[5], 0, "original raw partial HIP flag");
            assert_eq!(
                payload.chunk_schema(),
                prepared_schema(&partial, tracker.clone()).as_ref()
            );
        }
        for reverse in [false, true] {
            let inputs = if reverse {
                [right.clone(), left.clone()]
            } else {
                [left.clone(), right.clone()]
            };
            let result = run(&final_stage, &inputs, tracker.clone());
            assert_eq!(
                result
                    .record_batch()
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .value(0),
                100_091
            );
            assert_eq!(
                result.chunk_schema(),
                prepared_schema(&final_stage, tracker.clone()).as_ref()
            );
        }
    }
    assert_eq!(tracker.current(), 0);
}

#[test]
fn ds_hll_local_stage_after_dop_one_original_raw_and_empty_two_driver_lifecycle() {
    use novarocks_functions::AggregateKernelPhase as P;
    let tracker = MemTracker::new_root("DsHllLocalStageSingleAndEmpty");
    {
        let program = program(false);
        let source = input(&program);
        let single = factory(&program, P::Single);
        let state = RuntimeState::default();
        let mut operator = single.create(1, 0);
        operator.set_construction_factory(aggregate_fixture::runtime_construction_factory(
            tracker.clone(),
            single.error.clone(),
        ));
        operator.set_mem_tracker(tracker.clone());
        operator.prepare().expect("prepare actual aggregate schema");
        let mut processor =
            crate::exec::pipeline::test_driver::TestDriver::new(operator.as_mut(), &state);
        processor.input_without_output(source).unwrap();
        processor.signal_eos().unwrap();
        let result = processor
            .advance_to_boundary()
            .map(|step| step.output)
            .unwrap()
            .unwrap();
        assert_eq!(
            result
                .record_batch()
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0),
            100_093
        );
        drop(processor);
        drop(operator);
        assert!(
            tracker.current() > 0,
            "live output retains its actual creator"
        );
        drop(result);
        assert_eq!(tracker.current(), 0);
        let (partial, final_stage) = single.into_local_update_stages().unwrap();
        let left = run(&partial, &[], tracker.clone());
        let right = run(&partial, &[], tracker.clone());
        assert_eq!(left.len(), 1);
        assert_eq!(right.len(), 1);
        let result = run(&final_stage, &[left, right], tracker.clone());
        assert_eq!(result.len(), 1);
        assert_eq!(
            result
                .record_batch()
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0),
            0
        );
    }
    assert_eq!(tracker.current(), 0);
}
#[test]
fn ds_hll_local_stage_after_actual_partial_and_final_cancel_first_cause_no_replay() {
    use novarocks_functions::AggregateKernelPhase as P;
    let tracker = MemTracker::new_root("DsHllLocalStageNoReplay");
    for final_phase in [false, true] {
        let program = program(false);
        let source = input(&program);
        let (partial, final_stage) = factory(&program, P::Single)
            .into_local_update_stages()
            .unwrap();
        let source = Chunk::new_like(source.record_batch().slice(0, 50_000), &source);
        let partial_output = run(&partial, std::slice::from_ref(&source), tracker.clone());
        let phase = if final_phase { &final_stage } else { &partial };
        let first = if final_phase {
            partial_output.clone()
        } else {
            source.clone()
        };
        let _state = RuntimeState::default();
        let mut operator = phase.create(if final_phase { 1 } else { 2 }, 0);
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
        drop(processor);
        drop(operator);
        drop(partial_output);
        drop(source);
        assert_eq!(tracker.current(), 0);
    }
}
