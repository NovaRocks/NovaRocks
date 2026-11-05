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

//! One complete Direct compiled-pipeline execution. The original checked
//! Scan/aggregate fixture, connector stream and separate producer/consumer
//! ports remain the authors. The artifact is local literal membership only;
//! this does not attest Worker materialization, Hosted preparation or Native.
use super::*;
use crate::exec::operators::{ResultSinkBuffer, ResultSinkFactory};
use crate::exec::pipeline::binding::{ExchangeBindings, ScanBindings};
use crate::exec::pipeline::executor::prepare_compiled_program_pipeline_execution_with_profiler;
use crate::runtime::task::CompiledWriterBindings;
use std::time::Duration;

struct BothPorts {
    producer: Arc<super::super::super::Session>,
    consumer: Arc<ScanSession>,
}
impl rf::RuntimeFilterSession for BothPorts {
    fn open_producer(
        &self,
        request: rf::RuntimeFilterProducerOpenRequest,
    ) -> Result<
        rf::RuntimeFilterBindOutcome<rf::SharedRuntimeFilterProducer>,
        rf::RuntimeFilterContractViolation,
    > {
        rf::RuntimeFilterSession::open_producer(self.producer.as_ref(), request)
    }
    fn subscribe(
        &self,
        request: rf::RuntimeFilterSubscriptionRequest,
    ) -> Result<
        rf::RuntimeFilterBindOutcome<rf::RuntimeFilterSubscriptionPin>,
        rf::RuntimeFilterContractViolation,
    > {
        rf::RuntimeFilterSession::subscribe(self.consumer.as_ref(), request)
    }
    fn open_final_domain_completion(
        &self,
        _: rf::RuntimeFilterFinalDomainOpenRequest,
    ) -> Result<
        rf::RuntimeFilterBindOutcome<rf::SharedRuntimeFilterFinalDomainCompletion>,
        rf::RuntimeFilterContractViolation,
    > {
        panic!("this checked ordered-bound fixture has no FinalDomain route")
    }
}

#[test]
fn compiled_ordered_pipeline_direct_checked_scan_runs_the_actual_two_partition_chain() {
    let consumer = Consumer::new(Bind::Live);
    consumer.tracker.install_limit_once(64 << 20).unwrap();
    let tracker = consumer.tracker.clone();
    let producer = super::super::super::session(&consumer.fixture);
    let session = Arc::new(BothPorts {
        producer: producer.clone(),
        consumer: consumer.session.clone(),
    });
    let query = LiteralQuery::new();
    consumer.script([rf::LivePollOutcome::Updated {
        snapshot: consumer.snapshot(1, query.clone()),
        terminal: Some(rf::LiveTerminal::Completed),
    }]);
    let runtime = Arc::new(
        consumer
            .state
            .clone()
            .with_runtime_filter_session(Some(session.clone())),
    );
    let program = consumer.fixture.program.clone();
    let frozen_dop = program.graph().profile().pipeline_dop().get();
    assert_eq!(
        frozen_dop, 2,
        "the original LocalCompiler profile is authoritative"
    );
    let dop = i32::try_from(frozen_dop).unwrap();
    let addresses = program.scan_inputs().iter().collect::<Vec<_>>();
    assert_eq!(addresses.len(), 1);
    let (&local_scan, input) = addresses[0];
    assert!(matches!(
        program.graph().nodes()[local_scan.index()].kind(),
        ProgramNodeKind::Scan { .. }
    ));
    let scan_address = i32::try_from(input.scan_node).unwrap();
    let scan = crate::runtime::task::scan::compiled_fixture::FixtureScanOp::new(
        vec![consumer.fixture.input.clone()],
        false,
    );
    let mut scans = ScanBindings::default();
    scans.insert(scan_address, scan.clone());
    let output = ResultSinkBuffer::new();
    let prepared = prepare_compiled_program_pipeline_execution_with_profiler(
        program.clone(),
        Duration::from_millis(10),
        Box::new(ResultSinkFactory::new(output.clone())),
        ExchangeBindings::default(),
        scans,
        CompiledWriterBindings::default(),
        None,
        None,
        dop,
        runtime.clone(),
        consumer.effects.clone(),
    )
    .unwrap_or_else(|cause| {
        panic!("actual Direct prepare refused the checked ordered graph: {cause:?}")
    });
    let drivers = prepared.driver_count();
    assert!(drivers > 0);
    assert_eq!(prepared.submitted_driver_count(), 0);
    let running = prepared.start();
    assert_eq!(running.submitted_driver_count(), drivers);
    running.join().unwrap();
    assert!(
        running.stopped_fact().is_some(),
        "join observes actual exit of every submitted driver"
    );
    assert_eq!(scan.claims(), 1, "one actual source stream was claimed");
    assert_eq!(query.calls.load(Ordering::SeqCst), 8);
    let subscribed = consumer.session.requests.lock().unwrap();
    assert_eq!(subscribed.len(), 1);
    assert_eq!(subscribed[0].contract(), &consumer.session.expected);
    drop(subscribed);
    let opened = producer.opened.lock().unwrap();
    assert_eq!(
        opened.len(),
        2,
        "both actual Partial drivers opened their original producer port"
    );
    assert!(opened.iter().all(|r| r.contract() == &producer.expected
        && r.local_partition_count() == u32::try_from(frozen_dop).unwrap()));
    drop(opened);
    let events = producer.recorder.events.lock().unwrap();
    for partition in 0..u32::try_from(frozen_dop).unwrap() {
        assert_eq!(events.iter().filter(|event| matches!(event, super::super::super::Event::Close(p, _) if *p == partition)).count(), 1, "each actual local partition closes once, including an empty scheduled partition");
    }
    assert!(
        !events
            .iter()
            .any(|event| matches!(event, super::super::super::Event::Fail(_)))
    );
    drop(events);
    let effects = consumer.effects.0.lock().unwrap();
    assert_eq!(effects.len(), 1);
    assert_eq!(
        effects[0].binding_id(),
        consumer.session.expected.binding_id()
    );
    assert_eq!((effects[0].input_rows(), effects[0].output_rows()), (8, 4));
    drop(effects);
    let chunks = output.take_chunks();
    assert!(!chunks.is_empty());
    let declared = program.graph().nodes()[program.graph().root().index()]
        .output_layout()
        .schema();
    let mut actual = Vec::new();
    for chunk in &chunks {
        assert!(chunk.chunk_schema().is_typed());
        assert_eq!(chunk.record_batch().schema().as_ref(), declared.as_ref());
        assert_eq!(
            chunk
                .columns()
                .iter()
                .map(|column| column.data_type().clone())
                .collect::<Vec<_>>(),
            vec![
                DataType::Utf8,
                DataType::Int64,
                DataType::Int64,
                DataType::Int64,
                DataType::Int64
            ]
        );
        let keys = chunk.columns()[0]
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let values = chunk.columns()[1..]
            .iter()
            .map(|column| column.as_any().downcast_ref::<Int64Array>().unwrap())
            .collect::<Vec<_>>();
        for row in 0..chunk.len() {
            assert!(values.iter().all(|value| !value.is_null(row)));
            actual.push((
                (!keys.is_null(row)).then(|| keys.value(row).to_owned()),
                values[0].value(row),
                values[1].value(row),
                values[2].value(row),
                values[3].value(row),
            ));
        }
    }
    // Exact declared ASC/NULL-first final ordering, not a sorted test result.
    assert_eq!(
        actual,
        vec![(None, 2, 9, 2, 7), (Some("a".into()), 2, 11, 1, 10)]
    );
    let last = chunks
        .iter()
        .find(|chunk| !chunk.is_empty())
        .unwrap()
        .clone();
    drop(chunks);
    drop(output);
    drop(running);
    drop(scan);
    drop(program);
    drop(runtime);
    drop(session);
    drop(producer);
    drop(consumer);
    assert!(
        tracker.current() > 0,
        "a real result Chunk alias retains its original output construction stock"
    );
    std::thread::spawn(move || drop(last)).join().unwrap();
    assert_eq!(tracker.current(), 0);
}

#[path = "compiled_complete_ordered_tests.rs"]
mod complete_tests;
