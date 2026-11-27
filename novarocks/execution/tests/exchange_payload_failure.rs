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

//! Exchange owns packet-format refusal through the real std/Arrow writer.
//! These small, deterministic cases require no fixture and retain only their
//! local construction tracker; dropping the factory proves its actual exit.
use arrow::{
    datatypes::Schema,
    record_batch::{RecordBatch, RecordBatchOptions},
};
use novarocks_execution::{
    exec::{
        chunk::{Chunk, ChunkSchema},
        pipeline::operator::input_construction::{
            InputConstructionProfile, try_runtime_input_construction_factory,
        },
    },
    runtime::{
        exchange::encode_chunks_bounded_with_creation, mem_tracker::MemTracker,
        runtime_state::RuntimeErrorState,
    },
};
use novarocks_failure_contract::{FailureClass, FailureProjection, RepresentationBoundary, domain};
use novarocks_functions::{KernelEvaluationControl, KernelFailure};
use novarocks_type_contract::ValueTypeStorageLifetime;
use std::sync::Arc;
use std::time::Duration;
struct Control;
impl KernelEvaluationControl for Control {
    fn checkpoint(&self, _: u32) -> Result<(), KernelFailure> {
        Ok(())
    }
    fn wait(&self, _: Duration) -> Result<(), KernelFailure> {
        panic!("synchronous encoder never waits")
    }
}
#[test]
fn packet_refusal_keeps_exact_facts_across_direct_and_arrow_writes() {
    let schema =
        Arc::new(ChunkSchema::try_new_typed(vec![], ValueTypeStorageLifetime::pure()).unwrap());
    let batch = RecordBatch::try_new_with_options(
        Arc::new(Schema::empty()),
        vec![],
        &RecordBatchOptions::new().with_row_count(Some(3)),
    )
    .unwrap();
    let chunk = Chunk::try_new_with_chunk_schema(batch, schema).unwrap();
    let tracker = MemTracker::new_root("exchange packet test");
    let factory = try_runtime_input_construction_factory(
        InputConstructionProfile {
            max_request_bytes: 4 * 1024 * 1024,
            max_opaque_bytes: 4 * 1024 * 1024,
            max_work: 100000,
            max_depth: 64,
        },
        tracker.clone(),
        Arc::new(RuntimeErrorState::default()),
    )
    .unwrap();
    let baseline = tracker.current();
    let large = encode_chunks_bounded_with_creation(
        std::slice::from_ref(&chunk),
        true,
        65536,
        factory.as_ref(),
        &Control,
    )
    .unwrap();
    let (reference, retained) = large.into_parts();
    assert!(retained >= reference.len());
    assert!(retained <= 65536);
    for cap in [0, 9, 10, 11, reference.len() - 1] {
        let error = encode_chunks_bounded_with_creation(
            std::slice::from_ref(&chunk),
            true,
            cap,
            factory.as_ref(),
            &Control,
        )
        .unwrap_err();
        let fact = error.failure_fact();
        assert_eq!(
            fact.class(),
            FailureClass::Representation,
            "cap={cap}: {error}"
        );
        assert_eq!(fact.reason().domain(), domain::EXCHANGE.get());
        let extent = fact.representation().unwrap();
        assert_eq!(extent.boundary, RepresentationBoundary::SingleOutput);
        assert_eq!(extent.extent_limit, Some(cap as u64));
        assert!(extent.required.unwrap() > cap as u64);
        assert_eq!(extent.representable_prefix, None);
        assert!(fact.resource().is_none());
        assert_eq!(tracker.current(), baseline);
    }
    let exact = encode_chunks_bounded_with_creation(
        std::slice::from_ref(&chunk),
        true,
        reference.len(),
        factory.as_ref(),
        &Control,
    )
    .unwrap();
    assert_eq!(exact.into_parts().0, reference);
    drop(factory);
    assert_eq!(tracker.current(), 0);
}
