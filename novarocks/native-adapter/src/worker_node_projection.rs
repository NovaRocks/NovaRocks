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

//! Pure projection shared by the Node's bounded source and Native delivery.
//! No clock, I/O, opaque getter or captured application owner enters this port.

use novarocks_proto_models::novarocks as proto;
use novarocks_task_codec::operation::{
    CoveredCatchUpComplete, CoveredStatusStreamEvent, CoveredStatusStreamFact,
    NATIVE_GRPC_DECODED_MESSAGE_MAX_BYTES, covered_quiesce_fact_size_bound,
    encode_covered_status_event,
};
use novarocks_worker::{
    NodeCoveredObservationFact as Fact, NodeCoveredObservationFrame as Frame,
    ObservationProjection, ObservationProjectionError,
};
use prost::Message;
use std::num::NonZeroUsize;

pub(crate) fn observation_projection() -> ObservationProjection {
    ObservationProjection {
        fact_bytes,
        max_frame_bytes: NonZeroUsize::new(NATIVE_GRPC_DECODED_MESSAGE_MAX_BYTES)
            .expect("the frozen Native frame limit is positive"),
        // The retained catch-up policy is explicit and independent of a live
        // frame's bound. Source rejects a larger snapshot before completion.
        max_initial_bytes: NonZeroUsize::new(64 * 1024 * 1024)
            .expect("the frozen covered catch-up limit is positive"),
    }
}

fn stream_fact(fact: &Fact) -> CoveredStatusStreamFact {
    match fact {
        Fact::Status(value) => CoveredStatusStreamFact::Status(value.clone()),
        Fact::StatusUnchanged(value) => CoveredStatusStreamFact::StatusUnchanged(*value),
        Fact::Unknown(value) => CoveredStatusStreamFact::Unknown(*value),
        Fact::Gone(value) => CoveredStatusStreamFact::Gone(*value),
        Fact::TaskConvergence(value) => CoveredStatusStreamFact::TaskConvergence(*value),
        Fact::TaskConvergenceUnchanged(value) => {
            CoveredStatusStreamFact::TaskConvergenceUnchanged(*value)
        }
        Fact::ContextConvergence(value) => CoveredStatusStreamFact::ContextConvergence(*value),
        Fact::Quiesce(value) => CoveredStatusStreamFact::Quiesce(value.as_ref().clone()),
    }
}

fn fact_bytes(fact: &Fact) -> Result<usize, ObservationProjectionError> {
    if let Fact::Quiesce(receipt) = fact {
        return covered_quiesce_fact_size_bound(receipt)
            .ok_or(ObservationProjectionError::InvalidFact);
    }
    // Live revision occupies its largest legal varint. This is an explicit
    // conservative cost from the same encoder, not a guessed per-record size.
    encode_covered_status_event(&CoveredStatusStreamEvent {
        fact: stream_fact(fact),
        source_revision: Some(u64::MAX),
    })
    .map(|wire| wire.encoded_len())
    .map_err(|_| ObservationProjectionError::InvalidFact)
}

pub(crate) fn encode_frame(
    frame: &Frame,
    generation: u64,
) -> Result<proto::TaskStatusStreamEvent, tonic::Status> {
    let event = match frame {
        Frame::CatchUp(fact) => CoveredStatusStreamEvent {
            fact: stream_fact(fact),
            source_revision: None,
        },
        Frame::CatchUpComplete { initial_cut } => CoveredStatusStreamEvent {
            fact: CoveredStatusStreamFact::CatchUpComplete(CoveredCatchUpComplete {
                generation,
                initial_cut: *initial_cut,
            }),
            source_revision: None,
        },
        Frame::Live { revision, fact } => CoveredStatusStreamEvent {
            fact: stream_fact(fact),
            source_revision: Some(*revision),
        },
    };
    encode_covered_status_event(&event).map_err(|error| tonic::Status::internal(error.to_string()))
}
