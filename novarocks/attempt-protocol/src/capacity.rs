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

//! The core can request one synchronous queue-stage ledger transaction.
//! Returned identities grant no access or release authority outside the host.

use crate::config::DispatchLane;
use novarocks_types::BackendProcessId;
use std::num::NonZeroU64;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct ReservationToken(pub NonZeroU64);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum QueueStage {
    Granted(ReservationToken),
    TargetFull(BackendProcessId),
    ProcessFull,
}

/// Implementations may only charge the actual actor turn or reserve through
/// the same scope/ledger transaction. No callbacks, waiting, encoding or I/O.
/// The host retains every original step guard until atomic batch handoff.
pub trait StepContext {
    fn charge(&mut self, units: usize) -> bool;
    fn reserve_queue_stage(
        &mut self,
        lane: DispatchLane,
        control_progress: bool,
        target: BackendProcessId,
        records: usize,
        queued_bytes: usize,
    ) -> QueueStage;
}
