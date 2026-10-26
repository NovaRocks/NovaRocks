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

//! Evidence from the live failure owner's scope publication-drain stage.
//! Actor failure alone supplies no authority to abandon Core consumption.

use super::actors::WorkerActor;
use novarocks_control_runtime::driver::{ActorRuntime, DriverError, FailurePublicationSession};
use novarocks_control_runtime_core::identity::ActorId;

/// Only the failure-owner loop in this module can construct this stage after
/// walking its original Task/reply/batch inventories and committing all needed
/// failure projections and reply outboxes. Physical source/callback/send exit
/// and the other report consumers remain independent; waiting for those before
/// this stage could create a cycle with the Core consumption legs.
///
/// There is deliberately no constructor from a failed flag or one Task's
/// accepted snapshot. The loop must retain its original running cleanup work.
pub(crate) struct ScopePublicationsDrained<'a> {
    session: &'a FailurePublicationSession<WorkerActor>,
}
impl ScopePublicationsDrained<'_> {
    pub(crate) fn validate(
        &self,
        runtime: &ActorRuntime<WorkerActor>,
        actor: ActorId,
    ) -> Result<(), DriverError> {
        if self.session.matches_actor_runtime(runtime, actor)? {
            Ok(())
        } else {
            Err(DriverError::PublicationTargetMismatch)
        }
    }
}
