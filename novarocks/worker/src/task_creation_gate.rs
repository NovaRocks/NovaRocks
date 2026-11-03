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

use novarocks_execution_contract::task_execution::identity::ParticipationRef;

/// Adapter-owned gate that can hold task creation for one exact context.
///
/// Normal Worker operation uses [`NoopTaskCreationGate`]. A test adapter may
/// expose a real process-lifecycle rendezvous without making the Worker depend
/// on that adapter's fault model or transport implementation.
pub trait TaskCreationGate: Send + Sync {
    fn holds_task_creation(&self, participation: ParticipationRef) -> bool;

    fn wait_for_task_creation_release(&self, participation: ParticipationRef);
}

/// The production-neutral Worker default: no external gate holds admission.
pub struct NoopTaskCreationGate;

impl TaskCreationGate for NoopTaskCreationGate {
    fn holds_task_creation(&self, _participation: ParticipationRef) -> bool {
        false
    }

    fn wait_for_task_creation_release(&self, _participation: ParticipationRef) {}
}
