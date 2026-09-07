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

// MIGRATION: wired into the round once the typed producer lands.
mod execution;
mod query_registry;
mod scheduler;
#[allow(
    dead_code,
    reason = "Wired into execute_round by the typed producer cut in the same PR."
)]
pub(crate) mod split_assignment_round;
pub(crate) mod task_round;

pub use execution::FrontendDistributedQueryCoordinator;
pub(crate) use query_registry::{
    QueryLifecycleConvergenceErrorSource, QueryLifecycleConvergenceReader,
    QueryLifecycleConvergenceSnapshot, RuntimeFilterTerminalRollupSnapshot,
    RuntimeFilterTerminalRollupUnavailable,
};
pub use scheduler::{FrontendBackendSnapshot, FrontendFragmentScheduler};
