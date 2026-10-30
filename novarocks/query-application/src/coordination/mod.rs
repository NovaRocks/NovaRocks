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

mod actor;
mod actor_state;
mod delivery;
mod establish;
mod native_active_custody;
mod original_exit;
mod plan_activation;
mod recovery;
mod replacement;
mod result_pump;
mod root_relay;
mod runtime_registry;
mod schedule;
mod stand_down;
mod supervisor;

pub(crate) use actor::InternalReadyActorPort;
pub use actor::*;
pub use actor_state::*;
pub use delivery::*;
pub use establish::*;
pub(crate) use native_active_custody::*;
pub use original_exit::{
    LocalExecutionDisposition, LogicalActorExitPhase, LogicalExecutionExitGeometry,
    LogicalExecutionExitObserver, LogicalExecutionExitReceipt, LogicalExecutionExitView,
    RemoteExecutionDisposition,
};
pub(crate) use plan_activation::ActiveLogicalPlan;
pub use recovery::*;
pub use replacement::*;
pub use result_pump::*;
pub use root_relay::*;
pub use runtime_registry::*;
pub use schedule::*;
pub use stand_down::*;
pub use supervisor::*;

/// Enforces a process owner's explicit-shutdown invariant from its `Drop`.
///
/// A process owner that reaches its destructor without an explicit shutdown
/// has leaked the runtime it owns, and that is a defect worth failing on. The
/// invariant is deliberately not enforced while the thread is already
/// unwinding. A panic raised from a destructor during unwinding is not a
/// second reported failure: Rust turns it into a non-unwinding panic and
/// aborts the process, which in a test binary destroys every other test's
/// result and buries the original panic that is the real diagnosis. An owner
/// abandoned by an unwind was never given the chance to shut down, so the leak
/// this would report is a consequence of that panic rather than independent
/// evidence of a leak.
pub(crate) fn assert_shutdown_complete(shutdown_complete: bool, owner: &str) {
    assert!(
        shutdown_complete || std::thread::panicking(),
        "{owner} dropped without explicit shutdown"
    );
}
