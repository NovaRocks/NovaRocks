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

//! The original resource finisher and its one settlement continuation are
//! separate admitted work. A joiner receives neither registration authority.

use crate::{SettlementPermit, TaskIdentity, failure::FailureProjection};

/// Implemented by the admission owner over its original prepaid work. These
/// operations update work/exit metadata only; they never invoke a callback or
/// treat cancellation or dropping a bound role as positive exit evidence.
pub trait CompletionWorkCapability: Send {
    fn started(&mut self);
    fn completed(self: Box<Self>);
    /// The unique never-bound holder exited with no callback/captures attached.
    fn unused_exited(self: Box<Self>);
    fn unresolved(self: Box<Self>);
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Phase {
    Unbound,
    Bound,
    Started,
}

/// Non-cloneable authority attached to the original OnceRole. Its completion
/// requires the original call/deferred finish and capture disposal proofs.
#[must_use = "The original finisher keeps work until its actual role exit"]
pub struct CompletionWork {
    identity: Option<TaskIdentity>,
    phase: Phase,
    capability: Option<Box<dyn CompletionWorkCapability>>,
}
impl CompletionWork {
    pub fn from_admitted_role(
        identity: Option<TaskIdentity>,
        capability: Box<dyn CompletionWorkCapability>,
    ) -> Self {
        Self {
            identity,
            phase: Phase::Unbound,
            capability: Some(capability),
        }
    }
    pub const fn identity(&self) -> Option<TaskIdentity> {
        self.identity
    }
    pub fn bind(&mut self) {
        assert_eq!(self.phase, Phase::Unbound, "one original finisher binding");
        self.phase = Phase::Bound;
    }
    pub fn started(&mut self) {
        assert_eq!(self.phase, Phase::Bound, "one original finisher start");
        self.capability
            .as_mut()
            .expect("original finisher work")
            .started();
        self.phase = Phase::Started;
    }
    /// The original owner supplies actual exit, including a bound callback
    /// withdrawn before invocation whose captures have really been disposed.
    pub fn completed(mut self) {
        assert_ne!(
            self.phase,
            Phase::Unbound,
            "unbound role exits through its holder"
        );
        self.capability
            .take()
            .expect("original finisher work")
            .completed();
    }
}
impl Drop for CompletionWork {
    fn drop(&mut self) {
        if let Some(capability) = self.capability.take() {
            match self.phase {
                Phase::Unbound => capability.unused_exited(),
                Phase::Bound | Phase::Started => capability.unresolved(),
            }
        }
    }
}

/// Issued before constructing/adopting a dormant fragment. Moving this pair
/// gives Execution one finisher work role and one settlement registration.
pub struct AdmittedFinisher<C>
where
    C: std::error::Error + FailureProjection + Send + Sync + 'static,
{
    // Declaration order matters for an unused holder: revoke the unique
    // child registration before reporting the never-bound original role exit.
    settlement: SettlementPermit<C>,
    work: CompletionWork,
}
impl<C> AdmittedFinisher<C>
where
    C: std::error::Error + FailureProjection + Send + Sync + 'static,
{
    /// The issuer has already admitted both responsibilities in their common
    /// scope. The work capability belongs to the permit's exact role identity.
    pub fn from_admitted_role(
        settlement: SettlementPermit<C>,
        work: Box<dyn CompletionWorkCapability>,
    ) -> Self {
        let identity = settlement.identity();
        Self {
            settlement,
            work: CompletionWork {
                identity,
                phase: Phase::Unbound,
                capability: Some(work),
            },
        }
    }
    pub const fn identity(&self) -> Option<TaskIdentity> {
        self.work.identity()
    }
    pub fn into_parts(self) -> (SettlementPermit<C>, CompletionWork) {
        (self.settlement, self.work)
    }
}

/// The three original, named completion roles of one admitted fragment. The
/// settlement child belongs only to its resource finisher. Wall time and the
/// terminal publisher cannot manufacture or duplicate that registration.
pub struct AdmittedFragmentRoles<C>
where
    C: std::error::Error + FailureProjection + Send + Sync + 'static,
{
    finisher: AdmittedFinisher<C>,
    wall_time: CompletionWork,
    terminal_publisher: CompletionWork,
}
impl<C> AdmittedFragmentRoles<C>
where
    C: std::error::Error + FailureProjection + Send + Sync + 'static,
{
    pub fn from_admitted_roles(
        finisher: AdmittedFinisher<C>,
        wall_time: Box<dyn CompletionWorkCapability>,
        terminal_publisher: Box<dyn CompletionWorkCapability>,
    ) -> Self {
        let identity = finisher.identity();
        Self {
            finisher,
            wall_time: CompletionWork::from_admitted_role(identity, wall_time),
            terminal_publisher: CompletionWork::from_admitted_role(identity, terminal_publisher),
        }
    }
    pub const fn identity(&self) -> Option<TaskIdentity> {
        self.finisher.identity()
    }
    pub fn into_parts(self) -> (AdmittedFinisher<C>, CompletionWork, CompletionWork) {
        (self.finisher, self.wall_time, self.terminal_publisher)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex};
    struct Work(Arc<Mutex<Vec<&'static str>>>);
    impl CompletionWorkCapability for Work {
        fn started(&mut self) {
            self.0.lock().unwrap().push("started");
        }
        fn completed(self: Box<Self>) {
            self.0.lock().unwrap().push("completed");
        }
        fn unused_exited(self: Box<Self>) {
            self.0.lock().unwrap().push("unused_exited");
        }
        fn unresolved(self: Box<Self>) {
            self.0.lock().unwrap().push("unresolved");
        }
    }
    fn work(events: &Arc<Mutex<Vec<&'static str>>>) -> CompletionWork {
        CompletionWork {
            identity: None,
            phase: Phase::Unbound,
            capability: Some(Box::new(Work(events.clone()))),
        }
    }
    #[test]
    fn dropping_a_bound_or_started_finisher_never_reports_completion() {
        let events = Arc::new(Mutex::new(Vec::new()));
        let mut bound = work(&events);
        bound.bind();
        drop(bound);
        let mut started = work(&events);
        started.bind();
        started.started();
        drop(started);
        assert_eq!(
            *events.lock().unwrap(),
            ["unresolved", "started", "unresolved"]
        );
    }
    #[test]
    fn only_the_original_exit_path_reports_a_completed_bound_role() {
        let events = Arc::new(Mutex::new(Vec::new()));
        drop(work(&events));
        let mut active = work(&events);
        active.bind();
        active.started();
        active.completed();
        assert_eq!(
            *events.lock().unwrap(),
            ["unused_exited", "started", "completed"]
        );
    }

    #[derive(Debug)]
    struct Cause;
    impl std::fmt::Display for Cause {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str("finisher admission probe")
        }
    }
    impl std::error::Error for Cause {}
    impl FailureProjection for Cause {
        fn failure_fact(&self) -> crate::failure::FailureFact {
            crate::failure::FailureFact::new(
                crate::failure::FailureClass::Invariant,
                crate::failure::FailureReason::new(
                    crate::failure::domain::EXECUTION,
                    std::num::NonZeroU16::new(65000).unwrap(),
                ),
            )
        }
    }
    struct Child(Arc<Mutex<Vec<&'static str>>>);
    impl crate::SettlementCapability<Cause> for Child {
        fn register(self: Box<Self>, _: crate::SettledContinuation<Cause>) {
            panic!("unused child authority");
        }
        fn revoke_unused(self: Box<Self>) {
            self.0.lock().unwrap().push("child_revoked");
        }
    }
    #[test]
    fn an_unused_admitted_holder_revokes_child_authority_before_original_role_exit() {
        let events = Arc::new(Mutex::new(Vec::new()));
        let child = SettlementPermit::from_admitted_local_role(Box::new(Child(events.clone())));
        let finisher = AdmittedFinisher::from_admitted_role(child, Box::new(Work(events.clone())));
        assert!(finisher.identity().is_none());
        drop(finisher);
        assert_eq!(*events.lock().unwrap(), ["child_revoked", "unused_exited"]);
    }
}
