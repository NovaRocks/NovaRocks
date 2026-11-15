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

//! Independent checks over the original runtime's bounded history.
//!
//! Provenance: CR history.rs:33-35 distinguishes lossless capture from finished
//! execution; FA control_runtime/host.rs:3335-3375 checks exact original Work
//! ordinals and captures-before-result consumption; CR tests
//! exit_custody_lifecycle.rs:236-274 distinguishes unused actual exit/revoked
//! report from invented start/consumers; deterministic_actor_turn.rs:419-447
//! requires actual child Scope removal before its parent can retire;
//! failure_publication.rs:706-745 checks once-only actual input abandonment.
//! No Terminal/status observation is a physical exit fact. UNKNOWN is explicit
//! and never makes the completed-runtime checker succeed.

use crate::control_runtime::actor::FrontendActor;
use novarocks_control_runtime::{
    actor_turn::ActorRuntime,
    history::{HistoryCapture, HistorySlice},
    transaction::RuntimeLedger,
};
use novarocks_control_runtime_core::{
    history::{EffectHistoryPhase, HistoryActorTarget, HistoryEntry, HistoryFact},
    identity::{ActionId, Generation, InputId, ScopeId, ScopedWorkRef, SlotId},
};
use std::fmt;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct HistoryViolation {
    pub(crate) rule: &'static str,
    pub(crate) detail: String,
}
impl fmt::Display for HistoryViolation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.rule, self.detail)
    }
}
impl std::error::Error for HistoryViolation {}
fn violation(rule: &'static str, detail: impl fmt::Debug) -> HistoryViolation {
    HistoryViolation {
        rule,
        detail: format!("{detail:?}"),
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum WorkExpectation {
    Retired,
    UnusedRetired,
    UnboundRolledBack,
    Unknown,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum WorkObservation {
    Retired,
    UnusedRetired,
    UnboundRolledBack,
    Unknown {
        started: bool,
        body_exited: bool,
        captures_exited: bool,
    },
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum InputDisposition {
    Consumed,
    Abandoned,
    Unknown,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum EffectExpectation {
    Completed,
    PublishedThenCompleted,
    Unknown,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct EffectIdentity {
    pub(crate) target: HistoryActorTarget,
    pub(crate) input: InputId,
    pub(crate) action: ActionId,
    pub(crate) actor_generation: Generation,
    pub(crate) financial_work: ScopedWorkRef,
    pub(crate) author_generation: Generation,
}

/// One lossless capture prefix from ONE actual runtime/ledger namespace.
/// `slice.complete` only reports no lost facts. It never proves work finished.
#[derive(Clone)]
pub(crate) struct CapturedHistory {
    slice: HistorySlice,
}
impl CapturedHistory {
    pub(crate) fn drain(original: &HistoryCapture, budget: usize) -> Self {
        Self {
            slice: original.drain(budget),
        }
    }
    /// Append only the next drain of the SAME original capture. Ordinal
    /// validation below detects missing/repeated chunks; no facts are rebuilt.
    pub(crate) fn append_from(&mut self, original: &HistoryCapture, budget: usize) {
        let next = original.drain(budget);
        self.slice.entries.extend(next.entries);
        self.slice.complete &= next.complete;
        self.slice.error = self.slice.error.or(next.error);
        self.slice.remaining = next.remaining;
    }
    pub(crate) fn entries(&self) -> &[HistoryEntry] {
        &self.slice.entries
    }
    pub(crate) fn check_capture(&self) -> Result<(), HistoryViolation> {
        if !self.slice.complete || self.slice.error.is_some() {
            return Err(violation("capture lost facts", &self.slice));
        }
        if self.slice.remaining != 0 {
            return Err(violation("capture not drained", self.slice.remaining));
        }
        let mut previous: Option<(u64, novarocks_control_runtime_core::identity::Tick)> = None;
        for entry in self.entries() {
            let expected = match previous {
                None => 1,
                Some((ordinal, _)) => ordinal
                    .checked_add(1)
                    .ok_or_else(|| violation("original ordinal exhausted", entry))?,
            };
            if entry.ordinal != expected {
                return Err(violation("original contiguous ordinals", entry));
            }
            if previous.is_some_and(|(_, tick)| tick > entry.now) {
                return Err(violation("original monotonic observation time", entry));
            }
            previous = Some((entry.ordinal, entry.now));
        }
        Ok(())
    }
    fn once(
        &self,
        label: &'static str,
        mut matches: impl FnMut(&HistoryFact) -> bool,
    ) -> Result<Option<u64>, HistoryViolation> {
        let mut found = self.entries().iter().filter(|entry| matches(&entry.fact));
        let first = found.next().map(|entry| entry.ordinal);
        if let Some(duplicate) = found.next() {
            return Err(violation(label, ("duplicate original fact", duplicate)));
        }
        Ok(first)
    }
    fn required(&self, label: &'static str, value: Option<u64>) -> Result<u64, HistoryViolation> {
        value.ok_or_else(|| violation(label, "missing original fact"))
    }
    fn before(&self, label: &'static str, first: u64, second: u64) -> Result<(), HistoryViolation> {
        if first >= second {
            return Err(violation(label, (first, second)));
        }
        Ok(())
    }
    fn work_ordinals(
        &self,
        reference: ScopedWorkRef,
    ) -> Result<[Option<u64>; 4], HistoryViolation> {
        let ScopedWorkRef { scope, work } = reference;
        Ok([
            self.once("one original Work start", |fact| {
                *fact == HistoryFact::Started { scope, work }
            })?,
            self.once("one original body exit", |fact| {
                *fact == HistoryFact::BodyExited { scope, work }
            })?,
            self.once("one original captures exit", |fact| {
                *fact == HistoryFact::CapturesExited { scope, work }
            })?,
            self.once("one original Work retirement", |fact| {
                *fact == HistoryFact::WorkRetired { scope, work }
            })?,
        ])
    }
    pub(crate) fn check_work(
        &self,
        reference: ScopedWorkRef,
        expected: WorkExpectation,
    ) -> Result<WorkObservation, HistoryViolation> {
        self.check_capture()?;
        let ScopedWorkRef { scope, work } = reference;
        let declared = self.once("one original Work declaration", |fact| {
            matches!(fact,
            HistoryFact::WorkAdmitted { scope: s, work: w, .. } if *s == scope && *w == work)
        })?;
        // An intrinsic child root may be introduced directly by Admitted.
        // Additional reservations on one Work are not new Work declarations.
        let admitted = self.entries().iter().find_map(|entry| match entry.fact {
            HistoryFact::Admitted {
                scope: s, work: w, ..
            } if s == scope && w == work => Some(entry.ordinal),
            _ => None,
        });
        let admission = declared
            .or(admitted)
            .ok_or_else(|| violation("original Work admission", reference))?;
        let [started, body, captures, retired] = self.work_ordinals(reference)?;
        let rollback = self.once("one original unbound rollback", |fact| matches!(fact,
            HistoryFact::UnboundReservationRolledBack { scope: s, work: w, .. } if *s == scope && *w == work))?;
        if expected == WorkExpectation::UnboundRolledBack {
            let rollback = self.required("actual unbound rollback", rollback)?;
            self.before(
                "admission before actual unbound rollback",
                admission,
                rollback,
            )?;
            if [started, body, captures, retired]
                .iter()
                .any(Option::is_some)
            {
                return Err(violation(
                    "unbound rollback has no invented bound lifecycle",
                    reference,
                ));
            }
            let rolled_back = self
                .entries()
                .iter()
                .find_map(|entry| match entry.fact {
                    HistoryFact::UnboundReservationRolledBack { reservation, .. }
                        if entry.ordinal == rollback =>
                    {
                        Some(reservation)
                    }
                    _ => None,
                })
                .expect("required rollback fact");
            let reservation_matches = self.entries().iter().any(|entry| matches!(entry.fact,
                HistoryFact::Admitted { scope: s, work: w, reservation, .. } if s == scope && w == work && reservation == rolled_back && entry.ordinal < rollback));
            if !reservation_matches {
                return Err(violation(
                    "rollback needs exact original reservation",
                    reference,
                ));
            }
            return Ok(WorkObservation::UnboundRolledBack);
        }
        if rollback.is_some() {
            return Err(violation(
                "rollback is not bound exit or UNKNOWN",
                reference,
            ));
        }
        for ordinal in [started, body, captures, retired].into_iter().flatten() {
            self.before("admission before original lifecycle", admission, ordinal)?;
        }
        if let Some(body) = body
            && let Some(started) = started
        {
            self.before("Started before BodyExited", started, body)?;
        }
        if let Some(captures) = captures {
            self.before(
                "BodyExited before CapturesExited",
                self.required("original body before captures", body)?,
                captures,
            )?;
        }
        if let Some(retired) = retired {
            self.before(
                "CapturesExited before WorkRetired",
                self.required("actual captures before retirement", captures)?,
                retired,
            )?;
        }
        match expected {
            WorkExpectation::Unknown => {
                if retired.is_some() {
                    return Err(violation("UNKNOWN cannot claim retirement", reference));
                }
                Ok(WorkObservation::Unknown {
                    started: started.is_some(),
                    body_exited: body.is_some(),
                    captures_exited: captures.is_some(),
                })
            }
            WorkExpectation::UnboundRolledBack => unreachable!("handled above"),
            WorkExpectation::Retired | WorkExpectation::UnusedRetired => {
                self.required("actual original body exit", body)?;
                self.required("actual original captures exit", captures)?;
                self.required("actual original retirement", retired)?;
                if expected == WorkExpectation::Retired {
                    self.required("actual original Started", started)?;
                    Ok(WorkObservation::Retired)
                } else {
                    if started.is_some() {
                        return Err(violation("unused original cannot have Started", reference));
                    }
                    Ok(WorkObservation::UnusedRetired)
                }
            }
        }
    }
    pub(crate) fn check_input(
        &self,
        target: HistoryActorTarget,
        input: InputId,
        expected: InputDisposition,
    ) -> Result<(), HistoryViolation> {
        self.check_capture()?;
        let consumed = self.once("one original input consumption", |fact| {
            *fact == HistoryFact::InputConsumed { target, input }
        })?;
        let abandoned = self.once("one original input abandonment", |fact| {
            *fact == HistoryFact::InputAbandoned { target, input }
        })?;
        match expected {
            InputDisposition::Consumed if consumed.is_some() && abandoned.is_none() => Ok(()),
            InputDisposition::Abandoned if consumed.is_none() && abandoned.is_some() => Ok(()),
            InputDisposition::Unknown if consumed.is_none() && abandoned.is_none() => Ok(()),
            _ => Err(violation(
                "exact original input disposition",
                (target, input, consumed, abandoned, expected),
            )),
        }
    }
    /// Applicable only to an original causal producer whose result is supplied
    /// after its captures exit (FA host.rs:3335-3375). NOT a global LP/AP rule:
    /// standing AP facts and LP ActionResult may precede their producer exit.
    pub(crate) fn check_result_after_captures(
        &self,
        reference: ScopedWorkRef,
        target: HistoryActorTarget,
        input: InputId,
    ) -> Result<(), HistoryViolation> {
        self.check_capture()?;
        let captures = self.required(
            "result needs original capture exit",
            self.work_ordinals(reference)?[2],
        )?;
        self.check_input(target, input, InputDisposition::Consumed)?;
        let consumed = self.required(
            "original result consumed",
            self.once("one original result", |fact| {
                *fact == HistoryFact::InputConsumed { target, input }
            })?,
        )?;
        self.before(
            "CapturesExited before original result consumption",
            captures,
            consumed,
        )
    }
    pub(crate) fn check_unused_report(
        &self,
        reference: ScopedWorkRef,
        slot: SlotId,
    ) -> Result<(), HistoryViolation> {
        self.check_work(reference, WorkExpectation::UnusedRetired)?;
        let reserved = self.required(
            "original unused report reserved",
            self.once("one original report reservation", |fact| {
                *fact
                    == HistoryFact::SlotReserved {
                        scope: reference.scope,
                        slot,
                    }
            })?,
        )?;
        let retired = self.required(
            "actual unused report retirement",
            self.once("one original report retirement", |fact| {
                *fact == HistoryFact::SlotRetired { slot }
            })?,
        )?;
        self.before(
            "original report reservation before retirement",
            reserved,
            retired,
        )?;
        if self.entries().iter().any(|entry| matches!(entry.fact,
            HistoryFact::SlotFilled { slot: s } | HistoryFact::CompletionConsumed { slot: s, .. } | HistoryFact::SlotAbandoned { slot: s, .. } if s == slot)) {
            return Err(violation("revoked unused report has no invented publication or consumer", slot));
        }
        let captures = self.required(
            "unused report needs actual capture exit",
            self.work_ordinals(reference)?[2],
        )?;
        self.before(
            "actual captures before unused report retirement",
            captures,
            retired,
        )
    }
    pub(crate) fn check_effect(
        &self,
        identity: EffectIdentity,
        expected: EffectExpectation,
    ) -> Result<(), HistoryViolation> {
        self.check_capture()?;
        let mut phases = [None; 3];
        for entry in self.entries() {
            let HistoryFact::Effect {
                target,
                input,
                action,
                actor_generation,
                financial_work,
                author_generation,
                phase,
            } = entry.fact
            else {
                continue;
            };
            if (target, input, action) != (identity.target, identity.input, identity.action) {
                continue;
            }
            if (actor_generation, financial_work, author_generation)
                != (
                    identity.actor_generation,
                    identity.financial_work,
                    identity.author_generation,
                )
            {
                return Err(violation(
                    "same original effect actor and financial identity",
                    entry,
                ));
            }
            let index = match phase {
                EffectHistoryPhase::Dispatched => 0,
                EffectHistoryPhase::Published => 1,
                EffectHistoryPhase::Completed => 2,
            };
            if phases[index].replace(entry.ordinal).is_some() {
                return Err(violation("one original effect phase", entry));
            }
        }
        let dispatch = self.required("original effect dispatch", phases[0])?;
        let committed = self.required(
            "effect needs actual committed original batch",
            self.once("one original batch", |fact| {
                *fact
                    == HistoryFact::BatchCommitted {
                        target: identity.target,
                        actor_generation: identity.actor_generation,
                        input: identity.input,
                    }
            })?,
        )?;
        self.before("original batch commit before dispatch", committed, dispatch)?;
        if let Some(published) = phases[1] {
            self.before("dispatch before original publication", dispatch, published)?;
        }
        if let Some(completed) = phases[2] {
            self.before("dispatch before effect completion", dispatch, completed)?;
            if let Some(published) = phases[1] {
                self.before("publication before effect completion", published, completed)?;
            }
        }
        match expected {
            EffectExpectation::Unknown if phases[2].is_none() => Ok(()),
            EffectExpectation::Unknown => Err(violation(
                "UNKNOWN effect cannot claim completion",
                identity,
            )),
            EffectExpectation::Completed => self
                .required("actual original effect completion", phases[2])
                .map(|_| ()),
            EffectExpectation::PublishedThenCompleted => {
                self.required("actual original effect publication", phases[1])?;
                self.required("actual original effect completion", phases[2])
                    .map(|_| ())
            }
        }
    }
    pub(crate) fn check_scope_removed(&self, scope: ScopeId) -> Result<(), HistoryViolation> {
        self.check_capture()?;
        let opened = self.required(
            "actual original Scope opened",
            self.once(
                "one original Scope open",
                |fact| matches!(fact, HistoryFact::ScopeOpened { scope: s, .. } if *s == scope),
            )?,
        )?;
        let removed = self.required(
            "actual original Scope removed",
            self.once("one original Scope removal", |fact| {
                *fact == HistoryFact::ScopeRemoved { scope }
            })?,
        )?;
        self.before("original Scope open before removal", opened, removed)?;
        for reference in self
            .work_keys()
            .into_iter()
            .filter(|reference| reference.scope == scope)
        {
            let retirement = if let Some(retired) = self.work_ordinals(reference)?[3] {
                retired
            } else {
                self.check_work(reference, WorkExpectation::UnboundRolledBack)?;
                self.required("Scope removal needs actual rollback", self.once("one original rollback before Scope removal", |fact| matches!(fact, HistoryFact::UnboundReservationRolledBack { scope, work, .. } if *scope == reference.scope && *work == reference.work))?)?
            };
            self.before(
                "actual Work retirement before Scope removal",
                retirement,
                removed,
            )?;
        }
        Ok(())
    }
    pub(crate) fn check_child_scope_removed(
        &self,
        child: ScopeId,
        parent: ScopeId,
    ) -> Result<(), HistoryViolation> {
        self.check_scope_removed(child)?;
        self.check_scope_removed(parent)?;
        let link = self
            .entries()
            .iter()
            .find_map(|entry| match entry.fact {
                HistoryFact::ScopeOpened {
                    scope,
                    parent: Some(original),
                    ..
                } if scope == child && original.scope == parent => Some(original),
                _ => None,
            })
            .ok_or_else(|| violation("exact original child-parent Scope link", (child, parent)))?;
        let child_at = self.required(
            "actual child removal",
            self.once("one child removal", |fact| {
                *fact == HistoryFact::ScopeRemoved { scope: child }
            })?,
        )?;
        let parent_at = self.required(
            "actual parent removal",
            self.once("one parent removal", |fact| {
                *fact == HistoryFact::ScopeRemoved { scope: parent }
            })?,
        )?;
        let expected = if self.work_ordinals(link)?[0].is_some() {
            WorkExpectation::Retired
        } else {
            WorkExpectation::UnusedRetired
        };
        self.check_work(link, expected)?;
        let body = self.required(
            "actual intrinsic parent link body exit",
            self.work_ordinals(link)?[1],
        )?;
        self.before("child removal before intrinsic link exit", child_at, body)?;
        self.before(
            "actual child Scope removed before parent Scope",
            child_at,
            parent_at,
        )
    }
    pub(crate) fn work_keys(&self) -> Vec<ScopedWorkRef> {
        let mut keys = Vec::new();
        for entry in self.entries() {
            let reference = match entry.fact {
                HistoryFact::WorkAdmitted { scope, work, .. }
                | HistoryFact::Admitted { scope, work, .. }
                | HistoryFact::Started { scope, work }
                | HistoryFact::BodyExited { scope, work }
                | HistoryFact::CapturesExited { scope, work }
                | HistoryFact::WorkRetired { scope, work }
                | HistoryFact::WorkUnresolved { scope, work }
                | HistoryFact::UnboundReservationRolledBack { scope, work, .. } => {
                    ScopedWorkRef { scope, work }
                }
                _ => continue,
            };
            if !keys.contains(&reference) {
                keys.push(reference);
            }
        }
        keys
    }
    pub(crate) fn effect_keys(&self) -> Vec<EffectIdentity> {
        let mut keys = Vec::new();
        for entry in self.entries() {
            if let HistoryFact::Effect {
                target,
                input,
                action,
                actor_generation,
                financial_work,
                author_generation,
                ..
            } = entry.fact
            {
                let identity = EffectIdentity {
                    target,
                    input,
                    action,
                    actor_generation,
                    financial_work,
                    author_generation,
                };
                if !keys.contains(&identity) {
                    keys.push(identity);
                }
            }
        }
        keys
    }
    pub(crate) fn actor_targets(&self) -> Vec<HistoryActorTarget> {
        let mut keys = Vec::new();
        for entry in self.entries() {
            let target = match entry.fact {
                HistoryFact::Effect { target, .. }
                | HistoryFact::Claimed { target }
                | HistoryFact::InputConsumed { target, .. }
                | HistoryFact::InputAbandoned { target, .. }
                | HistoryFact::BatchCommitted { target, .. }
                | HistoryFact::FailurePublication { target, .. } => target,
                _ => continue,
            };
            if !keys.contains(&target) {
                keys.push(target);
            }
        }
        keys
    }
    pub(crate) fn scope_keys(&self) -> Vec<ScopeId> {
        let mut keys = Vec::new();
        for entry in self.entries() {
            if let HistoryFact::ScopeOpened { scope, .. } | HistoryFact::ScopeRemoved { scope } =
                entry.fact
                && !keys.contains(&scope)
            {
                keys.push(scope);
            }
        }
        keys
    }
    /// Use only AFTER the actual isolated fixture drains BOTH original owners.
    /// UNKNOWN fixtures instead check explicitly named prefixes above.
    pub(crate) fn check_completed_runtime(
        &self,
        runtime: &ActorRuntime<FrontendActor>,
        ledger: &RuntimeLedger,
    ) -> Result<(), HistoryViolation> {
        self.check_capture()?;
        if !runtime.is_empty() || !ledger.is_empty() {
            return Err(violation(
                "actual runtime and ledger still live",
                "not a completed fixture",
            ));
        }
        for reference in self.work_keys() {
            let expectation = if self.entries().iter().any(|entry| matches!(entry.fact, HistoryFact::UnboundReservationRolledBack { scope, work, .. } if scope == reference.scope && work == reference.work)) { WorkExpectation::UnboundRolledBack } else if self.work_ordinals(reference)?[0].is_some() { WorkExpectation::Retired } else { WorkExpectation::UnusedRetired };
            self.check_work(reference, expectation)?;
        }
        for identity in self.effect_keys() {
            self.check_effect(identity, EffectExpectation::Completed)?;
        }
        for scope in self.scope_keys() {
            self.check_scope_removed(scope)?;
        }
        for entry in self.entries() {
            if let HistoryFact::ScopeOpened {
                scope,
                parent: Some(parent),
                ..
            } = entry.fact
            {
                self.check_child_scope_removed(scope, parent.scope)?;
            }
            match entry.fact {
                HistoryFact::InputConsumed { target, input } => {
                    self.check_input(target, input, InputDisposition::Consumed)?
                }
                HistoryFact::InputAbandoned { target, input } => {
                    self.check_input(target, input, InputDisposition::Abandoned)?
                }
                _ => {}
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests;
