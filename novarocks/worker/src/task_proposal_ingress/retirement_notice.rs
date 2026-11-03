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

use super::*;
use novarocks_control_runtime_core::{completion::ConsumerKind, identity::ConsumerId};

/// At most two changed pairs, each with two originals and two recipients.
/// This stack-local wake scratch is neither an address table nor new capacity.
#[derive(Default)]
pub(super) struct RetirementWakes([Option<Weak<RequestReadySlot>>; 8]);
impl RetirementWakes {
    fn push(&mut self, original: Weak<RequestReadySlot>) {
        if self.0.iter().flatten().any(|slot| slot.ptr_eq(&original)) {
            return;
        }
        *self
            .0
            .iter_mut()
            .find(|slot| slot.is_none())
            .expect("two original pairs have at most eight recipients") = Some(original);
    }
    #[cfg(test)]
    pub(super) fn extend(&mut self, recipients: [Option<Weak<RequestReadySlot>>; 4]) {
        for recipient in recipients.into_iter().flatten() {
            self.push(recipient);
        }
    }
    #[cfg(test)]
    pub(super) fn snapshot_pair(
        state: &State,
        pair: &SourcePositionPair,
    ) -> [Option<Weak<RequestReadySlot>>; 4] {
        let [first, second] = state
            .observe_owned_pair(pair)
            .expect("same original pair remains in its bank");
        let [a, b] = first.custody.retirement_notices();
        let [c, d] = second.custody.retirement_notices();
        [a, b, c, d]
    }
    pub(super) fn capture_pair(&mut self, state: &State, pair: &SourcePositionPair) {
        for original in state
            .observe_owned_pair(pair)
            .expect("same original pair remains in its bank")
        {
            for notice in original.custody.retirement_notices().into_iter().flatten() {
                self.push(notice);
            }
        }
    }
    pub(super) fn capture_funding(&mut self, funding: &SourceFunding) {
        for original in &funding.custody {
            for notice in original.retirement_notices().into_iter().flatten() {
                self.push(notice);
            }
        }
    }
}
impl Drop for RetirementWakes {
    fn drop(&mut self) {
        for notice in self.0.iter().flatten() {
            if let Some(original) = notice.upgrade() {
                original.notify();
            }
        }
    }
}

fn foreign() -> IngressError {
    SourcePositionError::ForeignInventory.into()
}
fn preflight(
    funding: &SourceFunding,
    funding_reusable: [bool; 2],
    pair: &[SourcePositionObservation; 2],
    notice: &Weak<RequestReadySlot>,
) -> Result<(), IngressError> {
    if funding
        .custody
        .iter()
        .zip(funding_reusable)
        .any(|(original, reusable)| !reusable && !original.can_arm_retirement_notice(true, notice))
        || pair
            .iter()
            .any(|original| !original.custody.can_arm_retirement_notice(false, notice))
    {
        return Err(foreign());
    }
    Ok(())
}
fn bind(
    funding: &SourceFunding,
    funding_reusable: [bool; 2],
    pair: &[SourcePositionObservation; 2],
    notice: &Weak<RequestReadySlot>,
) {
    for (original, reusable) in funding.custody.iter().zip(funding_reusable) {
        if !reusable {
            original.arm_retirement_notice(true, notice);
        }
    }
    for original in pair {
        original.custody.arm_retirement_notice(false, notice);
    }
}
fn same_pair(
    left: &[SourcePositionObservation; 2],
    right: &[SourcePositionObservation; 2],
) -> bool {
    left.iter().zip(right).all(|(a, b)| {
        a.key.same_original(&b.key) && a.custody.report().same_slot(b.custody.report())
    })
}
fn validate_slot(
    original: &Arc<RequestReadySlot>,
    funding: &SourceFunding,
    pair: &[SourcePositionObservation; 2],
) -> Result<(), IngressError> {
    let financial_scope = funding.custody[0].financial_scope();
    if pair.iter().any(|source| {
        !source
            .custody
            .financial_scope()
            .same_ledger(financial_scope)
    }) {
        return Err(foreign());
    }
    let owner = original.participation_owner().ok_or_else(foreign)?;
    let expected = ConsumerKind::Owner {
        scope: owner.scope_id(),
        generation: owner.initial_epoch(),
    };
    for custody in funding
        .custody
        .iter()
        .chain(pair.iter().map(|p| &p.custody))
    {
        // The original report has already been removed at role retirement;
        // its still-resident header can need the caller's bounded reaper.
        if custody.phase() == novarocks_control_runtime::exit_custody::ExitCustodyPhase::Retired {
            continue;
        }
        if custody
            .report()
            .consumer_kind(ConsumerId::new(1).expect("original Source Core consumer"))
            .map_err(|_| foreign())?
            != expected
        {
            return Err(foreign());
        }
    }
    Ok(())
}
impl TaskProposalIngress {
    /// Borrow the original registered R address; all report/ready reads happen
    /// outside the ingress fence, followed by same-original revalidation.
    pub(crate) fn arm_rpc_retirement_notice(
        &self,
        cut: CutId,
        original: &Arc<RequestReadySlot>,
    ) -> Result<(), IngressError> {
        let (actor, work) = original.participation_author().ok_or_else(foreign)?;
        let owner = original.participation_owner().ok_or_else(foreign)?;
        if actor != owner.actor() {
            return Err(foreign());
        }
        let (funding, pair) = {
            let state = self
                .state
                .lock()
                .expect("original Task RPC retirement notice");
            let cell = state.cuts.get(&cut).ok_or(CutError::Unknown)?;
            let event = cell.event.as_ref().ok_or(CutError::NotReserved)?;
            if !matches!(event.kind, CausalEventKind::Request(_))
                || event.work != Some(work)
                || cell
                    .rpc_retirement_notice
                    .as_ref()
                    .is_some_and(|old| !old.ptr_eq(&Arc::downgrade(original)))
            {
                return Err(foreign());
            }
            (
                cell.funding.clone(),
                state.observe_owned_pair(cell.pair.as_ref().ok_or(CutError::Unknown)?)?,
            )
        };
        if funding.custody.iter().any(|source| {
            !original.belongs_to_financial_scope(source.financial_scope())
                || source.financial_scope().id() != work.scope
        }) {
            return Err(foreign());
        }
        validate_slot(original, &funding, &pair)?;
        let funding_reusable = funding.custody.each_ref().map(|source| source.reusable());
        let mut state = self
            .state
            .lock()
            .expect("original Task RPC retirement notice");
        let cell = state.cuts.get(&cut).ok_or(CutError::Unknown)?;
        let current = state.observe_owned_pair(cell.pair.as_ref().ok_or(CutError::Unknown)?)?;
        if cell.event.as_ref().and_then(|event| event.work) != Some(work)
            || !cell
                .funding
                .keys
                .iter()
                .zip(&funding.keys)
                .all(|(a, b)| a.same_original(b))
            || !same_pair(&pair, &current)
        {
            return Err(foreign());
        }
        let notice = Arc::downgrade(original);
        if cell
            .rpc_retirement_notice
            .as_ref()
            .is_some_and(|old| !old.ptr_eq(&notice))
        {
            return Err(foreign());
        }
        for ((key, custody), reusable) in funding
            .keys
            .iter()
            .zip(&funding.custody)
            .zip(funding_reusable)
        {
            state
                .sources
                .validate_funding_custody(key, custody, reusable)?;
        }
        preflight(&funding, funding_reusable, &current, &notice)?;
        bind(&funding, funding_reusable, &current, &notice);
        state
            .cuts
            .get_mut(&cut)
            .expect("same original RPC cut")
            .rpc_retirement_notice = Some(notice);
        Ok(())
    }
    /// The original cut moved this weak address into RetiredCut. A descriptor
    /// or a newly registered same-number slot cannot replace that recipient.
    pub(crate) fn arm_retired_rpc_retirement_notice(
        &self,
        retired: &RetiredCut,
        original: &Arc<RequestReadySlot>,
    ) -> Result<(), IngressError> {
        let notice = Arc::downgrade(original);
        if !retired
            .rpc_retirement_notice
            .as_ref()
            .is_some_and(|old| old.ptr_eq(&notice))
        {
            return Err(foreign());
        }
        let (_, work) = original.participation_author().ok_or_else(foreign)?;
        if retired.funding.custody.iter().any(|source| {
            !original.belongs_to_financial_scope(source.financial_scope())
                || source.financial_scope().id() != work.scope
        }) {
            return Err(foreign());
        }
        let pair = {
            let state = self.state.lock().expect("original retired RPC notice");
            state.observe_owned_pair(&retired.pair)?
        };
        validate_slot(original, &retired.funding, &pair)?;
        let funding_reusable = retired
            .funding
            .custody
            .each_ref()
            .map(|source| source.reusable());
        let state = self.state.lock().expect("original retired RPC notice");
        let current = state.observe_owned_pair(&retired.pair)?;
        if !same_pair(&pair, &current) {
            return Err(foreign());
        }
        for ((key, custody), reusable) in retired
            .funding
            .keys
            .iter()
            .zip(&retired.funding.custody)
            .zip(funding_reusable)
        {
            state
                .sources
                .validate_funding_custody(key, custody, reusable)?;
        }
        preflight(&retired.funding, funding_reusable, &current, &notice)?;
        bind(&retired.funding, funding_reusable, &current, &notice);
        Ok(())
    }
}
