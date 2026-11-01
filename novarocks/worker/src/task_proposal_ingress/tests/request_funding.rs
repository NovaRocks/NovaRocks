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

//! Unused source-pair swaps preserve original slot and role identities.
//! These component observations do not authorize an R refund after rotation.
//! These component roles use the fixture's actual observer-only source reports;
//! consuming the summary does not manufacture a Context actor Core receipt.

use super::*;
use novarocks_control_runtime::exit_role::ExitRolePhase;

fn consume_source_free_cut(f: &Fixture, cut: CutId) -> RetiredCut {
    drop_handoff(f.ingress.submit(TaskConclusionProposal::Succeeded));
    f.ingress.seal_cut(cut).unwrap();
    let summary = f.ingress.take_next_summary().unwrap();
    assert_eq!(summary.cut, Some(cut));
    assert_eq!(summary.summary.candidate_sources().count(), 0);
    f.ingress.summary_consumed(Some(cut)).unwrap();
    f.ingress.observe_cut_exit(cut, true, true).unwrap();
    f.ingress.retire_cut_unclaimed(cut).unwrap()
}

fn retire_owned_unused_pair(f: &Fixture, pair: SourcePositionPair, funding: &SourceFunding) {
    assert!(funding.matches_pair(&pair));
    for holder in f
        .ingress
        .take_unused_owned_pair(&pair)
        .unwrap()
        .into_iter()
        .flatten()
    {
        holder.exit();
    }
    for position in f.ingress.owned_pair_inventory(&pair).unwrap() {
        assert!(position.custody.actual_exit_ready().unwrap());
        // A source-free holder withdraws the original unused report rather
        // than publishing a source or acknowledging a nonexistent Core leg.
        assert_eq!(
            position.custody.report().consume(observer()),
            Err(
                novarocks_control_runtime::transaction::RuntimeError::Completion(
                    novarocks_control_runtime_core::completion::CompletionError::NotFilled
                )
            )
        );
        assert!(position.custody.try_retire().unwrap());
        assert!(!position.custody.reusable());
    }
    f.ledger.reap_scope_ready(f.scope.id(), 2).unwrap();
    let witnesses = f
        .ingress
        .prepare_owned_pair_reap(&pair)
        .unwrap()
        .map(|probe| probe.observe_reaped().unwrap());
    let storage = f
        .ingress
        .release_owned_pair(&pair, witnesses)
        .unwrap_or_else(|_| panic!("release the exact actually reaped funding pair"));
    release_storage(storage);
    assert!(funding.custody.iter().all(SourceCustody::reusable));
}

fn restore_and_retire(f: &Fixture, retired: RetiredCut) {
    let restored = f
        .ingress
        .restore_retired_funding_pair(retired)
        .unwrap_or_else(|(error, _original)| panic!("original funding restore refused: {error:?}"));
    retire_owned_unused_pair(f, restored.pair, &restored.funding);
}

#[test]
fn a_lone_source_free_cut_recovers_its_exact_funding_and_leaves_the_base_open_pair() {
    let f = Fixture::new(1);
    let base = f
        .ingress
        .pair_inventory(PairRoute::Open)
        .unwrap()
        .map(|position| position.key);
    f.reserve(CutId(1));
    let retired = consume_source_free_cut(&f, CutId(1));
    assert!(!retired.funding.matches_pair(&retired.pair));
    assert!(f.ingress.try_take_open_summary().unwrap().is_none());
    let before = f.scope.snapshot().unwrap();
    let version = f.ledger.change_version();
    let returned = f
        .ingress
        .restore_retired_funding_pair(retired)
        .unwrap_or_else(|(error, _original)| panic!("original funding restore refused: {error:?}"));
    assert!(returned.funding.matches_pair(&returned.pair));
    assert!(
        returned
            .pair
            .keys()
            .iter()
            .zip(&returned.funding.keys)
            .all(|(key, original)| key.same_original(original))
    );
    assert_eq!(f.scope.snapshot().unwrap(), before);
    assert_eq!(f.ledger.change_version(), version);
    assert!(
        returned
            .funding
            .custody
            .iter()
            .all(|role| role.phase() == ExitRolePhase::Admitted
                && !role.actual_exit_ready().unwrap()
                && !role.reusable())
    );
    let restored = f.ingress.pair_inventory(PairRoute::Open).unwrap();
    assert!(
        restored
            .iter()
            .zip(&base)
            .all(|(position, original)| position.key.same_original(original))
    );
    assert!(f.ingress.try_take_open_summary().unwrap().is_none());
    // No next request or Task close is needed to exit and retire this R's B.
    retire_owned_unused_pair(&f, returned.pair, &returned.funding);
    drop(returned.funding);
    f.finish();
}

#[test]
fn another_ledgers_same_numeric_funding_or_pair_is_refused_without_losing_either_original() {
    let f = Fixture::new(1);
    let other = Fixture::new(1);
    f.reserve(CutId(1));
    other.reserve(CutId(1));
    let own = consume_source_free_cut(&f, CutId(1));
    let foreign = consume_source_free_cut(&other, CutId(1));
    assert_eq!(f.scope.id(), other.scope.id());
    assert_eq!(
        own.funding.keys.each_ref().map(|key| key.address()),
        foreign.funding.keys.each_ref().map(|key| key.address())
    );
    let original_keys = own.pair.keys().map(Clone::clone);
    let own_before = f.scope.snapshot().unwrap();
    let other_before = other.scope.snapshot().unwrap();
    let (error, original_pair) = match f.ingress.restore_retired_funding_pair(RetiredCut {
        rpc_retirement_notice: None,
        funding: foreign.funding.clone(),
        pair: own.pair,
    }) {
        Err(refusal) => refusal,
        Ok(_) => panic!("foreign same-number funding cannot replace the original roles"),
    };
    assert_eq!(
        error,
        IngressError::Source(SourcePositionError::ForeignInventory)
    );
    assert!(
        original_pair
            .pair
            .keys()
            .iter()
            .zip(&original_keys)
            .all(|(key, original)| key.same_original(original))
    );
    let foreign_keys = foreign.pair.keys().map(Clone::clone);
    let (error, foreign_pair) = match f.ingress.restore_retired_funding_pair(RetiredCut {
        rpc_retirement_notice: None,
        funding: own.funding.clone(),
        pair: foreign.pair,
    }) {
        Err(refusal) => refusal,
        Ok(_) => panic!("foreign pair cannot enter this original inventory"),
    };
    assert_eq!(
        error,
        IngressError::Source(SourcePositionError::ForeignInventory)
    );
    assert!(
        foreign_pair
            .pair
            .keys()
            .iter()
            .zip(&foreign_keys)
            .all(|(key, original)| key.same_original(original))
    );
    assert_eq!(f.scope.snapshot().unwrap(), own_before);
    assert_eq!(other.scope.snapshot().unwrap(), other_before);
    restore_and_retire(
        &f,
        RetiredCut {
            rpc_retirement_notice: None,
            pair: original_pair.pair,
            funding: own.funding,
        },
    );
    restore_and_retire(
        &other,
        RetiredCut {
            rpc_retirement_notice: None,
            pair: foreign_pair.pair,
            funding: foreign.funding,
        },
    );
    f.finish();
    other.finish();
}

#[test]
fn a_pending_cuts_funding_cannot_be_stolen_by_an_earlier_returned_pair() {
    let f = Fixture::new(2);
    f.reserve(CutId(1));
    f.reserve(CutId(2));
    let pending_funding = f.ingress.cut_funding(CutId(2)).unwrap();
    let first = consume_source_free_cut(&f, CutId(1));
    let original_keys = first.pair.keys().map(Clone::clone);
    let pending_keys = f
        .ingress
        .pair_inventory(PairRoute::Cut(CutId(2)))
        .unwrap()
        .map(|position| position.key);
    let before = f.scope.snapshot().unwrap();
    let (error, returned) = match f.ingress.restore_retired_funding_pair(RetiredCut {
        rpc_retirement_notice: None,
        funding: pending_funding.clone(),
        pair: first.pair,
    }) {
        Err(refusal) => refusal,
        Ok(_) => panic!("a pending cut keeps its original paid pair"),
    };
    assert_eq!(error, IngressError::Cut(CutError::NotReady));
    assert_eq!(f.scope.snapshot().unwrap(), before);
    assert!(
        returned
            .pair
            .keys()
            .iter()
            .zip(&original_keys)
            .all(|(key, original)| key.same_original(original))
    );
    assert!(
        f.ingress
            .pair_inventory(PairRoute::Cut(CutId(2)))
            .unwrap()
            .iter()
            .zip(&pending_keys)
            .all(|(position, original)| position.key.same_original(original))
    );
    assert!(
        pending_funding
            .custody
            .iter()
            .all(|role| role.phase() == ExitRolePhase::Admitted)
    );
    // Complete the second actual cut, then dispose B and C by their real
    // funding identities. No ownership may be recovered by scanning cuts.
    let second = consume_source_free_cut(&f, CutId(2));
    retire_owned_unused_pair(&f, second.pair, &first.funding);
    restore_and_retire(
        &f,
        RetiredCut {
            rpc_retirement_notice: None,
            pair: returned.pair,
            funding: second.funding,
        },
    );
    drop(pending_funding);
    drop(first.funding);
    f.finish();
}

#[test]
fn a_claimed_funding_source_waits_for_its_last_real_alias_and_cannot_match_a_rearmed_slot() {
    let f = Fixture::new(1);
    f.reserve(CutId(1));
    let retired = consume_source_free_cut(&f, CutId(1));
    let original_pair = retired.pair.keys().map(Clone::clone);
    let drops = Arc::new(AtomicUsize::new(0));
    drop_handoff(f.ingress.submit(failure(&f, &drops)));
    let before = f.scope.snapshot().unwrap();
    let (error, retired) = match f.ingress.restore_retired_funding_pair(retired) {
        Err(refusal) => refusal,
        Ok(_) => panic!("a real retained source is not an unused funding permit"),
    };
    assert_eq!(
        error,
        IngressError::Source(SourcePositionError::NotAvailable)
    );
    assert_eq!(f.scope.snapshot().unwrap(), before);
    assert!(
        retired
            .pair
            .keys()
            .iter()
            .zip(&original_pair)
            .all(|(key, original)| key.same_original(original))
    );
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    let input = f.ingress.take_next_summary().unwrap();
    assert!(input.cut.is_none());
    let source = input.summary.candidate_sources().next().unwrap();
    let retained = Arc::new(f.ingress.take_source(source).unwrap());
    let last_alias = retained.clone();
    f.ingress.open_summary_consumed().unwrap();
    drop(retained);
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    assert!(
        retired
            .funding
            .custody
            .iter()
            .any(|role| !role.actual_exit_ready().unwrap())
    );
    assert!(
        f.ingress
            .prepare_pair_reap(PairRoute::ConsumingOpen)
            .unwrap()
            .into_iter()
            .any(|probe| probe.observe_reaped().is_err())
    );
    drop(last_alias);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    let reaped = f.reap(PairRoute::ConsumingOpen);
    f.ingress
        .finish_open_after_reap(reaped)
        .unwrap_or_else(|_| panic!("the actual source and all original aliases exited"));
    assert!(retired.funding.custody.iter().all(SourceCustody::reusable));
    // Fresh roles come from the still-live original fixture payer. Reusing a
    // numeric position neither resurrects the old Work nor its funding key.
    let reaped = f
        .ingress
        .prepare_pair_reap(PairRoute::ConsumingOpen)
        .unwrap()
        .map(|probe| probe.observe_reaped().unwrap());
    f.ingress
        .rearm_open_spare(reaped, f.slot_roles())
        .unwrap_or_else(|_| {
            panic!("the exact old headers reaped before fresh roles were admitted")
        });
    let (error, retired) = match f.ingress.restore_retired_funding_pair(retired) {
        Err(refusal) => refusal,
        Ok(_) => panic!("a replacement generation is not the retired original funding"),
    };
    assert_eq!(
        error,
        IngressError::Source(SourcePositionError::WrongGeneration)
    );
    assert!(
        retired
            .pair
            .keys()
            .iter()
            .zip(&original_pair)
            .all(|(key, original)| key.same_original(original))
    );
    let actual_pair_funding = f
        .ingress
        .state
        .lock()
        .unwrap()
        .funding(&retired.pair)
        .unwrap();
    retire_owned_unused_pair(&f, retired.pair, &actual_pair_funding);
    f.finish();
}

#[test]
fn repeated_fresh_request_funding_retires_without_accumulating_resident_source_positions() {
    let f = Fixture::new(1);
    let baseline = f.scope.snapshot().unwrap();
    let resident_baseline = f.ingress.state.lock().unwrap().sources.len();
    assert_eq!(resident_baseline, 4);
    let base_open = f
        .ingress
        .pair_inventory(PairRoute::Open)
        .unwrap()
        .map(|position| position.key);
    for n in 1..=64 {
        // Every request brings fresh actual paid roles. A prior request must
        // have fully retired before this independent one can be admitted.
        assert_eq!(f.scope.snapshot().unwrap(), baseline);
        assert_eq!(
            f.ingress.state.lock().unwrap().sources.len(),
            resident_baseline
        );
        f.reserve(CutId(n));
        assert_eq!(
            f.ingress.state.lock().unwrap().sources.len(),
            resident_baseline + 2
        );
        let retired = consume_source_free_cut(&f, CutId(n));
        restore_and_retire(&f, retired);
        // Check before even reserving the next request: no later cut or Task
        // close may release this one's original Source2 or its real headers.
        assert_eq!(f.scope.snapshot().unwrap(), baseline, "request {n}");
        assert_eq!(
            f.ingress.state.lock().unwrap().sources.len(),
            resident_baseline,
            "retired request {n} still occupies the bounded source inventory"
        );
        assert!(f.ingress.try_take_open_summary().unwrap().is_none());
        assert!(
            f.ingress
                .pair_inventory(PairRoute::Open)
                .unwrap()
                .iter()
                .zip(&base_open)
                .all(|(position, original)| position.key.same_original(original))
        );
    }
    assert!(f.scope.gates().unwrap().external_open);
    assert!(!f.scope.gates().unwrap().stopped);
    f.finish();
}

#[test]
fn funding_in_an_unclaimed_spare_waits_for_its_actual_summary_consumption() {
    let f = Fixture::new(1);
    f.reserve(CutId(1));
    let retired = consume_source_free_cut(&f, CutId(1));
    let original = retired.pair.keys().map(Clone::clone);
    drop_handoff(f.ingress.submit(TaskConclusionProposal::Succeeded));
    let summary = f.ingress.take_next_summary().unwrap();
    assert!(summary.cut.is_none());
    assert_eq!(summary.summary.candidate_sources().count(), 0);
    let before = f.scope.snapshot().unwrap();
    let (error, retired) = match f.ingress.restore_retired_funding_pair(retired) {
        Err(refusal) => refusal,
        Ok(_) => panic!("the consuming spare retains its own pending input"),
    };
    assert_eq!(error, IngressError::Cut(CutError::NotReady));
    assert_eq!(f.scope.snapshot().unwrap(), before);
    assert!(
        retired
            .pair
            .keys()
            .iter()
            .zip(&original)
            .all(|(key, original)| key.same_original(original))
    );
    f.ingress.open_summary_consumed().unwrap();
    f.ingress.finish_open_unclaimed().unwrap();
    let returned = f
        .ingress
        .restore_retired_funding_pair(retired)
        .unwrap_or_else(|(error, _)| {
            panic!("the actual unused spare can return original funding: {error:?}")
        });
    assert!(returned.funding.matches_pair(&returned.pair));
    assert!(
        returned
            .funding
            .custody
            .iter()
            .all(|role| role.phase() == ExitRolePhase::Admitted)
    );
    retire_owned_unused_pair(&f, returned.pair, &returned.funding);
    f.finish();
}
