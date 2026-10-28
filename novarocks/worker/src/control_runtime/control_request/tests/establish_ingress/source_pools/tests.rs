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
use crate::control_runtime::{
    establish_executors::{EstablishExecutorStartError, EstablishSubmitError},
    preparation::QueuedPreparation,
};

fn wait_original_ready(f: &Fixture, input: InputId) {
    let deadline = Instant::now() + WATCHDOG;
    loop {
        let version = f.runtime.change_version();
        if let Some(ready) = f.root.request_ready().next_ready() {
            assert_eq!(ready, input);
            return;
        }
        let remaining = deadline.saturating_duration_since(Instant::now());
        assert!(
            !remaining.is_zero(),
            "original source readiness watchdog expired"
        );
        f.runtime.wait_after_timeout(version, remaining);
    }
}
fn original_fees(f: &Fixture) -> [u64; 5] {
    [
        Dimension::Work,
        Dimension::Captures,
        Dimension::Inputs,
        Dimension::Replies,
        Dimension::Completions,
    ]
    .map(|dimension| f.ledger.used(dimension.id()))
}

#[test]
fn only_the_original_root_source_pools_can_take_getter_and_disposal_jobs() {
    let mut f = fixture(2);
    let mut foreign = fixture(2);
    let mut pools = SourcePoolGuard::start(&mut f);
    let foreign_pools = SourcePoolGuard::start(&mut foreign);
    let repeated_refused = match f
        .root
        .start_establish_executors(NonZeroUsize::new(1).unwrap(), NonZeroUsize::new(1).unwrap())
    {
        Err(refusal) => {
            let correct = matches!(refusal.error, EstablishExecutorStartError::AlreadyStarted)
                && refusal.owner.is_none();
            if let Some(partial) = refusal.owner {
                join(*partial);
            }
            correct
        }
        Ok(unexpected) => {
            join(unexpected);
            false
        }
    };
    let (raw, getters, drops) = request(&f, None, None, false);
    let (input, mut external) = admit(&mut f);
    let job = f
        .root
        .begin_establish_ingress(input, raw, PayloadId(1))
        .unwrap();
    let key = job.queue_key();
    let funded = original_fees(&f);
    let foreign_funded = original_fees(&foreign);
    let (getter_refused, job) = match foreign_pools
        .handles
        .as_ref()
        .unwrap()
        .try_submit_getter(job)
    {
        Err((error, original)) => (error == EstablishSubmitError::WrongOwner, original),
        Ok(()) => panic!("another Root cannot capture this original getter"),
    };
    let getter_untouched = job.queue_key() == key
        && original_fees(&f) == funded
        && original_fees(&foreign) == foreign_funded
        && getters.load(Ordering::SeqCst) == 0;
    pools.getter(job);
    wait_original_ready(&f, input);
    let disposal = finish_rpc(&mut f, input, &mut external);
    let funded_disposal = original_fees(&f);
    let (disposal_refused, disposal) = match foreign_pools
        .handles
        .as_ref()
        .unwrap()
        .try_submit_disposal(disposal)
    {
        Err((error, original)) => (error == EstablishSubmitError::WrongOwner, original),
        Ok(()) => panic!("another Root cannot capture this original raw disposal"),
    };
    let disposal_untouched = disposal.queue_key() == key
        && original_fees(&f) == funded_disposal
        && original_fees(&foreign) == foreign_funded
        && drops.load(Ordering::SeqCst) == 0;
    pools.disposal(disposal);
    let phase = wait_disposal(&f, input);
    drop(external);
    pools.finish();
    foreign_pools.finish();
    let retired = f.root.reap_ready_requests(8).unwrap();
    let charged = f.ledger.used(Dimension::Replies.id());
    f.finish();
    foreign.finish();
    assert!(repeated_refused);
    assert!(getter_refused && getter_untouched && disposal_refused && disposal_untouched);
    assert!(matches!(
        phase,
        ExitRolePhase::Exited | ExitRolePhase::Retired
    ));
    assert_eq!(getters.load(Ordering::SeqCst), 3);
    assert_eq!(drops.load(Ordering::SeqCst), 4);
    assert_eq!(retired, 1);
    assert_eq!(charged, 0);
}
