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

use super::SourceCustody;
use crate::control_runtime::request_ready::RequestReadySlot;
use std::sync::Weak;

/// One generation has one financial issuer and at most one current pair owner.
/// These exact weak addresses grant no Work, report or retirement authority.
#[derive(Default)]
pub(super) struct RetirementNotices {
    funding: Option<Weak<RequestReadySlot>>,
    pair: Option<Weak<RequestReadySlot>>,
}
impl SourceCustody {
    pub(crate) fn retirement_notices(&self) -> [Option<Weak<RequestReadySlot>>; 2] {
        let notices = self
            .0
            .retirement_notices
            .lock()
            .expect("original Source retirement notices");
        [notices.funding.clone(), notices.pair.clone()]
    }
    pub(crate) fn can_arm_retirement_notice(
        &self,
        funding: bool,
        notice: &Weak<RequestReadySlot>,
    ) -> bool {
        let notices = self
            .0
            .retirement_notices
            .lock()
            .expect("original Source retirement notices");
        let original = if funding {
            &notices.funding
        } else {
            &notices.pair
        };
        original
            .as_ref()
            .is_none_or(|old| old.ptr_eq(notice) || old.upgrade().is_none())
    }
    pub(crate) fn has_pair_retirement_notice(&self) -> bool {
        self.0
            .retirement_notices
            .lock()
            .expect("original Source retirement notices")
            .pair
            .as_ref()
            .is_some_and(|notice| notice.upgrade().is_some())
    }
    /// The original ingress fence serializes all notice writers. Preflight
    /// every affected original before mutating any of the finite recipients.
    pub(crate) fn arm_retirement_notice(&self, funding: bool, notice: &Weak<RequestReadySlot>) {
        let mut notices = self
            .0
            .retirement_notices
            .lock()
            .expect("original Source retirement notices");
        let original = if funding {
            &mut notices.funding
        } else {
            &mut notices.pair
        };
        assert!(
            original
                .as_ref()
                .is_none_or(|old| old.ptr_eq(notice) || old.upgrade().is_none()),
            "original Source retirement recipient was preflighted"
        );
        *original = Some(notice.clone());
    }
    /// Moving the unique pair back to a producer route removes only its old
    /// consumer address. Its independent financial issuer remains unchanged.
    pub(crate) fn clear_pair_retirement_notice(&self, expected: &Weak<RequestReadySlot>) {
        let mut notices = self
            .0
            .retirement_notices
            .lock()
            .expect("original Source retirement notices");
        if notices
            .pair
            .as_ref()
            .is_some_and(|old| old.ptr_eq(expected))
        {
            notices.pair = None;
        }
    }
    pub(super) fn notify_pending(&self) {
        let notices = self.retirement_notices();
        self.0.pending.pending();
        for notice in notices.into_iter().flatten() {
            if let Some(original) = notice.upgrade() {
                original.notify();
            }
        }
    }
}
