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

use novarocks_query_application::cancellation::{QueryCancellationReason, QueryCancellationSource};
use novarocks_query_application::publication::LakePublicationRuntimePolicy;

#[test]
fn cancellation_view_observes_injected_flag() {
    let cancelled = QueryCancellationSource::new();
    let view = cancelled.view();

    assert!(!view.is_cancelled());
    let _ = cancelled.request(QueryCancellationReason::ClientDisconnected);
    assert!(view.is_cancelled());
}

#[test]
fn statistics_attempt_timeout_must_be_positive() {
    let policy_for = |attempt_timeout| {
        LakePublicationRuntimePolicy::try_new(
            attempt_timeout,
            std::time::Duration::from_secs(45 * 60),
            std::time::Duration::from_secs(60),
            std::time::Duration::from_secs(5 * 60),
            std::time::Duration::from_secs(60),
        )
    };
    let policy =
        policy_for(std::time::Duration::from_secs(30 * 60)).expect("positive attempt timeout");
    assert_eq!(
        policy.max_attempt_duration(),
        std::time::Duration::from_secs(30 * 60)
    );
    assert!(
        policy_for(std::time::Duration::from_secs(30 * 60 + 1)).is_ok(),
        "the shared policy accepts longer attempts when its strict GC relationship is satisfied"
    );
    assert!(policy_for(std::time::Duration::ZERO).is_err());
}

#[test]
fn fragment_profile_set_preserves_fragment_profiles() {
    let profile = crate::query_execution::profile::FragmentProfileTree::for_fragment(
        7,
        novarocks_execution::runtime::profile::Profiler::new("fragment-7").to_native_tree(),
    );
    let profiles = crate::query_execution::outcome::FragmentProfileSet::new(vec![profile.clone()]);
    assert_eq!(profiles.into_profiles(), vec![profile]);
}
