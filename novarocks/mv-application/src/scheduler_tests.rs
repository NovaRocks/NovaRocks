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

use super::*;
use crate::persistence::identity::{NativeDataVersion, ObjectIdentity};
use crate::persistence::test_support::KeyedProjectionFixture;

fn stored(mv_id: i64, fixture: KeyedProjectionFixture) -> StoredKeyedMvProjection {
    StoredKeyedMvProjection {
        mv_id,
        facts: fixture.build().expect("valid scheduler projection"),
    }
}

fn target() -> MvTarget {
    MvTarget::from_parts(Some("ice"), "sales", "mv")
}

fn published_inputs(projection: &StoredKeyedMvProjection) -> Vec<PublicationInput> {
    projection
        .facts
        .publication()
        .expect("published keyed fixture")
        .inputs
        .clone()
}

fn configuration(policy: RefreshPolicy) -> ConfigurationDocument {
    ConfigurationDocument {
        refresh_policy: policy,
        paused: false,
        refresh_interval_ms: matches!(policy, RefreshPolicy::AsyncInterval).then_some(100),
        max_staleness_ms: None,
    }
}
fn input(id: u32, object: &[u8], version: &[u8]) -> PublicationInput {
    PublicationInput {
        relation_occurrence_id: id,
        object_id: ObjectIdentity::try_new(object.to_vec()).unwrap(),
        native_data_version: NativeDataVersion::try_new(version.to_vec()).unwrap(),
    }
}
// This policy-level pair is not an encoded P or a projection seed.
fn publication(inputs: Vec<PublicationInput>) -> (u64, Vec<PublicationInput>) {
    (1_000, inputs)
}
fn occurrence(id: u32) -> BindingOccurrence {
    let mut occurrence = crate::persistence::test_support::relation(id, 1, "base");
    occurrence.object_id = ObjectIdentity::try_new(b"source".to_vec()).unwrap();
    occurrence
}

#[test]
fn default_policy_preserves_the_deployed_scheduler_bounds() {
    assert_eq!(
        MvSchedulerConfig::default(),
        MvSchedulerConfig::new(false, 30_000, 1, 60_000, 1_800_000)
    );
}
#[test]
fn interval_uses_published_fact_time_not_accelerator_insertion_time() {
    let c = configuration(RefreshPolicy::AsyncInterval);
    let p = publication(vec![input(7, b"source", b"v1")]);
    assert_eq!(
        semantic_decision(&c, Some((p.0, p.1.as_slice())), 1_099, None),
        MvSchedulerSemanticDecision::IntervalNotDue {
            eligible_at_ms: 1_100
        }
    );
    assert_eq!(
        semantic_decision(&c, Some((p.0, p.1.as_slice())), 1_100, None),
        MvSchedulerSemanticDecision::IntervalDue
    );
    assert_eq!(
        semantic_decision(&c, None, 1_099, None),
        MvSchedulerSemanticDecision::IntervalDue
    );
}
#[test]
fn on_change_compares_each_occurrence_and_complete_opaque_revision() {
    let c = configuration(RefreshPolicy::AsyncOnChange);
    let inputs = vec![input(7, b"source", b"v1"), input(42, b"source", b"v2")];
    let p = publication(inputs.clone());
    let mut reordered = inputs.clone();
    reordered.reverse();
    assert_eq!(
        semantic_decision(&c, Some((p.0, p.1.as_slice())), 2_000, Some(&reordered)),
        MvSchedulerSemanticDecision::OnChangeNotDue
    );
    for changed in [
        vec![input(7, b"source", b"v2"), input(42, b"source", b"v1")],
        vec![input(7, b"replacement", b"v1"), inputs[1].clone()],
        vec![inputs[0].clone()],
    ] {
        assert_eq!(
            semantic_decision(&c, Some((p.0, p.1.as_slice())), 2_000, Some(&changed)),
            MvSchedulerSemanticDecision::OnChangeDue
        );
    }
}
#[test]
fn observations_require_every_occurrence_without_fqn_deduplication() {
    let definition = vec![occurrence(7), occurrence(42)];
    let current = vec![input(42, b"source", b"v2"), input(7, b"source", b"v1")];
    assert!(validate_current_inputs(&definition, &current).is_ok());
    for invalid in [
        vec![current[0].clone()],
        vec![current[0].clone(), current[0].clone()],
        vec![current[0].clone(), input(99, b"source", b"v1")],
        vec![current[0].clone(), input(7, b"replacement", b"v1")],
    ] {
        assert!(validate_current_inputs(&definition, &invalid).is_err());
    }
}
#[test]
fn paused_manual_and_missing_observation_are_explicit() {
    let mut paused = configuration(RefreshPolicy::AsyncInterval);
    paused.paused = true;
    assert_eq!(
        semantic_decision(&paused, None, 100, None),
        MvSchedulerSemanticDecision::Paused
    );
    assert_eq!(
        semantic_decision(&configuration(RefreshPolicy::Manual), None, 100, None),
        MvSchedulerSemanticDecision::Manual
    );
    assert!(matches!(
        semantic_decision(
            &configuration(RefreshPolicy::AsyncOnChange),
            None,
            100,
            None
        ),
        MvSchedulerSemanticDecision::Invalid { .. }
    ));
    assert_eq!(
        semantic_decision(
            &configuration(RefreshPolicy::AsyncOnChange),
            None,
            100,
            Some(&[input(7, b"source", b"empty-version")])
        ),
        MvSchedulerSemanticDecision::OnChangeDue
    );
}
#[test]
fn duplicate_inputs_never_equal_a_publication() {
    let expected = vec![input(7, b"source", b"v1"), input(42, b"source", b"v2")];
    assert!(!exact_inputs_match(
        &expected,
        &[expected[0].clone(), expected[0].clone()]
    ));
}

#[test]
fn concrete_interval_scheduler_consumes_the_validated_projection() {
    let projection = stored(7, KeyedProjectionFixture::new(target(), Some(11)));
    let mut scheduler = MvRefreshScheduler::new(MvSchedulerConfig::new(true, 30_000, 1, 10, 40));

    assert!(
        scheduler
            .observe_projection(projection, 1_700_000_061_000)
            .is_none()
    );
    let ready = scheduler.take_ready();
    assert_eq!(ready.len(), 1);
    assert_eq!(ready[0].projection().mv_id, 7);
    assert_eq!(ready[0].target(), &target());
}

#[test]
fn a_stale_observation_failure_cannot_block_a_newer_configuration() {
    let mut first = KeyedProjectionFixture::new(target(), Some(11));
    first.configuration = configuration(RefreshPolicy::AsyncOnChange);
    let first = stored(7, first);
    let mut second = KeyedProjectionFixture::new(target(), Some(11));
    second.configuration = configuration(RefreshPolicy::AsyncOnChange);
    second.configuration.max_staleness_ms = Some(1_000);
    let second = stored(7, second);
    let current = published_inputs(&second);
    let mut scheduler = MvRefreshScheduler::new(MvSchedulerConfig::new(true, 30_000, 1, 10, 40));

    let stale = scheduler
        .observe_projection(first, 1_700_000_002_000)
        .expect("first observation");
    let current_observation = scheduler
        .observe_projection(second, 1_700_000_002_000)
        .expect("new configuration observation");
    scheduler.record_observation_failure(
        &stale,
        MvRefreshDisposition::TerminalFailure("late failure".to_string().into()),
        1_700_000_002_000,
    );
    let mut changed = current;
    let source =
        crate::persistence::exact_revision::restore_persisted_object(&changed[0].object_id)
            .unwrap();
    let exact = novarocks_spi::connector::ConnectorExactSemanticRevision::try_from_table_object_and_snapshot(
            novarocks_spi::connector::ConnectorProviderId::parse("iceberg").unwrap(),
            &source,
            Some(18),
        ).unwrap();
    changed[0].native_data_version =
        crate::persistence::exact_revision::persist_exact_connector_revision(&exact)
            .unwrap()
            .1;
    scheduler.resolve_current_inputs(current_observation, changed, 1_700_000_002_000);

    assert_eq!(scheduler.take_ready().len(), 1);
}
#[test]
fn concrete_scheduler_has_explicit_terminal_decisions() {
    let mut scheduler = MvRefreshScheduler::new(MvSchedulerConfig::new(true, 30_000, 1, 10, 40));
    assert_eq!(
        scheduler.record(7, MvRefreshDisposition::Completed, 100),
        MvRefreshRuntimeDecision::Success
    );
    assert_eq!(
        scheduler.record(
            7,
            MvRefreshDisposition::TransientUnavailable("offline".to_string().into()),
            100
        ),
        MvRefreshRuntimeDecision::TransientBackoff {
            error: "offline".to_string().into(),
            retry_at_ms: 110
        }
    );
    for disposition in [
        MvRefreshDisposition::InvalidDefinition("bad".to_string().into()),
        MvRefreshDisposition::TerminalFailure("terminal".to_string().into()),
        MvRefreshDisposition::Corruption("corrupt".to_string().into()),
        MvRefreshDisposition::InvariantViolation("invariant".to_string().into()),
    ] {
        assert!(matches!(
            scheduler.record(7, disposition, 100),
            MvRefreshRuntimeDecision::Blocked { .. }
        ));
    }
}
#[test]
fn keyed_policy_rejects_partial_current_inputs_before_comparing_watermarks() {
    let mut fixture = KeyedProjectionFixture::new(target(), Some(11));
    fixture.configuration = configuration(RefreshPolicy::AsyncOnChange);
    fixture.publication.as_mut().unwrap().logical_result_rows = None;
    let projection = stored(7, fixture);
    let inputs = published_inputs(&projection);
    assert_eq!(
        mv_keyed_scheduler_semantic_decision(&projection.facts, 2_000, Some(&inputs)),
        MvSchedulerSemanticDecision::OnChangeNotDue,
    );
    assert!(matches!(
        mv_keyed_scheduler_semantic_decision(&projection.facts, 2_000, Some(&inputs[..1])),
        MvSchedulerSemanticDecision::Invalid { .. },
    ));
}
