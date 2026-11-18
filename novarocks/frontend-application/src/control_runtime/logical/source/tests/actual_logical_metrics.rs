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

//! Actual LP-only observation cut; the caller keeps all original teardown duties.

use crate::control_runtime::actor::FrontendEffect;
use crate::fixtures::{ProgressBudget, ProgressSelection, native_recipe::OriginalFixture};
use novarocks_control_runtime::{actor_turn::InputPhase, history::HistoryCapture};
use novarocks_control_runtime_core::{effect::EffectState, identity::Tick, mailbox::InputClass};
use novarocks_logical_execution_protocol::{
    effect::{GovernanceEffect, LogicalEffectBody, NativeEffect},
    start::LogicalOutputMode,
};
use prometheus::proto::MetricFamily;
use std::time::{Duration, Instant};

fn gauge(families: &[MetricFamily], family: &str, labels: &[(&str, &str)]) -> Option<f64> {
    let family = families.iter().find(|value| value.get_name() == family)?;
    family
        .get_metric()
        .iter()
        .find(|metric| {
            metric.get_label().len() == labels.len()
                && labels.iter().all(|(name, value)| {
                    metric
                        .get_label()
                        .iter()
                        .any(|label| label.get_name() == *name && label.get_value() == *value)
                })
        })
        .map(|metric| metric.get_gauge().get_value())
}
fn assert_gauge(families: &[MetricFamily], family: &str, labels: &[(&str, &str)], value: u64) {
    // These bounded fixture counts/ticks are exactly representable by the collector.
    assert!(value <= (1_u64 << 53));
    assert_eq!(gauge(families, family, labels), Some(value as f64));
}

/// Observe only; never claim the pending ActivateAttempt, drain history, or close
/// the fixture. Its caller continues the SAME actual Source/QA/WC cleanup.
pub(super) async fn assert_before_attempt(
    original: &mut OriginalFixture,
    history: &HistoryCapture,
) {
    let deadline = Instant::now() + Duration::from_secs(5);
    // start_observed_in mounts the SAME request; its supervisor still owns
    // preparation and installation. This dedicated process has no other Host.
    // The installer publishes the ports before inserting its real Host entry.
    while original.process.host.is_empty() {
        crate::query_execution::native_execution_adapter::source_cohort::tests::drive(
            &mut original.joint,
        );
        assert!(
            Instant::now() < deadline,
            "same observed request did not install its original logical Host"
        );
        tokio::task::yield_now().await;
    }
    let bound = original.logical.binding.bound().unwrap();
    let actor = bound.registration.actor();
    let activation = bound.start.first_attempt.activation;
    assert_eq!(
        bound.start.policy.output,
        LogicalOutputMode::InternalResultStream
    );
    assert!(original.logical.probe.installed.lock().unwrap().is_none());
    assert!(
        original
            .process
            .actor_runtime()
            .committed_batches(actor, None, 64)
            .unwrap()
            .is_empty()
    );
    let registry = prometheus::Registry::new();
    let metrics = original.process.metrics(&registry).unwrap();

    // Registry commit can follow Host installation. Drive only that SAME
    // Host until its controlled Bootstrap job really fills the original input.
    // No Core poll or native activation can occur during this preparation.
    loop {
        crate::query_execution::native_execution_adapter::source_cohort::tests::drive(
            &mut original.joint,
        );
        original
            .fixture
            .progress(
                ProgressSelection::All,
                ProgressBudget {
                    actors: 0,
                    hosts: 1,
                    timers: 0,
                },
            )
            .await;
        if original
            .process
            .actor_runtime()
            .input_inventory(actor, None, 64)
            .unwrap()
            .iter()
            .any(|input| input.class == InputClass::Lifecycle && input.phase == InputPhase::Filled)
        {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "original Registry/Bootstrap did not fill the LP's first input"
        );
    }
    let before = original
        .process
        .actor_runtime()
        .observation_snapshot()
        .unwrap();
    metrics.sample();
    let before_metrics = registry.gather();
    let before_host = gauge(
        &before_metrics,
        "novarocks_frontend_control_runtime_host_turns",
        &[("resource", "turns")],
    )
    .unwrap();
    let before_handoffs = gauge(
        &before_metrics,
        "novarocks_frontend_control_runtime_host_turns",
        &[("resource", "handoffs")],
    )
    .unwrap();
    let (input, effects) = loop {
        original
            .fixture
            .progress(
                ProgressSelection::All,
                ProgressBudget {
                    actors: 1,
                    hosts: 0,
                    timers: 0,
                },
            )
            .await;
        let mut found = None;
        for (_, input) in original
            .process
            .actor_runtime()
            .committed_batches(actor, None, 64)
            .unwrap()
        {
            let effects = original
                .process
                .actor_runtime()
                .effect_inventory(actor, input, None, 64)
                .unwrap();
            for (descriptor, state) in &effects {
                if let Some(FrontendEffect::Logical(envelope)) = original
                    .process
                    .actor_runtime()
                    .pending_effect_metadata(actor, input, descriptor.id)
                    .unwrap()
                    && let crate::control_runtime::logical::adapter::LogicalHostEffect::Protocol(
                        effect,
                    ) = &envelope.body
                    && matches!(&effect.body, LogicalEffectBody::Native(NativeEffect::ActivateAttempt { activation: actual, .. }) if *actual == activation)
                {
                    assert_eq!(*state, EffectState::Pending);
                    assert_eq!(descriptor.id.get(), effect.action.get());
                    assert_eq!(descriptor.work.scope, bound.registration.scope_id());
                    assert!(found.is_none(), "only one original initial activation");
                    found = Some((input, effects.clone()));
                }
            }
        }
        if let Some(found) = found {
            break found;
        }
        assert!(
            Instant::now() < deadline,
            "original Bootstrap/Started did not commit its LP activation"
        );
    };

    // This Started batch's two original effects have not been claimed:
    // RegisterObligation, then Native ActivateAttempt. Other real startup
    // inputs may already have committed their own independent batches.
    assert_eq!(effects.len(), 2);
    assert!(
        effects
            .iter()
            .all(|(_, state)| *state == EffectState::Pending)
    );
    let first = original
        .process
        .actor_runtime()
        .pending_effect_metadata(actor, input, effects[0].0.id)
        .unwrap()
        .unwrap();
    assert!(matches!(first, FrontendEffect::Logical(envelope)
        if matches!(&envelope.body, crate::control_runtime::logical::adapter::LogicalHostEffect::Protocol(effect)
            if matches!(&effect.body, LogicalEffectBody::Governance(GovernanceEffect::RegisterObligation { activation: actual, .. }) if *actual == activation))));
    assert!(original.logical.probe.installed.lock().unwrap().is_none());
    assert!(original.logical.probe.root.lock().unwrap().is_none());
    assert!(
        !original
            .process
            .actor_runtime()
            .snapshot(actor)
            .unwrap()
            .failed
    );
    // No Host turn has retired any of these actual committed batches. Count
    // them in original commitment order, including a real zero-effect Timer.
    // Do not infer the number of Core rounds from our desired Started cut.
    assert!(before.inputs.iter().all(|input| input.steps == 0));
    let batches = original
        .process
        .actor_runtime()
        .committed_batches(actor, None, 64)
        .unwrap();
    let mut class_steps = [0_u64; InputClass::ALL.len()];
    let mut per_step_effects = Vec::new();
    // These exact two original purposes run without a Core action. Their
    // actual exit fills HostSettled, whose adapter path does not enter Core.
    // Started uses the independent Bootstrap inlet, not this job-exit source.
    let geometry = crate::control_runtime::logical::geometry::LogicalGeometry::derive(
        bound.registration.scope_id(),
        &bound.start,
    )
    .unwrap();
    let mut routing = crate::control_runtime::logical::purpose::LogicalPurposes::new(&bound.start);
    let carrier_sources = [
        crate::control_runtime::logical::adapter::LogicalHostEffect::Bootstrap,
        crate::control_runtime::logical::adapter::LogicalHostEffect::ClientCloseObservation,
    ]
    .map(|effect| geometry.source(routing.effect(&effect).unwrap()).unwrap());
    for (ordinal, batch_input) in &batches {
        let custody = original
            .process
            .actor_runtime()
            .input_custody(actor, *batch_input)
            .unwrap()
            .expect("same unretired original input");
        assert_eq!(custody.phase, InputPhase::Committed);
        class_steps[custody.class.index()] += 1;
        let records = original
            .process
            .actor_runtime()
            .effect_inventory(actor, *batch_input, None, 64)
            .unwrap();
        assert!(
            records
                .iter()
                .all(|(_, state)| *state == EffectState::Pending)
        );
        for (descriptor, _) in &records {
            let effect = original
                .process
                .actor_runtime()
                .pending_effect_metadata(actor, *batch_input, descriptor.id)
                .unwrap()
                .unwrap();
            let FrontendEffect::Logical(envelope) = effect else {
                panic!("only the original LP may produce effects before AP installation")
            };
            let crate::control_runtime::logical::adapter::LogicalHostEffect::Protocol(effect) =
                &envelope.body
            else {
                panic!("no join-ready transition precedes this pending activation")
            };
            assert!(
                matches!(&effect.body,
                LogicalEffectBody::Native(NativeEffect::ActivateAttempt { activation: actual, .. })
                | LogicalEffectBody::Governance(GovernanceEffect::RegisterObligation { activation: actual, .. }) if *actual == activation)
                    || matches!(
                        &effect.body,
                        LogicalEffectBody::RegistryJoinReply { ready: false, .. }
                    ),
                "only original startup actions and a negative Registry readiness reply precede AP installation"
            );
        }
        eprintln!(
            "original LP metrics committed cut: batch={ordinal:?}, input={batch_input:?}, source={:?}, class={:?}, phase={:?}, effects={:?}",
            custody.source, custody.class, custody.phase, records
        );
        if carrier_sources.contains(&custody.source) {
            assert_eq!(custody.class, InputClass::Lifecycle);
            assert!(custody.standing_input_id.is_some());
            assert!(
                records.is_empty(),
                "carrier-only HostSettled emits no Core effect"
            );
        } else {
            per_step_effects.push(u64::try_from(records.len()).unwrap());
        }
    }
    let total_effects = per_step_effects.iter().sum::<u64>();
    let snapshot = bound.links.try_logical_observation().unwrap();
    assert_eq!(
        snapshot.completed_steps,
        u64::try_from(per_step_effects.len()).unwrap()
    );
    assert_eq!(
        snapshot.last_step_effects,
        *per_step_effects.last().unwrap()
    );
    assert_eq!(snapshot.total_effects, total_effects);
    assert_eq!(
        snapshot.max_step_effects,
        *per_step_effects.iter().max().unwrap()
    );
    // The two real pending service calls await results/exits. The Registry's
    // original readiness reply itself is a reply, not another service action.
    assert_eq!(snapshot.outstanding_actions, 2);
    // A non-Timer Started marks the fair scan dirty. Finishing that original
    // scan requests its bounded continuation at the SAME observed origin,
    // even though these two effects were already emitted in full.
    let original_tick =
        Tick(u64::try_from(bound.start.started_at.since_origin().as_nanos()).unwrap());
    assert_eq!(original.process.clock.now(), original_tick);
    assert_eq!(
        original
            .process
            .actor_runtime()
            .snapshot(actor)
            .unwrap()
            .last_progress,
        original_tick
    );
    assert_eq!(snapshot.next_deadline_ns, Some(original_tick.0));
    assert_eq!(
        original.process.actor_runtime().next_deadline(),
        Some(original_tick)
    );

    let after = original
        .process
        .actor_runtime()
        .observation_snapshot()
        .unwrap();
    let standings = original
        .process
        .actor_runtime()
        .standing_input_inventory(actor, None, usize::MAX)
        .unwrap();
    for class in InputClass::ALL {
        let old = before.inputs[class.index()];
        let current = after.inputs[class.index()];
        assert_eq!(current.steps - old.steps, class_steps[class.index()]);
        assert_eq!(current.panicked_steps, old.panicked_steps);
        assert!(
            current.input_credits_held
                >= u64::try_from(standings.iter().filter(|role| role.class == class).count())
                    .unwrap()
        );
    }
    assert!(
        after.inputs[InputClass::Lifecycle.index()].charged_work
            > before.inputs[InputClass::Lifecycle.index()].charged_work
    );
    metrics.sample();
    let families = registry.gather();
    for source in ["counters", "effects", "host", "logical"] {
        assert_gauge(
            &families,
            "novarocks_frontend_control_runtime_observation_available",
            &[("source", source)],
            1,
        );
    }
    for current in after.inputs {
        let class = format!("{:?}", current.class);
        for (resource, value) in [
            ("steps", current.steps),
            ("credits_held", current.input_credits_held),
            ("charged_work", current.charged_work),
            ("max_step_work", current.maximum_step_work),
            ("panicked_steps", current.panicked_steps),
        ] {
            assert_gauge(
                &families,
                "novarocks_frontend_control_runtime_inputs",
                &[("class", &class), ("resource", resource)],
                value,
            );
        }
        for (kind, duration) in [
            ("handoff_to_step", current.handoff_to_step),
            ("step_elapsed", current.step_elapsed),
        ] {
            let duration = duration.expect("same real observed fixture clock");
            for (suffix, value) in [
                ("samples", duration.samples),
                ("total_ns", duration.total_ns),
                ("max_ns", duration.maximum_ns),
            ] {
                let resource = format!("{kind}_{suffix}");
                assert_gauge(
                    &families,
                    "novarocks_frontend_control_runtime_inputs",
                    &[("class", &class), ("resource", &resource)],
                    value,
                );
            }
        }
    }
    let pending = original
        .process
        .host
        .try_effect_observation()
        .unwrap()
        .unwrap();
    assert_eq!(pending.pending, total_effects);
    assert_eq!(pending.claimed_not_handed_off, 0);
    assert_eq!(
        pending.unindexed_inputs,
        u64::try_from(batches.len()).unwrap()
    );
    for (state, value) in [
        ("pending", pending.pending),
        ("claimed_not_handed_off", pending.claimed_not_handed_off),
        ("unindexed_inputs", pending.unindexed_inputs),
    ] {
        assert_gauge(
            &families,
            "novarocks_frontend_control_runtime_pending_effects",
            &[("state", state)],
            value,
        );
    }
    for (resource, value) in [
        ("actors", 1),
        ("core_steps", snapshot.completed_steps),
        ("last_step_effects", snapshot.last_step_effects),
        ("emitted_effects", snapshot.total_effects),
        ("max_step_effects", snapshot.max_step_effects),
        ("outstanding_actions", snapshot.outstanding_actions),
        ("deadline_present", 1),
    ] {
        assert_gauge(
            &families,
            "novarocks_frontend_control_runtime_logical",
            &[("resource", resource)],
            value,
        );
    }
    assert_gauge(
        &families,
        "novarocks_frontend_control_runtime_logical",
        &[("resource", "next_deadline_ns")],
        original_tick.0,
    );
    assert_eq!(
        gauge(
            &families,
            "novarocks_frontend_control_runtime_host_turns",
            &[("resource", "turns")]
        ),
        Some(before_host)
    );
    assert_eq!(
        gauge(
            &families,
            "novarocks_frontend_control_runtime_host_turns",
            &[("resource", "handoffs")]
        ),
        Some(before_handoffs)
    );
    for (name, allowed) in [
        (
            "novarocks_frontend_control_runtime_inputs",
            &["class", "resource"][..],
        ),
        (
            "novarocks_frontend_control_runtime_logical",
            &["resource"][..],
        ),
        (
            "novarocks_frontend_control_runtime_pending_effects",
            &["state"][..],
        ),
        (
            "novarocks_frontend_control_runtime_host_turns",
            &["resource"][..],
        ),
    ] {
        let family = families
            .iter()
            .find(|family| family.get_name() == name)
            .unwrap();
        assert!(family.get_metric().iter().all(|metric| {
            metric
                .get_label()
                .iter()
                .all(|label| allowed.contains(&label.get_name()))
        }));
    }
    // Do not consume the caller's actual history prefix; its final checker
    // still needs the same original Work/Scope/input/effect facts.
    assert!(history.is_complete());
}
