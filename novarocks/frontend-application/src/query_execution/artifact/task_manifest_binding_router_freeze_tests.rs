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

// Included only inside task_manifest_binding's existing cfg(test) module.
// Reuse its production bind_facts component seam and original support helpers.
mod completed_router_placement_fixture {
    use std::collections::{BTreeMap, BTreeSet};
    use std::num::NonZeroU32;
    use std::sync::Arc;

    use novarocks_execution_contract::{ExchangeEdgeId, QueryContextRef, TaskIdentity};
    use novarocks_query_application::api::BackendTopologySnapshot;
    use novarocks_types::{
        AttemptId, BackendProcessId, FrontendProcessId, QueryExecutionId, QueryId, StageId, TaskId,
    };

    use super::super::{
        FrozenAttemptTopology, PreparedManifestFacts, ProjectedEdge, ProjectedFragment,
        ProjectedTask, ScheduleFacts, TaskManifestBinding,
    };

    impl TaskManifestBinding {
        /// Bind explicit finite placements to the actual normal-encoder template.
        /// This exercises Native manifest custody and role projection, not the
        /// scheduler policy evaluator, Task activation or provider execution.
        pub(crate) fn assert_completed_router_placements_for_test(
            encoded: crate::query_execution::physical_encoding::EncodedCompletedPlan,
            source_fragment: crate::query_execution::artifact::FragmentId,
            summary: crate::native::fragment_encoder::frozen::StaticSinkTarget,
            writers: &[(
                crate::query_execution::artifact::FragmentId,
                novarocks_physical_plan::NodeId,
                novarocks_physical_plan::WriteTargetOrdinal,
            )],
        ) {
            let version = encoded.semantic_candidate.plan().version();
            let original_source = Arc::clone(encoded.native.get(source_fragment).unwrap());
            let template = encoded.into_attempt_template(version);
            assert_eq!(
                writers.len(),
                2,
                "the original MERGE fixture has two writer occurrences"
            );
            assert_ne!(
                writers[0].0, writers[1].0,
                "provider routes have independent writer fragments"
            );
            assert!(
                writers
                    .iter()
                    .all(|(fragment, _, _)| *fragment != source_fragment)
            );
            // Source and writer cardinalities deliberately differ in both epochs.
            // Only the structural binding is under test; no arbitrary DOP is
            // spliced into a LocalProgram or provider writer runtime.
            for widths in [[3_usize, 2, 1], [1_usize, 3, 2]] {
                let execution = QueryExecutionId::new(
                    QueryId::new(0x61, i64::try_from(widths[0]).unwrap()),
                    AttemptId::new(1).unwrap(),
                )
                .unwrap();
                let bound_template = crate::query_execution::artifact::RequestBoundNativeTemplate {
                    execution,
                    template: template.native.fork_for_attempt(),
                    affinity: Arc::new(
                        crate::query_execution::artifact::PreparedDistributedAttemptAffinity,
                    ),
                };
                let prepared =
                    PreparedManifestFacts::from_template(&bound_template.template).unwrap();
                assert!(
                    prepared.scans.is_empty(),
                    "literal SQL acquires no provider scans"
                );
                let processes: [BackendProcessId; 3] =
                    std::array::from_fn(|_| BackendProcessId::new_v7());
                let frontend = FrontendProcessId::new_v7();
                let mut tasks_by_fragment = BTreeMap::new();
                let mut contexts = BTreeMap::new();
                let mut fragments = Vec::new();
                let mut next_task = 1_u32;
                for (index, &fragment) in prepared.fragments.iter().enumerate() {
                    let stage_id = StageId::new(u32::try_from(index + 1).unwrap()).unwrap();
                    let width = if fragment == source_fragment {
                        widths[0]
                    } else if let Some((_, _, ordinal)) = writers
                        .iter()
                        .find(|(writer_fragment, _, _)| *writer_fragment == fragment)
                    {
                        match ordinal.get() {
                            0 => widths[1],
                            1 => widths[2],
                            _ => panic!("the original fixture has two targets"),
                        }
                    } else {
                        1
                    };
                    let mut tasks = Vec::new();
                    for (instance_index, &backend) in processes[..width].iter().enumerate() {
                        let context = *contexts
                            .entry(backend)
                            .or_insert_with(|| QueryContextRef::new(execution, frontend, backend));
                        let identity = TaskIdentity::new(
                            execution,
                            stage_id,
                            TaskId::new(next_task).unwrap(),
                            backend,
                        );
                        next_task += 1;
                        tasks.push(ProjectedTask {
                            identity,
                            context,
                            instance_index,
                            scan_work: Vec::new(),
                        });
                    }
                    tasks_by_fragment.insert(
                        fragment,
                        tasks.iter().map(|task| task.identity).collect::<Vec<_>>(),
                    );
                    fragments.push(ProjectedFragment {
                        fragment_id: fragment,
                        stage_id,
                        tasks,
                    });
                }
                let root = tasks_by_fragment[&prepared.execution_anchor][0];
                assert_eq!(tasks_by_fragment[&prepared.execution_anchor].len(), 1);
                let mut receiver_sources = BTreeMap::<(_, _), BTreeSet<TaskIdentity>>::new();
                for &(source, target, receiver) in prepared.edges.keys() {
                    receiver_sources
                        .entry((target, receiver))
                        .or_default()
                        .extend(tasks_by_fragment[&source].iter().copied());
                }
                let receiver_ordinals = receiver_sources
                    .into_iter()
                    .map(|(receiver, tasks)| {
                        let ordinals = tasks
                            .into_iter()
                            .enumerate()
                            .map(|(ordinal, task)| (task, u32::try_from(ordinal).unwrap()))
                            .collect::<BTreeMap<_, _>>();
                        (receiver, ordinals)
                    })
                    .collect::<BTreeMap<_, _>>();
                let edges = prepared
                    .edges
                    .keys()
                    .enumerate()
                    .map(|(index, &(source, target, receiver))| {
                        let ordinals = &receiver_ordinals[&(target, receiver)];
                        ProjectedEdge {
                            edge_id: ExchangeEdgeId::new(u32::try_from(index + 1).unwrap())
                                .unwrap(),
                            source_fragment_id: source,
                            target_fragment_id: target,
                            target_exchange_node_id: receiver,
                            producers: tasks_by_fragment[&source]
                                .iter()
                                .map(|&task| (task, ordinals[&task]))
                                .collect(),
                            destinations: tasks_by_fragment[&target].clone(),
                            sender_count: NonZeroU32::new(u32::try_from(ordinals.len()).unwrap())
                                .unwrap(),
                        }
                    })
                    .collect();
                let schedule = ScheduleFacts {
                    execution,
                    fragments,
                    edges,
                    contexts: contexts.values().copied().collect(),
                    root,
                };
                let targets = processes
                    .iter()
                    .enumerate()
                    .map(|(index, &process)| {
                        super::full_bind_supported(super::live_target(
                            index,
                            process,
                            u16::try_from(19010 + index).unwrap(),
                        ))
                    })
                    .collect();
                let (revision, backends, eligible) = FrozenAttemptTopology::capture(
                    BackendTopologySnapshot::try_new(0x61, targets).unwrap(),
                )
                .unwrap()
                .into_parts();
                let manifest = TaskManifestBinding::bind_facts(
                    bound_template,
                    execution,
                    schedule,
                    revision,
                    backends,
                    eligible,
                )
                .expect("the actual frozen Router template binds exact finite placements");
                manifest
                    .validate_submission_fragment(execution, &original_source)
                    .unwrap();
                let expected_sources = tasks_by_fragment[&source_fragment]
                    .iter()
                    .copied()
                    .collect::<BTreeSet<_>>();
                let actual_sources = manifest
                    .summary_sources()
                    .map(|(task, output)| {
                        assert_eq!(
                            output, summary,
                            "each source retains the exact original summary edge fact"
                        );
                        task
                    })
                    .collect::<BTreeSet<_>>();
                assert_eq!(actual_sources, expected_sources);
                assert_eq!(actual_sources.len(), widths[0]);
                let expected_writers = writers
                    .iter()
                    .flat_map(|&(fragment, node, target)| {
                        tasks_by_fragment[&fragment]
                            .iter()
                            .map(move |&task| (task, fragment, node, target))
                    })
                    .collect::<BTreeSet<_>>();
                let actual_writers = manifest
                    .writer_cohorts()
                    .map(|cohort| {
                        (
                            cohort.task(),
                            cohort.fragment(),
                            cohort.writer().node(),
                            cohort.writer().target(),
                        )
                    })
                    .collect::<BTreeSet<_>>();
                assert_eq!(actual_writers, expected_writers);
                assert_eq!(actual_writers.len(), widths[1] + widths[2]);
                assert!(
                    actual_writers
                        .iter()
                        .all(|(task, _, _, _)| !actual_sources.contains(task)),
                    "summary membership does not grant writer authority"
                );
                let summary_edge = manifest
                    .edges()
                    .iter()
                    .find(|edge| {
                        edge.source_fragment_id() == source_fragment
                            && edge.target_fragment_id() == summary.target_fragment_id()
                            && edge.target_exchange_node_id() == summary.target_exchange_node_id()
                    })
                    .expect("the exact frozen summary receiver is bound");
                assert_eq!(
                    summary_edge.delivery(),
                    novarocks_execution_contract::ExchangeDeliveryRequirement::MustDrain
                );
                assert_eq!(
                    summary_edge
                        .producers()
                        .iter()
                        .map(|producer| producer.task())
                        .collect::<BTreeSet<_>>(),
                    actual_sources
                );
                assert_eq!(summary_edge.sender_count().get() as usize, widths[0]);
                for edge in manifest.edges() {
                    assert_eq!(
                        edge.producers()
                            .iter()
                            .map(|producer| producer.task())
                            .collect::<BTreeSet<_>>(),
                        tasks_by_fragment[&edge.source_fragment_id()]
                            .iter()
                            .copied()
                            .collect()
                    );
                    assert_eq!(
                        edge.destinations().iter().copied().collect::<BTreeSet<_>>(),
                        tasks_by_fragment[&edge.target_fragment_id()]
                            .iter()
                            .copied()
                            .collect()
                    );
                }
            }
        }
    }
}
