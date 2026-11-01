// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file distributed
// with this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use novarocks_physical_plan::{FragmentId, StagePlacementPolicy as Placement};

// Pure scheduling needs no cluster process. These facts use the same actual
// schedule/edge author as production and independent expected widths.
fn independently_placed_write(work: NativeScanWork) -> ExecutionSchedulingFacts {
    let mut plan = scan_edge_inputs(work);
    plan.execution_anchor_fragment_id = 4;
    plan.topological_fragment_order = vec![1, 2, 3, 4];
    plan.fragments[1].placement = Placement::Spread {
        max_instances: NonZeroU32::new(3),
    };
    plan.fragments.push(FragmentSchedulingFacts {
        fragment_id: 3,
        placement: Placement::Spread {
            max_instances: NonZeroU32::new(2),
        },
        scans: Vec::new(),
    });
    plan.fragments.push(FragmentSchedulingFacts {
        fragment_id: 4,
        placement: Placement::Singleton,
        scans: Vec::new(),
    });
    plan.edges = [(1, 2, false), (1, 3, true), (2, 4, false), (3, 4, false)]
        .into_iter()
        .enumerate()
        .map(|(index, (source, target, hash))| SchedulingEdgeFacts {
            source_fragment_id: source,
            target_fragment_id: target,
            target_exchange_node_id: index as i32,
            stream_kind: if target == 4 {
                SchedulingStreamKind::Gather
            } else if hash {
                SchedulingStreamKind::Partitioned
            } else {
                SchedulingStreamKind::Other
            },
            hash_partitioned: hash,
        })
        .collect();
    plan
}

fn place(
    plan: &ExecutionSchedulingFacts,
    backends: &[BackendProcessId],
) -> Result<AttemptSchedule, AttemptScheduleError> {
    build_attempt_schedule(
        execution(7),
        FrontendProcessId::new_v7(),
        backends,
        plan,
        #[cfg(novarocks_dev_controls)]
        Ok(None),
    )
}

fn widths(schedule: &AttemptSchedule) -> Vec<usize> {
    schedule
        .fragments()
        .iter()
        .map(|f| f.tasks().len())
        .collect()
}

#[test]
fn independent_writer_widths_ignore_source_width_and_registry_order() {
    let live = backends(4);
    for source_units in [1, 4] {
        let plan = independently_placed_write(NativeScanWork::FrozenUnits {
            count: NonZeroUsize::new(source_units).unwrap(),
        });
        let first = place(&plan, &live).unwrap();
        assert_eq!(widths(&first), [source_units, 3, 2, 1]);
        let mut reversed = live.clone();
        reversed.reverse();
        let second = place(&plan, &reversed).unwrap();
        assert_eq!(widths(&second), [source_units, 3, 2, 1]);
        for (left, right) in first.fragments().iter().zip(second.fragments()) {
            assert_eq!(
                left.tasks()
                    .iter()
                    .map(|t| t.identity().backend_process_id())
                    .collect::<Vec<_>>(),
                right
                    .tasks()
                    .iter()
                    .map(|t| t.identity().backend_process_id())
                    .collect::<Vec<_>>()
            );
        }
        assert_eq!(first.edges()[0].producers().len(), source_units);
        assert_eq!(first.edges()[0].destinations().len(), 3);
        assert_eq!(first.edges()[1].destinations().len(), 2);
    }
}

#[test]
fn empty_work_and_one_backend_keep_all_required_end_participants() {
    let mut plan = independently_placed_write(NativeScanWork::Empty);
    assert_eq!(widths(&place(&plan, &backends(4)).unwrap()), [1, 3, 2, 1]);
    assert_eq!(widths(&place(&plan, &backends(1)).unwrap()), [1, 1, 1, 1]);
    plan.fragments[1].placement = Placement::Spread {
        max_instances: None,
    };
    assert_eq!(widths(&place(&plan, &backends(4)).unwrap()), [1, 4, 2, 1]);
}

#[test]
fn follow_sources_reads_only_the_explicit_predecessor_set() {
    let mut plan = independently_placed_write(NativeScanWork::Empty);
    plan.fragments.push(FragmentSchedulingFacts {
        fragment_id: 5,
        placement: Placement::FollowSources {
            sources: Box::from([FragmentId::new(3)]),
        },
        scans: Vec::new(),
    });
    plan.topological_fragment_order = vec![1, 2, 3, 5, 4];
    for source in [2, 3] {
        plan.edges.push(SchedulingEdgeFacts {
            source_fragment_id: source,
            target_fragment_id: 5,
            target_exchange_node_id: 10 + source as i32,
            stream_kind: SchedulingStreamKind::Partitioned,
            hash_partitioned: true,
        });
    }
    let schedule = place(&plan, &backends(4)).unwrap();
    assert_eq!(widths(&schedule), [1, 3, 2, 1, 2]);
    plan.fragments[4].placement = Placement::FollowSources {
        sources: Box::from([FragmentId::new(2), FragmentId::new(3)]),
    };
    assert_eq!(
        widths(&place(&plan, &backends(4)).unwrap()),
        [1, 3, 2, 1, 3]
    );
}

#[test]
fn conflicting_stage_policies_refuse_instead_of_silently_overriding_width() {
    let original = independently_placed_write(NativeScanWork::Empty);
    let live = backends(4);
    let mut plan = original.clone();
    plan.fragments[3].placement = Placement::Spread {
        max_instances: NonZeroU32::new(1),
    };
    assert!(
        place(&plan, &live)
            .unwrap_err()
            .to_string()
            .contains("anchor must declare singleton")
    );
    plan = original.clone();
    plan.edges[0].stream_kind = SchedulingStreamKind::Gather;
    assert!(
        place(&plan, &live)
            .unwrap_err()
            .to_string()
            .contains("gather input conflicts")
    );
    plan = original.clone();
    plan.fragments[1].placement = Placement::ScanWork;
    assert!(
        place(&plan, &live)
            .unwrap_err()
            .to_string()
            .contains("requires a declared scan")
    );
    for sources in [
        vec![],
        vec![FragmentId::new(1), FragmentId::new(1)],
        vec![FragmentId::new(3)],
    ] {
        plan = original.clone();
        plan.fragments[1].placement = Placement::FollowSources {
            sources: sources.into_boxed_slice(),
        };
        assert!(place(&plan, &live).is_err());
    }
    plan = original;
    plan.topological_fragment_order = vec![2, 1, 3, 4];
    assert!(
        place(&plan, &live)
            .unwrap_err()
            .to_string()
            .contains("topological fragment order")
    );
}
