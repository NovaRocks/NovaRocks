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

//! Original RF cursor and error distinctions at the pure attempt owner.
//! Feedback-admission replies below are passive external facts, not FA I/O proof.

use super::*;
use crate::error::TaskExecutionError;
use crate::graph::TaskGraph;
use novarocks_types::TaskId;

fn same_backend_tasks() -> (QueryTaskExecution, TaskIdentity, TaskIdentity) {
    let (mut frozen, _) = crate::test_support::frozen_graph_with_strict_input(false);
    let first = frozen.tasks[&TaskId::new(1).unwrap()].identity;
    let original = frozen.tasks[&TaskId::new(2).unwrap()].identity;
    let second = TaskIdentity::new(
        original.query_execution_id(),
        original.stage_id(),
        original.task_id(),
        first.backend_process_id(),
    );
    let participation = frozen.tasks[&first.task_id()].participation;
    let node = frozen.tasks.get_mut(&second.task_id()).unwrap();
    node.identity = second;
    node.participation = participation;
    for tasks in frozen.stages.values_mut() {
        for task in tasks.iter_mut() {
            if *task == original {
                *task = second;
            }
        }
    }
    for edge in frozen.edges.values_mut() {
        for task in edge
            .producers
            .iter_mut()
            .chain(edge.destinations.iter_mut())
        {
            if *task == original {
                *task = second;
            }
        }
    }
    let execution =
        crate::test_support::execution_with_graph(TaskGraph::from_frozen(frozen).unwrap());
    (execution, first, second)
}

#[test]
fn original_two_tasks_on_one_backend_keep_independent_feedback_cursors() {
    let (mut execution, first, second) = same_backend_tasks();
    assert_eq!(first.backend_process_id(), second.backend_process_id());
    assert_ne!(first, second);
    let source = PayloadRef(key(11));
    execution.bind_dynamic_feedback(Some(source));
    assert!(matches!(advance(&mut execution, true, 1),
        Some(EffectBody::StartPump { pump, kind: PumpKind::DynamicFilters }) if pump == source));
    assert!(
        advance(&mut execution, true, 2).is_none(),
        "no advertisement means no fetch"
    );
    let mut reads = Vec::new();
    for (task, read_id, admit_id, idle_id, payload) in [
        (first, 3, 4, 5, PayloadRef(key(41))),
        (second, 7, 8, 9, PayloadRef(key(42))),
    ] {
        advertise(&mut execution, task, 2, 1);
        let Some(EffectBody::ReadDynamicFilters {
            task: actual,
            request,
            advertised,
            acknowledged,
            ..
        }) = advance(&mut execution, true, read_id)
        else {
            panic!("each original Task at the same version owes its own read");
        };
        assert_eq!(
            (actual, request, advertised, acknowledged),
            (task, token(read_id), version(1), None)
        );
        reads.push((actual, acknowledged));
        assert!(
            execution
                .observe_dynamic_read(
                    request,
                    task,
                    DynamicFilterRead::Available {
                        version: version(1),
                        feedback: payload,
                    }
                )
                .is_none()
        );
        assert!(matches!(advance(&mut execution, true, admit_id),
            Some(EffectBody::AdmitDynamicFilterFeedback { request: r, task: t, version: v, feedback })
                if r == request && t == task && v == version(1) && feedback == payload));
        assert!(
            execution
                .observe_dynamic_admission(request, task, Ok(()))
                .is_none()
        );
        assert!(advance(&mut execution, true, idle_id).is_none());
        advertise(&mut execution, task, 3, 1);
        assert!(
            advance(&mut execution, true, idle_id + 1).is_none(),
            "the same Task advertisement is not read twice"
        );
    }
    assert_eq!(reads, [(first, None), (second, None)]);
    assert_eq!(
        execution.dynamic_feedback.cursors[&first].settled,
        Some(version(1))
    );
    assert_eq!(
        execution.dynamic_feedback.cursors[&second].settled,
        Some(version(1))
    );
}

#[test]
fn original_unretained_advertised_version_four_is_settled_once() {
    let mut execution = execution_with_strict_input();
    let task = execution.graph().root_identity();
    execution.bind_dynamic_feedback(Some(PayloadRef(key(1))));
    advertise(&mut execution, task, 2, 4);
    assert!(matches!(
        advance(&mut execution, true, 1),
        Some(EffectBody::StartPump { .. })
    ));
    assert!(matches!(advance(&mut execution, true, 2),
        Some(EffectBody::ReadDynamicFilters { task: actual, request, advertised, acknowledged: None, .. })
            if actual == task && request == token(2) && advertised == version(4)));
    assert!(
        execution
            .observe_dynamic_read(token(2), task, DynamicFilterRead::Empty)
            .is_none()
    );
    assert_eq!(
        execution.dynamic_feedback.cursors[&task].settled,
        Some(version(4))
    );
    for n in 3..=5 {
        assert!(
            advance(&mut execution, true, n).is_none(),
            "the empty original version is not fetched again"
        );
    }
}

#[test]
fn original_unavailable_read_retries_after_progress_and_preserves_exact_refusal() {
    let mut execution = execution_with_strict_input();
    let task = execution.graph().root_identity();
    execution.bind_dynamic_feedback(Some(PayloadRef(key(1))));
    advertise(&mut execution, task, 2, 1);
    assert!(matches!(
        advance(&mut execution, true, 1),
        Some(EffectBody::StartPump { .. })
    ));
    let Some(EffectBody::ReadDynamicFilters {
        operation: first,
        request,
        task: actual,
        acknowledged,
        ..
    }) = advance(&mut execution, true, 2)
    else {
        panic!("the advertised original Task must be fetched");
    };
    assert_eq!((actual, request, acknowledged), (task, token(2), None));
    assert!(
        execution
            .observe_dynamic_read(
                request,
                task,
                DynamicFilterRead::Unavailable(HostFailureRef(key(91)))
            )
            .is_none()
    );
    assert!(
        advance(&mut execution, true, 3).is_none(),
        "the read's own completion is not independent retry progress"
    );
    execution.signal_dynamic_feedback_progress();
    let Some(EffectBody::ReadDynamicFilters {
        operation: retry,
        request,
        task: actual,
        advertised,
        acknowledged,
    }) = advance(&mut execution, true, 4)
    else {
        panic!("the original lost read retries after independent protocol progress");
    };
    assert_ne!(retry, first);
    assert_eq!(
        (actual, request, advertised, acknowledged),
        (task, token(4), version(1), None)
    );
    let original = AttemptFailure::TaskProtocol(Box::new(TaskExecutionError::Schedule(format!(
        "dynamic filter read for task {task} was refused: not observable"
    ))));
    assert_eq!(
        execution.observe_dynamic_read(request, task, DynamicFilterRead::Refused(original.clone())),
        Some(original)
    );
    assert_eq!(execution.dynamic_feedback.cursors[&task].settled, None);
}

#[test]
fn original_undeclared_feedback_source_emits_no_owner_or_read() {
    let mut execution = execution_with_strict_input();
    let task = execution.graph().root_identity();
    assert!(execution.dynamic_feedback.source.is_none());
    advertise(&mut execution, task, 2, 1);
    for n in 1..=3 {
        assert!(advance(&mut execution, true, n).is_none());
        assert!(!execution.has_dynamic_feedback_work(true));
    }
}
