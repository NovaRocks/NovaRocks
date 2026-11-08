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
use crate::task_execution::protocol_facts::AttemptProtocolFactsReceiver;
use novarocks_attempt_protocol::publication::{ProtocolPublication, ProtocolTaskChange};
use novarocks_execution_contract::{TaskOutputFacts, TaskState, TaskStatus, TaskStatusVersion};
use novarocks_physical_plan::PlanVersionId;
use novarocks_query_application::api::PlanSeal;
use novarocks_result_contract::PerKind;
use std::collections::BTreeSet;

fn fixture() -> (
    PublicationRecord,
    AttemptProtocolFactsReceiver,
    TaskIdentity,
    PlanSeal,
) {
    // Reuse the real application-bound graph before any Task or lifecycle work.
    let round = crate::task_execution::tests::manifest_unstarted_regression_fixture();
    let graph = round.execution().graph();
    let root = graph.root_identity();
    let plan = PlanSeal::Version(PlanVersionId::try_new([192; 16]).unwrap());
    let (protocol, receiver) = AttemptProtocolFactsPublisher::for_graph(
        plan,
        graph,
        &BTreeSet::new(),
        &PerKind {
            write_counts: None,
            delete_verification: None,
        },
    )
    .unwrap();
    let activation = AttemptActivationIdentity::from_parts(92, root.query_execution_id(), 1);
    (
        PublicationRecord {
            activation,
            root: None,
            protocol,
            profile: None,
            diagnostic_records: None,
            ready: None,
            #[cfg(novarocks_dev_controls)]
            root_observer: None,
        },
        receiver,
        root,
        plan,
    )
}

fn batch(revision: u64, changes: Vec<ProtocolTaskChange>) -> Publication {
    Publication::ProtocolBatch(ProtocolPublication {
        owner_revision: revision,
        changes,
        selected_failure: None,
        root_success_sealed: false,
    })
}

#[test]
fn protocol_publication_preserves_actual_publish_and_noop_receipts() {
    let (mut record, mut receiver, root, plan) = fixture();
    let activation = record.activation;
    assert_eq!(
        record.publish(batch(0, Vec::new()), None).unwrap(),
        PublicationApplied::ProtocolBatch { published: true }
    );
    let initial = receiver
        .take_changes(activation, root, plan)
        .unwrap()
        .unwrap();
    assert!(initial.is_initial());
    let initial_revision = initial.revision();
    assert_eq!(
        record.publish(batch(0, Vec::new()), None).unwrap(),
        PublicationApplied::ProtocolBatch { published: false }
    );
    assert!(
        receiver
            .take_changes(activation, root, plan)
            .unwrap()
            .is_none()
    );

    let status = TaskStatus::try_new(
        root,
        TaskStatusVersion::new(1).unwrap(),
        TaskState::Running,
        None,
        TaskOutputFacts::default(),
    )
    .unwrap();
    assert_eq!(
        record
            .publish(
                batch(
                    1,
                    vec![ProtocolTaskChange {
                        identity: root,
                        status: Some(status.clone()),
                        first_writer_witness: None,
                    }]
                ),
                None
            )
            .unwrap(),
        PublicationApplied::ProtocolBatch { published: true }
    );
    let delta = receiver
        .take_changes(activation, root, plan)
        .unwrap()
        .unwrap();
    assert!(!delta.is_initial());
    assert_eq!(delta.revision(), initial_revision + 1);
    assert_eq!(delta.tasks().len(), 1);
    assert_eq!(delta.tasks()[0].identity(), root);
    assert_eq!(delta.tasks()[0].status(), Some(&status));
    assert_eq!(
        record.publish(batch(1, Vec::new()), None).unwrap(),
        PublicationApplied::ProtocolBatch { published: false }
    );
    assert!(
        receiver
            .take_changes(activation, root, plan)
            .unwrap()
            .is_none()
    );
}

#[test]
fn protocol_publication_preserves_typed_error_without_readmodel_mutation() {
    let (mut record, mut receiver, root, plan) = fixture();
    let activation = record.activation;
    record.publish(batch(0, Vec::new()), None).unwrap();
    receiver
        .take_changes(activation, root, plan)
        .unwrap()
        .unwrap();
    // An unknown Task must be rejected by the original frozen slot author.
    let foreign = TaskIdentity::new(
        root.query_execution_id(),
        root.stage_id(),
        novarocks_types::TaskId::new(u32::MAX).unwrap(),
        root.backend_process_id(),
    );
    let result = record.publish(
        batch(
            1,
            vec![ProtocolTaskChange {
                identity: foreign,
                status: None,
                first_writer_witness: None,
            }],
        ),
        None,
    );
    assert!(matches!(
        result,
        Err(PublicationError::Protocol(TaskExecutionError::Schedule(_)))
    ));
    assert!(
        receiver
            .take_changes(activation, root, plan)
            .unwrap()
            .is_none()
    );
    assert_eq!(
        record.publish(batch(1, Vec::new()), None).unwrap(),
        PublicationApplied::ProtocolBatch { published: true }
    );
    assert!(
        receiver
            .take_changes(activation, root, plan)
            .unwrap()
            .is_some()
    );
}

#[test]
fn final_info_publication_checks_original_profile_before_resolving_a_reference() {
    let (mut record, _, root, _) = fixture();
    let reference = novarocks_attempt_protocol::input::FinalTaskInfoRef::new(
        novarocks_coordination_contract::AttemptRequestToken::new(std::num::NonZeroU64::MIN),
        root,
    );
    assert!(matches!(
        record.publish(Publication::FinalTaskInfo(reference), None),
        Err(PublicationError::MissingProfileObserver)
    ));
}
