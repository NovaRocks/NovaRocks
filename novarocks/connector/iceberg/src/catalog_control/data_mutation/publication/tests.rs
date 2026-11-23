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

mod registration;

use super::*;
use crate::catalog_control::data_mutation::tests::{
    exact_provider_with_empty_table, table_context, write_external_parquet,
};
use crate::commit::model::ArtifactKind;
use novarocks_spi::connector::ConnectorMetadata;
use std::sync::atomic::{AtomicUsize, Ordering};

#[derive(Clone, Copy)]
enum Behavior {
    Commit,
    PanicAfterIssue,
    Capacity(usize),
}

struct ControlledPublisher {
    base: StagingBase,
    template: MutationRecoveryTemplate,
    journal: PublicationJournal<DataPublication>,
    calls: Arc<AtomicUsize>,
    checked: Arc<Mutex<Option<ExternalMutationEvidence>>>,
    behavior: Behavior,
}

#[async_trait]
impl Publisher for ControlledPublisher {
    async fn load_target(
        &self,
        attempt: &IcebergCommitAttempt,
    ) -> crate::iceberg::Result<StagingBase> {
        attempt.check_active()?;
        let StagingBase::Existing {
            metadata,
            metadata_location,
        } = &self.base
        else {
            unreachable!("data mutation test base")
        };
        Ok(StagingBase::Existing {
            metadata: metadata.clone(),
            metadata_location: metadata_location.clone(),
        })
    }
    fn preflight_recovery(
        &self,
        request: &FrozenRequest,
        operation: &IcebergCommitOperation,
    ) -> crate::iceberg::Result<()> {
        let facts = FrozenPublicationFacts::from_request(request, operation)?;
        let evidence = self
            .template
            .evidence(RecoveryPublication::Dispatched {
                facts: Box::new(facts),
            })
            .map_err(format_error)?;
        let receipt = self
            .template
            .publication_receipt(request.ref_snapshot_after(request.target_ref()).unwrap())
            .map_err(format_error)?;
        *self.checked.lock().unwrap() = Some(evidence.clone());
        self.journal.record(evidence, receipt, request)
    }
    async fn dispatch_once(&self, request: FrozenRequest) -> CatalogOutcome<CommitProof> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        match self.behavior {
            Behavior::Commit | Behavior::Capacity(_) => CatalogOutcome::committed(
                CommitProof::applied(request.ref_snapshot_after(request.target_ref())),
                ExternalMutationEffect::Applied,
            ),
            Behavior::PanicAfterIssue => panic!("injected data mutation bridge after issue"),
        }
    }
}

fn exercise_truncate(behavior: Behavior) {
    let (_executor, _warehouse, provider) = exact_provider_with_empty_table();
    let source = tempfile::tempdir().unwrap();
    let parquet = write_external_parquet(source.path(), vec![1, 2, 3]);
    let before = std::fs::read(&parquet).unwrap();
    let adapter = IcebergDataMutationAdapter::try_new(provider.clone()).unwrap();
    let metadata = provider
        .load_table(
            novarocks_spi::connector::ConnectorTableRequest {
                table: novarocks_spi::connector::ConnectorTableIdentity {
                    instance_id: provider.descriptor().instance_id.clone(),
                    namespace: "db".into(),
                    table: "t".into(),
                },
                resolution: novarocks_spi::connector::ConnectorTableResolution::StrictBaseTable,
                context: table_context(),
            },
            &mut novarocks_spi::connector::read_stack::ConnectorReadSchemaCreation::new(
                novarocks_type_contract::ValueTypeStorageLifetime::pure(),
                &mut |_| Ok(()),
                &mut |_| Ok(()),
            ),
        )
        .unwrap();
    let plan = adapter
        .plan_mutation(
            ConnectorDataMutationPlanningRequest::try_new(
                ConnectorMutationOperationId::new(),
                adapter.binding_key().clone(),
                ConnectorDataMutationOperation::truncate_main(metadata.table),
                table_context(),
            )
            .unwrap(),
        )
        .unwrap();
    let cached = adapter
        .plans
        .lock()
        .unwrap()
        .get(&plan.operation_id())
        .unwrap()
        .clone();
    let (payload, _) = cached.private.truncate_parts().unwrap();
    let marker = adapter.marker(&plan, payload);
    let template = MutationRecoveryTemplate::new(
        adapter.descriptor.clone(),
        adapter.key.clone(),
        plan.clone(),
        payload.into(),
    );
    let runtime = provider.runtime();
    let table = runtime
        .load_table_for_request("db", "t", &table_context())
        .unwrap()
        .into_table();
    let base = StagingBase::Existing {
        metadata: table.metadata().clone(),
        metadata_location: table.metadata_location().unwrap().into(),
    };
    let operation = IcebergCommitOperation::new(
        OperationToken::from_mutation(plan.operation_id()),
        table.metadata().location(),
        runtime.resources().planning_binding().clone(),
        table_context(),
        runtime.resources().catalog_runtime().clone(),
        OperationLimits::default(),
    )
    .unwrap();
    if let Behavior::Capacity(count) = behavior {
        // Unselected allocations remain owned, including outputs that were never opened.
        // Prepare them before the publication attempt so the recovery envelope records
        // an actual earlier attempt, rather than inventing a future attempt in preflight.
        let allocation = operation.begin_attempt().unwrap();
        for _ in 0..count {
            allocation.allocate(ArtifactKind::Manifest).unwrap();
        }
    }
    let journal = PublicationJournal::new(operation.token());
    let calls = Arc::new(AtomicUsize::new(0));
    let checked = Arc::new(Mutex::new(None));
    let publisher = journal.observe(Arc::new(ControlledPublisher {
        base,
        template,
        journal: journal.clone(),
        calls: calls.clone(),
        checked: checked.clone(),
        behavior,
    }));
    let intent = freeze_intent(&cached.private, &operation, &marker).unwrap();
    assert!(matches!(intent.protocol(), CommitProtocol::Truncate));
    assert_eq!(intent.start(), None);
    assert!(intent.dependencies().is_empty());
    let policy = RetryPolicy::from_properties(&HashMap::from([
        ("commit.retry.num-retries".into(), "1".into()),
        ("commit.retry.min-wait-ms".into(), "0".into()),
        ("commit.retry.max-wait-ms".into(), "0".into()),
    ]))
    .unwrap();
    let preparer = crate::commit::filter_delete::FilterDeletePreparer::always_true();
    let running = operation.clone();
    let result = operation.runtime().block_on(async move {
        attempt::run(
            &running,
            &intent,
            &PreparedChange::default(),
            &[&preparer],
            &publisher,
            policy,
        )
        .await
    });
    let backend = RegisteredIcebergDataMutationBackend::new(provider.clone());
    let outcome = backend
        .project_bridge(&operation, &journal, result)
        .unwrap();
    assert_eq!(std::fs::read(&parquet).unwrap(), before);
    assert!(
        operation
            .artifacts()
            .unwrap()
            .iter()
            .all(|record| !matches!(
                record.origin,
                crate::commit::model::ArtifactOrigin::Received { .. }
            ) && record.object.path() != format!("file://{}", parquet.display()))
    );
    match behavior {
        Behavior::PanicAfterIssue => {
            assert_eq!(calls.load(Ordering::SeqCst), 1);
            let ExternalMutationOutcome::CommitUnknown { evidence, .. } = outcome else {
                panic!("issued mutation lost its unknown verdict")
            };
            let payload: IcebergDataMutationEvidenceV3 =
                decode_canonical_json(evidence.provider_payload(), "actual recovery").unwrap();
            assert!(matches!(
                &payload.publication,
                RecoveryPublication::Dispatched { .. }
            ));
            validate_recovery_facts(&payload, plan.operation_id()).unwrap();
            let records = operation.artifacts().unwrap();
            assert!(!records.is_empty());
            for record in records {
                assert!(
                    std::path::Path::new(record.object.path().strip_prefix("file://").unwrap())
                        .exists()
                );
            }
            assert_eq!(evidence, checked.lock().unwrap().clone().unwrap());
            let before = operation.recovery_artifacts();
            let secondary = project_recovery_bridge(
                &operation,
                &journal,
                Err("injected second bridge failure".into()),
            );
            let ExternalMutationOutcome::CommitUnknown {
                evidence: secondary,
                ..
            } = secondary
            else {
                panic!("secondary bridge lost its exact issued carrier")
            };
            assert_eq!(secondary, evidence);
            assert_eq!(operation.recovery_artifacts(), before);
            assert_eq!(calls.load(Ordering::SeqCst), 1);
        }
        Behavior::Commit => {
            let ExternalMutationOutcome::KnownCommitted {
                receipt,
                finalization: ExternalMutationFinalization::Complete,
                ..
            } = outcome
            else {
                panic!("controlled mutation lost its commit proof")
            };
            let first = backend
                .project_bridge(
                    &operation,
                    &journal,
                    Err("injected first post-proof bridge failure".into()),
                )
                .unwrap();
            let second = project_recovery_bridge(
                &operation,
                &journal,
                Err("injected second recovery bridge failure".into()),
            );
            for outcome in [first, second] {
                let ExternalMutationOutcome::KnownCommitted {
                    receipt: preserved,
                    finalization: ExternalMutationFinalization::Failed(_),
                    ..
                } = outcome
                else {
                    panic!("post-proof bridge failure downgraded a proven commit")
                };
                assert_eq!(preserved, receipt);
            }
            for record in operation.recovery_artifacts() {
                assert!(
                    std::path::Path::new(record.object.path().trim_start_matches("file://"))
                        .exists()
                );
            }
            assert_eq!(calls.load(Ordering::SeqCst), 1);
        }
        Behavior::Capacity(_) => {
            assert_eq!(calls.load(Ordering::SeqCst), 1);
            assert!(
                matches!(
                    outcome,
                    ExternalMutationOutcome::KnownCommitted {
                        finalization: ExternalMutationFinalization::Complete,
                        ..
                    }
                ),
                "{outcome:?}"
            );
            let evidence = checked.lock().unwrap().clone().unwrap();
            assert!(
                evidence.provider_payload().len()
                    <= novarocks_spi::connector::MAX_EXTERNAL_MUTATION_EVIDENCE_BYTES
            );
            assert!(operation.artifacts().unwrap().is_empty());
        }
    }
}

#[test]
fn mutation_large_owned_ledger_publishes_with_bounded_recovery() {
    exercise_truncate(Behavior::Capacity(1200));
}

#[test]
fn mutation_secondary_recovery_bridge_keeps_exact_unknown_or_committed_outcome() {
    exercise_truncate(Behavior::PanicAfterIssue);
    exercise_truncate(Behavior::Commit);
}

mod delete;
