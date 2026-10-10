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

#![cfg(feature = "connector-conformance")]

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use bytes::Bytes;
use novarocks_spi::connector::{
    ConnectorControlRuntimeId, ConnectorDataMutation, ConnectorDataMutationExecuteRequest,
    ConnectorDataMutationLease, ConnectorDataMutationOperation, ConnectorDataMutationPlan,
    ConnectorDataMutationPlanSummary, ConnectorDataMutationPlanningRequest,
    ConnectorDataMutationReceipt, ConnectorDataMutationReconcileRequest,
    ConnectorDataMutationSourceScope, ConnectorError, ConnectorErrorKind,
    ConnectorInstanceDescriptor, ConnectorInstanceId, ConnectorListTablesRequest,
    ConnectorMetadata, ConnectorMutationFailure, ConnectorMutationFailureKind,
    ConnectorMutationOperationId, ConnectorNamespaceRequest, ConnectorProviderBindingKey,
    ConnectorProviderId, ConnectorRequestContext, ConnectorTableHandle, ConnectorTableIdentity,
    ConnectorTableMetadata, ConnectorTableRequest, ExternalMutationEffect,
    ExternalMutationEvidence, ExternalMutationFinalization, ExternalMutationOutcome,
    MAX_CONNECTOR_DATA_MUTATION_FILES, MAX_CONNECTOR_DATA_MUTATION_PROVIDER_PAYLOAD_BYTES,
    ProviderBindingEpoch,
};

fn context() -> ConnectorRequestContext {
    ConnectorRequestContext::try_new(
        Instant::now() + Duration::from_secs(30),
        novarocks_spi::connector::ConnectorStopOwner::new().view(),
        16 * 1024 * 1024,
        64 * 1024 * 1024,
    )
    .expect("context")
}

struct FakeMetadata {
    instance_id: ConnectorInstanceId,
}

impl ConnectorMetadata for FakeMetadata {
    fn instance_id(&self) -> &ConnectorInstanceId {
        &self.instance_id
    }

    fn namespace_exists(&self, _: ConnectorNamespaceRequest) -> Result<bool, ConnectorError> {
        unreachable!()
    }

    fn table_exists(&self, _: ConnectorTableRequest) -> Result<bool, ConnectorError> {
        unreachable!()
    }

    fn list_tables(
        &self,
        _: ConnectorListTablesRequest,
    ) -> Result<Vec<ConnectorTableIdentity>, ConnectorError> {
        unreachable!()
    }

    fn load_table(
        &self,
        _: ConnectorTableRequest,
    ) -> Result<ConnectorTableMetadata, ConnectorError> {
        unreachable!()
    }
}

#[derive(Clone, Copy)]
enum ExecuteMode {
    Committed,
    Uncommitted,
    Unknown,
    ForeignReceipt,
}

struct FakeDataMutation {
    descriptor: ConnectorInstanceDescriptor,
    key: ConnectorProviderBindingKey,
    plans: Mutex<HashMap<ConnectorMutationOperationId, ([u8; 32], ConnectorDataMutationPlan)>>,
    mode: Mutex<ExecuteMode>,
    execute_calls: Mutex<usize>,
}

impl FakeDataMutation {
    fn new(provider: &str, instance: &str, incarnation: [u8; 16]) -> Arc<Self> {
        let instance_id = ConnectorInstanceId::parse(instance).expect("instance ID");
        Arc::new(Self {
            descriptor: ConnectorInstanceDescriptor {
                provider_id: ConnectorProviderId::parse(provider).expect("provider ID"),
                instance_id: instance_id.clone(),
            },
            key: ConnectorProviderBindingKey {
                instance_id,
                incarnation: ProviderBindingEpoch::from_bytes(incarnation),
            },
            plans: Mutex::new(HashMap::new()),
            mode: Mutex::new(ExecuteMode::Committed),
            execute_calls: Mutex::new(0),
        })
    }

    fn lease(self: &Arc<Self>) -> ConnectorDataMutationLease {
        ConnectorDataMutationLease::new(
            self.descriptor.clone(),
            ConnectorControlRuntimeId::from_bytes([1; 16]),
            self.key.incarnation,
            Arc::new(FakeMetadata {
                instance_id: self.key.instance_id.clone(),
            }),
            self.clone(),
            || {},
        )
        .expect("lease")
    }

    fn receipt(
        &self,
        plan: &ConnectorDataMutationPlan,
        descriptor: ConnectorInstanceDescriptor,
    ) -> ConnectorDataMutationReceipt {
        ConnectorDataMutationReceipt::try_new(
            descriptor,
            self.key.incarnation,
            plan.operation_id(),
            plan.operation_kind(),
            plan.request_digest(),
            plan.plan_digest(),
            plan.state_digest(),
            plan.summary(),
            Bytes::from_static(b"secret-receipt"),
        )
        .expect("receipt")
    }

    fn evidence(&self, plan: &ConnectorDataMutationPlan) -> ExternalMutationEvidence {
        ExternalMutationEvidence::try_new(
            1,
            self.descriptor.clone(),
            self.key.incarnation,
            plan.operation_id(),
            plan.operation_kind(),
            Bytes::from_static(b"secret-evidence"),
        )
        .expect("evidence")
    }
}

impl ConnectorDataMutation for FakeDataMutation {
    fn descriptor(&self) -> &ConnectorInstanceDescriptor {
        &self.descriptor
    }

    fn binding_key(&self) -> &ConnectorProviderBindingKey {
        &self.key
    }

    fn plan_mutation(
        &self,
        request: ConnectorDataMutationPlanningRequest,
    ) -> Result<ConnectorDataMutationPlan, ConnectorError> {
        let mut plans = self.plans.lock().expect("plans");
        if let Some((digest, plan)) = plans.get(&request.operation_id()) {
            if digest == &request.request_digest() {
                return Ok(plan.clone());
            }
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "operation request digest conflict",
            ));
        }
        let plan = ConnectorDataMutationPlan::try_new(
            &request,
            [7; 32],
            ConnectorDataMutationPlanSummary::try_new(2, 11, 101).expect("summary"),
            Some(source_scope()),
            Some(
                novarocks_spi::connector::ConnectorDataMutationAddFilesDomain::try_new_caller_managed_stable([9; 32])
                    .expect("domain"),
            ),
            Bytes::from_static(b"secret-plan"),
        )?;
        plans.insert(
            request.operation_id(),
            (request.request_digest(), plan.clone()),
        );
        Ok(plan)
    }

    fn execute(
        &self,
        request: ConnectorDataMutationExecuteRequest,
    ) -> Result<ExternalMutationOutcome<ConnectorDataMutationReceipt>, ConnectorError> {
        *self.execute_calls.lock().expect("calls") += 1;
        let failure = ConnectorMutationFailure::new(
            ConnectorMutationFailureKind::Unavailable,
            "scripted external result",
        );
        Ok(match *self.mode.lock().expect("mode") {
            ExecuteMode::Committed => ExternalMutationOutcome::KnownCommitted {
                effect: ExternalMutationEffect::Applied,
                receipt: self.receipt(&request.plan, self.descriptor.clone()),
                finalization: ExternalMutationFinalization::Complete,
            },
            ExecuteMode::Uncommitted => ExternalMutationOutcome::KnownUncommitted {
                failure,
                cleanup: novarocks_spi::connector::ExternalMutationFinalization::Complete,
            },
            ExecuteMode::Unknown => ExternalMutationOutcome::CommitUnknown {
                failure,
                evidence: self.evidence(&request.plan),
            },
            ExecuteMode::ForeignReceipt => ExternalMutationOutcome::KnownCommitted {
                effect: ExternalMutationEffect::Applied,
                receipt: self.receipt(
                    &request.plan,
                    ConnectorInstanceDescriptor {
                        provider_id: self.descriptor.provider_id.clone(),
                        instance_id: ConnectorInstanceId::parse("foreign").expect("foreign"),
                    },
                ),
                finalization: ExternalMutationFinalization::Complete,
            },
        })
    }

    fn reconcile(
        &self,
        request: ConnectorDataMutationReconcileRequest,
    ) -> Result<ExternalMutationOutcome<ConnectorDataMutationReceipt>, ConnectorError> {
        let plan = self
            .plans
            .lock()
            .expect("plans")
            .get(&request.operation_id)
            .expect("planned operation")
            .1
            .clone();
        Ok(ExternalMutationOutcome::KnownCommitted {
            effect: ExternalMutationEffect::Applied,
            receipt: self.receipt(&plan, self.descriptor.clone()),
            finalization: ExternalMutationFinalization::Complete,
        })
    }
}

fn planning_request(
    fake: &FakeDataMutation,
    operation_id: ConnectorMutationOperationId,
    source: &str,
) -> ConnectorDataMutationPlanningRequest {
    let table = ConnectorTableHandle::try_new(
        fake.key.instance_id.clone(),
        Bytes::from_static(b"secret-table-handle"),
    )
    .expect("table handle");
    ConnectorDataMutationPlanningRequest::try_new(
        operation_id,
        fake.key.clone(),
        ConnectorDataMutationOperation::register_existing_files(table, source).expect("operation"),
        context(),
    )
    .expect("planning request")
}

fn source_scope() -> ConnectorDataMutationSourceScope {
    ConnectorDataMutationSourceScope::try_new_directory([7; 32]).expect("source scope")
}

#[test]
fn two_non_iceberg_providers_obey_replay_conflict_and_redaction_contracts() {
    for fake in [
        FakeDataMutation::new("fake-alpha", "alpha", [1; 16]),
        FakeDataMutation::new("fake-beta", "beta", [2; 16]),
    ] {
        let lease = fake.lease();
        let operation_id = ConnectorMutationOperationId::from_bytes([3; 16]);
        let request = planning_request(&fake, operation_id, "s3://bucket/source");
        let plan = lease.plan_mutation(request.clone()).expect("first plan");
        assert_eq!(
            lease.plan_mutation(request).expect("idempotent replay"),
            plan
        );
        let conflict = planning_request(&fake, operation_id, "s3://bucket/changed");
        assert_eq!(
            lease
                .plan_mutation(conflict)
                .expect_err("same operation with another request must fail")
                .kind(),
            ConnectorErrorKind::InvalidRequest
        );

        let debug = format!("{plan:?}");
        assert!(debug.contains("provider_payload_len"));
        assert!(!debug.contains("secret-plan"));
        assert!(!debug.contains("secret-table-handle"));
    }
}

#[test]
fn lease_validates_all_execute_and_reconcile_outcomes() {
    let fake = FakeDataMutation::new("fake-alpha", "alpha", [4; 16]);
    let lease = fake.lease();

    for mode in [ExecuteMode::Committed, ExecuteMode::Uncommitted] {
        *fake.mode.lock().expect("mode") = mode;
        let request = planning_request(&fake, ConnectorMutationOperationId::new(), "s3://x");
        let plan = lease.plan_mutation(request).expect("plan");
        lease
            .execute(
                ConnectorDataMutationExecuteRequest::try_new(plan, context()).expect("execute"),
            )
            .expect("valid outcome");
    }

    *fake.mode.lock().expect("mode") = ExecuteMode::Unknown;
    let request = planning_request(&fake, ConnectorMutationOperationId::new(), "s3://unknown");
    let plan = lease.plan_mutation(request).expect("plan");
    let outcome = lease
        .execute(
            ConnectorDataMutationExecuteRequest::try_new(plan.clone(), context()).expect("execute"),
        )
        .expect("unknown outcome");
    let ExternalMutationOutcome::CommitUnknown { evidence, .. } = outcome else {
        panic!("expected unknown outcome")
    };
    let reconciled = lease
        .reconcile(
            ConnectorDataMutationReconcileRequest::try_new(&plan, evidence, context())
                .expect("reconcile request"),
        )
        .expect("reconcile");
    assert!(matches!(
        reconciled,
        ExternalMutationOutcome::KnownCommitted { .. }
    ));

    *fake.mode.lock().expect("mode") = ExecuteMode::ForeignReceipt;
    let request = planning_request(&fake, ConnectorMutationOperationId::new(), "s3://foreign");
    let plan = lease.plan_mutation(request).expect("plan");
    assert_eq!(
        lease
            .execute(
                ConnectorDataMutationExecuteRequest::try_new(plan, context()).expect("execute")
            )
            .expect_err("foreign receipt must be rejected")
            .kind(),
        ConnectorErrorKind::InvalidRequest
    );
}

#[test]
fn bounds_and_exact_generation_fail_closed() {
    assert_eq!(
        ConnectorDataMutationPlanSummary::try_new(MAX_CONNECTOR_DATA_MUTATION_FILES + 1, 0, 0,)
            .expect_err("file bound")
            .kind(),
        ConnectorErrorKind::ResourceExhausted
    );

    let fake = FakeDataMutation::new("fake-beta", "beta", [8; 16]);
    let request = planning_request(&fake, ConnectorMutationOperationId::new(), "s3://bounded");
    assert_eq!(
        ConnectorDataMutationPlan::try_new(
            &request,
            [0; 32],
            ConnectorDataMutationPlanSummary::default(),
            Some(source_scope()),
            Some(
                novarocks_spi::connector::ConnectorDataMutationAddFilesDomain::try_new_caller_managed_stable([9; 32])
                    .expect("domain"),
            ),
            Bytes::from(vec![
                0;
                MAX_CONNECTOR_DATA_MUTATION_PROVIDER_PAYLOAD_BYTES + 1
            ]),
        )
        .expect_err("payload bound")
        .kind(),
        ConnectorErrorKind::ResourceExhausted
    );

    assert_eq!(
        ConnectorDataMutationLease::new(
            fake.descriptor.clone(),
            ConnectorControlRuntimeId::from_bytes([2; 16]),
            ProviderBindingEpoch::from_bytes([9; 16]),
            Arc::new(FakeMetadata {
                instance_id: fake.key.instance_id.clone(),
            }),
            fake,
            || {},
        )
        .err()
        .expect("wrong generation")
        .kind(),
        ConnectorErrorKind::InvalidRequest
    );
}
