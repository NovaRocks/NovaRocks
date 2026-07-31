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

use std::sync::{Arc, Mutex};

use bytes::Bytes;
use novarocks_frontend::connector::{
    ConnectorControlHost, ConnectorControlRetirement, ConnectorControlRetirementSink,
};
use novarocks_spi::connector::{
    ConnectorBeginScanRequest, ConnectorCatalogMutation, ConnectorCatalogMutationReceipt,
    ConnectorCatalogMutationReconcileRequest, ConnectorCatalogMutationRequest,
    ConnectorCatalogMutationResolver, ConnectorControlBinding, ConnectorControlResolver,
    ConnectorError, ConnectorErrorKind, ConnectorExecutionBindingKey,
    ConnectorExecutionDeclaration, ConnectorExecutionDistribution, ConnectorInstanceDescriptor,
    ConnectorInstanceId, ConnectorInstanceIncarnation, ConnectorListTablesRequest,
    ConnectorMetadata, ConnectorNamespaceRequest, ConnectorProviderId, ConnectorScan,
    ConnectorScanHandle, ConnectorScanPlanning, ConnectorSplit, ConnectorSplitPlanningRequest,
    ConnectorStatistics, ConnectorStatisticsResolver, ConnectorTableHandle, ConnectorTableMetadata,
    ConnectorTableRequest, ExternalMutationOutcome, StatisticsAccuracy, StatisticsCoverage,
    StatisticsDataVersion, StatisticsEvidence, StatisticsEvidenceRevision, StatisticsMetricRequest,
    StatisticsProvenance, StatisticsReadRequest, StatisticsReader,
};

struct TestControl {
    instance_id: ConnectorInstanceId,
    incarnation: ConnectorInstanceIncarnation,
}

impl ConnectorMetadata for TestControl {
    fn instance_id(&self) -> &ConnectorInstanceId {
        &self.instance_id
    }

    fn namespace_exists(
        &self,
        _request: ConnectorNamespaceRequest,
    ) -> Result<bool, ConnectorError> {
        Err(unsupported())
    }

    fn table_exists(&self, _request: ConnectorTableRequest) -> Result<bool, ConnectorError> {
        Err(unsupported())
    }

    fn list_tables(
        &self,
        _request: ConnectorListTablesRequest,
    ) -> Result<Vec<novarocks_spi::connector::ConnectorTableIdentity>, ConnectorError> {
        Err(unsupported())
    }

    fn load_table(
        &self,
        _request: ConnectorTableRequest,
    ) -> Result<ConnectorTableMetadata, ConnectorError> {
        Err(unsupported())
    }
}

impl ConnectorScanPlanning for TestControl {
    fn instance_id(&self) -> &ConnectorInstanceId {
        &self.instance_id
    }

    fn begin_scan(
        &self,
        _table: &ConnectorTableHandle,
        _request: ConnectorBeginScanRequest,
    ) -> Result<ConnectorScan, ConnectorError> {
        Err(unsupported())
    }

    fn plan_splits(
        &self,
        _scan: &ConnectorScanHandle,
        _request: ConnectorSplitPlanningRequest,
    ) -> Result<novarocks_spi::connector::ConnectorSplitPlanningResult, ConnectorError> {
        Err(unsupported())
    }
}

impl ConnectorExecutionDistribution for TestControl {
    fn declaration(
        &self,
        _context: &novarocks_spi::connector::ConnectorRequestContext,
    ) -> Result<ConnectorExecutionDeclaration, ConnectorError> {
        ConnectorExecutionDeclaration::try_new(
            ConnectorInstanceDescriptor {
                provider_id: ConnectorProviderId::parse("iceberg").expect("provider ID"),
                instance_id: self.instance_id.clone(),
            },
            self.incarnation,
            Bytes::from_static(b"binding=default"),
        )
    }
}

fn binding(incarnation: u8) -> ConnectorControlBinding {
    let provider = Arc::new(TestControl {
        instance_id: ConnectorInstanceId::parse("catalog.analytics").expect("instance ID"),
        incarnation: ConnectorInstanceIncarnation::from_bytes([incarnation; 16]),
    });
    ConnectorControlBinding::try_new(
        ConnectorInstanceDescriptor {
            provider_id: ConnectorProviderId::parse("iceberg").expect("provider ID"),
            instance_id: provider.instance_id.clone(),
        },
        provider.incarnation,
        provider.clone(),
        provider.clone(),
        provider,
        None,
    )
    .expect("control binding")
}

struct TestMutation {
    descriptor: ConnectorInstanceDescriptor,
    incarnation: ConnectorInstanceIncarnation,
}

impl ConnectorCatalogMutation for TestMutation {
    fn descriptor(&self) -> &ConnectorInstanceDescriptor {
        &self.descriptor
    }

    fn incarnation(&self) -> ConnectorInstanceIncarnation {
        self.incarnation
    }

    fn execute(
        &self,
        _request: ConnectorCatalogMutationRequest,
    ) -> Result<ExternalMutationOutcome<ConnectorCatalogMutationReceipt>, ConnectorError> {
        Err(unsupported())
    }

    fn reconcile(
        &self,
        _request: ConnectorCatalogMutationReconcileRequest,
    ) -> Result<ExternalMutationOutcome<ConnectorCatalogMutationReceipt>, ConnectorError> {
        Err(unsupported())
    }
}

fn binding_with_mutation(incarnation: u8) -> ConnectorControlBinding {
    let binding = binding(incarnation);
    let descriptor = binding.descriptor().clone();
    let incarnation = binding.incarnation();
    let provider = Arc::new(TestControl {
        instance_id: descriptor.instance_id.clone(),
        incarnation,
    });
    ConnectorControlBinding::try_new(
        descriptor.clone(),
        incarnation,
        provider.clone(),
        provider.clone(),
        provider,
        Some(Arc::new(TestMutation {
            descriptor,
            incarnation,
        })),
    )
    .expect("control binding with mutation")
}

struct TestStatistics {
    descriptor: ConnectorInstanceDescriptor,
    incarnation: ConnectorInstanceIncarnation,
}

impl StatisticsReader for TestStatistics {
    fn descriptor(&self) -> &ConnectorInstanceDescriptor {
        &self.descriptor
    }

    fn incarnation(&self) -> ConnectorInstanceIncarnation {
        self.incarnation
    }

    fn read_statistics(
        &self,
        _request: StatisticsReadRequest,
    ) -> Result<StatisticsEvidence, ConnectorError> {
        Ok(StatisticsEvidence {
            data_version: StatisticsDataVersion::try_new(Bytes::from_static(b"data-v1"))?,
            evidence_revision: StatisticsEvidenceRevision::try_new(Bytes::from_static(
                b"evidence-v1",
            ))?,
            coverage: StatisticsCoverage::Full,
            accuracy: StatisticsAccuracy::Exact,
            interval: None,
            provenance: StatisticsProvenance::ProviderArtifact,
            metrics: Default::default(),
        })
    }
}

impl ConnectorStatistics for TestStatistics {}

fn binding_with_statistics(incarnation: u8) -> ConnectorControlBinding {
    let binding = binding(incarnation);
    let descriptor = binding.descriptor().clone();
    let incarnation = binding.incarnation();
    let provider = Arc::new(TestControl {
        instance_id: descriptor.instance_id.clone(),
        incarnation,
    });
    ConnectorControlBinding::try_new_with_statistics(
        descriptor.clone(),
        incarnation,
        provider.clone(),
        provider.clone(),
        provider,
        None,
        Some(Arc::new(TestStatistics {
            descriptor,
            incarnation,
        })),
    )
    .expect("control binding with statistics")
}

#[derive(Default)]
struct RecordingRetirementSink(Mutex<Vec<ConnectorControlRetirement>>);

impl ConnectorControlRetirementSink for RecordingRetirementSink {
    fn retire(&self, retirement: ConnectorControlRetirement) {
        self.0.lock().expect("retirement sink").push(retirement);
    }
}

#[test]
fn lease_drain_dispatches_retirement_to_installed_backends() {
    let host = ConnectorControlHost::new();
    let sink = Arc::new(RecordingRetirementSink::default());
    host.set_retirement_sink(sink.clone());
    let instance_id = ConnectorInstanceId::parse("catalog.analytics").expect("instance ID");
    host.register(binding(7)).expect("register old generation");
    let lease = host.acquire_current(&instance_id).expect("planning lease");
    let old_key = ConnectorExecutionBindingKey {
        instance_id: instance_id.clone(),
        incarnation: ConnectorInstanceIncarnation::from_bytes([7; 16]),
    };
    host.record_installed_backend(&old_key, "127.0.0.1:18080")
        .expect("record ensure acknowledgement");
    host.retire_current(&instance_id)
        .expect("retire old generation");
    assert!(sink.0.lock().expect("retirement sink").is_empty());

    drop(lease);

    let dispatched = sink.0.lock().expect("retirement sink");
    assert_eq!(dispatched.len(), 1);
    assert_eq!(dispatched[0].key, old_key);
    assert_eq!(
        dispatched[0].installed_backends,
        vec![String::from("127.0.0.1:18080")]
    );
    assert!(host.take_ready_retires().expect("retire queue").is_empty());
}

#[test]
fn mutation_lease_fences_retirement_and_missing_capability_is_unsupported() {
    let host = ConnectorControlHost::new();
    let instance_id = ConnectorInstanceId::parse("catalog.analytics").expect("instance ID");
    host.register(binding(7))
        .expect("register no-mutation generation");
    let error = match host.acquire_current_mutation(&instance_id) {
        Ok(_) => panic!("missing capability must fail"),
        Err(error) => error,
    };
    assert_eq!(error.kind(), ConnectorErrorKind::Unsupported);
    host.retire_current(&instance_id)
        .expect("retire no-mutation generation");
    assert_eq!(host.take_ready_retires().expect("retire queue").len(), 1);
    host.register(binding_with_mutation(8))
        .expect("register replacement generation");
    let planning = host.acquire_current(&instance_id).expect("planning lease");
    let mutation = host
        .acquire_current_mutation(&instance_id)
        .expect("mutation lease");
    host.retire_current(&instance_id)
        .expect("retire generation");
    assert!(host.take_ready_retires().expect("retire queue").is_empty());
    drop(planning);
    assert!(host.take_ready_retires().expect("retire queue").is_empty());
    drop(mutation);
    assert_eq!(host.take_ready_retires().expect("retire queue").len(), 1);
}

#[test]
fn statistics_lease_fences_retirement_and_missing_capability_is_unsupported() {
    let host = ConnectorControlHost::new();
    let instance_id = ConnectorInstanceId::parse("catalog.analytics").expect("instance ID");
    host.register(binding(7))
        .expect("register no-statistics generation");
    let error = match host.acquire_current_statistics(&instance_id) {
        Ok(_) => panic!("missing capability must fail"),
        Err(error) => error,
    };
    assert_eq!(error.kind(), ConnectorErrorKind::Unsupported);
    host.retire_current(&instance_id)
        .expect("retire no-statistics generation");
    assert_eq!(host.take_ready_retires().expect("retire queue").len(), 1);

    host.register(binding_with_statistics(8))
        .expect("register replacement generation");
    let lease = host
        .acquire_current_statistics(&instance_id)
        .expect("statistics lease");
    assert!(!lease.supports_collection());
    host.retire_current(&instance_id)
        .expect("retire statistics generation");
    assert!(host.take_ready_retires().expect("retire queue").is_empty());
    drop(lease);
    assert_eq!(host.take_ready_retires().expect("retire queue").len(), 1);
}

fn unsupported() -> ConnectorError {
    ConnectorError::new(
        ConnectorErrorKind::Unsupported,
        "test-only control capability",
    )
}
