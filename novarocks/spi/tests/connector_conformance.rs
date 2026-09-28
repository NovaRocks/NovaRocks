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

use std::sync::Arc;
use std::time::{Duration, Instant};

use novarocks_spi::connector::{
    ConnectorBeginScanRequest, ConnectorControlBinding, ConnectorDataMutation,
    ConnectorDataMutationExecuteRequest, ConnectorDataMutationPlan,
    ConnectorDataMutationPlanningRequest, ConnectorDataMutationReceipt,
    ConnectorDataMutationReconcileRequest, ConnectorError, ConnectorErrorKind,
    ConnectorExecutionDistribution, ConnectorInstanceDescriptor, ConnectorInstanceId,
    ConnectorListTablesRequest, ConnectorListViewsRequest, ConnectorMetadata,
    ConnectorNamespaceRequest, ConnectorPredicateDisposition, ConnectorPredicateDispositionKind,
    ConnectorProviderBinding, ConnectorProviderBindingKey, ConnectorProviderId,
    ConnectorReadNamedReference, ConnectorReadReferenceFacts, ConnectorReadReferenceKind,
    ConnectorReadSnapshotLogEntry, ConnectorScalarType, ConnectorScalarValue, ConnectorScan,
    ConnectorScanHandle, ConnectorScanPlanning, ConnectorSplitPlanningMetrics,
    ConnectorSplitPlanningRequest, ConnectorSplitPlanningResult, ConnectorStaticComparisonOp,
    ConnectorStaticPredicate, ConnectorStaticPredicateColumn, ConnectorStaticPredicateId,
    ConnectorStaticPredicateKind, ConnectorStatistics, ConnectorTableHandle,
    ConnectorTableIdentity, ConnectorTableMetadata, ConnectorTableRequest, ConnectorViewIdentity,
    ConnectorViewMetadata, ConnectorViewMetadataValue, ConnectorViewRequest,
    ExternalMutationOutcome, ProviderBindingEpoch, StatisticsBasisRelation, StatisticsDataVersion,
    StatisticsEvidence, StatisticsEvidenceRevision, StatisticsMetric, StatisticsMetricObservation,
    StatisticsMetricSource, StatisticsMetricState, StatisticsMetricValue, StatisticsNumericNature,
    StatisticsReadRequest, StatisticsRowCoverage, normalize_predicate_dispositions,
    validate_static_predicates,
};

struct OwnerPlanning {
    instance_id: ConnectorInstanceId,
}

impl ConnectorScanPlanning for OwnerPlanning {
    fn instance_id(&self) -> &ConnectorInstanceId {
        &self.instance_id
    }

    fn begin_scan(
        &self,
        _: &ConnectorTableHandle,
        _: ConnectorBeginScanRequest,
    ) -> Result<ConnectorScan, ConnectorError> {
        unreachable!("control binding construction must not begin a scan")
    }

    fn plan_splits(
        &self,
        _: &ConnectorScanHandle,
        _: ConnectorSplitPlanningRequest,
    ) -> Result<ConnectorSplitPlanningResult, ConnectorError> {
        unreachable!("control binding construction must not plan splits")
    }
}

struct OwnerDistribution {
    descriptor: ConnectorInstanceDescriptor,
    incarnation: ProviderBindingEpoch,
}

impl ConnectorExecutionDistribution for OwnerDistribution {
    fn declaration(
        &self,
        _: &novarocks_spi::connector::ConnectorRequestContext,
    ) -> Result<ConnectorProviderBinding, ConnectorError> {
        ConnectorProviderBinding::iceberg(
            self.descriptor.instance_id.as_str(),
            self.incarnation.to_bytes(),
            "owner-fixture",
        )
        .map_err(|error| ConnectorError::new(ConnectorErrorKind::InvalidRequest, error.to_string()))
    }
}

struct OwnerMetadata {
    instance_id: ConnectorInstanceId,
}

impl OwnerMetadata {
    fn new(instance_id: &str) -> Self {
        Self {
            instance_id: ConnectorInstanceId::parse(instance_id).expect("instance ID"),
        }
    }
}

impl ConnectorMetadata for OwnerMetadata {
    fn instance_id(&self) -> &ConnectorInstanceId {
        &self.instance_id
    }

    fn namespace_exists(
        &self,
        _request: ConnectorNamespaceRequest,
    ) -> Result<bool, ConnectorError> {
        unreachable!("instance construction must not resolve metadata")
    }

    fn table_exists(&self, _request: ConnectorTableRequest) -> Result<bool, ConnectorError> {
        unreachable!("instance construction must not resolve metadata")
    }

    fn list_tables(
        &self,
        _request: ConnectorListTablesRequest,
    ) -> Result<Vec<ConnectorTableIdentity>, ConnectorError> {
        unreachable!("instance construction must not resolve metadata")
    }

    fn load_table(
        &self,
        _request: ConnectorTableRequest,
    ) -> Result<ConnectorTableMetadata, ConnectorError> {
        unreachable!("instance construction must not resolve metadata")
    }
}

fn descriptor(instance_id: &str) -> ConnectorInstanceDescriptor {
    ConnectorInstanceDescriptor {
        provider_id: ConnectorProviderId::parse("file").expect("provider ID"),
        instance_id: ConnectorInstanceId::parse(instance_id).expect("instance ID"),
    }
}

#[test]
fn control_binding_rejects_metadata_owned_by_another_instance() {
    let descriptor = descriptor("file");
    assert_eq!(
        ConnectorControlBinding::try_new(
            descriptor.clone(),
            ProviderBindingEpoch::from_bytes([1; 16]),
            Arc::new(OwnerMetadata::new("foreign")),
            Arc::new(OwnerPlanning {
                instance_id: descriptor.instance_id.clone(),
            }),
            Arc::new(OwnerDistribution {
                descriptor,
                incarnation: ProviderBindingEpoch::from_bytes([1; 16]),
            }),
            None,
        )
        .err()
        .expect("a host must not attach foreign metadata")
        .kind(),
        ConnectorErrorKind::InvalidRequest
    );
}

struct OwnerStatistics {
    descriptor: ConnectorInstanceDescriptor,
    incarnation: ProviderBindingEpoch,
}

impl novarocks_spi::connector::StatisticsReader for OwnerStatistics {
    fn descriptor(&self) -> &ConnectorInstanceDescriptor {
        &self.descriptor
    }

    fn incarnation(&self) -> ProviderBindingEpoch {
        self.incarnation
    }

    fn read_statistics(
        &self,
        _: StatisticsReadRequest,
    ) -> Result<StatisticsEvidence, ConnectorError> {
        unreachable!("control binding construction must not read statistics")
    }
}

impl ConnectorStatistics for OwnerStatistics {}

/// A sketch-derived NDV is an estimate in both directions, so no provider may
/// present one as exact or as a one-sided bound. The check lives in the only
/// constructor and the fields are private, so this holds for any provider
/// outside the SPI crate — which is what this test, in a separate crate,
/// demonstrates.
#[test]
fn no_provider_can_present_a_theta_sketch_as_an_exact_value() {
    let data_version = StatisticsDataVersion::try_new(bytes::Bytes::from_static(b"data-v1"))
        .expect("data version");
    let revision =
        StatisticsEvidenceRevision::try_new(bytes::Bytes::from_static(b"rev-1")).expect("revision");
    let theta = StatisticsMetric::ThetaNdv {
        column: Arc::from("k"),
    };
    let labelled = |nature| {
        StatisticsEvidence::try_new(
            data_version.clone(),
            revision.clone(),
            StatisticsRowCoverage::AllVisibleRows,
            std::collections::BTreeMap::from([(
                theta.clone(),
                StatisticsMetricState::Available(StatisticsMetricObservation::new(
                    StatisticsMetricValue::F64(3.0),
                    data_version.clone(),
                    StatisticsMetricSource::VisibleRowScan,
                    nature,
                    StatisticsBasisRelation::Identical,
                )),
            )]),
        )
    };

    for nature in [
        StatisticsNumericNature::Exact,
        StatisticsNumericNature::UpperBound,
        StatisticsNumericNature::LowerBound,
    ] {
        assert_eq!(
            labelled(nature)
                .expect_err("a Theta sketch must not be labelled {nature:?}")
                .kind(),
            ConnectorErrorKind::InvalidRequest
        );
    }
    labelled(StatisticsNumericNature::TwoSidedApproximate)
        .expect("two-sided approximate is the only admissible labelling");
}

#[test]
fn control_binding_rejects_statistics_owned_by_another_generation() {
    let descriptor = descriptor("file");
    let incarnation = ProviderBindingEpoch::from_bytes([1; 16]);
    let foreign = Arc::new(OwnerStatistics {
        descriptor: descriptor.clone(),
        incarnation: ProviderBindingEpoch::from_bytes([2; 16]),
    });
    assert_eq!(
        ConnectorControlBinding::try_new_with_statistics(
            descriptor.clone(),
            incarnation,
            Arc::new(OwnerMetadata::new("file")),
            Arc::new(OwnerPlanning {
                instance_id: descriptor.instance_id.clone(),
            }),
            Arc::new(OwnerDistribution {
                descriptor,
                incarnation,
            }),
            None,
            Some(foreign),
        )
        .err()
        .expect("a host must not attach foreign statistics")
        .kind(),
        ConnectorErrorKind::InvalidRequest
    );
}

fn spi5b_context(
    max_total_payload_bytes: usize,
) -> novarocks_spi::connector::ConnectorRequestContext {
    novarocks_spi::connector::ConnectorRequestContext::try_new(
        Instant::now() + Duration::from_secs(30),
        novarocks_spi::connector::ConnectorStopOwner::new().view(),
        max_total_payload_bytes,
        max_total_payload_bytes,
    )
    .expect("SPI-5B request context")
}

#[test]
fn spi5b_reference_facts_sort_and_validate_references_against_snapshots() {
    let facts = ConnectorReadReferenceFacts::try_new(
        vec![7, 3],
        vec![
            ConnectorReadSnapshotLogEntry {
                snapshot_id: 7,
                timestamp_millis: 20,
            },
            ConnectorReadSnapshotLogEntry {
                snapshot_id: 3,
                timestamp_millis: 10,
            },
        ],
        vec![
            ConnectorReadNamedReference {
                name: Arc::from("main"),
                kind: ConnectorReadReferenceKind::Branch,
                snapshot_id: 7,
            },
            ConnectorReadNamedReference {
                name: Arc::from("release"),
                kind: ConnectorReadReferenceKind::Tag,
                snapshot_id: 3,
            },
        ],
        Some(7),
        &spi5b_context(4096),
    )
    .expect("well-formed reference facts");

    assert_eq!(facts.snapshot_ids(), &[3, 7]);
    assert_eq!(
        facts
            .snapshot_log()
            .iter()
            .map(|entry| entry.snapshot_id)
            .collect::<Vec<_>>(),
        vec![3, 7]
    );
    assert_eq!(
        facts
            .named_references()
            .iter()
            .map(|reference| reference.name.as_ref())
            .collect::<Vec<_>>(),
        vec!["main", "release"]
    );
    assert_eq!(facts.current_snapshot_id(), Some(7));

    assert_eq!(
        ConnectorReadReferenceFacts::try_new(
            vec![3],
            vec![],
            vec![ConnectorReadNamedReference {
                name: Arc::from("missing"),
                kind: ConnectorReadReferenceKind::Branch,
                snapshot_id: 99,
            }],
            None,
            &spi5b_context(4096),
        )
        .expect_err("references to an unknown snapshot must fail")
        .kind(),
        ConnectorErrorKind::CorruptData
    );
}

#[test]
fn spi5b_reference_facts_enforce_the_request_total_payload_budget() {
    assert_eq!(
        ConnectorReadReferenceFacts::try_new(
            vec![1],
            vec![],
            vec![ConnectorReadNamedReference {
                name: Arc::from("reference-name-that-does-not-fit"),
                kind: ConnectorReadReferenceKind::Branch,
                snapshot_id: 1,
            }],
            Some(1),
            &spi5b_context(32),
        )
        .expect_err("facts must honor the request total payload budget")
        .kind(),
        ConnectorErrorKind::ResourceExhausted
    );
}

struct OwnerViewMetadata {
    descriptor: ConnectorInstanceDescriptor,
    incarnation: ProviderBindingEpoch,
}

impl ConnectorViewMetadata for OwnerViewMetadata {
    fn descriptor(&self) -> &ConnectorInstanceDescriptor {
        &self.descriptor
    }

    fn incarnation(&self) -> ProviderBindingEpoch {
        self.incarnation
    }

    fn view_exists(&self, _: ConnectorViewRequest) -> Result<bool, ConnectorError> {
        unreachable!("control binding construction must not resolve views")
    }

    fn load_view(
        &self,
        _: ConnectorViewRequest,
    ) -> Result<ConnectorViewMetadataValue, ConnectorError> {
        unreachable!("control binding construction must not resolve views")
    }

    fn list_views(
        &self,
        _: ConnectorListViewsRequest,
    ) -> Result<Vec<ConnectorViewIdentity>, ConnectorError> {
        unreachable!("control binding construction must not resolve views")
    }
}

#[test]
fn spi5b_control_binding_rejects_view_capability_owned_by_another_generation() {
    let descriptor = descriptor("file");
    let incarnation = ProviderBindingEpoch::from_bytes([1; 16]);
    let binding = ConnectorControlBinding::try_new(
        descriptor.clone(),
        incarnation,
        Arc::new(OwnerMetadata::new("file")),
        Arc::new(OwnerPlanning {
            instance_id: descriptor.instance_id.clone(),
        }),
        Arc::new(OwnerDistribution {
            descriptor: descriptor.clone(),
            incarnation,
        }),
        None,
    )
    .expect("base binding");
    let foreign = Arc::new(OwnerViewMetadata {
        descriptor,
        incarnation: ProviderBindingEpoch::from_bytes([2; 16]),
    });

    let error = match binding.try_with_view_metadata(Some(foreign)) {
        Ok(_) => panic!("a host must not attach a foreign view capability"),
        Err(error) => error,
    };
    assert_eq!(error.kind(), ConnectorErrorKind::InvalidRequest);
}

struct OwnerDataMutation {
    descriptor: ConnectorInstanceDescriptor,
    key: ConnectorProviderBindingKey,
}

impl ConnectorDataMutation for OwnerDataMutation {
    fn descriptor(&self) -> &ConnectorInstanceDescriptor {
        &self.descriptor
    }

    fn binding_key(&self) -> &ConnectorProviderBindingKey {
        &self.key
    }

    fn plan_mutation(
        &self,
        _: ConnectorDataMutationPlanningRequest,
    ) -> Result<ConnectorDataMutationPlan, ConnectorError> {
        unreachable!("binding construction must not plan a mutation")
    }

    fn execute(
        &self,
        _: ConnectorDataMutationExecuteRequest,
    ) -> Result<ExternalMutationOutcome<ConnectorDataMutationReceipt>, ConnectorError> {
        unreachable!("binding construction must not execute a mutation")
    }

    fn reconcile(
        &self,
        _: ConnectorDataMutationReconcileRequest,
    ) -> Result<ExternalMutationOutcome<ConnectorDataMutationReceipt>, ConnectorError> {
        unreachable!("binding construction must not reconcile a mutation")
    }
}

#[test]
fn control_binding_rejects_data_mutation_owned_by_another_generation() {
    let descriptor = descriptor("file");
    let incarnation = ProviderBindingEpoch::from_bytes([1; 16]);
    let foreign = Arc::new(OwnerDataMutation {
        descriptor: descriptor.clone(),
        key: ConnectorProviderBindingKey {
            instance_id: descriptor.instance_id.clone(),
            incarnation: ProviderBindingEpoch::from_bytes([2; 16]),
        },
    });
    assert_eq!(
        ConnectorControlBinding::try_new_with_data_mutation(
            descriptor.clone(),
            incarnation,
            Arc::new(OwnerMetadata::new("file")),
            Arc::new(OwnerPlanning {
                instance_id: descriptor.instance_id.clone(),
            }),
            Arc::new(OwnerDistribution {
                descriptor,
                incarnation,
            }),
            None,
            Some(foreign),
        )
        .err()
        .expect("a host must not attach foreign data mutation")
        .kind(),
        ConnectorErrorKind::InvalidRequest
    );
}

fn static_int_predicate(id: u32) -> ConnectorStaticPredicate {
    ConnectorStaticPredicate {
        id: ConnectorStaticPredicateId(id),
        column: ConnectorStaticPredicateColumn {
            field_ordinal: 2,
            data_type: ConnectorScalarType::Int32,
            nullable: false,
        },
        kind: ConnectorStaticPredicateKind::Comparison {
            op: ConnectorStaticComparisonOp::Ge,
            literal: ConnectorScalarValue::Int32(42),
        },
    }
}

#[test]
fn static_predicate_conformance_normalizes_a_total_out_of_order_response() {
    let predicates = vec![static_int_predicate(4), static_int_predicate(8)];
    let normalized = normalize_predicate_dispositions(
        &predicates,
        &[
            ConnectorPredicateDisposition {
                predicate_id: ConnectorStaticPredicateId(8),
                kind: ConnectorPredicateDispositionKind::Unsupported,
            },
            ConnectorPredicateDisposition {
                predicate_id: ConnectorStaticPredicateId(4),
                kind: ConnectorPredicateDispositionKind::Exact,
            },
        ],
    )
    .expect("total response is valid");

    assert_eq!(normalized[0].predicate_id, ConnectorStaticPredicateId(4));
    assert_eq!(normalized[0].kind, ConnectorPredicateDispositionKind::Exact);
    assert_eq!(normalized[1].predicate_id, ConnectorStaticPredicateId(8));
}

#[test]
fn static_predicate_conformance_rejects_unknown_or_duplicate_response_ids() {
    let predicates = vec![static_int_predicate(4), static_int_predicate(8)];
    let unknown = normalize_predicate_dispositions(
        &predicates,
        &[
            ConnectorPredicateDisposition {
                predicate_id: ConnectorStaticPredicateId(4),
                kind: ConnectorPredicateDispositionKind::Unsupported,
            },
            ConnectorPredicateDisposition {
                predicate_id: ConnectorStaticPredicateId(9),
                kind: ConnectorPredicateDispositionKind::Unsupported,
            },
        ],
    )
    .expect_err("unknown ID is malformed provider output");
    assert_eq!(unknown.kind(), ConnectorErrorKind::CorruptData);

    let duplicate = normalize_predicate_dispositions(
        &predicates,
        &[
            ConnectorPredicateDisposition {
                predicate_id: ConnectorStaticPredicateId(4),
                kind: ConnectorPredicateDispositionKind::Unsupported,
            },
            ConnectorPredicateDisposition {
                predicate_id: ConnectorStaticPredicateId(4),
                kind: ConnectorPredicateDispositionKind::PruningOnly,
            },
        ],
    )
    .expect_err("duplicate ID is malformed provider output");
    assert_eq!(duplicate.kind(), ConnectorErrorKind::CorruptData);
}

#[test]
fn static_predicate_conformance_rejects_type_mismatch_and_invalid_planning_metrics() {
    let mut predicate = static_int_predicate(4);
    predicate.kind = ConnectorStaticPredicateKind::Comparison {
        op: ConnectorStaticComparisonOp::Eq,
        literal: ConnectorScalarValue::Int64(42),
    };
    assert_eq!(
        validate_static_predicates(&[predicate])
            .expect_err("literal and column types must match")
            .kind(),
        ConnectorErrorKind::InvalidRequest
    );

    assert_eq!(
        novarocks_spi::connector::ConnectorSplitPlanningResult::try_new(
            Vec::new(),
            ConnectorSplitPlanningMetrics {
                candidate_units_considered: 1,
                candidate_units_pruned: 2,
                ..ConnectorSplitPlanningMetrics::default()
            },
        )
        .expect_err("pruned candidates cannot exceed considered candidates")
        .kind(),
        ConnectorErrorKind::CorruptData
    );
}
