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

//! Cross-check the borrowed invoice against the original operation and method
//! codecs. These fixtures create no Native execution or resource authority.
use super::wire_size as size;
use super::*;
use crate::domain::{WireContent, WireCredential, stored_credential, stored_message};
use novarocks_execution_contract::task_execution::{
    domain::{
        ContentFingerprint, CredentialEpoch, CredentialLeaseId, DomainVersion, EdgeOpenVersion,
        ExchangeEdgeId, PlanNodeId, SplitOffer,
    },
    lease::LeaseSequence,
    operation::{
        CredentialUpdate, QueryContextDomainUpdate, SplitAssignmentIntent, TaskDomainUpdate,
    },
    status::{AbortCause, CancelReason},
};
use novarocks_proto_models::{catalog, filter};
use novarocks_types::identity::{StageId, TaskId};

fn pair(bound: u32) -> (QueryContextRef, TaskIdentity) {
    let (context, task) = super::covered_wire_tests::context_and_task();
    (
        context,
        TaskIdentity::new(
            task.query_execution_id(),
            StageId::new(bound).unwrap(),
            TaskId::new(bound).unwrap(),
            task.backend_process_id(),
        ),
    )
}
fn ordinary(invoice: size::SizeResult, actual: novarocks::TaskOperation) {
    let item = invoice.unwrap();
    assert_eq!(item, actual.encoded_len());
    let batch = encode_operation_batch(vec![actual], TransportBudget::DEFAULT).unwrap();
    assert_eq!(size::batch_item(item).unwrap(), batch.encoded_len());
}
fn control(invoice: size::SizeResult, actual: novarocks::TaskOperation) {
    let batch = encode_control_operation_batch(vec![actual], TransportBudget::DEFAULT).unwrap();
    assert_eq!(invoice.unwrap(), batch.operations[0].encoded_len());
    assert_eq!(
        size::batch_item(invoice.unwrap()).unwrap(),
        batch.encoded_len()
    );
}
fn catalog(width: usize) -> catalog::CatalogSet {
    if width == 0 {
        return catalog::CatalogSet::default();
    }
    catalog::CatalogSet {
        catalogs: vec![catalog::CatalogProperties {
            handle: Some(catalog::CatalogHandle {
                catalog_name: "c".repeat(width),
                version: vec![0x71; 32],
            }),
            provider_id: "iceberg".into(),
            config_format_version: 1,
            execution_properties: vec![catalog::CatalogProperty {
                key: "k".into(),
                value: "v".repeat(width),
            }],
            credential_bindings: vec![],
        }],
    }
}
fn contribution(value: u32) -> novarocks::RuntimeFilterContribution {
    novarocks::RuntimeFilterContribution {
        participant_id: value,
        ..Default::default()
    }
}
fn descriptor(epoch: u64, width: usize) -> novarocks::CredentialLeaseDescriptor {
    use novarocks_spi::connector::{
        CatalogHandle, CatalogVersion, ConnectorInstanceId, CredentialLeaseDescriptor,
        CredentialLeaseId, CredentialLeaseProvider, StorageAccessDomainId,
        StorageCredentialScopePrefix,
    };
    // Same original typed descriptor factory as domain.rs::descriptor_for.
    novarocks_proto_codec::lifecycle::encode_credential_lease_descriptor(
        &CredentialLeaseDescriptor::try_new(
            CredentialLeaseId::try_from_bytes([1; 16]).unwrap(),
            epoch,
            CatalogHandle::new(
                ConnectorInstanceId::parse("warehouse").unwrap(),
                CatalogVersion::from_bytes([7; 32]),
            ),
            CredentialLeaseProvider::S3,
            vec![
                StorageCredentialScopePrefix::try_from_normalized(&format!(
                    "s3://bucket/{}",
                    "x".repeat(width.min(
                        novarocks_spi::connector::MAX_STORAGE_CREDENTIAL_SCOPE_PREFIX_BYTES
                            - "s3://bucket/".len(),
                    ))
                ))
                .unwrap(),
            ],
            99,
            true,
            None,
            StorageAccessDomainId::from_bytes([8; 32]),
        )
        .unwrap(),
    )
}
fn credential(value: u64, nonempty: bool) -> CredentialUpdate {
    let descriptors = if nonempty {
        vec![descriptor(value.max(1), value as usize)]
    } else {
        vec![]
    };
    CredentialUpdate::new(
        CredentialLeaseId::new(value),
        CredentialEpoch::new(value.max(1)).unwrap(),
        Arc::new(WireCredential::decode(&descriptors, FieldPath::root("credential")).unwrap()),
    )
}
fn establish_request(
    context: QueryContextRef,
    catalog: Arc<dyn CodecOwnedContent>,
    filter: Arc<dyn CodecOwnedContent>,
    options: Arc<dyn CodecOwnedContent>,
    credential: CredentialUpdate,
    wait: u64,
) -> EstablishQueryContext {
    EstablishQueryContext::new(
        TaskOperationId::new_v7(),
        context,
        AdmissionTicketId::try_from_bytes([0x61; 16]).unwrap(),
        catalog,
        filter,
        options,
        credential,
        LeaseValidFor::new(Duration::from_millis(wait)).unwrap(),
    )
}
fn encoded_establish(value: &EstablishQueryContext) -> novarocks::TaskOperation {
    let catalog = stored_message::<catalog::CatalogSet>(value.catalog_binding().as_ref())
        .unwrap()
        .clone();
    let filter = stored_message::<novarocks::RuntimeFilterContribution>(
        value.initial_runtime_filter().as_ref(),
    )
    .unwrap()
    .clone();
    let options =
        *stored_message::<novarocks::QueryOptions>(value.query_options().as_ref()).unwrap();
    let material = stored_credential(value.initial_credential().material().as_ref()).unwrap();
    encode_establish_query_context(
        value,
        catalog,
        filter,
        novarocks::QueryContextCredentialDomain {
            lease_id: value.initial_credential().lease_id().get(),
            epoch: value.initial_credential().epoch().get(),
            descriptors: material.descriptors().to_vec(),
        },
        options,
        NativeCompatibilityId::new([0x71; 32]),
    )
}

#[test]
fn acquire_establish_and_renew_match_original_wire_at_scalar_and_nested_length_boundaries() {
    for value in [1u32, 127, 128, 16383, 16384] {
        let (context, _) = pair(value);
        let acquire=AcquireQueryContextAdmissionTicket::new(TaskOperationId::new_v7(),context,
            LeaseValidFor::new(Duration::from_millis(value as u64)).unwrap(),NativeCompatibilityId::new([0x71;32]),
            novarocks_execution_contract::task_execution::identity::AdmissionEpochCapability::try_from_bytes([0x62;16]).unwrap());
        ordinary(
            size::acquire(acquire),
            encode_acquire_query_context_admission_ticket(acquire),
        );
        for sequence in [0, value as u64] {
            let renew = RenewQueryExecutionLease::new(
                TaskOperationId::new_v7(),
                context,
                LeaseSequence::new(sequence),
                LeaseValidFor::new(Duration::from_millis(value as u64)).unwrap(),
            );
            ordinary(size::renew(&renew, false), encode_renew_lease(&renew));
            control(size::renew(&renew, true), encode_renew_lease(&renew));
        }
    }
    for width in [0usize, 1, 127, 128, 16383, 16384] {
        let (context, _) = pair(1);
        let options = novarocks::QueryOptions {
            query_mem_limit: width as i64,
            pipeline_dop: if width == 0 { 0 } else { 128 },
            ..Default::default()
        };
        let value = establish_request(
            context,
            Arc::new(WireContent::new(
                ESTABLISH_CATALOG_DOMAIN_TAG,
                catalog(width),
            )),
            Arc::new(WireContent::new(
                ESTABLISH_FILTER_DOMAIN_TAG,
                contribution(width as u32),
            )),
            Arc::new(WireContent::new(
                ESTABLISH_QUERY_OPTIONS_DOMAIN_TAG,
                options,
            )),
            credential(width as u64, width != 0),
            width.max(1) as u64,
        );
        ordinary(size::establish(&value), encoded_establish(&value));
        // Initial LeaseSequence is genuinely zero; optional default messages
        // remain present, while their scalar/empty repeated fields are omitted.
        assert_eq!(value.initial_lease_sequence(), LeaseSequence::INITIAL);
    }
}

#[test]
fn all_three_context_domain_advances_match_their_original_codec() {
    for value in [1u64, 127, 128, 16383, 16384] {
        let (context, _) = pair(1);
        let domains = [
            QueryContextDomainUpdate::CatalogBinding {
                version: DomainVersion::new(value).unwrap(),
                payload: Arc::new(WireContent::new(
                    b"size.catalog",
                    catalog(if value == 1 { 0 } else { value as usize }),
                )),
            },
            QueryContextDomainUpdate::SharedDynamicFilter {
                version: DomainVersion::new(value).unwrap(),
                payload: Arc::new(WireContent::new(
                    b"size.filter",
                    contribution(if value == 1 { 0 } else { value as u32 }),
                )),
            },
            QueryContextDomainUpdate::Credential(credential(value, value != 1)),
        ];
        for domain in domains {
            let wire = crate::domain::encode_neutral_query_context_domain(
                &domain,
                FieldPath::root("domain"),
            )
            .unwrap();
            assert_eq!(
                size::query_context_domain(&domain).unwrap(),
                wire.encoded_len()
            );
            let request =
                AdvanceQueryContextDomain::new(TaskOperationId::new_v7(), context, domain);
            ordinary(
                size::advance(&request),
                encode_advance_query_context_domain(&request, wire),
            );
        }
    }
}

#[test]
fn update_all_four_domains_and_control_close_match_original_projection() {
    for bound in [1u32, 127, 128, 16383, 16384] {
        let (_, identity) = pair(bound);
        let split = TaskDomainUpdate::SplitAssignment(SplitAssignmentIntent::new(
            PlanNodeId::new(0).unwrap(),
            SplitOffer::Seal,
            Arc::new(WireContent::new(
                b"size.split",
                novarocks_proto_models::connector_read::SplitAssignment {
                    plan_node_id: if bound == 1 { 0 } else { bound as i32 },
                    splits: vec![],
                    no_more_splits: bound != 1,
                },
            )),
        ));
        let dynamic = TaskDomainUpdate::TaskDynamicFilter {
            version: DomainVersion::new(bound as u64).unwrap(),
            payload: Arc::new(WireContent::new(
                b"size.dynamic",
                filter::RuntimeFilterEnvelope {
                    schema_digest: vec![0x71; if bound == 1 { 0 } else { 32 }],
                    payload: vec![0x42; if bound == 1 { 0 } else { bound as usize }],
                    ..Default::default()
                },
            )),
        };
        let edges = TaskDomainUpdate::OpenExchangeEdges {
            version: EdgeOpenVersion::new(bound).unwrap(),
            edges: vec![
                ExchangeEdgeId::new(bound).unwrap(),
                ExchangeEdgeId::new(bound + 1).unwrap(),
            ],
        };
        let close = TaskDomainUpdate::CloseExchangeDestination {
            version: DomainVersion::new(bound as u64).unwrap(),
            edge: ExchangeEdgeId::new(bound).unwrap(),
            destination: identity,
        };
        let all = vec![split, dynamic, edges, close];
        let original = all
            .iter()
            .map(|domain| {
                crate::domain::encode_neutral_task_domain(domain, FieldPath::root("domain"))
                    .unwrap()
            })
            .collect();
        let request =
            UpdateTask::try_new(TaskOperationId::new_v7(), identity, all.clone()).unwrap();
        ordinary(
            size::update(&request, false),
            encode_update_task(&request, original),
        );
        for domain in all {
            let wire =
                crate::domain::encode_neutral_task_domain(&domain, FieldPath::root("domain"))
                    .unwrap();
            assert_eq!(size::task_domain(&domain).unwrap(), wire.encoded_len());
            let control_close =
                matches!(&domain, TaskDomainUpdate::CloseExchangeDestination { .. });
            let request =
                UpdateTask::try_new(TaskOperationId::new_v7(), identity, vec![domain]).unwrap();
            ordinary(
                size::update(&request, false),
                encode_update_task(&request, vec![wire.clone()]),
            );
            if control_close {
                control(
                    size::update(&request, true),
                    encode_update_task(&request, vec![wire]),
                );
            } else {
                assert_eq!(
                    size::update(&request, true),
                    Err(size::WireSizeError::NotCodecProduced)
                );
                assert!(
                    encode_control_operation_batch(
                        vec![encode_update_task(&request, vec![wire])],
                        TransportBudget::DEFAULT
                    )
                    .is_err()
                );
            }
        }
    }
    let (_, identity) = pair(1);
    for count in [0usize, 1, 63, 64, 127, 128, 256] {
        let domain = TaskDomainUpdate::OpenExchangeEdges {
            version: EdgeOpenVersion::FIRST,
            edges: (1..=count)
                .map(|id| ExchangeEdgeId::new(id as u32).unwrap())
                .collect(),
        };
        let wire =
            crate::domain::encode_neutral_task_domain(&domain, FieldPath::root("packed")).unwrap();
        assert_eq!(size::task_domain(&domain).unwrap(), wire.encoded_len());
    }
    for count in [1usize, 2, 127, 128, 256] {
        let domains = (1..=count)
            .map(|id| TaskDomainUpdate::CloseExchangeDestination {
                version: DomainVersion::FIRST,
                edge: ExchangeEdgeId::new(id as u32).unwrap(),
                destination: identity,
            })
            .collect::<Vec<_>>();
        let original = domains
            .iter()
            .map(|domain| {
                crate::domain::encode_neutral_task_domain(domain, FieldPath::root("close")).unwrap()
            })
            .collect();
        let request = UpdateTask::try_new(TaskOperationId::new_v7(), identity, domains).unwrap();
        control(
            size::update(&request, true),
            encode_update_task(&request, original),
        );
    }
    assert!(UpdateTask::try_new(TaskOperationId::new_v7(), identity, vec![]).is_err());
    let empty = TaskDomainUpdate::OpenExchangeEdges {
        version: EdgeOpenVersion::FIRST,
        edges: vec![],
    };
    let wire =
        crate::domain::encode_neutral_task_domain(&empty, FieldPath::root("domain")).unwrap();
    assert_eq!(size::task_domain(&empty).unwrap(), wire.encoded_len());
    // Size observation does not bypass the original nonempty edge-set gate.
    assert!(UpdateTask::try_new(TaskOperationId::new_v7(), identity, vec![empty]).is_err());
}

#[test]
fn cancel_abort_release_and_quiesce_match_both_real_method_codecs() {
    for bound in [1u32, 127, 128, 16383, 16384] {
        let (context, identity) = pair(bound);
        let cancel = CancelTask::new(
            TaskOperationId::new_v7(),
            identity,
            CancelReason::UpstreamNoLongerNeeded,
        );
        ordinary(size::cancel(cancel, false), encode_cancel_task(cancel));
        control(size::cancel(cancel, true), encode_cancel_task(cancel));
        for cause in [
            AbortCause::QueryFailed,
            AbortCause::LeaseExpired,
            AbortCause::PeerTaskFailed,
        ] {
            let abort = AbortQueryContext::new(TaskOperationId::new_v7(), context, cause);
            ordinary(size::abort(abort, false), encode_abort_query_context(abort));
            control(size::abort(abort, true), encode_abort_query_context(abort));
        }
        let release = ReleaseQueryContext::new(TaskOperationId::new_v7(), context);
        ordinary(
            size::release(release, false),
            encode_release_query_context(release),
        );
        control(
            size::release(release, true),
            encode_release_query_context(release),
        );
        let quiesce = QuiesceQueryContext::new(TaskOperationId::new_v7(), context);
        ordinary(
            size::quiesce(quiesce, false),
            encode_quiesce_query_context(quiesce),
        );
        control(
            size::quiesce(quiesce, true),
            encode_quiesce_query_context(quiesce),
        );
    }
}

#[test]
fn create_prices_empty_fields_and_every_original_length_prefix_boundary() {
    for wait in [1u64, 127, 128, 16383, 16384] {
        let envelope = OperationEnvelope::new(
            TaskOperationId::new_v7(),
            OperationKind::CreateTask,
            MaxWait::new(Duration::from_millis(wait)).unwrap(),
        );
        for len in [0usize, 1, 126, 127, 128, 16383, 16384] {
            let fragment = FrozenBytes::freeze(bytes::Bytes::from(vec![0x31; len]));
            let metadata = FrozenBytes::freeze(bytes::Bytes::from(vec![0x42; len]));
            ordinary(
                size::create(envelope, fragment.len(), metadata.len()),
                encode_create_task(envelope, &fragment, &metadata),
            );
        }
    }
}

#[derive(Debug)]
struct ForeignContent;
impl CodecOwnedContent for ForeignContent {
    fn fingerprint(
        &self,
    ) -> novarocks_execution_contract::task_execution::domain::ContentFingerprint {
        ContentFingerprint::from_bytes([0; 16])
    }
    fn encoded_len(&self) -> usize {
        0
    }
}
struct ForeignCredential;
impl novarocks_execution_contract::task_execution::domain::ConfidentialContent
    for ForeignCredential
{
    fn encoded_len(&self) -> usize {
        0
    }
    fn matches(
        &self,
        _: &dyn novarocks_execution_contract::task_execution::domain::ConfidentialContent,
    ) -> bool {
        false
    }
}
#[test]
fn a_numeric_length_or_foreign_type_is_not_a_codec_produced_source() {
    let foreign: Arc<dyn CodecOwnedContent> = Arc::new(ForeignContent);
    let (context, _) = pair(1);
    let bad_credential = CredentialUpdate::new(
        CredentialLeaseId::new(1),
        CredentialEpoch::FIRST,
        Arc::new(ForeignCredential),
    );
    for domain in [
        QueryContextDomainUpdate::CatalogBinding {
            version: DomainVersion::FIRST,
            payload: foreign.clone(),
        },
        QueryContextDomainUpdate::SharedDynamicFilter {
            version: DomainVersion::FIRST,
            payload: foreign.clone(),
        },
        QueryContextDomainUpdate::Credential(bad_credential.clone()),
    ] {
        assert_eq!(
            size::query_context_domain(&domain),
            Err(size::WireSizeError::NotCodecProduced)
        );
        assert!(
            crate::domain::encode_neutral_query_context_domain(&domain, FieldPath::root("domain"))
                .is_err()
        );
        let advance = AdvanceQueryContextDomain::new(TaskOperationId::new_v7(), context, domain);
        assert_eq!(
            size::advance(&advance),
            Err(size::WireSizeError::NotCodecProduced)
        );
    }
    for domain in [
        TaskDomainUpdate::SplitAssignment(SplitAssignmentIntent::new(
            PlanNodeId::new(0).unwrap(),
            SplitOffer::Seal,
            foreign.clone(),
        )),
        TaskDomainUpdate::TaskDynamicFilter {
            version: DomainVersion::FIRST,
            payload: foreign.clone(),
        },
    ] {
        assert_eq!(
            size::task_domain(&domain),
            Err(size::WireSizeError::NotCodecProduced)
        );
        assert!(
            crate::domain::encode_neutral_task_domain(&domain, FieldPath::root("domain")).is_err()
        );
    }
    let good_catalog: Arc<dyn CodecOwnedContent> =
        Arc::new(WireContent::new(ESTABLISH_CATALOG_DOMAIN_TAG, catalog(0)));
    let good_filter: Arc<dyn CodecOwnedContent> = Arc::new(WireContent::new(
        ESTABLISH_FILTER_DOMAIN_TAG,
        contribution(0),
    ));
    let good_options: Arc<dyn CodecOwnedContent> = Arc::new(WireContent::new(
        ESTABLISH_QUERY_OPTIONS_DOMAIN_TAG,
        novarocks::QueryOptions::default(),
    ));
    let values = [
        establish_request(
            context,
            foreign.clone(),
            good_filter.clone(),
            good_options.clone(),
            credential(1, false),
            1,
        ),
        establish_request(
            context,
            good_catalog.clone(),
            foreign.clone(),
            good_options.clone(),
            credential(1, false),
            1,
        ),
        establish_request(
            context,
            good_catalog.clone(),
            good_filter.clone(),
            foreign.clone(),
            credential(1, false),
            1,
        ),
        establish_request(
            context,
            good_catalog,
            good_filter,
            good_options,
            bad_credential,
            1,
        ),
    ];
    for value in values {
        assert_eq!(
            size::establish(&value),
            Err(size::WireSizeError::NotCodecProduced)
        );
    }
    // A codec-produced value of the wrong concrete message is also refused.
    let wrong = TaskDomainUpdate::TaskDynamicFilter {
        version: DomainVersion::FIRST,
        payload: Arc::new(WireContent::new(b"wrong", catalog(0))),
    };
    assert_eq!(
        size::task_domain(&wrong),
        Err(size::WireSizeError::NotCodecProduced)
    );
}
