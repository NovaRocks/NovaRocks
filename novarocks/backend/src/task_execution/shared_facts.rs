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

//! Turning one query context's neutral shared facts back into the typed
//! values their owners consume.
//!
//! The task protocol hands a backend its shared facts as neutral content: a
//! catalog binding, a runtime filter, and a credential rotation are all
//! opaque handles by the time they reach an owner, because the neutral layer
//! cannot name a generated message. That is the right boundary for the
//! protocol and the wrong one for the catalog manager and the filter
//! participant, which consume typed values and nothing else.
//!
//! This module is the single place that crosses back. Each accessor states the
//! type it expects and refuses anything else, so a handle carrying the wrong
//! domain's payload fails closed here instead of being misread one layer down.
//! Nothing else in the backend may name a wire type to get at a payload.

use novarocks_execution::task_execution::descriptor::PhysicalFragmentPlan;
use novarocks_execution::task_execution::domain::{CodecOwnedContent, DomainVersion};
use novarocks_execution::task_execution::identity::TaskIdentity;
use novarocks_execution::task_execution::operation::CredentialUpdate;
use novarocks_execution::task_execution::status::TaskFailureCategory;
use novarocks_proto_codec::catalog::CatalogSet;
use novarocks_proto_codec::task_execution::descriptor::WireFragmentPlan;
use novarocks_proto_codec::task_execution::domain::{
    WireCredential, encode_task_dynamic_filter_domain, stored_credential, stored_message,
};
use novarocks_proto_codec::task_execution::identity::encode_task_identity;
use novarocks_proto_models::novarocks as proto;
use novarocks_spi::connector::CatalogProperties;

use super::host::{HostRejection, TaskDynamicFilterRead};

/// The catalogs one establish asks this backend to materialize.
///
/// Validation is `CatalogSet`'s own: this recovers the message and asks it to
/// parse itself, so the reachability rules stay in one place.
pub fn catalog_bindings(
    payload: &dyn CodecOwnedContent,
) -> Result<Vec<CatalogProperties>, HostRejection> {
    let raw = stored_message::<novarocks_proto_models::catalog::CatalogSet>(payload)
        .ok_or_else(|| internal("catalog binding payload is not a catalog set"))?;
    CatalogSet::parse(raw.clone())
        .and_then(|catalog_set| catalog_set.catalogs())
        .map_err(|error| protocol(&format!("catalog binding is invalid: {error}")))
}

/// The runtime filter contribution one establish or shared advance installs.
///
/// The shared domain carries a whole *install* — participant id, lifecycle,
/// channels — not a runtime artifact. Those are different payload families
/// under different domain tags, and naming the expected type here is what
/// keeps them from being confused: a task's filter envelope reaching this
/// accessor answers `None` rather than decoding as an install.
pub fn runtime_filter_install(
    payload: &dyn CodecOwnedContent,
) -> Result<&proto::RuntimeFilterContribution, HostRejection> {
    stored_message::<proto::RuntimeFilterContribution>(payload)
        .ok_or_else(|| internal("runtime filter payload is not a participant contribution"))
}

/// The material one credential rotation carries.
///
/// This is the one accessor that returns a secret. It exists because a vended
/// credential has to reach the slot that installs it, and it hands back a type
/// that still has no `Debug` and no fingerprint — so the material can be
/// installed and dropped, and nothing more.
pub fn credential_material(update: &CredentialUpdate) -> Result<&WireCredential, HostRejection> {
    stored_credential(update.material().as_ref())
        .ok_or_else(|| internal("credential payload is not a decoded credential rotation"))
}

/// Projects one task's dynamic filter domains onto a fetch response.
///
/// `read` is `None` when the task has advertised nothing. The response then
/// carries version zero, which is this field family's "nothing": a published
/// `DomainVersion` is nonzero, so it cannot collide with one.
///
/// The alternative — answering an advertised version with an empty domain list
/// — would claim that the version carries no content, which is a different and
/// false statement. So a payload that cannot be projected is an error, not an
/// empty answer.
pub fn encode_dynamic_filter_read(
    identity: TaskIdentity,
    read: Option<&TaskDynamicFilterRead>,
) -> Result<proto::FetchTaskDynamicFiltersResponse, HostRejection> {
    let (version, domains) = match read {
        Some(read) => {
            let domain = encode_task_dynamic_filter_domain(read.version(), read.payload().as_ref())
                .map_err(|error| {
                    // Name the version: a reader that cannot be told which
                    // advertisement failed has no way to correlate this with
                    // the producer that published it.
                    internal(&format!(
                        "dynamic filter version {} cannot be projected: {error}",
                        read.version().get()
                    ))
                })?;
            (read.version().get(), vec![domain])
        }
        None => (0, Vec::new()),
    };
    Ok(proto::FetchTaskDynamicFiltersResponse {
        identity: Some(encode_task_identity(identity)),
        version,
        domains,
    })
}

/// Recovers the encoded fragment plan a descriptor carries.
///
/// A descriptor's plan is an `Arc<dyn PhysicalFragmentPlan>` that answers only
/// the contract version and the sink kind, because those are the only
/// questions the neutral layer may ask of a physical plan. Submitting one
/// needs the generated message itself, and only the crate that produced it can
/// name that type. A plan this backend did not receive over its own codec is
/// refused rather than guessed at.
pub fn fragment_plan(plan: &dyn PhysicalFragmentPlan) -> Result<&WireFragmentPlan, HostRejection> {
    plan.stored_representation()
        .and_then(|stored| stored.downcast_ref::<WireFragmentPlan>())
        .ok_or_else(|| internal("task descriptor plan is not a codec-produced fragment plan"))
}

/// The version a projection will report, without building it.
///
/// A caller that has to decide an outcome before encoding needs this; encoding
/// twice to find out would be the only alternative.
pub const fn dynamic_filter_read_version(
    read: Option<&TaskDynamicFilterRead>,
) -> Option<DomainVersion> {
    match read {
        Some(read) => Some(read.version()),
        None => None,
    }
}

/// A payload this process stored and cannot read back.
///
/// Every one of these is an invariant violation inside this binary rather than
/// something a peer can cause: the codec produced the handle, so failing to
/// recognise it means two owners disagree about which domain they are holding.
fn internal(detail: &str) -> HostRejection {
    HostRejection::new(TaskFailureCategory::Internal, detail)
}

/// Content that arrived well-formed and says something illegal.
fn protocol(detail: &str) -> HostRejection {
    HostRejection::new(TaskFailureCategory::Protocol, detail)
}

#[cfg(test)]
mod tests {
    use super::{
        catalog_bindings, credential_material, encode_dynamic_filter_read, fragment_plan,
        runtime_filter_install,
    };

    use std::sync::Arc;

    use novarocks_execution::task_execution::domain::{
        CodecOwnedContent, CredentialEpoch, CredentialLeaseId, DomainVersion,
    };
    use novarocks_execution::task_execution::identity::TaskIdentity;
    use novarocks_execution::task_execution::operation::CredentialUpdate;
    use novarocks_proto_codec::FieldPath;
    use novarocks_proto_codec::task_execution::domain::{WireContent, WireCredential};
    use novarocks_proto_models::{catalog, filter, novarocks as proto};
    use novarocks_types::identity::{
        AttemptId, BackendProcessId, QueryExecutionId, QueryId, StageId, TaskId,
    };

    use crate::task_execution::host::TaskDynamicFilterRead;

    const SECRET_SENTINEL: &str = "NOVAROCKS_SECRET_SENTINEL";

    fn identity() -> TaskIdentity {
        TaskIdentity::new(
            QueryExecutionId::new(QueryId::new(7, 9), AttemptId::new(1).expect("nonzero"))
                .expect("legal execution"),
            StageId::new(1).expect("nonzero stage"),
            TaskId::new(1).expect("nonzero task"),
            BackendProcessId::new_v7(),
        )
    }

    fn wire<T: prost::Message + 'static>(
        tag: &'static [u8],
        value: T,
    ) -> Arc<dyn CodecOwnedContent> {
        Arc::new(WireContent::new(tag, value))
    }

    #[test]
    fn an_empty_catalog_set_binds_to_no_catalog() {
        let payload = wire(b"catalog", catalog::CatalogSet::default());
        assert!(
            catalog_bindings(payload.as_ref())
                .expect("an empty set is legal")
                .is_empty()
        );
    }

    #[test]
    fn a_payload_of_the_wrong_domain_is_refused_rather_than_reinterpreted() {
        // Every one of these handles is well-formed content of a *different*
        // domain. Without the expected type stated at each accessor these
        // would be read as whatever the caller happened to want.
        let filter_envelope = wire(b"filter", filter::RuntimeFilterEnvelope::default());
        let contribution = wire(b"contribution", proto::RuntimeFilterContribution::default());
        let catalog_set = wire(b"catalog", catalog::CatalogSet::default());

        assert!(catalog_bindings(filter_envelope.as_ref()).is_err());
        assert!(catalog_bindings(contribution.as_ref()).is_err());
        assert!(runtime_filter_install(catalog_set.as_ref()).is_err());
        assert!(
            runtime_filter_install(filter_envelope.as_ref()).is_err(),
            "a task's filter envelope is not a participant install"
        );
        assert!(runtime_filter_install(contribution.as_ref()).is_ok());
    }

    #[test]
    fn a_task_that_advertised_nothing_reads_as_version_zero() {
        let response = encode_dynamic_filter_read(identity(), None).expect("a settled empty read");
        assert_eq!(response.version, 0);
        assert!(response.domains.is_empty());
    }

    #[test]
    fn an_advertised_domain_reads_back_as_its_envelope_at_its_version() {
        let envelope = filter::RuntimeFilterEnvelope {
            channel_id: 11,
            deployment_epoch: 3,
            ..filter::RuntimeFilterEnvelope::default()
        };
        let version = DomainVersion::new(4).expect("nonzero");
        let read = TaskDynamicFilterRead::new(
            version,
            Arc::new(WireContent::new(b"filter", envelope.clone())),
        );

        let response =
            encode_dynamic_filter_read(identity(), Some(&read)).expect("a projectable payload");

        assert_eq!(response.version, 4);
        assert_eq!(
            response.domains,
            vec![proto::TaskDynamicFilterDomain {
                version: 4,
                envelope: Some(envelope),
            }],
            "the response must carry the exact envelope the task advertised"
        );
    }

    #[test]
    fn an_unprojectable_advertised_payload_is_an_error_not_an_empty_answer() {
        // A version says "there is content at this version". Answering it with
        // an empty list would be a false statement, so this must fail.
        let read = TaskDynamicFilterRead::new(
            DomainVersion::new(2).expect("nonzero"),
            wire(b"catalog", catalog::CatalogSet::default()),
        );
        let rejection = encode_dynamic_filter_read(identity(), Some(&read))
            .expect_err("a non-envelope payload has no projection");
        assert!(
            rejection
                .detail()
                .as_str()
                .contains("not a runtime filter envelope"),
            "{rejection}"
        );
    }

    #[test]
    fn credential_material_is_recoverable_only_through_its_own_accessor() {
        let material = Arc::new(
            WireCredential::decode(&[], &[], FieldPath::root("credential"))
                .expect("an empty rotation is legal"),
        );
        let update = CredentialUpdate::new(
            CredentialLeaseId::new(1),
            CredentialEpoch::FIRST,
            Arc::clone(&material)
                as Arc<dyn novarocks_execution::task_execution::ConfidentialContent>,
        );

        let recovered = credential_material(&update).expect("codec-produced material");
        assert!(recovered.is_empty());

        // The accessor is the only way in: the update itself still renders
        // redacted, and the material has no Debug at all.
        let rendered = format!("{update:?}");
        assert!(!rendered.contains(SECRET_SENTINEL), "{rendered}");
        assert!(rendered.contains("<redacted>"), "{rendered}");
    }

    #[test]
    fn a_descriptor_plan_projects_back_only_when_this_codec_produced_it() {
        use novarocks_execution::exec::fragment::program::{
            FragmentContractVersion, FragmentSinkKind,
        };
        use novarocks_execution::task_execution::descriptor::PhysicalFragmentPlan;
        use novarocks_execution::task_execution::domain::ContentFingerprint;
        use novarocks_proto_codec::task_execution::descriptor::WireFragmentPlan;
        use novarocks_proto_models::plan;

        let wire = WireFragmentPlan::parse(
            proto::TaskFragmentPlan {
                plan: Some(plan::PlanFragment {
                    fragment_id: 4,
                    sink: Some(plan::DataSink {
                        kind: Some(plan::data_sink::Kind::Result(true)),
                    }),
                    ..Default::default()
                }),
                instance_params: Some(proto::InstanceParams {
                    fragment_instance_id: Some(novarocks_proto_models::common::UniqueId {
                        hi: 11,
                        lo: 12,
                    }),
                    ..Default::default()
                }),
            },
            FieldPath::root("plan"),
        )
        .expect("a legal fragment plan");

        // Through the neutral handle, which is how a descriptor carries it.
        let handle: Arc<dyn PhysicalFragmentPlan> = Arc::new(wire);
        let recovered = fragment_plan(handle.as_ref()).expect("this codec produced it");
        assert_eq!(recovered.plan().fragment_id, 4);
        assert_eq!(
            recovered
                .instance_params()
                .fragment_instance_id
                .as_ref()
                .map(|id| (id.hi, id.lo)),
            Some((11, 12))
        );

        // A plan this backend did not receive over its own codec cannot be
        // submitted, and saying so is better than guessing at its shape.
        #[derive(Debug)]
        struct ForeignPlan;
        impl CodecOwnedContent for ForeignPlan {
            fn fingerprint(&self) -> ContentFingerprint {
                ContentFingerprint::from_bytes([0x5a; 16])
            }
            fn encoded_len(&self) -> usize {
                1
            }
        }
        impl PhysicalFragmentPlan for ForeignPlan {
            fn contract_version(&self) -> FragmentContractVersion {
                FragmentContractVersion::CURRENT
            }
            fn sink_kind(&self) -> FragmentSinkKind {
                FragmentSinkKind::Result
            }
        }
        let foreign: Arc<dyn PhysicalFragmentPlan> = Arc::new(ForeignPlan);
        assert!(fragment_plan(foreign.as_ref()).is_err());
    }
}
