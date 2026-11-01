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

//! Native projections of task-protocol shared facts.
//!
//! Worker owns the lifecycle verdict and role products consume the typed
//! result.  This adapter is the sole place that recovers generated payloads
//! from codec-owned handles.

use prost::Message;
use sha2::{Digest, Sha256};

use novarocks_execution::runtime::query_options::QueryOptions;
use novarocks_execution_contract::task_execution::domain::{CodecOwnedContent, ContentFingerprint};
use novarocks_execution_contract::task_execution::operation::CredentialUpdate;
use novarocks_execution_contract::task_execution::status::TaskFailureCategory;
use novarocks_proto_codec::catalog::CatalogSet;
use novarocks_proto_codec::lifecycle::terminal::QueryTerminalProfileContributionTelemetry;
use novarocks_proto_models::novarocks as proto;
use novarocks_spi::connector::CatalogProperties;
use novarocks_task_codec::domain::{WireCredential, stored_credential, stored_message};
use novarocks_worker::{
    FrozenCodecContent, HostRejection, ReleasedContextEvidence, RuntimeFilterReleaseObservation,
};

use crate::query_options::decode_query_options;

const RELEASE_RUNTIME_FILTER_EVIDENCE_DOMAIN_TAG: &[u8] =
    b"novarocks.task_execution.release.runtime_filter_evidence.v1";

pub fn catalog_bindings(
    payload: &dyn CodecOwnedContent,
) -> Result<Vec<CatalogProperties>, HostRejection> {
    let raw = stored_message::<novarocks_proto_models::catalog::CatalogSet>(payload)
        .ok_or_else(|| carrier_mismatch(SharedFactCarrier::Catalog))?;
    CatalogSet::parse(raw.clone())
        .and_then(|catalog_set| catalog_set.catalogs())
        .map_err(|error| HostRejection::from_domain(TaskFailureCategory::Protocol, error))
}

pub fn runtime_filter_install(
    payload: &dyn CodecOwnedContent,
) -> Result<&proto::RuntimeFilterContribution, HostRejection> {
    stored_message::<proto::RuntimeFilterContribution>(payload)
        .ok_or_else(|| carrier_mismatch(SharedFactCarrier::RuntimeFilter))
}

pub fn sealed_runtime_filter_evidence(
    telemetry: QueryTerminalProfileContributionTelemetry,
) -> ReleasedContextEvidence {
    let observation = if telemetry.available().is_some() {
        RuntimeFilterReleaseObservation::Available
    } else {
        RuntimeFilterReleaseObservation::Unavailable
    };
    // The validated original is encoded once while its host-owned backing is
    // still supervised. Cold retention keeps no generated value or alias hook.
    let bytes = telemetry.as_proto().encode_to_vec();
    let mut hasher = Sha256::new();
    hasher.update(RELEASE_RUNTIME_FILTER_EVIDENCE_DOMAIN_TAG);
    hasher.update(&bytes);
    let digest = hasher.finalize();
    let mut fingerprint = [0u8; 16];
    fingerprint.copy_from_slice(&digest[..16]);
    ReleasedContextEvidence::with_runtime_filter(
        FrozenCodecContent::new(
            RELEASE_RUNTIME_FILTER_EVIDENCE_DOMAIN_TAG,
            ContentFingerprint::from_bytes(fingerprint),
            bytes.into(),
        ),
        observation,
    )
}

pub fn release_runtime_filter_telemetry(
    evidence: &ReleasedContextEvidence,
) -> Result<Option<QueryTerminalProfileContributionTelemetry>, HostRejection> {
    evidence
        .runtime_filter()
        .map(|payload| {
            if payload.domain_tag() != RELEASE_RUNTIME_FILTER_EVIDENCE_DOMAIN_TAG {
                return Err(carrier_mismatch(SharedFactCarrier::Terminal));
            }
            let raw = proto::QueryTerminalProfileContributionTelemetry::decode(payload.bytes())
                .map_err(|_| carrier_mismatch(SharedFactCarrier::Terminal))?;
            QueryTerminalProfileContributionTelemetry::parse(raw)
                .map_err(|error| HostRejection::from_domain(TaskFailureCategory::Internal, error))
        })
        .transpose()
}

pub fn query_options(payload: &dyn CodecOwnedContent) -> Result<QueryOptions, HostRejection> {
    let raw = stored_message::<proto::QueryOptions>(payload)
        .ok_or_else(|| carrier_mismatch(SharedFactCarrier::QueryOptions))?;
    Ok(decode_query_options(raw))
}

pub fn credential_material(update: &CredentialUpdate) -> Result<&WireCredential, HostRejection> {
    stored_credential(update.material().as_ref())
        .ok_or_else(|| carrier_mismatch(SharedFactCarrier::Credential))
}

/// These are owned-codec carrier type mismatches, independent of their
/// diagnostic wording. Connector/protocol errors retain their own projections.
#[derive(Debug)]
enum SharedFactCarrier {
    Catalog,
    RuntimeFilter,
    Terminal,
    QueryOptions,
    Credential,
}
impl std::fmt::Display for SharedFactCarrier {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Catalog => "catalog binding payload is not a catalog set",
            Self::RuntimeFilter => "runtime filter payload is not a participant contribution",
            Self::Terminal => "release runtime-filter evidence is not terminal telemetry",
            Self::QueryOptions => "query options payload is not a native query options message",
            Self::Credential => "credential payload is not a decoded credential rotation",
        })
    }
}
impl std::error::Error for SharedFactCarrier {}
impl novarocks_execution_contract::failure::FailureProjection for SharedFactCarrier {
    fn failure_fact(&self) -> novarocks_execution_contract::failure::FailureFact {
        use novarocks_execution_contract::failure::*;
        let code = match self {
            Self::Catalog => 800,
            Self::RuntimeFilter => 801,
            Self::Terminal => 802,
            Self::QueryOptions => 803,
            Self::Credential => 804,
        };
        FailureFact::new(
            FailureClass::Invariant,
            FailureReason::new(domain::NATIVE, std::num::NonZeroU16::new(code).unwrap()),
        )
    }
}
fn carrier_mismatch(source: SharedFactCarrier) -> HostRejection {
    let detail = novarocks_execution_contract::SafeDetail::truncating_display(&source);
    HostRejection::from_domain_with_detail(TaskFailureCategory::Internal, source, detail)
}

#[cfg(test)]
mod tests {
    use super::{
        catalog_bindings, credential_material, release_runtime_filter_telemetry,
        runtime_filter_install,
    };
    use novarocks_execution_contract::task_execution::domain::{
        CodecOwnedContent, CredentialEpoch, CredentialLeaseId,
    };
    use novarocks_execution_contract::task_execution::operation::CredentialUpdate;
    use novarocks_proto_codec::FieldPath;
    use novarocks_proto_models::{catalog, filter, novarocks as proto};
    use novarocks_task_codec::domain::{WireContent, WireCredential};
    use novarocks_worker::{
        FrozenCodecContent, ReleasedContextEvidence, RuntimeFilterReleaseObservation,
    };
    use prost::Message;
    use std::sync::Arc;

    fn wire<T: prost::Message + 'static>(
        tag: &'static [u8],
        value: T,
    ) -> Arc<dyn CodecOwnedContent> {
        Arc::new(WireContent::new(tag, value))
    }

    #[test]
    fn foreign_domain_payloads_are_not_reinterpreted() {
        let filter_envelope = wire(b"filter", filter::RuntimeFilterEnvelope::default());
        let contribution = wire(b"contribution", proto::RuntimeFilterContribution::default());
        let catalog_set = wire(b"catalog", catalog::CatalogSet::default());
        assert!(catalog_bindings(filter_envelope.as_ref()).is_err());
        assert!(catalog_bindings(contribution.as_ref()).is_err());
        assert!(runtime_filter_install(catalog_set.as_ref()).is_err());
        assert!(runtime_filter_install(filter_envelope.as_ref()).is_err());
        assert!(runtime_filter_install(contribution.as_ref()).is_ok());
    }

    #[test]
    fn empty_catalog_set_projects_to_no_bindings() {
        let payload = wire(b"catalog", catalog::CatalogSet::default());
        assert!(
            catalog_bindings(payload.as_ref())
                .expect("empty set is legal")
                .is_empty()
        );
    }

    #[test]
    fn credential_projection_accepts_only_codec_owned_confidential_material() {
        let material =
            WireCredential::decode(&[], FieldPath::root("credential")).expect("empty rotation");
        let update = CredentialUpdate::new(
            CredentialLeaseId::new(1),
            CredentialEpoch::new(1).expect("epoch"),
            Arc::new(material) as Arc<dyn novarocks_execution_contract::ConfidentialContent>,
        );
        assert!(
            credential_material(&update)
                .expect("codec-owned material")
                .is_empty()
        );
        assert!(format!("{update:?}").contains("<redacted>"));
    }

    #[test]
    fn foreign_release_evidence_is_refused_rather_than_encoded_as_absent() {
        let foreign = WireContent::new(b"catalog", catalog::CatalogSet::default());
        let evidence = ReleasedContextEvidence::with_runtime_filter(
            FrozenCodecContent::new(
                b"catalog",
                foreign.fingerprint(),
                foreign.wire().encode_to_vec().into(),
            ),
            RuntimeFilterReleaseObservation::Unavailable,
        );
        assert_eq!(evidence.runtime_filter().unwrap().domain_tag(), b"catalog");
        let rejection = release_runtime_filter_telemetry(&evidence)
            .expect_err("only terminal telemetry may be encoded on release");
        assert_eq!(
            rejection.category(),
            novarocks_execution_contract::task_execution::status::TaskFailureCategory::Internal
        );
    }
}
