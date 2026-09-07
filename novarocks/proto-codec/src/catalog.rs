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

//! Validated generated catalog carriers.

use prost::Message;

use crate::{FieldPath, ProtocolError, ProtocolErrorKind};
use novarocks_proto_models::catalog as wire;
use novarocks_spi::connector::{
    CATALOG_VERSION_BYTES, CatalogCredentialBinding, CatalogCredentialMode,
    CatalogCredentialPurpose, CatalogHandle, CatalogProperties, CatalogProperty,
    CatalogProviderKind, CatalogVersion, ConnectorInstanceId, CredentialConsumerRole,
    MAX_CATALOG_SET_BYTES, MAX_CATALOGS_PER_QUERY, MAX_PRUNE_CATALOG_SET_BYTES,
    MAX_REACHABLE_CATALOGS_PER_PRUNE, StaticCredentialReference,
};

/// One exact, validated query-wide catalog contribution.
#[derive(Clone, Debug, PartialEq)]
pub struct CatalogSet {
    raw: wire::CatalogSet,
}

/// One complete, validated reachability snapshot for one backend.
///
/// The snapshot intentionally carries the full `CatalogHandle` rather than a
/// catalog name: pruning a newly installed incarnation from an older query's
/// reachability set would otherwise be possible.
#[derive(Clone, Debug, PartialEq)]
pub struct PruneCatalogsRequest {
    raw: wire::PruneCatalogsRequest,
}

/// The closed outcome of a catalog reachability prune request.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PruneCatalogsOutcome {
    Accepted,
    Rejected { safe_detail: String },
}

/// Validated response to a reachability prune request.
#[derive(Clone, Debug, PartialEq)]
pub struct PruneCatalogsResponse {
    raw: wire::PruneCatalogsResponse,
}

impl CatalogSet {
    pub fn new(
        catalogs: impl IntoIterator<Item = CatalogProperties>,
    ) -> Result<Self, ProtocolError> {
        Self::parse(wire::CatalogSet {
            catalogs: catalogs
                .into_iter()
                .map(encode_catalog_properties)
                .collect(),
        })
    }

    pub fn parse(raw: wire::CatalogSet) -> Result<Self, ProtocolError> {
        if raw.encoded_len() > MAX_CATALOG_SET_BYTES {
            return Err(resource_exhausted(
                FieldPath::root("catalog_set"),
                "encoded catalog set exceeds 1 MiB",
            ));
        }
        if raw.catalogs.len() > MAX_CATALOGS_PER_QUERY {
            return Err(resource_exhausted(
                FieldPath::root("catalog_set").field("catalogs"),
                "catalog set exceeds 256 entries",
            ));
        }
        let mut previous_name = None;
        for (index, properties) in raw.catalogs.iter().cloned().enumerate() {
            let properties = decode_catalog_properties(
                properties,
                FieldPath::root("catalog_set")
                    .field("catalogs")
                    .index(index),
            )?;
            let name = properties.handle().catalog_name().as_str().to_owned();
            if previous_name
                .as_deref()
                .is_some_and(|previous| previous >= name.as_str())
            {
                return Err(invalid(
                    FieldPath::root("catalog_set")
                        .field("catalogs")
                        .index(index),
                    "catalogs must be strictly sorted by catalog name with no duplicate name",
                ));
            }
            previous_name = Some(name);
        }
        Ok(Self { raw })
    }

    pub const fn as_proto(&self) -> &wire::CatalogSet {
        &self.raw
    }

    pub fn catalogs(&self) -> Result<Vec<CatalogProperties>, ProtocolError> {
        self.raw
            .catalogs
            .iter()
            .cloned()
            .enumerate()
            .map(|(index, properties)| {
                decode_catalog_properties(
                    properties,
                    FieldPath::root("catalog_set")
                        .field("catalogs")
                        .index(index),
                )
            })
            .collect()
    }
}

impl PruneCatalogsRequest {
    pub fn new(
        reachable_catalogs: impl IntoIterator<Item = CatalogHandle>,
    ) -> Result<Self, ProtocolError> {
        Self::parse(wire::PruneCatalogsRequest {
            reachable_catalogs: reachable_catalogs
                .into_iter()
                .map(|handle| encode_catalog_handle(&handle))
                .collect(),
        })
    }

    pub fn parse(raw: wire::PruneCatalogsRequest) -> Result<Self, ProtocolError> {
        if raw.encoded_len() > MAX_PRUNE_CATALOG_SET_BYTES {
            return Err(resource_exhausted(
                FieldPath::root("prune_catalogs_request"),
                "encoded reachable catalog snapshot exceeds 8 MiB",
            ));
        }
        if raw.reachable_catalogs.len() > MAX_REACHABLE_CATALOGS_PER_PRUNE {
            return Err(resource_exhausted(
                FieldPath::root("prune_catalogs_request").field("reachable_catalogs"),
                "reachable catalog snapshot exceeds 65536 entries",
            ));
        }

        let mut previous = None;
        for (index, handle) in raw.reachable_catalogs.iter().cloned().enumerate() {
            let handle = decode_catalog_handle(
                handle,
                FieldPath::root("prune_catalogs_request")
                    .field("reachable_catalogs")
                    .index(index),
            )?;
            if previous
                .as_ref()
                .is_some_and(|previous: &CatalogHandle| previous >= &handle)
            {
                return Err(invalid(
                    FieldPath::root("prune_catalogs_request")
                        .field("reachable_catalogs")
                        .index(index),
                    "reachable catalogs must be strictly sorted by catalog handle with no duplicate",
                ));
            }
            previous = Some(handle);
        }
        Ok(Self { raw })
    }

    pub const fn as_proto(&self) -> &wire::PruneCatalogsRequest {
        &self.raw
    }

    pub fn reachable_catalogs(&self) -> Result<Vec<CatalogHandle>, ProtocolError> {
        self.raw
            .reachable_catalogs
            .iter()
            .cloned()
            .enumerate()
            .map(|(index, handle)| {
                decode_catalog_handle(
                    handle,
                    FieldPath::root("prune_catalogs_request")
                        .field("reachable_catalogs")
                        .index(index),
                )
            })
            .collect()
    }
}

impl PruneCatalogsResponse {
    pub fn accepted() -> Self {
        Self {
            raw: wire::PruneCatalogsResponse {
                outcome: Some(wire::prune_catalogs_response::Outcome::Accepted(
                    wire::PruneCatalogsAccepted {},
                )),
            },
        }
    }

    pub fn rejected(safe_detail: impl Into<String>) -> Result<Self, ProtocolError> {
        let safe_detail = safe_detail.into();
        validate_safe_text(
            &safe_detail,
            FieldPath::root("prune_catalogs_response")
                .field("rejected")
                .field("safe_detail"),
        )?;
        Ok(Self {
            raw: wire::PruneCatalogsResponse {
                outcome: Some(wire::prune_catalogs_response::Outcome::Rejected(
                    wire::PruneCatalogsRejected { safe_detail },
                )),
            },
        })
    }

    pub fn parse(raw: wire::PruneCatalogsResponse) -> Result<Self, ProtocolError> {
        use wire::prune_catalogs_response::Outcome;

        match raw.outcome.as_ref() {
            Some(Outcome::Accepted(_)) => Ok(Self { raw }),
            Some(Outcome::Rejected(rejected)) => {
                validate_safe_text(
                    &rejected.safe_detail,
                    FieldPath::root("prune_catalogs_response")
                        .field("rejected")
                        .field("safe_detail"),
                )?;
                Ok(Self { raw })
            }
            None => Err(missing(
                FieldPath::root("prune_catalogs_response").field("outcome"),
                "prune catalogs outcome is required and must be known",
            )),
        }
    }

    pub const fn as_proto(&self) -> &wire::PruneCatalogsResponse {
        &self.raw
    }

    pub fn outcome(&self) -> PruneCatalogsOutcome {
        match self.raw.outcome.as_ref() {
            Some(wire::prune_catalogs_response::Outcome::Accepted(_)) => {
                PruneCatalogsOutcome::Accepted
            }
            Some(wire::prune_catalogs_response::Outcome::Rejected(rejected)) => {
                PruneCatalogsOutcome::Rejected {
                    safe_detail: rejected.safe_detail.clone(),
                }
            }
            None => unreachable!("PruneCatalogsResponse::parse validates the required outcome"),
        }
    }
}

pub fn encode_catalog_handle(handle: &CatalogHandle) -> wire::CatalogHandle {
    wire::CatalogHandle {
        catalog_name: handle.catalog_name().as_str().to_owned(),
        version: handle.version().as_bytes().to_vec(),
    }
}

pub fn decode_catalog_handle(
    raw: wire::CatalogHandle,
    root: FieldPath,
) -> Result<CatalogHandle, ProtocolError> {
    let catalog_name = ConnectorInstanceId::try_from_canonical(&raw.catalog_name)
        .map_err(|error| invalid(root.clone().field("catalog_name"), error.to_string()))?;
    let version: [u8; CATALOG_VERSION_BYTES] = raw.version.try_into().map_err(|_| {
        invalid(
            root.field("version"),
            "catalog version must contain exactly 32 bytes",
        )
    })?;
    Ok(CatalogHandle::new(
        catalog_name,
        CatalogVersion::from_bytes(version),
    ))
}

pub fn encode_catalog_properties(properties: CatalogProperties) -> wire::CatalogProperties {
    wire::CatalogProperties {
        handle: Some(encode_catalog_handle(properties.handle())),
        provider_kind: encode_provider_kind(properties.provider_kind()) as i32,
        config_format_version: properties.config_format_version(),
        execution_properties: properties
            .execution_properties()
            .iter()
            .map(|property| wire::CatalogProperty {
                key: property.key().to_owned(),
                value: property.value().to_owned(),
            })
            .collect(),
        credential_bindings: properties
            .credential_bindings()
            .iter()
            .cloned()
            .map(encode_catalog_credential_binding)
            .collect(),
    }
}

pub fn decode_catalog_properties(
    raw: wire::CatalogProperties,
    root: FieldPath,
) -> Result<CatalogProperties, ProtocolError> {
    let handle = raw
        .handle
        .ok_or_else(|| missing(root.clone().field("handle"), "catalog handle is required"))
        .and_then(|handle| decode_catalog_handle(handle, root.clone().field("handle")))?;
    let provider_kind =
        decode_provider_kind(raw.provider_kind, root.clone().field("provider_kind"))?;
    let mut properties = Vec::with_capacity(raw.execution_properties.len());
    for (index, property) in raw.execution_properties.into_iter().enumerate() {
        let decoded = CatalogProperty::new(&property.key, &property.value).map_err(|error| {
            invalid(
                root.clone().field("execution_properties").index(index),
                error.to_string(),
            )
        })?;
        if properties
            .last()
            .is_some_and(|previous: &CatalogProperty| previous.key() >= decoded.key())
        {
            return Err(invalid(
                root.clone().field("execution_properties").index(index),
                "catalog properties must be strictly sorted by key with no duplicate key",
            ));
        }
        properties.push(decoded);
    }
    let mut bindings = Vec::with_capacity(raw.credential_bindings.len());
    for (index, binding) in raw.credential_bindings.into_iter().enumerate() {
        let path = root.clone().field("credential_bindings").index(index);
        let decoded = decode_catalog_credential_binding(binding, path.clone())?;
        if bindings.last().is_some_and(|previous| previous >= &decoded) {
            return Err(invalid(
                path,
                "catalog credential bindings must be strictly sorted with no duplicate purpose",
            ));
        }
        bindings.push(decoded);
    }
    CatalogProperties::new(
        handle,
        provider_kind,
        raw.config_format_version,
        properties,
        bindings,
    )
    .map_err(|error| invalid(root, error.to_string()))
}

fn encode_catalog_credential_binding(
    binding: CatalogCredentialBinding,
) -> wire::CatalogCredentialBinding {
    let mode = match binding.mode() {
        CatalogCredentialMode::Static(reference) => {
            wire::catalog_credential_binding::Mode::StaticCredential(
                wire::StaticCredentialReference {
                    name: reference.name().to_owned(),
                    generation: reference.generation().to_owned(),
                },
            )
        }
        CatalogCredentialMode::Vended => {
            wire::catalog_credential_binding::Mode::VendedCredential(wire::VendedCredential {})
        }
    };
    wire::CatalogCredentialBinding {
        purpose: encode_credential_purpose(binding.purpose()) as i32,
        consumer_role: encode_consumer_role(binding.consumer_role()) as i32,
        mode: Some(mode),
    }
}

fn decode_catalog_credential_binding(
    raw: wire::CatalogCredentialBinding,
    root: FieldPath,
) -> Result<CatalogCredentialBinding, ProtocolError> {
    let purpose = decode_credential_purpose(raw.purpose, root.clone().field("purpose"))?;
    let consumer_role =
        decode_consumer_role(raw.consumer_role, root.clone().field("consumer_role"))?;
    let mode = match raw
        .mode
        .ok_or_else(|| missing(root.clone().field("mode"), "credential mode is required"))?
    {
        wire::catalog_credential_binding::Mode::StaticCredential(reference) => {
            CatalogCredentialMode::Static(
                StaticCredentialReference::try_new(&reference.name, &reference.generation)
                    .map_err(|error| {
                        invalid(root.clone().field("static_credential"), error.to_string())
                    })?,
            )
        }
        wire::catalog_credential_binding::Mode::VendedCredential(_) => {
            CatalogCredentialMode::Vended
        }
    };
    CatalogCredentialBinding::try_new(purpose, consumer_role, mode)
        .map_err(|error| invalid(root, error.to_string()))
}

fn encode_credential_purpose(value: CatalogCredentialPurpose) -> wire::CatalogCredentialPurpose {
    match value {
        CatalogCredentialPurpose::CatalogControl => wire::CatalogCredentialPurpose::CatalogControl,
        CatalogCredentialPurpose::ObjectStoreData => {
            wire::CatalogCredentialPurpose::ObjectStoreData
        }
    }
}

fn decode_credential_purpose(
    value: i32,
    root: FieldPath,
) -> Result<CatalogCredentialPurpose, ProtocolError> {
    match wire::CatalogCredentialPurpose::try_from(value) {
        Ok(wire::CatalogCredentialPurpose::CatalogControl) => {
            Ok(CatalogCredentialPurpose::CatalogControl)
        }
        Ok(wire::CatalogCredentialPurpose::ObjectStoreData) => {
            Ok(CatalogCredentialPurpose::ObjectStoreData)
        }
        _ => Err(invalid(root, "catalog credential purpose is invalid")),
    }
}

fn encode_consumer_role(value: CredentialConsumerRole) -> wire::CredentialConsumerRole {
    match value {
        CredentialConsumerRole::Frontend => wire::CredentialConsumerRole::Frontend,
        CredentialConsumerRole::Backend => wire::CredentialConsumerRole::Backend,
        CredentialConsumerRole::FrontendAndBackend => {
            wire::CredentialConsumerRole::FrontendAndBackend
        }
    }
}

fn decode_consumer_role(
    value: i32,
    root: FieldPath,
) -> Result<CredentialConsumerRole, ProtocolError> {
    match wire::CredentialConsumerRole::try_from(value) {
        Ok(wire::CredentialConsumerRole::Frontend) => Ok(CredentialConsumerRole::Frontend),
        Ok(wire::CredentialConsumerRole::Backend) => Ok(CredentialConsumerRole::Backend),
        Ok(wire::CredentialConsumerRole::FrontendAndBackend) => {
            Ok(CredentialConsumerRole::FrontendAndBackend)
        }
        _ => Err(invalid(root, "catalog credential consumer role is invalid")),
    }
}

fn encode_provider_kind(value: CatalogProviderKind) -> wire::CatalogProviderKind {
    match value {
        CatalogProviderKind::Iceberg => wire::CatalogProviderKind::Iceberg,
        CatalogProviderKind::StarRocks => wire::CatalogProviderKind::Starrocks,
    }
}

fn decode_provider_kind(value: i32, root: FieldPath) -> Result<CatalogProviderKind, ProtocolError> {
    match wire::CatalogProviderKind::try_from(value) {
        Ok(wire::CatalogProviderKind::Iceberg) => Ok(CatalogProviderKind::Iceberg),
        Ok(wire::CatalogProviderKind::Starrocks) => Ok(CatalogProviderKind::StarRocks),
        _ => Err(invalid(
            root,
            "catalog provider kind is required and must be known",
        )),
    }
}

fn invalid(path: FieldPath, detail: impl Into<String>) -> ProtocolError {
    ProtocolError::new(path, ProtocolErrorKind::InvalidValue, detail)
}

fn missing(path: FieldPath, detail: impl Into<String>) -> ProtocolError {
    ProtocolError::new(path, ProtocolErrorKind::MissingField, detail)
}

fn resource_exhausted(path: FieldPath, detail: impl Into<String>) -> ProtocolError {
    ProtocolError::new(path, ProtocolErrorKind::Capacity, detail)
}

fn validate_safe_text(value: &str, root: FieldPath) -> Result<(), ProtocolError> {
    if value.trim().is_empty() || value.len() > 512 {
        return Err(invalid(
            root,
            "safe diagnostic text must be nonempty and at most 512 bytes",
        ));
    }
    if value.contains('\n') || value.contains('\r') {
        return Err(invalid(root, "safe diagnostic text must be a single line"));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn properties(name: &str, version: u8) -> CatalogProperties {
        CatalogProperties::new(
            CatalogHandle::new(
                ConnectorInstanceId::try_from_canonical(name).unwrap(),
                CatalogVersion::from_bytes([version; CATALOG_VERSION_BYTES]),
            ),
            CatalogProviderKind::Iceberg,
            1,
            vec![CatalogProperty::new("warehouse", "s3://warehouse").unwrap()],
            vec![
                CatalogCredentialBinding::try_new(
                    CatalogCredentialPurpose::ObjectStoreData,
                    CredentialConsumerRole::FrontendAndBackend,
                    CatalogCredentialMode::Static(
                        StaticCredentialReference::try_new("object_store", "v1").unwrap(),
                    ),
                )
                .unwrap(),
            ],
        )
        .unwrap()
    }

    #[test]
    fn catalog_set_round_trips_exact_sorted_values() {
        let set = CatalogSet::new([properties("alpha", 1), properties("beta", 2)]).unwrap();
        let decoded = CatalogSet::parse(set.as_proto().clone())
            .unwrap()
            .catalogs()
            .unwrap();
        assert_eq!(decoded.len(), 2);
        assert_eq!(decoded[1].handle().catalog_name().as_str(), "beta");
    }

    #[test]
    fn catalog_set_rejects_noncanonical_versions_and_order() {
        let mut raw = CatalogSet::new([properties("alpha", 1)])
            .unwrap()
            .as_proto()
            .clone();
        raw.catalogs[0].handle.as_mut().unwrap().version.clear();
        assert!(CatalogSet::parse(raw).is_err());

        let raw = wire::CatalogSet {
            catalogs: vec![
                encode_catalog_properties(properties("beta", 2)),
                encode_catalog_properties(properties("alpha", 1)),
            ],
        };
        assert!(CatalogSet::parse(raw).is_err());
    }

    #[test]
    fn prune_catalogs_request_round_trips_a_complete_sorted_snapshot() {
        let request = PruneCatalogsRequest::new([
            properties("alpha", 1).handle().clone(),
            properties("beta", 2).handle().clone(),
        ])
        .unwrap();

        let decoded = PruneCatalogsRequest::parse(request.as_proto().clone())
            .unwrap()
            .reachable_catalogs()
            .unwrap();
        assert_eq!(decoded.len(), 2);
        assert_eq!(decoded[0].catalog_name().as_str(), "alpha");
        assert_eq!(decoded[1].catalog_name().as_str(), "beta");
    }

    #[test]
    fn prune_catalogs_request_rejects_duplicate_and_malformed_handles() {
        let duplicate = wire::PruneCatalogsRequest {
            reachable_catalogs: vec![
                encode_catalog_handle(properties("alpha", 1).handle()),
                encode_catalog_handle(properties("alpha", 1).handle()),
            ],
        };
        let duplicate_error = PruneCatalogsRequest::parse(duplicate)
            .expect_err("duplicate catalog handles must be rejected");
        assert!(duplicate_error.detail().contains("strictly sorted"));

        let malformed = wire::PruneCatalogsRequest {
            reachable_catalogs: vec![wire::CatalogHandle {
                catalog_name: "alpha".into(),
                version: vec![1],
            }],
        };
        let malformed_error = PruneCatalogsRequest::parse(malformed)
            .expect_err("catalog handles require exactly 32 version bytes");
        assert!(malformed_error.detail().contains("exactly 32 bytes"));
    }

    #[test]
    fn prune_catalogs_response_is_closed_and_safe() {
        assert_eq!(
            PruneCatalogsResponse::parse(PruneCatalogsResponse::accepted().as_proto().clone())
                .unwrap()
                .outcome(),
            PruneCatalogsOutcome::Accepted
        );
        assert_eq!(
            PruneCatalogsResponse::rejected("manager is shutting down")
                .unwrap()
                .outcome(),
            PruneCatalogsOutcome::Rejected {
                safe_detail: "manager is shutting down".into(),
            }
        );

        let unsafe_detail = wire::PruneCatalogsResponse {
            outcome: Some(wire::prune_catalogs_response::Outcome::Rejected(
                wire::PruneCatalogsRejected {
                    safe_detail: "line one\nline two".into(),
                },
            )),
        };
        assert!(PruneCatalogsResponse::parse(unsafe_detail).is_err());

        // Prost discards unknown oneof tags. The validated carrier must still
        // reject that decoded result instead of interpreting it as accepted.
        let unknown_oneof = wire::PruneCatalogsResponse::decode([0x1a, 0x00].as_slice()).unwrap();
        let unknown_error = PruneCatalogsResponse::parse(unknown_oneof)
            .expect_err("an unknown outcome must fail closed");
        assert!(unknown_error.detail().contains("must be known"));
    }
}
