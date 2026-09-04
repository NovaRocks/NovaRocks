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

use std::fmt;

use novarocks_proto_models::FILE_DESCRIPTOR_SET;
use novarocks_types::NativeCompatibilityId;
use sha2::{Digest, Sha256};

const GIT_HASH: &str = env!("NOVAROCKS_GIT_HASH");
const GIT_TIME: &str = env!("NOVAROCKS_GIT_TIME");
const NATIVE_BUILD_IDENTITY: &str = env!("NOVAROCKS_NATIVE_BUILD_IDENTITY");

// Design: ADR-0121 (docs/adr/ADR-0121-native-compatibility-islands-and-ingress-admission.md)
/// Domain separator for the immutable Native compatibility identity encoding.
pub const NATIVE_COMPATIBILITY_DOMAIN: &[u8] = b"novarocks.native-compatibility-id/v3\0";

/// Explicit compatibility epoch for an execution-contract change that cannot
/// be represented by the descriptor or the closed carrier manifest.
#[cfg(not(feature = "native-compatibility-test-fixture"))]
pub const NATIVE_COMPAT_EPOCH: u64 = 2;

/// Test-only alternate epoch used to produce an actual different-island binary.
#[cfg(feature = "native-compatibility-test-fixture")]
pub const NATIVE_COMPAT_EPOCH: u64 = 3;

#[cfg(all(feature = "native-compatibility-test-fixture", not(debug_assertions)))]
compile_error!("native-compatibility-test-fixture is only supported by debug and dev-opt builds");

/// One statically linked Native carrier declaration.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NativeCarrierDeclaration {
    provider_id: Box<str>,
    contract_revision: u64,
}

impl NativeCarrierDeclaration {
    pub fn try_new(
        provider_id: impl AsRef<str>,
        contract_revision: u64,
    ) -> Result<Self, NativeCompatibilityError> {
        let provider_id = provider_id.as_ref();
        if provider_id.is_empty() {
            return Err(NativeCompatibilityError::EmptyProviderId);
        }
        if provider_id.len() > u16::MAX as usize {
            return Err(NativeCompatibilityError::ProviderIdTooLong {
                actual: provider_id.len(),
            });
        }
        if contract_revision == 0 {
            return Err(NativeCompatibilityError::ZeroCarrierRevision {
                provider_id: provider_id.into(),
            });
        }
        Ok(Self {
            provider_id: provider_id.into(),
            contract_revision,
        })
    }

    pub fn provider_id(&self) -> &str {
        &self.provider_id
    }

    pub const fn contract_revision(&self) -> u64 {
        self.contract_revision
    }
}

/// Immutable material used to identify one Native compatibility island.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NativeCompatibilityMaterial {
    id: NativeCompatibilityId,
    descriptor_digest: [u8; 32],
    function_catalog_digest: [u8; 32],
    execution_implementation_manifest_digest: [u8; 32],
    epoch: u64,
    carriers: Box<[NativeCarrierDeclaration]>,
}

impl NativeCompatibilityMaterial {
    pub const fn id(&self) -> NativeCompatibilityId {
        self.id
    }

    pub const fn descriptor_digest(&self) -> [u8; 32] {
        self.descriptor_digest
    }

    pub const fn function_catalog_digest(&self) -> [u8; 32] {
        self.function_catalog_digest
    }

    pub const fn execution_implementation_manifest_digest(&self) -> [u8; 32] {
        self.execution_implementation_manifest_digest
    }

    pub const fn epoch(&self) -> u64 {
        self.epoch
    }

    pub fn carriers(&self) -> &[NativeCarrierDeclaration] {
        &self.carriers
    }
}

/// Fail-closed validation error for native compatibility material.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum NativeCompatibilityError {
    EmptyDescriptorSet,
    EmptyCarrierManifest,
    TooManyCarriers {
        actual: usize,
    },
    EmptyProviderId,
    ProviderIdTooLong {
        actual: usize,
    },
    ZeroCarrierRevision {
        provider_id: Box<str>,
    },
    CarrierManifestNotStrictlySorted {
        previous: Box<str>,
        current: Box<str>,
    },
}

impl fmt::Display for NativeCompatibilityError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::EmptyDescriptorSet => {
                formatter.write_str("native compatibility descriptor set is empty")
            }
            Self::EmptyCarrierManifest => {
                formatter.write_str("native compatibility carrier manifest is empty")
            }
            Self::TooManyCarriers { actual } => {
                write!(
                    formatter,
                    "native compatibility carrier manifest has {actual} entries"
                )
            }
            Self::EmptyProviderId => {
                formatter.write_str("native compatibility provider id is empty")
            }
            Self::ProviderIdTooLong { actual } => {
                write!(
                    formatter,
                    "native compatibility provider id is {actual} bytes, exceeding u16"
                )
            }
            Self::ZeroCarrierRevision { provider_id } => {
                write!(
                    formatter,
                    "native compatibility carrier {provider_id} has zero revision"
                )
            }
            Self::CarrierManifestNotStrictlySorted { previous, current } => write!(
                formatter,
                "native compatibility carrier manifest is not strictly sorted: {previous} then {current}"
            ),
        }
    }
}

impl std::error::Error for NativeCompatibilityError {}

/// Derives exact compatibility material from a descriptor set and a Server-owned
/// static carrier manifest. The input order is part of the validation contract:
/// callers must provide an already strictly sorted declaration set.
pub fn derive_native_compatibility_material(
    descriptor_set: &[u8],
    carriers: impl IntoIterator<Item = NativeCarrierDeclaration>,
    function_catalog_digest: [u8; 32],
    execution_implementation_manifest_digest: [u8; 32],
    epoch: u64,
) -> Result<NativeCompatibilityMaterial, NativeCompatibilityError> {
    if descriptor_set.is_empty() {
        return Err(NativeCompatibilityError::EmptyDescriptorSet);
    }
    let carriers = carriers.into_iter().collect::<Vec<_>>();
    if carriers.is_empty() {
        return Err(NativeCompatibilityError::EmptyCarrierManifest);
    }
    if carriers.len() > u32::MAX as usize {
        return Err(NativeCompatibilityError::TooManyCarriers {
            actual: carriers.len(),
        });
    }
    for pair in carriers.windows(2) {
        if pair[0].provider_id().as_bytes() >= pair[1].provider_id().as_bytes() {
            return Err(NativeCompatibilityError::CarrierManifestNotStrictlySorted {
                previous: pair[0].provider_id().into(),
                current: pair[1].provider_id().into(),
            });
        }
    }

    let descriptor_digest: [u8; 32] = Sha256::digest(descriptor_set).into();
    let mut hasher = Sha256::new();
    hasher.update(NATIVE_COMPATIBILITY_DOMAIN);
    hasher.update(descriptor_digest);
    hasher.update(function_catalog_digest);
    hasher.update(execution_implementation_manifest_digest);
    hasher.update(
        u32::try_from(carriers.len())
            .expect("carrier count was checked against u32")
            .to_be_bytes(),
    );
    for carrier in &carriers {
        let provider_id = carrier.provider_id().as_bytes();
        hasher.update(
            u16::try_from(provider_id.len())
                .expect("carrier provider id length was checked against u16")
                .to_be_bytes(),
        );
        hasher.update(provider_id);
        hasher.update(carrier.contract_revision().to_be_bytes());
    }
    hasher.update(epoch.to_be_bytes());

    Ok(NativeCompatibilityMaterial {
        id: NativeCompatibilityId::new(hasher.finalize().into()),
        descriptor_digest,
        function_catalog_digest,
        execution_implementation_manifest_digest,
        epoch,
        carriers: carriers.into_boxed_slice(),
    })
}

/// Derives the material for the repository's current Protocol descriptor.
pub fn derive_repository_native_compatibility_material(
    carriers: impl IntoIterator<Item = NativeCarrierDeclaration>,
    function_catalog_digest: [u8; 32],
    execution_implementation_manifest_digest: [u8; 32],
) -> Result<NativeCompatibilityMaterial, NativeCompatibilityError> {
    derive_native_compatibility_material(
        FILE_DESCRIPTOR_SET,
        carriers,
        function_catalog_digest,
        execution_implementation_manifest_digest,
        NATIVE_COMPAT_EPOCH,
    )
}

/// Immutable release identity used to admit native Backend processes.
///
/// It is supplied explicitly at build time or derived from the full Git commit.
/// It is intentionally distinct from the shorter human-facing version strings.
pub const fn native_build_identity() -> &'static str {
    NATIVE_BUILD_IDENTITY
}

/// Short version string reported via heartbeat, e.g. "novarocks-1b9f054a".
/// Matches StarRocks BE convention of "version-commit".
pub fn short_version() -> &'static str {
    static VERSION: std::sync::OnceLock<String> = std::sync::OnceLock::new();
    VERSION.get_or_init(|| format!("novarocks-{GIT_HASH}"))
}

/// Full version string including commit time for logging at startup.
pub fn full_version() -> &'static str {
    static VERSION: std::sync::OnceLock<String> = std::sync::OnceLock::new();
    VERSION.get_or_init(|| format!("novarocks-{GIT_HASH} ({GIT_TIME})"))
}

#[cfg(test)]
mod tests {
    use super::{
        NATIVE_COMPAT_EPOCH, NativeCarrierDeclaration, NativeCompatibilityError,
        derive_native_compatibility_material, native_build_identity,
    };

    fn carriers() -> [NativeCarrierDeclaration; 2] {
        [
            NativeCarrierDeclaration::try_new("iceberg", 1).expect("iceberg declaration"),
            NativeCarrierDeclaration::try_new("starrocks", 1).expect("starrocks declaration"),
        ]
    }

    #[test]
    fn native_build_identity_is_present_and_not_unknown() {
        let identity = native_build_identity();
        assert!(!identity.is_empty());
        assert_ne!(identity, "unknown");
        assert!(identity.len() <= 128);
        assert!(identity.chars().all(|character| {
            character.is_ascii_alphanumeric() || matches!(character, '.' | '_' | '-')
        }));
    }

    #[test]
    fn native_compatibility_material_matches_the_frozen_golden_vector() {
        let material = derive_native_compatibility_material(
            b"descriptor-v1",
            carriers(),
            [0x31; 32],
            [0x41; 32],
            1,
        )
        .expect("valid material");

        assert_eq!(
            material.id().to_string(),
            "1cfda438a3098656c9db5560d2864f20c9be7077806e1057b85c39fef97c01fd"
        );
        assert_eq!(material.function_catalog_digest(), [0x31; 32]);
        assert_eq!(
            material.execution_implementation_manifest_digest(),
            [0x41; 32]
        );
        assert_eq!(material.epoch(), 1);
        assert_eq!(material.carriers().len(), 2);
    }

    #[test]
    fn native_compatibility_material_changes_for_every_contract_input() {
        let original = derive_native_compatibility_material(
            b"descriptor-v1",
            carriers(),
            [0x31; 32],
            [0x41; 32],
            1,
        )
        .expect("original material");
        let descriptor = derive_native_compatibility_material(
            b"descriptor-v2",
            carriers(),
            [0x31; 32],
            [0x41; 32],
            1,
        )
        .expect("descriptor material");
        let provider_revision = derive_native_compatibility_material(
            b"descriptor-v1",
            [
                NativeCarrierDeclaration::try_new("iceberg", 2).expect("iceberg declaration"),
                NativeCarrierDeclaration::try_new("starrocks", 1).expect("starrocks declaration"),
            ],
            [0x31; 32],
            [0x41; 32],
            1,
        )
        .expect("provider revision material");
        let catalog_only = derive_native_compatibility_material(
            b"descriptor-v1",
            carriers(),
            [0x32; 32],
            [0x41; 32],
            1,
        )
        .expect("catalog-only material");
        let implementation_only = derive_native_compatibility_material(
            b"descriptor-v1",
            carriers(),
            [0x31; 32],
            [0x42; 32],
            1,
        )
        .expect("implementation-only material");
        let epoch = derive_native_compatibility_material(
            b"descriptor-v1",
            carriers(),
            [0x31; 32],
            [0x41; 32],
            2,
        )
        .expect("epoch material");

        assert_ne!(original.id(), descriptor.id());
        assert_ne!(original.id(), provider_revision.id());
        assert_ne!(original.id(), catalog_only.id());
        assert_ne!(original.id(), implementation_only.id());
        assert_ne!(original.id(), epoch.id());
    }

    #[test]
    fn native_compatibility_material_rejects_noncanonical_carrier_manifests() {
        let duplicate = NativeCarrierDeclaration::try_new("iceberg", 1).expect("declaration");
        let error = derive_native_compatibility_material(
            b"descriptor-v1",
            [duplicate.clone(), duplicate],
            [0x31; 32],
            [0x41; 32],
            1,
        )
        .expect_err("duplicate provider ids must fail");
        assert!(matches!(
            error,
            NativeCompatibilityError::CarrierManifestNotStrictlySorted { .. }
        ));

        let reversed = derive_native_compatibility_material(
            b"descriptor-v1",
            [
                NativeCarrierDeclaration::try_new("starrocks", 1).expect("starrocks declaration"),
                NativeCarrierDeclaration::try_new("iceberg", 1).expect("iceberg declaration"),
            ],
            [0x31; 32],
            [0x41; 32],
            1,
        )
        .expect_err("reordered provider ids must fail");
        assert!(matches!(
            reversed,
            NativeCompatibilityError::CarrierManifestNotStrictlySorted { .. }
        ));
    }

    #[test]
    fn test_fixture_epoch_is_explicit_and_never_ambient() {
        #[cfg(feature = "native-compatibility-test-fixture")]
        assert_eq!(NATIVE_COMPAT_EPOCH, 3);
        #[cfg(not(feature = "native-compatibility-test-fixture"))]
        assert_eq!(NATIVE_COMPAT_EPOCH, 2);
    }
}
