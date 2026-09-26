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

// Design: ADR-0124 (docs/adr/ADR-0124-native-compatibility-islands-and-ingress-admission.md)
/// Domain separator for the immutable Native compatibility identity encoding.
pub const NATIVE_COMPATIBILITY_DOMAIN: &[u8] = b"novarocks.native-compatibility-id/v5\0";

/// Domain separator for the pure physical-plan contract component.
const PLAN_CONTRACT_DOMAIN: &[u8] = b"novarocks.physical-plan-contract/v1\0";

/// Explicit compatibility epoch for an execution-contract change that cannot
/// be represented by the descriptor or the closed carrier manifest.
///
/// Epoch 4 adds Accepted/Installed task creation and the normal context
/// Quiesce operation, generation-required covered observation, and exact
/// per-destination normal exchange closure with actual-stop convergence, and
/// exact advertised per-context preparation positions.
/// An epoch-3 process interprets the same task operation
/// boundary under the synchronous creation and direct release contract, so
/// the two never share an island.
#[cfg(not(feature = "native-compatibility-test-fixture"))]
pub const NATIVE_COMPAT_EPOCH: u64 = 4;

/// Test-only alternate epoch used to produce an actual different-island binary.
#[cfg(feature = "native-compatibility-test-fixture")]
pub const NATIVE_COMPAT_EPOCH: u64 = 5;

#[cfg(all(feature = "native-compatibility-test-fixture", not(debug_assertions)))]
compile_error!("native-compatibility-test-fixture is only supported by debug and dev-opt builds");

/// One statically linked Native carrier declaration.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NativeCarrierDeclaration {
    provider_id: Box<str>,
    contract_revision: u64,
    private_descriptor_digest: [u8; 32],
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
            private_descriptor_digest: [0; 32],
        })
    }

    /// Declare the exact provider-private descriptor compiled into the
    /// process. This keeps private protobuf evolution inside Native
    /// compatibility admission without exposing provider messages in the
    /// repository-wide descriptor set.
    pub fn try_new_with_private_descriptor(
        provider_id: impl AsRef<str>,
        contract_revision: u64,
        private_descriptor: &[u8],
    ) -> Result<Self, NativeCompatibilityError> {
        if private_descriptor.is_empty() {
            return Err(NativeCompatibilityError::EmptyPrivateDescriptor {
                provider_id: provider_id.as_ref().into(),
            });
        }
        let mut declaration = Self::try_new(provider_id, contract_revision)?;
        declaration.private_descriptor_digest = Sha256::digest(private_descriptor).into();
        Ok(declaration)
    }

    pub fn provider_id(&self) -> &str {
        &self.provider_id
    }

    pub const fn contract_revision(&self) -> u64 {
        self.contract_revision
    }

    pub const fn private_descriptor_digest(&self) -> [u8; 32] {
        self.private_descriptor_digest
    }
}

/// Immutable material used to identify one Native compatibility island.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NativeCompatibilityMaterial {
    id: NativeCompatibilityId,
    descriptor_digest: [u8; 32],
    function_catalog_digest: [u8; 32],
    execution_implementation_manifest_digest: [u8; 32],
    plan_contract_revision: u32,
    plan_contract_digest: [u8; 32],
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

    /// Revision supplied by Server from its statically linked plan contract.
    pub const fn plan_contract_revision(&self) -> u32 {
        self.plan_contract_revision
    }

    /// Exact component evidence for diagnosing a plan-contract mismatch.
    /// Admission still compares the complete Native compatibility identity.
    pub const fn plan_contract_digest(&self) -> [u8; 32] {
        self.plan_contract_digest
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
    ZeroPlanContractRevision,
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
    EmptyPrivateDescriptor {
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
            Self::ZeroPlanContractRevision => {
                formatter.write_str("native compatibility plan contract revision is zero")
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
            Self::EmptyPrivateDescriptor { provider_id } => write!(
                formatter,
                "native compatibility carrier {provider_id} has an empty private descriptor"
            ),
            Self::CarrierManifestNotStrictlySorted { previous, current } => write!(
                formatter,
                "native compatibility carrier manifest is not strictly sorted: {previous} then {current}"
            ),
        }
    }
}

impl std::error::Error for NativeCompatibilityError {}

/// Derives exact compatibility material from a descriptor set and a Server-owned
/// static carrier manifest and plan contract revision. The input order is part
/// of the validation contract: callers must provide an already strictly sorted
/// declaration set. This crate never selects a plan contract on Server's behalf.
pub fn derive_native_compatibility_material(
    descriptor_set: &[u8],
    carriers: impl IntoIterator<Item = NativeCarrierDeclaration>,
    function_catalog_digest: [u8; 32],
    execution_implementation_manifest_digest: [u8; 32],
    plan_contract_revision: u32,
    epoch: u64,
) -> Result<NativeCompatibilityMaterial, NativeCompatibilityError> {
    if descriptor_set.is_empty() {
        return Err(NativeCompatibilityError::EmptyDescriptorSet);
    }
    if plan_contract_revision == 0 {
        return Err(NativeCompatibilityError::ZeroPlanContractRevision);
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
    let plan_contract_digest: [u8; 32] = {
        let mut hasher = Sha256::new();
        hasher.update(PLAN_CONTRACT_DOMAIN);
        hasher.update(plan_contract_revision.to_be_bytes());
        hasher.finalize().into()
    };
    let mut hasher = Sha256::new();
    hasher.update(NATIVE_COMPATIBILITY_DOMAIN);
    hasher.update(descriptor_digest);
    hasher.update(function_catalog_digest);
    hasher.update(execution_implementation_manifest_digest);
    hasher.update(plan_contract_digest);
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
        hasher.update(carrier.private_descriptor_digest());
    }
    hasher.update(epoch.to_be_bytes());

    Ok(NativeCompatibilityMaterial {
        id: NativeCompatibilityId::new(hasher.finalize().into()),
        descriptor_digest,
        function_catalog_digest,
        execution_implementation_manifest_digest,
        plan_contract_revision,
        plan_contract_digest,
        epoch,
        carriers: carriers.into_boxed_slice(),
    })
}

/// Derives the material for the repository's current Protocol descriptor.
pub fn derive_repository_native_compatibility_material(
    carriers: impl IntoIterator<Item = NativeCarrierDeclaration>,
    function_catalog_digest: [u8; 32],
    execution_implementation_manifest_digest: [u8; 32],
    plan_contract_revision: u32,
) -> Result<NativeCompatibilityMaterial, NativeCompatibilityError> {
    derive_native_compatibility_material(
        FILE_DESCRIPTOR_SET,
        carriers,
        function_catalog_digest,
        execution_implementation_manifest_digest,
        plan_contract_revision,
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
            1,
        )
        .expect("valid material");

        assert_eq!(
            material.id().to_string(),
            "f7966eb8ccdf6b851ce864a7516573d52c196c8edb046918f0615e0bcfc9aaba"
        );
        assert_eq!(material.function_catalog_digest(), [0x31; 32]);
        assert_eq!(
            material.execution_implementation_manifest_digest(),
            [0x41; 32]
        );
        assert_eq!(material.plan_contract_revision(), 1);
        assert_eq!(
            material.plan_contract_digest(),
            [
                0x52, 0x72, 0xbc, 0x01, 0x7c, 0xab, 0x20, 0xf5, 0xb2, 0xe1, 0x9d, 0xb0, 0x35, 0x58,
                0x11, 0x08, 0x77, 0x10, 0xe0, 0xd8, 0x48, 0x16, 0xd8, 0x47, 0x24, 0xb5, 0x67, 0x0f,
                0xa3, 0xdd, 0x6b, 0x04,
            ]
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
            1,
        )
        .expect("original material");
        let descriptor = derive_native_compatibility_material(
            b"descriptor-v2",
            carriers(),
            [0x31; 32],
            [0x41; 32],
            1,
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
            1,
        )
        .expect("provider revision material");
        let private_descriptor = derive_native_compatibility_material(
            b"descriptor-v1",
            [
                NativeCarrierDeclaration::try_new_with_private_descriptor(
                    "iceberg",
                    1,
                    b"iceberg-private-v2",
                )
                .expect("iceberg private declaration"),
                NativeCarrierDeclaration::try_new("starrocks", 1).expect("starrocks declaration"),
            ],
            [0x31; 32],
            [0x41; 32],
            1,
            1,
        )
        .expect("private descriptor material");
        let catalog_only = derive_native_compatibility_material(
            b"descriptor-v1",
            carriers(),
            [0x32; 32],
            [0x41; 32],
            1,
            1,
        )
        .expect("catalog-only material");
        let implementation_only = derive_native_compatibility_material(
            b"descriptor-v1",
            carriers(),
            [0x31; 32],
            [0x42; 32],
            1,
            1,
        )
        .expect("implementation-only material");
        let epoch = derive_native_compatibility_material(
            b"descriptor-v1",
            carriers(),
            [0x31; 32],
            [0x41; 32],
            1,
            2,
        )
        .expect("epoch material");
        let plan_contract = derive_native_compatibility_material(
            b"descriptor-v1",
            carriers(),
            [0x31; 32],
            [0x41; 32],
            2,
            1,
        )
        .expect("plan contract material");

        assert_ne!(original.id(), descriptor.id());
        assert_ne!(original.id(), provider_revision.id());
        assert_ne!(original.id(), private_descriptor.id());
        assert_ne!(original.id(), catalog_only.id());
        assert_ne!(original.id(), implementation_only.id());
        assert_ne!(original.id(), epoch.id());
        assert_ne!(original.id(), plan_contract.id());
        assert_ne!(epoch.id(), plan_contract.id());
        assert_ne!(
            original.plan_contract_digest(),
            plan_contract.plan_contract_digest()
        );
        assert_eq!(
            original.descriptor_digest(),
            plan_contract.descriptor_digest()
        );
        assert_eq!(
            original.function_catalog_digest(),
            plan_contract.function_catalog_digest()
        );
        assert_eq!(
            original.execution_implementation_manifest_digest(),
            plan_contract.execution_implementation_manifest_digest()
        );
        assert_eq!(original.carriers(), plan_contract.carriers());
        assert_eq!(original.epoch(), plan_contract.epoch());
    }

    #[test]
    fn native_compatibility_material_rejects_zero_plan_contract_revision() {
        let error = derive_native_compatibility_material(
            b"descriptor-v1",
            carriers(),
            [0x31; 32],
            [0x41; 32],
            0,
            1,
        )
        .expect_err("a plan contract revision must be explicit and nonzero");

        assert_eq!(error, NativeCompatibilityError::ZeroPlanContractRevision);
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
            1,
        )
        .expect_err("reordered provider ids must fail");
        assert!(matches!(
            reversed,
            NativeCompatibilityError::CarrierManifestNotStrictlySorted { .. }
        ));
    }

    /// Epoch 2 is the retired creation contract: its processes read a copied
    /// instance parameter set and judge a replayed create by its content. A
    /// process built at that epoch must land on another island, so the
    /// current epoch differs from it and actually reaches the identity.
    #[test]
    fn the_current_epoch_cuts_off_every_retired_creation_contract_process() {
        const RETIRED_CREATION_CONTRACT_EPOCH: u64 = 2;
        assert_ne!(NATIVE_COMPAT_EPOCH, RETIRED_CREATION_CONTRACT_EPOCH);
        let at = |epoch| {
            derive_native_compatibility_material(
                b"descriptor-v1",
                carriers(),
                [0x31; 32],
                [0x41; 32],
                1,
                epoch,
            )
            .expect("valid material")
        };
        let current = at(NATIVE_COMPAT_EPOCH);
        let retired = at(RETIRED_CREATION_CONTRACT_EPOCH);
        assert_eq!(current.epoch(), NATIVE_COMPAT_EPOCH);
        assert_ne!(
            current.id(),
            retired.id(),
            "the epoch is part of the compatibility identity"
        );
    }

    #[test]
    fn accepted_creation_and_quiesce_cut_off_the_synchronous_creation_epoch() {
        const SYNCHRONOUS_CREATION_EPOCH: u64 = 3;
        assert_ne!(NATIVE_COMPAT_EPOCH, SYNCHRONOUS_CREATION_EPOCH);
        let at = |epoch| {
            derive_native_compatibility_material(
                b"descriptor-v1",
                carriers(),
                [0x31; 32],
                [0x41; 32],
                1,
                epoch,
            )
            .expect("valid material")
        };
        assert_ne!(
            at(NATIVE_COMPAT_EPOCH).id(),
            at(SYNCHRONOUS_CREATION_EPOCH).id()
        );
    }

    #[test]
    fn test_fixture_epoch_is_explicit_and_never_ambient() {
        #[cfg(feature = "native-compatibility-test-fixture")]
        assert_eq!(NATIVE_COMPAT_EPOCH, 5);
        #[cfg(not(feature = "native-compatibility-test-fixture"))]
        assert_eq!(NATIVE_COMPAT_EPOCH, 4);
    }
}
