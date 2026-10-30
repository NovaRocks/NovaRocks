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

#[cfg(all(
    feature = "native-compatibility-test-fixture",
    not(novarocks_dev_controls)
))]
compile_error!("native-compatibility-test-fixture requires test-controls");

/// Capabilities compiled by this owner; activation still requires a role session.
pub const fn compiled_test_capabilities() -> (&'static str, &'static [&'static str]) {
    (
        env!("CARGO_PKG_NAME"),
        &[
            #[cfg(novarocks_dev_controls)]
            "test-controls",
            #[cfg(feature = "native-compatibility-test-fixture")]
            "native-compatibility-test-fixture",
        ],
    )
}

use std::fmt;

use novarocks_proto_models::FILE_DESCRIPTOR_SET;
use novarocks_types::NativeCompatibilityId;
use sha2::{Digest, Sha256};

// Design: ADR-0124 (docs/adr/ADR-0124-native-compatibility-islands-and-ingress-admission.md)
/// Domain separator for the immutable Native compatibility identity encoding.
///
/// Version 8 freezes change-stream output roles, exact stream close dispositions
/// and the independent hierarchy hash. The static interpreter component keeps
/// its sealed catalogue encoding.
pub const NATIVE_COMPATIBILITY_DOMAIN: &[u8] = b"novarocks.native-compatibility-id/v8\0";

/// Domain separator for the pure physical-plan contract component.
const PLAN_CONTRACT_DOMAIN: &[u8] = b"novarocks.physical-plan-contract/v1\0";

/// Domain separator for the static plan interpreter component.
const STATIC_PLAN_INTERPRETER_DOMAIN: &[u8] = b"novarocks.static-plan-interpreter/v1\0";

/// The one static plan interpreter a process composes.
///
/// Production composes the compiled package carrier and its exact sealed pure
/// catalogue. The component retains that catalogue digest so different pure
/// support remains incompatible even when all other process inputs agree.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StaticPlanInterpreter {
    /// Reads the physical package carrier and compiles it with the sealed
    /// pure function catalogue this digest identifies.
    CompiledPackage {
        pure_function_catalog_digest: [u8; 32],
    },
}

impl StaticPlanInterpreter {
    /// Exact component evidence: the domain, a fixed tag, and the sealed
    /// pure catalogue digest a compiled-package interpreter executes with.
    fn component_digest(self) -> [u8; 32] {
        let mut hasher = Sha256::new();
        hasher.update(STATIC_PLAN_INTERPRETER_DOMAIN);
        match self {
            Self::CompiledPackage {
                pure_function_catalog_digest,
            } => {
                hasher.update([2]);
                hasher.update(pure_function_catalog_digest);
            }
        }
        hasher.finalize().into()
    }
}

/// Explicit compatibility epoch for an execution-contract change that cannot
/// be represented by the descriptor or the closed carrier manifest.
///
/// Epoch 10 combines the complete change-stream runtime, exact summary-source
/// obligations and pre-success strict input validation with the epoch-9 value
/// semantics. Epoch-9 binaries cannot enforce the complete write obligations
/// and must remain in a distinct production compatibility island.
/// Epoch 9 freezes unique case-insensitive struct-field ordinals, checked TIME
/// conversion overflow, and NULL-preserving selection semantics. These execution
/// rules are not encoded by function metadata, descriptors or carrier layouts.
/// Epoch 8 adds independent logical write results, explicit stage placement,
/// selected-action assertions and exact-stream cumulative close dispositions.
/// Epoch 7 removes the retired evaluator implementation from compatibility
/// material. Catalogue, provider, descriptor and compiled static facts retain
/// their own authority. Pre-F3 binaries stay in another island.
/// Epoch 6 adds signed Native caller role and exact process identity claims.
/// Epoch-5 receivers reject these closed JWT fields, so the two cannot share
/// an island. Diagnostic subjects do not provide process identity.
/// Epoch 5 preserves terminal Task control authority when a root result is
/// revoked and its result plane explicitly awaits that control observation.
/// Epoch 4 adds Accepted/Installed task creation and the normal context
/// Quiesce operation, generation-required covered observation, and exact
/// per-destination normal exchange closure with actual-stop convergence, and
/// exact advertised per-context preparation positions.
/// An epoch-3 process interprets the same task operation
/// boundary under the synchronous creation and direct release contract, so
/// the two never share an island.
#[cfg(not(feature = "native-compatibility-test-fixture"))]
pub const NATIVE_COMPAT_EPOCH: u64 = 10;

/// Test-only alternate epoch used to produce an actual different-island binary.
#[cfg(feature = "native-compatibility-test-fixture")]
pub const NATIVE_COMPAT_EPOCH: u64 = 11;

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
    plan_contract_revision: u32,
    plan_contract_digest: [u8; 32],
    static_plan_interpreter: StaticPlanInterpreter,
    static_plan_interpreter_digest: [u8; 32],
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

    /// Revision supplied by Server from its statically linked plan contract.
    pub const fn plan_contract_revision(&self) -> u32 {
        self.plan_contract_revision
    }

    /// Exact component evidence for diagnosing a plan-contract mismatch.
    /// Admission still compares the complete Native compatibility identity.
    pub const fn plan_contract_digest(&self) -> [u8; 32] {
        self.plan_contract_digest
    }

    /// The static plan interpreter Server composed for this process.
    pub const fn static_plan_interpreter(&self) -> StaticPlanInterpreter {
        self.static_plan_interpreter
    }

    /// Exact component evidence for diagnosing an interpreter mismatch.
    /// Admission still compares the complete Native compatibility identity.
    pub const fn static_plan_interpreter_digest(&self) -> [u8; 32] {
        self.static_plan_interpreter_digest
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
/// static carrier manifest, plan contract revision and static plan
/// interpreter. The input order is part of the validation contract: callers
/// must provide an already strictly sorted declaration set. This crate never
/// selects a plan contract or an interpreter on Server's behalf.
pub fn derive_native_compatibility_material(
    descriptor_set: &[u8],
    carriers: impl IntoIterator<Item = NativeCarrierDeclaration>,
    function_catalog_digest: [u8; 32],
    plan_contract_revision: u32,
    static_plan_interpreter: StaticPlanInterpreter,
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
    let static_plan_interpreter_digest = static_plan_interpreter.component_digest();
    let mut hasher = Sha256::new();
    hasher.update(NATIVE_COMPATIBILITY_DOMAIN);
    hasher.update(descriptor_digest);
    hasher.update(function_catalog_digest);
    hasher.update(plan_contract_digest);
    hasher.update(static_plan_interpreter_digest);
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
        plan_contract_revision,
        plan_contract_digest,
        static_plan_interpreter,
        static_plan_interpreter_digest,
        epoch,
        carriers: carriers.into_boxed_slice(),
    })
}

/// Derives the material for the repository's current Protocol descriptor.
pub fn derive_repository_native_compatibility_material(
    carriers: impl IntoIterator<Item = NativeCarrierDeclaration>,
    function_catalog_digest: [u8; 32],
    plan_contract_revision: u32,
    static_plan_interpreter: StaticPlanInterpreter,
) -> Result<NativeCompatibilityMaterial, NativeCompatibilityError> {
    derive_native_compatibility_material(
        FILE_DESCRIPTOR_SET,
        carriers,
        function_catalog_digest,
        plan_contract_revision,
        static_plan_interpreter,
        NATIVE_COMPAT_EPOCH,
    )
}

#[cfg(test)]
mod tests {
    use super::{
        NATIVE_COMPAT_EPOCH, NativeCarrierDeclaration, NativeCompatibilityError,
        StaticPlanInterpreter, derive_native_compatibility_material,
    };

    const COMPILED: StaticPlanInterpreter = StaticPlanInterpreter::CompiledPackage {
        pure_function_catalog_digest: [0x51; 32],
    };

    fn carriers() -> [NativeCarrierDeclaration; 2] {
        [
            NativeCarrierDeclaration::try_new("iceberg", 1).expect("iceberg declaration"),
            NativeCarrierDeclaration::try_new("starrocks", 1).expect("starrocks declaration"),
        ]
    }

    #[test]
    fn native_compatibility_material_matches_the_frozen_golden_vector() {
        let material = derive_native_compatibility_material(
            b"descriptor-v1",
            carriers(),
            [0x31; 32],
            1,
            COMPILED,
            1,
        )
        .expect("valid material");

        assert_eq!(
            material.id().to_string(),
            "75a4b247c42dbd484cb49d189891c03d0a844add97aa3d185ad2e4fe505dcf3b"
        );
        assert_eq!(material.function_catalog_digest(), [0x31; 32]);
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
            1,
            COMPILED,
            1,
        )
        .expect("original material");
        let descriptor = derive_native_compatibility_material(
            b"descriptor-v2",
            carriers(),
            [0x31; 32],
            1,
            COMPILED,
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
            1,
            COMPILED,
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
            1,
            COMPILED,
            1,
        )
        .expect("private descriptor material");
        let catalog_only = derive_native_compatibility_material(
            b"descriptor-v1",
            carriers(),
            [0x32; 32],
            1,
            COMPILED,
            1,
        )
        .expect("catalog-only material");
        let epoch = derive_native_compatibility_material(
            b"descriptor-v1",
            carriers(),
            [0x31; 32],
            1,
            COMPILED,
            2,
        )
        .expect("epoch material");
        let plan_contract = derive_native_compatibility_material(
            b"descriptor-v1",
            carriers(),
            [0x31; 32],
            2,
            COMPILED,
            1,
        )
        .expect("plan contract material");

        assert_ne!(original.id(), descriptor.id());
        assert_ne!(original.id(), provider_revision.id());
        assert_ne!(original.id(), private_descriptor.id());
        assert_ne!(original.id(), catalog_only.id());
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
        assert_eq!(original.carriers(), plan_contract.carriers());
        assert_eq!(original.epoch(), plan_contract.epoch());
    }

    #[test]
    fn native_compatibility_material_rejects_zero_plan_contract_revision() {
        let error = derive_native_compatibility_material(
            b"descriptor-v1",
            carriers(),
            [0x31; 32],
            0,
            COMPILED,
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
            1,
            COMPILED,
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
            1,
            COMPILED,
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
                1,
                COMPILED,
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
                1,
                COMPILED,
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
        assert_eq!(NATIVE_COMPAT_EPOCH, 11);
        #[cfg(not(feature = "native-compatibility-test-fixture"))]
        assert_eq!(NATIVE_COMPAT_EPOCH, 10);
    }
    #[test]
    fn arrow_selection_and_time_semantics_cut_off_the_previous_epoch() {
        let at = |epoch| {
            derive_native_compatibility_material(
                b"same-descriptor",
                carriers(),
                [0x31; 32],
                1,
                COMPILED,
                epoch,
            )
            .unwrap()
        };
        assert_ne!(NATIVE_COMPAT_EPOCH, 8);
        assert_ne!(at(NATIVE_COMPAT_EPOCH).id(), at(8).id());
    }

    #[test]
    fn complete_write_runtime_does_not_share_the_upstream_value_epoch() {
        let at = |epoch| {
            derive_native_compatibility_material(
                b"same-descriptor",
                carriers(),
                [0x31; 32],
                1,
                COMPILED,
                epoch,
            )
            .unwrap()
        };
        assert_ne!(NATIVE_COMPAT_EPOCH, 9);
        assert_ne!(at(NATIVE_COMPAT_EPOCH).id(), at(9).id());
    }

    #[test]
    fn cumulative_exchange_close_does_not_share_the_pre_change_stream_epoch() {
        let at = |epoch| {
            derive_native_compatibility_material(
                b"same-descriptor",
                carriers(),
                [0x31; 32],
                1,
                COMPILED,
                epoch,
            )
            .unwrap()
        };
        assert_ne!(NATIVE_COMPAT_EPOCH, 7);
        assert_ne!(at(NATIVE_COMPAT_EPOCH).id(), at(7).id());
    }

    #[test]
    fn signed_process_claims_do_not_share_the_subject_only_epoch() {
        let at = |epoch| {
            derive_native_compatibility_material(
                b"same-descriptor",
                carriers(),
                [0x31; 32],
                1,
                COMPILED,
                epoch,
            )
            .unwrap()
        };
        assert_ne!(at(NATIVE_COMPAT_EPOCH).id(), at(5).id());
    }

    #[test]
    fn terminal_control_authority_cuts_off_opaque_result_error_epoch() {
        let at = |epoch| {
            derive_native_compatibility_material(
                b"same-descriptor",
                carriers(),
                [0x31; 32],
                1,
                COMPILED,
                epoch,
            )
            .unwrap()
        };
        assert_ne!(NATIVE_COMPAT_EPOCH, 4);
        assert_ne!(at(NATIVE_COMPAT_EPOCH).id(), at(4).id());
    }

    fn with_interpreter(interpreter: StaticPlanInterpreter) -> super::NativeCompatibilityMaterial {
        derive_native_compatibility_material(
            b"descriptor-v1",
            carriers(),
            [0x31; 32],
            1,
            interpreter,
            1,
        )
        .expect("valid material")
    }

    #[test]
    fn a_compiled_package_interpreter_matches_its_frozen_golden_vector() {
        let material = with_interpreter(StaticPlanInterpreter::CompiledPackage {
            pure_function_catalog_digest: [0x51; 32],
        });
        assert_eq!(
            material.id().to_string(),
            "75a4b247c42dbd484cb49d189891c03d0a844add97aa3d185ad2e4fe505dcf3b"
        );
        assert_eq!(
            material.static_plan_interpreter(),
            StaticPlanInterpreter::CompiledPackage {
                pure_function_catalog_digest: [0x51; 32],
            }
        );
    }

    /// Two binaries built from one tree share every descriptor, catalogue,
    /// pure-interpreter and plan digest. The interpreter each composed is the
    /// only input that tells their islands apart, and it alone must.
    #[test]
    fn the_static_plan_interpreter_alone_separates_islands_built_from_one_tree() {
        let compiled = with_interpreter(StaticPlanInterpreter::CompiledPackage {
            pure_function_catalog_digest: [0x51; 32],
        });
        let other_catalog = with_interpreter(StaticPlanInterpreter::CompiledPackage {
            pure_function_catalog_digest: [0x52; 32],
        });

        assert_ne!(compiled.id(), other_catalog.id());
        assert_ne!(
            compiled.static_plan_interpreter_digest(),
            other_catalog.static_plan_interpreter_digest()
        );
        {
            let material = &other_catalog;
            assert_eq!(material.descriptor_digest(), compiled.descriptor_digest());
            assert_eq!(
                material.function_catalog_digest(),
                compiled.function_catalog_digest()
            );
            assert_eq!(
                material.plan_contract_digest(),
                compiled.plan_contract_digest()
            );
            assert_eq!(material.epoch(), compiled.epoch());
            assert_eq!(material.carriers(), compiled.carriers());
        }
    }

    /// Production identity is a pure function of its inputs: deriving it
    /// again yields the same island, never a per-process value.
    #[test]
    fn retired_evaluator_material_does_not_share_the_pre_f3_epoch() {
        let at = |epoch| {
            derive_native_compatibility_material(
                b"descriptor-v1",
                carriers(),
                [0x31; 32],
                1,
                COMPILED,
                epoch,
            )
            .unwrap()
        };
        assert_ne!(NATIVE_COMPAT_EPOCH, 6);
        assert_ne!(at(NATIVE_COMPAT_EPOCH).id(), at(6).id());
        // Frozen pre-F3 vector has the same visible fixture inputs plus its
        // retired [0x41;32] implementation digest. It cannot identify this ABI.
        assert_ne!(
            at(1).id().to_string(),
            "75500bcc71f4993afd30d08e60cd09da95ad15aa0e8b6817951b641d17cd5d2f"
        );
    }

    #[test]
    fn compiled_component_keeps_the_frozen_v1_domain_tag_and_pure_digest() {
        // Independent SHA256 vector: domain || tag 2 || 32 bytes of 0x51.
        // Changing the outer material must not rewrite this composed fact.
        assert_eq!(
            COMPILED.component_digest(),
            [
                0xcb, 0x15, 0x99, 0x9b, 0xd4, 0x5a, 0x89, 0xb8, 0x40, 0xbe, 0xae, 0xa1, 0x12, 0x25,
                0x5a, 0x26, 0xca, 0x77, 0x1a, 0xd5, 0x70, 0x07, 0x1e, 0xee, 0x3a, 0x7c, 0x8f, 0x02,
                0xa9, 0xe1, 0x74, 0x34
            ]
        );
    }

    #[test]
    fn a_compiled_identity_is_deterministic() {
        let interpreter = StaticPlanInterpreter::CompiledPackage {
            pure_function_catalog_digest: [0x51; 32],
        };
        assert_eq!(
            with_interpreter(interpreter).id(),
            with_interpreter(interpreter).id()
        );
        assert_eq!(
            with_interpreter(interpreter).static_plan_interpreter(),
            interpreter
        );
    }
}

// Refuse flags that would make capability reporting disagree with compiled seams.
#[cfg(any(
    all(novarocks_dev_controls, not(debug_assertions)),
    all(not(novarocks_dev_controls), debug_assertions)
))]
compile_error!("novarocks_dev_controls must match debug_assertions");
