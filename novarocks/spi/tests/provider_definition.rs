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

use novarocks_spi::connector::provider::{
    ConnectorCodecDeclaration, ProviderContractDefinition, ProviderReadCodecDefinitions,
    ProviderReadContractDefinition, ProviderWriteCodecDefinitions, ProviderWriteContractDefinition,
    SealedProviderRegistry,
};
use novarocks_spi::connector::{
    ConnectorCodecCategory, ConnectorCodecErrorKind, ConnectorCodecRevision, ConnectorProviderId,
};
use sha2::{Digest, Sha256};

fn declaration(provider: &str, category: ConnectorCodecCategory) -> ConnectorCodecDeclaration {
    ConnectorCodecDeclaration::try_new(
        ConnectorProviderId::parse(provider).unwrap(),
        category,
        ConnectorCodecRevision::try_new(1).unwrap(),
        format!("{provider}.{category:?}"),
        format!("descriptor:{provider}:{category:?}"),
    )
    .unwrap()
}

fn read(provider: &str) -> ProviderReadContractDefinition {
    ProviderReadContractDefinition::new(
        ProviderReadCodecDefinitions::try_new(
            declaration(provider, ConnectorCodecCategory::ReadTable),
            declaration(provider, ConnectorCodecCategory::ReadView),
            declaration(provider, ConnectorCodecCategory::ReadColumn),
            declaration(provider, ConnectorCodecCategory::ReadSplit),
        )
        .unwrap(),
    )
}

fn write(provider: &str) -> ProviderWriteContractDefinition {
    ProviderWriteContractDefinition::new(
        ProviderWriteCodecDefinitions::try_new(
            declaration(provider, ConnectorCodecCategory::WriteHandle),
            declaration(provider, ConnectorCodecCategory::CommitFragment),
        )
        .unwrap(),
    )
}

#[test]
fn read_only_provider_has_one_complete_read_group_and_no_writer() {
    let definition = ProviderContractDefinition::read_only(
        ConnectorProviderId::parse("paimon").unwrap(),
        read("paimon"),
    )
    .unwrap();
    assert_eq!(definition.declarations().len(), 4);
    assert!(definition.write().is_none());
}

#[test]
fn read_write_provider_has_both_complete_groups() {
    let definition = ProviderContractDefinition::read_write(
        ConnectorProviderId::parse("iceberg").unwrap(),
        read("iceberg"),
        write("iceberg"),
    )
    .unwrap();
    assert_eq!(definition.declarations().len(), 6);
    assert!(definition.write().is_some());
}

#[test]
fn group_rejects_wrong_category_and_mixed_provider_identity() {
    let category_error = ProviderReadCodecDefinitions::try_new(
        declaration("iceberg", ConnectorCodecCategory::ReadSplit),
        declaration("iceberg", ConnectorCodecCategory::ReadView),
        declaration("iceberg", ConnectorCodecCategory::ReadColumn),
        declaration("iceberg", ConnectorCodecCategory::ReadSplit),
    )
    .unwrap_err();
    assert_eq!(
        category_error.kind(),
        ConnectorCodecErrorKind::InconsistentFields
    );

    let identity_error = ProviderReadCodecDefinitions::try_new(
        declaration("iceberg", ConnectorCodecCategory::ReadTable),
        declaration("paimon", ConnectorCodecCategory::ReadView),
        declaration("iceberg", ConnectorCodecCategory::ReadColumn),
        declaration("iceberg", ConnectorCodecCategory::ReadSplit),
    )
    .unwrap_err();
    assert_eq!(
        identity_error.kind(),
        ConnectorCodecErrorKind::InconsistentFields
    );
}

#[test]
fn registry_is_order_stable_and_rejects_duplicate_provider_identity() {
    let iceberg = ProviderContractDefinition::read_write(
        ConnectorProviderId::parse("iceberg").unwrap(),
        read("iceberg"),
        write("iceberg"),
    )
    .unwrap();
    let paimon = ProviderContractDefinition::read_only(
        ConnectorProviderId::parse("paimon").unwrap(),
        read("paimon"),
    )
    .unwrap();
    let registry = SealedProviderRegistry::seal([paimon.clone(), iceberg.clone()]).unwrap();
    assert_eq!(registry.definitions()[0].provider_id().as_str(), "iceberg");
    assert_eq!(registry.definitions()[1].provider_id().as_str(), "paimon");
    assert!(
        registry
            .get(&ConnectorProviderId::parse("paimon").unwrap())
            .is_some()
    );

    let error = SealedProviderRegistry::seal([paimon.clone(), paimon]).unwrap_err();
    assert_eq!(error.kind(), ConnectorCodecErrorKind::DuplicateField);
}

#[test]
fn definitions_reject_cross_provider_capability_groups() {
    let error = ProviderContractDefinition::read_write(
        ConnectorProviderId::parse("iceberg").unwrap(),
        read("iceberg"),
        write("paimon"),
    )
    .unwrap_err();
    assert_eq!(error.kind(), ConnectorCodecErrorKind::InconsistentFields);
}

#[test]
fn codec_declarations_are_bounded_and_digest_the_exact_registered_descriptor() {
    let descriptor = b"provider-private-schema-v1";
    let value = ConnectorCodecDeclaration::try_new(
        ConnectorProviderId::parse("iceberg").unwrap(),
        ConnectorCodecCategory::ReadSplit,
        ConnectorCodecRevision::try_new(7).unwrap(),
        "iceberg.read-split.v7",
        descriptor,
    )
    .unwrap();
    assert_eq!(value.revision().get(), 7);
    assert_eq!(value.descriptor(), descriptor);
    assert_eq!(
        value.descriptor_sha256(),
        &<[u8; 32]>::from(Sha256::digest(descriptor))
    );

    for (format_name, descriptor) in [
        ("", b"schema".as_slice()),
        ("iceberg.读取", b"schema".as_slice()),
        ("iceberg.read", b"".as_slice()),
    ] {
        assert_eq!(
            ConnectorCodecDeclaration::try_new(
                ConnectorProviderId::parse("iceberg").unwrap(),
                ConnectorCodecCategory::ReadSplit,
                ConnectorCodecRevision::try_new(1).unwrap(),
                format_name,
                descriptor,
            )
            .unwrap_err()
            .kind(),
            ConnectorCodecErrorKind::InvalidValue
        );
    }
    assert_eq!(
        ConnectorCodecDeclaration::try_new(
            ConnectorProviderId::parse("iceberg").unwrap(),
            ConnectorCodecCategory::ReadSplit,
            ConnectorCodecRevision::try_new(1).unwrap(),
            "x".repeat(129),
            b"schema",
        )
        .unwrap_err()
        .kind(),
        ConnectorCodecErrorKind::InvalidValue
    );
    assert_eq!(
        ConnectorCodecDeclaration::try_new(
            ConnectorProviderId::parse("iceberg").unwrap(),
            ConnectorCodecCategory::ReadSplit,
            ConnectorCodecRevision::try_new(1).unwrap(),
            "iceberg.read",
            vec![0; 1024 * 1024 + 1],
        )
        .unwrap_err()
        .kind(),
        ConnectorCodecErrorKind::InvalidValue
    );
}

#[test]
fn sealing_static_definitions_does_not_reopen_or_interpret_provider_material() {
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct DescriptorProbe<'a> {
        reads: &'a AtomicUsize,
    }

    impl AsRef<[u8]> for DescriptorProbe<'_> {
        fn as_ref(&self) -> &[u8] {
            self.reads.fetch_add(1, Ordering::SeqCst);
            b"provider-private-schema"
        }
    }

    let reads = AtomicUsize::new(0);
    let split = ConnectorCodecDeclaration::try_new(
        ConnectorProviderId::parse("probe").unwrap(),
        ConnectorCodecCategory::ReadSplit,
        ConnectorCodecRevision::try_new(1).unwrap(),
        "probe.read-split.v1",
        DescriptorProbe { reads: &reads },
    )
    .unwrap();
    let reads_after_definition = reads.load(Ordering::SeqCst);
    let definition = ProviderContractDefinition::read_only(
        ConnectorProviderId::parse("probe").unwrap(),
        ProviderReadContractDefinition::new(
            ProviderReadCodecDefinitions::try_new(
                declaration("probe", ConnectorCodecCategory::ReadTable),
                declaration("probe", ConnectorCodecCategory::ReadView),
                declaration("probe", ConnectorCodecCategory::ReadColumn),
                split,
            )
            .unwrap(),
        ),
    )
    .unwrap();

    let registry = SealedProviderRegistry::seal([definition]).unwrap();
    assert_eq!(registry.definitions().len(), 1);
    assert_eq!(reads.load(Ordering::SeqCst), reads_after_definition);
}
