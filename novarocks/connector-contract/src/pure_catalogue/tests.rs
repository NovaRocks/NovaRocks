// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

use super::*;
use crate::*;
use arrow_schema::{DataType, Field, Schema};
use bytes::Bytes;
use novarocks_type_contract::ValueLogicalType;
use std::sync::{
    Mutex,
    atomic::{AtomicUsize, Ordering},
};

#[derive(Default)]
struct Control {
    trace: Mutex<Vec<u32>>,
    refusal: Option<(CompileControlError, Stop)>,
}
#[derive(Clone, Copy)]
enum Stop {
    Entry,
    Quantum,
    Call(usize),
}
impl PureCompileControl for Control {
    fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        assert_eq!(phase, CompilePhase::ProviderValidation);
        assert!(units <= 256);
        let mut trace = self.trace.lock().unwrap();
        trace.push(units);
        if let Some((error, stop)) = self.refusal.clone() {
            let refuse = match stop {
                Stop::Entry => trace.len() == 1,
                Stop::Quantum => units == 256,
                Stop::Call(index) => trace.len() == index,
            };
            if refuse {
                return Err(error);
            }
        }
        Ok(())
    }
}
fn id(name: &str) -> ConnectorProviderId {
    ConnectorProviderId::parse(name).unwrap()
}
fn binding(name: &str) -> ConnectorReadBinding {
    let instance = ConnectorInstanceId::try_from_canonical("lake").unwrap();
    ConnectorReadBinding::new(
        ConnectorInstanceDescriptor {
            provider_id: id(name),
            instance_id: instance.clone(),
        },
        CatalogHandle::new(instance, CatalogVersion::from_bytes([3; 32])),
    )
}
fn payload(
    binding: &ConnectorReadBinding,
    category: ConnectorCodecCategory,
    bytes: Bytes,
) -> ConnectorEncodedPayload {
    ConnectorEncodedPayload::new(
        ConnectorEnvelopeHeader::new(
            binding.descriptor().provider_id.clone(),
            binding.catalog_handle().clone(),
            category,
            ConnectorCodecRevision::try_new(1).unwrap(),
        ),
        bytes,
    )
}
fn frozen_read(name: &str, count: usize) -> FrozenConnectorRead {
    let binding = binding(name);
    let draft = ConnectorReadRelationRecipeDraft::try_new(
        binding.clone(),
        ConnectorReadRelationPayload::new(
            ConnectorReadRelationKind::Table,
            payload(
                &binding,
                ConnectorCodecCategory::ReadTable,
                Bytes::from_static(b"read-table"),
            ),
            payload(
                &binding,
                ConnectorCodecCategory::ReadView,
                Bytes::from_static(b"read-view"),
            ),
        ),
        (0..count)
            .map(|i| {
                payload(
                    &binding,
                    ConnectorCodecCategory::ReadColumn,
                    Bytes::copy_from_slice(&(i as u32).to_le_bytes()),
                )
            })
            .collect(),
    )
    .unwrap();
    let scan = FrozenConnectorScan::try_new(
        draft,
        (0..count)
            .map(|i| {
                StaticScanAssignment::new(Arc::from(format!("v{i}")), ConnectorValueType::BigInt)
            })
            .collect(),
        TupleDomain::all(),
        TupleDomain::all(),
        None,
        vec![],
    )
    .unwrap();
    let source = ConnectorReadStaticFacts::try_new(
        ConnectorReadInputVersion::try_new([9]).unwrap(),
        [7; 32],
        ConnectorReadProperties::try_new(ConnectorReadDistribution::Unconstrained, vec![]).unwrap(),
        ConnectorReadArtifactCoverage::NoArtifactInputs,
        vec![],
    )
    .unwrap();
    let public = ConnectorReadPublicFacts::try_new(
        source,
        None,
        Schema::new(
            (0..count)
                .map(|i| Field::new(format!("v{i}"), DataType::Int64, false))
                .collect::<Vec<_>>(),
        ),
        vec![ValueLogicalType::Physical; count],
    )
    .unwrap();
    FrozenConnectorRead::try_new(scan, public).unwrap()
}
fn field(i: u8) -> ConnectorWriteFieldBinding {
    ConnectorWriteFieldBinding::new(
        ConnectorWriteFieldToken::from_bytes([i; 32]),
        Field::new(format!("v{i}"), DataType::Int64, false),
    )
}
fn write_draft(name: &str, input: ConnectorWriteInputShape) -> ConnectorWriteRecipeDraft {
    let binding = binding(name);
    ConnectorWriteRecipeDraft::try_new(
        ConnectorWriteBinding::new(
            binding.descriptor().clone(),
            binding.catalog_handle().clone(),
        ),
        payload(
            &binding,
            ConnectorCodecCategory::WriteHandle,
            Bytes::from_static(b"writer"),
        ),
        input,
    )
    .unwrap()
}
fn write(name: &str) -> ConnectorWriteRecipeDraft {
    write_draft(
        name,
        ConnectorWriteInputShape::Data {
            fields: vec![field(1)],
        },
    )
}

#[derive(Clone, Default)]
enum Behavior {
    #[default]
    Normal,
    Provider(ConnectorErrorKind),
    Control(CompileControlError),
    ReadHeaderMutation,
    ReadRetainedOverflow,
    WriteInputMutation,
}
struct Port {
    provider: ConnectorProviderId,
    behavior: Behavior,
    reads: AtomicUsize,
    writes: AtomicUsize,
    controls: Mutex<Vec<usize>>,
    shapes: Mutex<Vec<&'static str>>,
}
impl Port {
    fn new(name: &str, behavior: Behavior) -> Arc<Self> {
        Arc::new(Self {
            provider: id(name),
            behavior,
            reads: AtomicUsize::new(0),
            writes: AtomicUsize::new(0),
            controls: Mutex::default(),
            shapes: Mutex::default(),
        })
    }
    fn begin<'a>(
        &self,
        control: &'a dyn PureCompileControl,
    ) -> Result<CompileCheckpoints<'a>, PureProviderCompileError<ConnectorError>> {
        self.controls
            .lock()
            .unwrap()
            .push(control as *const dyn PureCompileControl as *const () as usize);
        CompileCheckpoints::try_new(control, CompilePhase::ProviderValidation).map_err(Into::into)
    }
    fn refusal(&self) -> Result<(), PureProviderCompileError<ConnectorError>> {
        match self.behavior.clone() {
            Behavior::Provider(kind) => Err(PureProviderCompileError::Provider(
                ConnectorError::new(kind, "private refusal"),
            )),
            Behavior::Control(error) => Err(PureProviderCompileError::Control(error)),
            _ => Ok(()),
        }
    }
}
impl ConnectorReadProgramCompiler for Port {
    type Error = ConnectorError;
    fn compile_private(
        &self,
        frozen: &FrozenConnectorRead,
        control: &dyn PureCompileControl,
    ) -> Result<ConnectorReadRelationRecipeDraft, PureProviderCompileError<ConnectorError>> {
        self.reads.fetch_add(1, Ordering::Relaxed);
        let mut work = self.begin(control)?;
        let draft = frozen.scan().recipe();
        if draft.binding().descriptor().provider_id != self.provider
            || frozen.public_facts().source().input_version().as_bytes() != [9]
            || frozen.public_facts().source().selection_digest() != [7; 32]
            || draft.relation().table().payload().as_ref() != b"read-table"
            || draft.relation().view().payload().as_ref() != b"read-view"
        {
            return Err(PureProviderCompileError::Provider(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "wrong complete read facts",
            )));
        }
        for (i, field) in frozen.public_facts().schema().fields().iter().enumerate() {
            let matches = field.name() == &format!("v{i}")
                && field.data_type() == &DataType::Int64
                && !field.is_nullable()
                && frozen.public_facts().logical_types()[i] == ValueLogicalType::Physical
                && draft.columns()[i].payload().as_ref() == (i as u32).to_le_bytes();
            work.step()?;
            if !matches {
                return Err(PureProviderCompileError::Provider(ConnectorError::new(
                    ConnectorErrorKind::InvalidRequest,
                    "wrong complete read column",
                )));
            }
        }
        work.finish()?;
        self.refusal()?;
        let canonical = |original: &ConnectorEncodedPayload| {
            let revision = if matches!(self.behavior, Behavior::ReadHeaderMutation) {
                2
            } else {
                1
            };
            ConnectorEncodedPayload::new(
                ConnectorEnvelopeHeader::new(
                    original.header().provider_id().clone(),
                    original.header().catalog().clone(),
                    original.header().category(),
                    ConnectorCodecRevision::try_new(revision).unwrap(),
                ),
                Bytes::copy_from_slice(self.provider.as_str().as_bytes()),
            )
        };
        let table = if matches!(self.behavior, Behavior::ReadRetainedOverflow) {
            ConnectorEncodedPayload::new(
                draft.relation().table().header().clone(),
                Bytes::from(vec![0; MAX_STATIC_SCAN_RETAINED_BYTES]),
            )
        } else {
            canonical(draft.relation().table())
        };
        ConnectorReadRelationRecipeDraft::try_new(
            draft.binding().clone(),
            ConnectorReadRelationPayload::new(
                draft.relation().kind(),
                table,
                canonical(draft.relation().view()),
            ),
            draft.columns().iter().map(canonical).collect(),
        )
        .map_err(|error| {
            PureProviderCompileError::Provider(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                error.to_string(),
            ))
        })
    }
}
impl ConnectorWriteRecipeCompiler for Port {
    type Error = ConnectorError;
    fn compile_private(
        &self,
        draft: &ConnectorWriteRecipeDraft,
        control: &dyn PureCompileControl,
    ) -> Result<ConnectorWriteRecipeDraft, PureProviderCompileError<ConnectorError>> {
        self.writes.fetch_add(1, Ordering::Relaxed);
        let mut work = self.begin(control)?;
        if draft.binding().descriptor().provider_id != self.provider
            || draft.payload().payload().as_ref() != b"writer"
        {
            return Err(PureProviderCompileError::Provider(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "wrong complete write facts",
            )));
        }
        let shape = match draft.input() {
            ConnectorWriteInputShape::Data { .. } => "data",
            ConnectorWriteInputShape::RowLineage { .. } => "lineage",
            ConnectorWriteInputShape::PositionDelete { .. } => "position",
            ConnectorWriteInputShape::DeletionVector { .. } => "vector",
            ConnectorWriteInputShape::EqualityDelete { .. } => "equality",
        };
        for field in draft.input().fields_iter() {
            assert_eq!(field.field().data_type(), &DataType::Int64);
            work.step()?;
        }
        self.shapes.lock().unwrap().push(shape);
        work.finish()?;
        self.refusal()?;
        let input = if matches!(self.behavior, Behavior::WriteInputMutation) {
            ConnectorWriteInputShape::Data {
                fields: vec![field(2)],
            }
        } else {
            draft.input().clone()
        };
        ConnectorWriteRecipeDraft::try_new(
            draft.binding().clone(),
            ConnectorEncodedPayload::new(
                draft.payload().header().clone(),
                Bytes::copy_from_slice(self.provider.as_str().as_bytes()),
            ),
            input,
        )
        .map_err(PureProviderCompileError::Provider)
    }
}
fn definition(
    port: &Arc<Port>,
    read: bool,
    write: bool,
) -> PureProviderProgramDefinition<ConnectorError> {
    PureProviderProgramDefinition::new(
        port.provider.clone(),
        read.then(|| {
            Arc::clone(port) as Arc<dyn ConnectorReadProgramCompiler<Error = ConnectorError>>
        }),
        write.then(|| {
            Arc::clone(port) as Arc<dyn ConnectorWriteRecipeCompiler<Error = ConnectorError>>
        }),
    )
}
fn catalogue(
    port: &Arc<Port>,
    read: bool,
    write: bool,
) -> PureProviderProgramCatalog<ConnectorError> {
    PureProviderProgramCatalog::try_new(
        &[PureProviderManifestEntry::new(
            port.provider.clone(),
            read,
            write,
        )],
        vec![definition(port, read, write)],
        &Control::default(),
    )
    .unwrap()
}
fn errors() -> [CompileControlError; 3] {
    [
        CompileControlError::Cancelled,
        CompileControlError::DeadlineExceeded,
        CompileControlError::ResourceExhausted,
    ]
}

#[test]
fn manifest_rejects_duplicate_missing_extra_empty_and_mismatched_facets() {
    let alpha = Port::new("alpha", Behavior::Normal);
    let beta = Port::new("beta", Behavior::Normal);
    let entry = PureProviderManifestEntry::new(id("alpha"), true, false);
    let cases = [
        (
            vec![entry.clone(), entry.clone()],
            vec![definition(&alpha, true, false)],
            PureProviderCatalogError::DuplicateManifest(id("alpha")),
        ),
        (
            vec![entry.clone()],
            vec![],
            PureProviderCatalogError::MissingDefinition(id("alpha")),
        ),
        (
            vec![entry.clone()],
            vec![
                definition(&alpha, true, false),
                definition(&alpha, true, false),
            ],
            PureProviderCatalogError::DuplicateDefinition(id("alpha")),
        ),
        (
            vec![entry.clone()],
            vec![definition(&beta, true, false)],
            PureProviderCatalogError::UnexpectedDefinition(id("beta")),
        ),
        (
            vec![PureProviderManifestEntry::new(id("alpha"), false, false)],
            vec![],
            PureProviderCatalogError::EmptyManifestEntry(id("alpha")),
        ),
        (
            vec![entry.clone()],
            vec![definition(&alpha, false, true)],
            PureProviderCatalogError::CapabilityMismatch(id("alpha")),
        ),
        (
            vec![entry],
            vec![definition(&alpha, true, true)],
            PureProviderCatalogError::CapabilityMismatch(id("alpha")),
        ),
    ];
    for (manifest, definitions, expected) in cases {
        let error = match PureProviderProgramCatalog::try_new(
            &manifest,
            definitions,
            &Control::default(),
        ) {
            Ok(_) => panic!("invalid installed catalogue accepted"),
            Err(error) => error,
        };
        assert_eq!(error, expected);
    }
    assert_eq!(alpha.reads.load(Ordering::Relaxed), 0);
    assert_eq!(alpha.writes.load(Ordering::Relaxed), 0);
}

#[test]
fn exact_provider_lookup_dispatches_only_the_complete_installed_port() {
    let alpha = Port::new("alpha", Behavior::Normal);
    let beta = Port::new("beta", Behavior::Normal);
    let catalog = PureProviderProgramCatalog::try_new(
        &[
            PureProviderManifestEntry::new(id("beta"), true, true),
            PureProviderManifestEntry::new(id("alpha"), true, true),
        ],
        vec![
            definition(&alpha, true, true),
            definition(&beta, true, true),
        ],
        &Control::default(),
    )
    .unwrap();
    assert_eq!(catalog.provider_count(), 2);
    for name in ["alpha", "beta"] {
        let original = frozen_read(name, 2);
        let owner = Control::default();
        let result = catalog.compile_read(&original, &owner).unwrap();
        assert_eq!(result.frozen().public_facts(), original.public_facts());
        assert_eq!(
            result
                .frozen()
                .scan()
                .recipe()
                .relation()
                .table()
                .payload()
                .as_ref(),
            name.as_bytes()
        );
        let output = catalog.compile_write(&write(name), &owner).unwrap();
        assert_eq!(output.draft().payload().payload().as_ref(), name.as_bytes());
        let chosen = if name == "alpha" { &alpha } else { &beta };
        assert_eq!(
            chosen.controls.lock().unwrap().as_slice(),
            &[&owner as *const Control as usize; 2]
        );
    }
    assert_eq!(alpha.reads.load(Ordering::Relaxed), 1);
    assert_eq!(beta.reads.load(Ordering::Relaxed), 1);
    assert_eq!(alpha.writes.load(Ordering::Relaxed), 1);
    assert_eq!(beta.writes.load(Ordering::Relaxed), 1);
}

#[test]
fn missing_provider_and_unavailable_facet_never_call_other_ports() {
    let reader = Port::new("reader", Behavior::Normal);
    let writer = Port::new("writer", Behavior::Normal);
    let read_catalog = catalogue(&reader, true, false);
    let write_catalog = catalogue(&writer, false, true);
    assert!(
        matches!(read_catalog.compile_read(&frozen_read("missing", 1), &Control::default()),
        Err(PureProviderProgramError::Catalog(PureProviderCatalogError::MissingProvider(provider))) if provider == id("missing"))
    );
    assert!(
        matches!(read_catalog.compile_write(&write("reader"), &Control::default()),
        Err(PureProviderProgramError::Catalog(PureProviderCatalogError::WriteUnavailable(provider))) if provider == id("reader"))
    );
    assert!(
        matches!(write_catalog.compile_read(&frozen_read("writer", 1), &Control::default()),
        Err(PureProviderProgramError::Catalog(PureProviderCatalogError::ReadUnavailable(provider))) if provider == id("writer"))
    );
    assert_eq!(reader.reads.load(Ordering::Relaxed), 0);
    assert_eq!(reader.writes.load(Ordering::Relaxed), 0);
    assert_eq!(writer.reads.load(Ordering::Relaxed), 0);
    assert_eq!(writer.writes.load(Ordering::Relaxed), 0);
}

#[test]
fn writer_roles_and_all_complete_input_fields_survive_the_existing_seal() {
    let port = Port::new("alpha", Behavior::Normal);
    let catalog = catalogue(&port, false, true);
    for input in [
        ConnectorWriteInputShape::Data {
            fields: vec![field(1)],
        },
        ConnectorWriteInputShape::RowLineage {
            data_fields: vec![field(1)],
            row_identity_fields: vec![field(2)],
        },
        ConnectorWriteInputShape::PositionDelete {
            identity_fields: vec![field(1)],
            partition_source_fields: vec![field(2)],
        },
        ConnectorWriteInputShape::DeletionVector {
            identity_fields: vec![field(1)],
            partition_source_fields: vec![field(2)],
        },
        ConnectorWriteInputShape::EqualityDelete {
            equality_fields: vec![field(1)],
        },
    ] {
        let draft = write_draft("alpha", input.clone());
        assert_eq!(
            catalog
                .compile_write(&draft, &Control::default())
                .unwrap()
                .draft()
                .input(),
            &input
        );
    }
    assert_eq!(
        port.shapes.lock().unwrap().as_slice(),
        &["data", "lineage", "position", "vector", "equality"]
    );
}

#[test]
fn canonicalization_cannot_replace_checked_read_headers_or_write_public_input() {
    let reader = Port::new("alpha", Behavior::ReadHeaderMutation);
    assert!(
        matches!(catalogue(&reader, true, false).compile_read(&frozen_read("alpha", 1), &Control::default()),
        Err(PureProviderProgramError::Contract(error)) if error.kind() == ConnectorErrorKind::InvalidRequest)
    );
    let writer = Port::new("alpha", Behavior::WriteInputMutation);
    assert!(
        matches!(catalogue(&writer, false, true).compile_write(&write("alpha"), &Control::default()),
        Err(PureProviderProgramError::Contract(error)) if error.kind() == ConnectorErrorKind::InvalidRequest)
    );
}

#[test]
fn original_control_failures_remain_typed_at_registration_entry_and_private_quantum() {
    for error in errors() {
        let port = Port::new("alpha", Behavior::Normal);
        let owner = Control {
            trace: Mutex::default(),
            refusal: Some((error.clone(), Stop::Entry)),
        };
        assert!(
            matches!(PureProviderProgramCatalog::try_new(&[PureProviderManifestEntry::new(id("alpha"), true, true)],
            vec![definition(&port, true, true)], &owner), Err(PureProviderCatalogError::Control(cause)) if cause == error)
        );
        let catalog = catalogue(&port, true, true);
        for write_call in [false, true] {
            let owner = Control {
                trace: Mutex::default(),
                refusal: Some((error.clone(), Stop::Entry)),
            };
            let result = if write_call {
                catalog.compile_write(&write("alpha"), &owner).map(|_| ())
            } else {
                catalog
                    .compile_read(&frozen_read("alpha", 1), &owner)
                    .map(|_| ())
            };
            assert!(
                matches!(result, Err(PureProviderProgramError::Control(cause)) if cause == error)
            );
        }
        assert_eq!(port.reads.load(Ordering::Relaxed), 0);
        assert_eq!(port.writes.load(Ordering::Relaxed), 0);
        let owner = Control {
            trace: Mutex::default(),
            refusal: Some((error.clone(), Stop::Quantum)),
        };
        assert!(
            matches!(catalog.compile_read(&frozen_read("alpha", 320), &owner), Err(PureProviderProgramError::Control(cause)) if cause == error)
        );
        assert_eq!(owner.trace.lock().unwrap().last(), Some(&256));
        assert_eq!(port.reads.load(Ordering::Relaxed), 1);
    }
}

#[test]
fn ordinary_read_provider_and_catalog_errors_keep_the_original_error_and_control_prefix() {
    let port = Port::new(
        "alpha",
        Behavior::Provider(ConnectorErrorKind::InvalidRequest),
    );
    let catalog = catalogue(&port, true, true);
    let frozen = frozen_read("alpha", 1);
    let baseline = Control::default();
    let Err(PureProviderProgramError::Provider(original)) =
        catalog.compile_read(&frozen, &baseline)
    else {
        panic!("original read provider error required");
    };
    assert_eq!(original.message(), "private refusal");
    let trace = baseline.trace.lock().unwrap().clone();
    for cause in errors() {
        for at in 1..=trace.len() {
            let owner = Control {
                trace: Mutex::default(),
                refusal: Some((cause.clone(), Stop::Call(at))),
            };
            assert!(
                matches!(catalog.compile_read(&frozen, &owner), Err(PureProviderProgramError::Control(actual)) if actual == cause)
            );
            assert_eq!(*owner.trace.lock().unwrap(), trace[..at]);
        }
        let owner = Control {
            trace: Mutex::default(),
            refusal: Some((cause, Stop::Call(trace.len() + 1))),
        };
        let Err(PureProviderProgramError::Provider(actual)) = catalog.compile_read(&frozen, &owner)
        else {
            panic!("a later callback replaced the original provider error");
        };
        assert_eq!(actual, original);
        assert_eq!(*owner.trace.lock().unwrap(), trace);
    }
    let missing = frozen_read("missing", 1);
    let baseline = Control::default();
    assert!(
        matches!(catalog.compile_read(&missing, &baseline), Err(PureProviderProgramError::Catalog(PureProviderCatalogError::MissingProvider(actual))) if actual == id("missing"))
    );
    let trace = baseline.trace.lock().unwrap().clone();
    assert_eq!(trace, [0]);
    for cause in errors() {
        let early = Control {
            trace: Mutex::default(),
            refusal: Some((cause.clone(), Stop::Call(1))),
        };
        assert!(
            matches!(catalog.compile_read(&missing, &early), Err(PureProviderProgramError::Control(actual)) if actual == cause)
        );
        let late = Control {
            trace: Mutex::default(),
            refusal: Some((cause, Stop::Call(2))),
        };
        assert!(
            matches!(catalog.compile_read(&missing, &late), Err(PureProviderProgramError::Catalog(PureProviderCatalogError::MissingProvider(actual))) if actual == id("missing"))
        );
        assert_eq!(*late.trace.lock().unwrap(), trace);
    }
}

#[test]
fn ordinary_writer_provider_error_keeps_original_cause_and_actual_control_prefix() {
    let port = Port::new(
        "alpha",
        Behavior::Provider(ConnectorErrorKind::InvalidRequest),
    );
    let catalog = catalogue(&port, true, true);
    let draft = write("alpha");
    let baseline = Control::default();
    let Err(PureProviderProgramError::Provider(original)) =
        catalog.compile_write(&draft, &baseline)
    else {
        panic!("original provider error required");
    };
    assert_eq!(original.message(), "private refusal");
    let trace = baseline.trace.lock().unwrap().clone();
    for cause in errors() {
        for at in 1..=trace.len() {
            let early = Control {
                trace: Mutex::default(),
                refusal: Some((cause.clone(), Stop::Call(at))),
            };
            assert!(
                matches!(catalog.compile_write(&draft, &early), Err(PureProviderProgramError::Control(actual)) if actual == cause)
            );
            assert_eq!(*early.trace.lock().unwrap(), trace[..at]);
        }
        let late = Control {
            trace: Mutex::default(),
            refusal: Some((cause, Stop::Call(trace.len() + 1))),
        };
        let Err(PureProviderProgramError::Provider(actual)) = catalog.compile_write(&draft, &late)
        else {
            panic!("late callback replaced original provider error");
        };
        assert_eq!(actual, original);
        assert_eq!(*late.trace.lock().unwrap(), trace);
    }
}

#[test]
fn provider_contract_resource_and_primary_control_failures_keep_distinct_owners() {
    let port = Port::new(
        "alpha",
        Behavior::Provider(ConnectorErrorKind::ResourceExhausted),
    );
    let catalog = catalogue(&port, true, true);
    assert!(
        matches!(catalog.compile_read(&frozen_read("alpha", 1), &Control::default()),
        Err(PureProviderProgramError::Provider(error)) if error.kind() == ConnectorErrorKind::ResourceExhausted)
    );
    assert!(
        matches!(catalog.compile_write(&write("alpha"), &Control::default()),
        Err(PureProviderProgramError::Provider(error)) if error.kind() == ConnectorErrorKind::ResourceExhausted)
    );
    let overflow = Port::new("alpha", Behavior::ReadRetainedOverflow);
    assert!(
        matches!(catalogue(&overflow, true, false).compile_read(&frozen_read("alpha", 1), &Control::default()),
        Err(PureProviderProgramError::Contract(error)) if error.kind() == ConnectorErrorKind::ResourceExhausted)
    );
    for cause in errors() {
        let port = Port::new("alpha", Behavior::Control(cause.clone()));
        let catalog = catalogue(&port, true, true);
        assert!(
            matches!(catalog.compile_read(&frozen_read("alpha", 1), &Control::default()), Err(PureProviderProgramError::Control(error)) if error == cause)
        );
        assert!(
            matches!(catalog.compile_write(&write("alpha"), &Control::default()), Err(PureProviderProgramError::Control(error)) if error == cause)
        );
    }
}
