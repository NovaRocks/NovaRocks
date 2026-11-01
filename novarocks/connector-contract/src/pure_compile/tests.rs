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

use super::*;
use crate::*;
use arrow_schema::{DataType, Field, Schema};
use bytes::Bytes;
use novarocks_type_contract::{
    CompileCheckpoints, CompilePhase, PureCompileControl, ValueLogicalType,
};
use std::{
    num::NonZeroU64,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
};

#[derive(Default)]
struct Calls {
    hooks: AtomicUsize,
    in_hook: AtomicBool,
}
#[derive(Clone, Copy, Default)]
enum Target {
    #[default]
    Any,
    Hook,
    Wrapper,
}
#[derive(Default)]
struct Control {
    failure: Option<CompileControlError>,
    positive_only: bool,
    target: Target,
    calls: Arc<Calls>,
    work: Mutex<Vec<(bool, u32)>>,
}
impl PureCompileControl for Control {
    fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        assert_eq!(phase, CompilePhase::ProviderValidation);
        assert!(units <= novarocks_type_contract::MAX_UNOBSERVED_COMPILE_WORK);
        let private = self.calls.in_hook.load(Ordering::Relaxed);
        self.work.lock().unwrap().push((private, units));
        let target = match self.target {
            Target::Any => true,
            Target::Hook => private,
            Target::Wrapper => !private,
        };
        if target
            && (!self.positive_only || units > 0)
            && let Some(failure) = self.failure.clone()
        {
            Err(failure)
        } else {
            Ok(())
        }
    }
}
fn read_binding() -> ConnectorReadBinding {
    let instance = ConnectorInstanceId::parse("pure-fixture").unwrap();
    ConnectorReadBinding::new(
        ConnectorInstanceDescriptor {
            provider_id: ConnectorProviderId::parse("fixture").unwrap(),
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
fn frozen_read(count: usize) -> FrozenConnectorRead {
    let binding = read_binding();
    let recipe = ConnectorReadRelationRecipeDraft::try_new(
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
            .map(|ordinal| {
                payload(
                    &binding,
                    ConnectorCodecCategory::ReadColumn,
                    Bytes::copy_from_slice(&(ordinal as u32).to_le_bytes()),
                )
            })
            .collect(),
    )
    .unwrap();
    let scan = FrozenConnectorScan::try_new(
        recipe,
        (0..count)
            .map(|i| {
                StaticScanAssignment::new(Arc::from(format!("v{i}")), ConnectorValueType::BigInt)
            })
            .collect(),
        TupleDomain::all(),
        TupleDomain::all(),
        None,
        vec![],
        NonZeroU64::new(100).unwrap(),
        NonZeroU64::new(4096).unwrap(),
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
fn token(id: u32) -> ConnectorWriteFieldToken {
    let mut bytes = [0; 32];
    bytes[..4].copy_from_slice(&id.to_le_bytes());
    ConnectorWriteFieldToken::from_bytes(bytes)
}
fn field(id: u32) -> ConnectorWriteFieldBinding {
    ConnectorWriteFieldBinding::new(
        token(id),
        Field::new(format!("v{id}"), DataType::Int64, false),
    )
}
fn draft(shape: ConnectorWriteInputShape) -> ConnectorWriteRecipeDraft {
    let read = read_binding();
    let binding =
        ConnectorWriteBinding::new(read.descriptor().clone(), read.catalog_handle().clone());
    let handle = payload(
        &read,
        ConnectorCodecCategory::WriteHandle,
        Bytes::from_static(b"write-private"),
    );
    ConnectorWriteRecipeDraft::try_new(binding, handle, shape).unwrap()
}
fn data(count: usize) -> ConnectorWriteInputShape {
    ConnectorWriteInputShape::Data {
        fields: (0..count as u32).map(field).collect(),
    }
}
struct Compiler {
    calls: Arc<Calls>,
    provider_error: Option<ConnectorError>,
    write_override: Option<ConnectorWriteInputShape>,
}
impl Compiler {
    fn normal() -> Self {
        Self {
            calls: Arc::new(Calls::default()),
            provider_error: None,
            write_override: None,
        }
    }
    fn control(
        &self,
        failure: Option<CompileControlError>,
        positive_only: bool,
        target: Target,
    ) -> Control {
        Control {
            failure,
            positive_only,
            target,
            calls: self.calls.clone(),
            work: Mutex::default(),
        }
    }
    fn invalid(message: &str) -> PureProviderCompileError<ConnectorError> {
        PureProviderCompileError::Provider(ConnectorError::new(
            ConnectorErrorKind::InvalidRequest,
            message,
        ))
    }
}
impl ConnectorReadProgramCompiler for Compiler {
    type Error = ConnectorError;
    fn compile_private(
        &self,
        frozen: &FrozenConnectorRead,
        control: &dyn PureCompileControl,
    ) -> Result<ConnectorReadRelationRecipeDraft, PureProviderCompileError<Self::Error>> {
        self.calls.hooks.fetch_add(1, Ordering::Relaxed);
        self.calls.in_hook.store(true, Ordering::Relaxed);
        let mut work = CompileCheckpoints::try_new(control, CompilePhase::ProviderValidation)?;
        let original = frozen.scan().recipe();
        if frozen.public_facts().source().input_version().as_bytes() != [9]
            || frozen.public_facts().source().selection_digest() != [7; 32]
            || original.relation().table().payload().as_ref() != b"read-table"
            || original.relation().view().payload().as_ref() != b"read-view"
        {
            return Err(Self::invalid(
                "fixture private read identity differs from its complete frozen input",
            ));
        }
        for (ordinal, ((column, assignment), schema)) in original
            .columns()
            .iter()
            .zip(frozen.scan().assignments())
            .zip(frozen.public_facts().schema().fields())
            .enumerate()
        {
            if column.payload().as_ref() != (ordinal as u32).to_le_bytes()
                || assignment.variable() != format!("v{ordinal}")
                || assignment.value_type() != ConnectorValueType::BigInt
                || schema.name() != assignment.variable()
                || schema.data_type() != &DataType::Int64
            {
                return Err(Self::invalid(
                    "fixture private column differs from frozen public ordinal",
                ));
            }
            work.step()?;
        }
        work.finish()?;
        self.calls.in_hook.store(false, Ordering::Relaxed);
        if let Some(error) = &self.provider_error {
            return Err(PureProviderCompileError::Provider(error.clone()));
        }
        let canonical = |source: &ConnectorEncodedPayload| {
            let mut bytes = b"canonical:".to_vec();
            bytes.extend_from_slice(source.payload());
            ConnectorEncodedPayload::new(source.header().clone(), Bytes::from(bytes))
        };
        ConnectorReadRelationRecipeDraft::try_new(
            original.binding().clone(),
            ConnectorReadRelationPayload::new(
                original.relation().kind(),
                canonical(original.relation().table()),
                canonical(original.relation().view()),
            ),
            original.columns().iter().map(canonical).collect(),
        )
        .map_err(|error| Self::invalid(&error.to_string()))
    }
}
impl ConnectorWriteRecipeCompiler for Compiler {
    type Error = ConnectorError;
    fn compile_private(
        &self,
        original: &ConnectorWriteRecipeDraft,
        control: &dyn PureCompileControl,
    ) -> Result<ConnectorWriteRecipeDraft, PureProviderCompileError<Self::Error>> {
        self.calls.hooks.fetch_add(1, Ordering::Relaxed);
        self.calls.in_hook.store(true, Ordering::Relaxed);
        let mut work = CompileCheckpoints::try_new(control, CompilePhase::ProviderValidation)?;
        if original.payload().payload().as_ref() != b"write-private"
            || original.binding().catalog_handle().version() != CatalogVersion::from_bytes([3; 32])
        {
            return Err(Self::invalid(
                "fixture private writer handle differs from frozen generation",
            ));
        }
        for binding in original.input().fields_iter() {
            let id = u32::from_le_bytes(binding.token().to_bytes()[..4].try_into().unwrap());
            if binding.field().name() != &format!("v{id}") {
                return Err(Self::invalid(
                    "fixture private field token differs from provider field name",
                ));
            }
            work.step()?;
        }
        work.finish()?;
        self.calls.in_hook.store(false, Ordering::Relaxed);
        if let Some(error) = &self.provider_error {
            return Err(PureProviderCompileError::Provider(error.clone()));
        }
        let canonical = ConnectorEncodedPayload::new(
            original.payload().header().clone(),
            Bytes::from_static(b"canonical-write"),
        );
        ConnectorWriteRecipeDraft::try_new(
            original.binding().clone(),
            canonical,
            self.write_override
                .as_ref()
                .unwrap_or(original.input())
                .clone(),
        )
        .map_err(PureProviderCompileError::Provider)
    }
}
#[test]
fn real_frozen_read_and_writer_hooks_validate_private_identity_before_canonical_compilation() {
    let frozen = frozen_read(2);
    let compiler = Compiler::normal();
    let control = compiler.control(None, false, Target::Any);
    let read = ConnectorReadProgramRecipe::try_compile_with_provider(&frozen, &compiler, &control)
        .unwrap();
    assert_eq!(compiler.calls.hooks.load(Ordering::Relaxed), 1);
    assert_eq!(read.frozen().public_facts(), frozen.public_facts());
    assert_eq!(
        read.frozen()
            .scan()
            .recipe()
            .relation()
            .table()
            .payload()
            .as_ref(),
        b"canonical:read-table"
    );
    let original = draft(data(2));
    let compiler = Compiler::normal();
    let control = compiler.control(None, false, Target::Any);
    let write =
        ConnectorWriteRecipe::try_compile_with_provider(&original, &compiler, &control).unwrap();
    assert_eq!(compiler.calls.hooks.load(Ordering::Relaxed), 1);
    assert_eq!(write.draft().input(), original.input());
    assert_eq!(
        write.draft().payload().payload().as_ref(),
        b"canonical-write"
    );
}

#[test]
fn wrapper_entry_control_rejection_never_calls_read_or_write_private_hook() {
    let frozen = frozen_read(1);
    let original = draft(data(1));
    for failure in [
        CompileControlError::Cancelled,
        CompileControlError::DeadlineExceeded,
        CompileControlError::ResourceExhausted,
    ] {
        let compiler = Compiler::normal();
        let control = compiler.control(Some(failure.clone()), false, Target::Wrapper);
        assert!(
            matches!(ConnectorReadProgramRecipe::try_compile_with_provider(&frozen,&compiler,&control),
            Err(ConnectorReadProgramCompileError::Control(actual)) if actual==failure)
        );
        assert_eq!(compiler.calls.hooks.load(Ordering::Relaxed), 0);
        assert_eq!(*control.work.lock().unwrap(), [(false, 0)]);
        let compiler = Compiler::normal();
        let control = compiler.control(Some(failure.clone()), false, Target::Wrapper);
        assert!(
            matches!(ConnectorWriteRecipe::try_compile_with_provider(&original,&compiler,&control),
            Err(ConnectorWriteRecipeCompileError::Control(actual)) if actual==failure)
        );
        assert_eq!(compiler.calls.hooks.load(Ordering::Relaxed), 0);
        assert_eq!(*control.work.lock().unwrap(), [(false, 0)]);
    }
}
#[test]
fn private_hook_entry_and_positive_quantum_controls_remain_outer_typed_control() {
    let frozen = frozen_read(300);
    let original = draft(data(300));
    for failure in [
        CompileControlError::Cancelled,
        CompileControlError::DeadlineExceeded,
        CompileControlError::ResourceExhausted,
    ] {
        for positive_only in [false, true] {
            let compiler = Compiler::normal();
            let control = compiler.control(Some(failure.clone()), positive_only, Target::Hook);
            assert!(
                matches!(ConnectorReadProgramRecipe::try_compile_with_provider(&frozen,&compiler,&control),
                Err(ConnectorReadProgramCompileError::Control(actual)) if actual==failure)
            );
            assert_eq!(compiler.calls.hooks.load(Ordering::Relaxed), 1);
            let work = control.work.lock().unwrap();
            if positive_only {
                assert!(work.contains(&(true, 256)));
            } else {
                // Entry and the explicit owner-handoff flush both check control.
                assert_eq!(*work, [(false, 0), (false, 0), (true, 0)]);
            }
            let compiler = Compiler::normal();
            let control = compiler.control(Some(failure.clone()), positive_only, Target::Hook);
            assert!(
                matches!(ConnectorWriteRecipe::try_compile_with_provider(&original,&compiler,&control),
                Err(ConnectorWriteRecipeCompileError::Control(actual)) if actual==failure)
            );
            assert_eq!(compiler.calls.hooks.load(Ordering::Relaxed), 1);
            let work = control.work.lock().unwrap();
            if positive_only {
                assert!(work.contains(&(true, 256)));
            } else {
                // Entry and the explicit owner-handoff flush both check control.
                assert_eq!(*work, [(false, 0), (false, 0), (true, 0)]);
            }
        }
    }
}
#[test]
fn ordinary_and_resource_provider_failures_are_preserved_as_provider_errors() {
    let frozen = frozen_read(2);
    let original = draft(data(2));
    for kind in [
        ConnectorErrorKind::Unavailable,
        ConnectorErrorKind::ResourceExhausted,
    ] {
        let expected = ConnectorError::new(kind, "fixture provider refusal")
            .with_retryable_before_progress()
            .with_cleanup_context("fixture cleanup fact");
        let mut compiler = Compiler::normal();
        compiler.provider_error = Some(expected.clone());
        let control = compiler.control(None, false, Target::Any);
        match ConnectorReadProgramRecipe::try_compile_with_provider(&frozen, &compiler, &control)
            .unwrap_err()
        {
            ConnectorReadProgramCompileError::Provider(error) => assert_eq!(error, expected),
            other => panic!("provider classification changed: {other:?}"),
        }
        assert_eq!(compiler.calls.hooks.load(Ordering::Relaxed), 1);
        let mut compiler = Compiler::normal();
        compiler.provider_error = Some(expected.clone());
        let control = compiler.control(None, false, Target::Any);
        match ConnectorWriteRecipe::try_compile_with_provider(&original, &compiler, &control)
            .unwrap_err()
        {
            ConnectorWriteRecipeCompileError::Provider(error) => assert_eq!(error, expected),
            other => panic!("provider classification changed: {other:?}"),
        }
        assert_eq!(compiler.calls.hooks.load(Ordering::Relaxed), 1);
    }
}
#[test]
fn fixture_hooks_reject_real_private_identity_mismatches_not_just_injected_results() {
    let frozen = frozen_read(1);
    let recipe = frozen.scan().recipe();
    let table = ConnectorEncodedPayload::new(
        recipe.relation().table().header().clone(),
        Bytes::from_static(b"foreign-table"),
    );
    let malformed = ConnectorReadRelationRecipeDraft::try_new(
        recipe.binding().clone(),
        ConnectorReadRelationPayload::new(
            recipe.relation().kind(),
            table,
            recipe.relation().view().clone(),
        ),
        recipe.columns().to_vec(),
    )
    .unwrap();
    let scan = frozen.scan().try_replace_private_recipe(malformed).unwrap();
    let changed = FrozenConnectorRead::try_new(scan, frozen.public_facts().clone()).unwrap();
    let compiler = Compiler::normal();
    let control = compiler.control(None, false, Target::Any);
    assert!(
        matches!(ConnectorReadProgramRecipe::try_compile_with_provider(&changed,&compiler,&control),
        Err(ConnectorReadProgramCompileError::Provider(error)) if error.kind()==ConnectorErrorKind::InvalidRequest)
    );
    let malformed = draft(ConnectorWriteInputShape::Data {
        fields: vec![ConnectorWriteFieldBinding::new(
            token(99),
            Field::new("v0", DataType::Int64, false),
        )],
    });
    let compiler = Compiler::normal();
    let control = compiler.control(None, false, Target::Any);
    assert!(
        matches!(ConnectorWriteRecipe::try_compile_with_provider(&malformed,&compiler,&control),
        Err(ConnectorWriteRecipeCompileError::Provider(error)) if error.kind()==ConnectorErrorKind::InvalidRequest)
    );
}
#[test]
fn public_writer_field_comparison_observes_midwork_control_after_private_hook_success() {
    let original = draft(data(300));
    for failure in [
        CompileControlError::Cancelled,
        CompileControlError::DeadlineExceeded,
        CompileControlError::ResourceExhausted,
    ] {
        let compiler = Compiler::normal();
        let control = compiler.control(Some(failure.clone()), true, Target::Wrapper);
        assert!(
            matches!(ConnectorWriteRecipe::try_compile_with_provider(&original,&compiler,&control),
            Err(ConnectorWriteRecipeCompileError::Control(actual)) if actual==failure)
        );
        assert_eq!(compiler.calls.hooks.load(Ordering::Relaxed), 1);
        let work = control.work.lock().unwrap();
        assert!(work.contains(&(true, 256)));
        assert!(work.contains(&(false, 256)));
    }
}
fn assert_contract_mismatch(
    original: ConnectorWriteInputShape,
    canonical: ConnectorWriteInputShape,
) {
    let original = draft(original);
    let mut compiler = Compiler::normal();
    compiler.write_override = Some(canonical);
    let control = compiler.control(None, false, Target::Any);
    match ConnectorWriteRecipe::try_compile_with_provider(&original, &compiler, &control)
        .unwrap_err()
    {
        ConnectorWriteRecipeCompileError::Contract(error) => {
            assert_eq!(error.kind(), ConnectorErrorKind::InvalidRequest)
        }
        other => panic!(
            "a valid canonical public layout change must be rejected by the wrapper: {other:?}"
        ),
    }
    assert_eq!(compiler.calls.hooks.load(Ordering::Relaxed), 1);
}
#[test]
fn all_five_writer_shape_variants_compile_but_variant_and_group_boundary_changes_do_not() {
    let shapes = [
        data(3),
        ConnectorWriteInputShape::RowLineage {
            data_fields: vec![field(0), field(1)],
            row_identity_fields: vec![field(2)],
        },
        ConnectorWriteInputShape::PositionDelete {
            identity_fields: vec![field(0)],
            partition_source_fields: vec![field(1), field(2)],
        },
        ConnectorWriteInputShape::DeletionVector {
            identity_fields: vec![field(0)],
            partition_source_fields: vec![field(1), field(2)],
        },
        ConnectorWriteInputShape::EqualityDelete {
            equality_fields: vec![field(0), field(1), field(2)],
        },
    ];
    for shape in &shapes {
        let original = draft(shape.clone());
        let compiler = Compiler::normal();
        let control = compiler.control(None, false, Target::Any);
        assert_eq!(
            ConnectorWriteRecipe::try_compile_with_provider(&original, &compiler, &control)
                .unwrap()
                .draft()
                .input(),
            shape
        );
    }
    assert_contract_mismatch(shapes[0].clone(), shapes[4].clone());
    assert_contract_mismatch(shapes[2].clone(), shapes[3].clone());
    assert_contract_mismatch(
        shapes[1].clone(),
        ConnectorWriteInputShape::RowLineage {
            data_fields: vec![field(0)],
            row_identity_fields: vec![field(1), field(2)],
        },
    );
    assert_contract_mismatch(
        shapes[2].clone(),
        ConnectorWriteInputShape::PositionDelete {
            identity_fields: vec![field(0), field(1)],
            partition_source_fields: vec![field(2)],
        },
    );
    assert_contract_mismatch(
        shapes[3].clone(),
        ConnectorWriteInputShape::DeletionVector {
            identity_fields: vec![field(0), field(1)],
            partition_source_fields: vec![field(2)],
        },
    );
}
#[test]
fn ordered_tokens_names_types_nullability_and_metadata_are_exact_writer_public_facts() {
    assert_contract_mismatch(
        data(2),
        ConnectorWriteInputShape::Data {
            fields: vec![field(1), field(0)],
        },
    );
    for changed in [
        ConnectorWriteFieldBinding::new(token(9), field(0).field().clone()),
        ConnectorWriteFieldBinding::new(
            token(0),
            Field::new("changed-name", DataType::Int64, false),
        ),
        ConnectorWriteFieldBinding::new(token(0), Field::new("v0", DataType::Int32, false)),
        ConnectorWriteFieldBinding::new(token(0), Field::new("v0", DataType::Int64, true)),
        ConnectorWriteFieldBinding::new(
            token(0),
            Field::new("v0", DataType::Int64, false).with_metadata(std::collections::HashMap::<
                String,
                String,
            >::from([(
                "annotation".into(),
                "changed".into(),
            )])),
        ),
    ] {
        assert_contract_mismatch(
            data(1),
            ConnectorWriteInputShape::Data {
                fields: vec![changed],
            },
        );
    }
}
#[allow(deprecated)]
fn dictionary(id: i64, ordered: bool) -> Field {
    Field::new_dict(
        "v0",
        DataType::Dictionary(Box::new(DataType::Int8), Box::new(DataType::Utf8)),
        false,
        id,
        ordered,
    )
}
#[test]
fn dictionary_physical_id_ordering_and_nested_field_metadata_cannot_change() {
    let shape = |field: Field| ConnectorWriteInputShape::Data {
        fields: vec![ConnectorWriteFieldBinding::new(token(0), field)],
    };
    let original = dictionary(17, false);
    for canonical in [dictionary(18, false), dictionary(17, true)] {
        assert_eq!(original, canonical); // Arrow's ordinary equality omits these facts.
        assert_contract_mismatch(shape(original.clone()), shape(canonical));
    }
    let structure = |annotation: &str| {
        Field::new(
            "v0",
            DataType::Struct(
                vec![Field::new("nested", DataType::Int64, false).with_metadata(
                    std::collections::HashMap::<String, String>::from([(
                        "annotation".into(),
                        annotation.into(),
                    )]),
                )]
                .into(),
            ),
            false,
        )
    };
    assert_contract_mismatch(shape(structure("one")), shape(structure("two")));
    let nested_dictionary = |id: i64| {
        Field::new(
            "v0",
            DataType::Struct(vec![dictionary(id, false)].into()),
            false,
        )
    };
    assert_contract_mismatch(shape(nested_dictionary(17)), shape(nested_dictionary(18)));
}
#[test]
fn a_single_struct_with_five_thousand_fields_stays_inside_the_writer_owner_domain() {
    let nested = (0..5000)
        .map(|id| Field::new(format!("n{id}"), DataType::Int64, false))
        .collect::<Vec<_>>();
    let original = draft(ConnectorWriteInputShape::Data {
        fields: vec![ConnectorWriteFieldBinding::new(
            token(0),
            Field::new("v0", DataType::Struct(nested.into()), false),
        )],
    });
    assert_eq!(original.input().field_count(), 1);
    assert!(original.charged_bytes() < MAX_WRITE_RELATION_DECODED_SCHEMA_BYTES);
    let compiler = Compiler::normal();
    let control = compiler.control(None, false, Target::Any);
    let recipe =
        ConnectorWriteRecipe::try_compile_with_provider(&original, &compiler, &control).unwrap();
    assert_eq!(compiler.calls.hooks.load(Ordering::Relaxed), 1);
    let control = compiler.control(None, false, Target::Wrapper);
    let mut work = CompileCheckpoints::try_new(&control, CompilePhase::ProviderValidation).unwrap();
    assert!(
        original
            .input()
            .same_layout_observed::<CompileControlError>(recipe.draft().input(), || work.step())
            .unwrap()
    );
    work.finish().unwrap();
    let observed = control.work.lock().unwrap();
    assert!(observed.contains(&(false, 256)));
    assert!(
        observed
            .iter()
            .map(|(_, units)| *units as usize)
            .sum::<usize>()
            > 5000
    );
    let first = original.input().fields_iter().next().unwrap().field();
    let second = recipe.draft().input().fields_iter().next().unwrap().field();
    let (DataType::Struct(first), DataType::Struct(second)) =
        (first.data_type(), second.data_type())
    else {
        unreachable!()
    };
    assert_eq!(first.len(), 5000);
    assert_eq!(second.len(), 5000);
    assert!(!std::ptr::eq(first[0].as_ref(), second[0].as_ref()));
    // These owner-validated fields are compared by borrow. This test makes no
    // claim that draft construction/cloning/canonical schema copies observe
    // PureCompileControl, or that production private compilation is wired.
}

#[test]
fn wide_nested_writer_comparison_preserves_positive_quantum_control_without_a_foreign_node_gate() {
    let nested = (0..5000)
        .map(|i| Field::new(format!("n{i}"), DataType::Int64, false))
        .collect::<Vec<_>>();
    let original = draft(ConnectorWriteInputShape::Data {
        fields: vec![ConnectorWriteFieldBinding::new(
            token(0),
            Field::new("v0", DataType::Struct(nested.into()), false),
        )],
    });
    for failure in [
        CompileControlError::Cancelled,
        CompileControlError::DeadlineExceeded,
        CompileControlError::ResourceExhausted,
    ] {
        let compiler = Compiler::normal();
        let control = compiler.control(Some(failure.clone()), true, Target::Wrapper);
        assert!(
            matches!(ConnectorWriteRecipe::try_compile_with_provider(&original,&compiler,&control),
            Err(ConnectorWriteRecipeCompileError::Control(actual)) if actual==failure)
        );
        assert_eq!(compiler.calls.hooks.load(Ordering::Relaxed), 1);
        assert!(control.work.lock().unwrap().contains(&(false, 256)));
    }
}
