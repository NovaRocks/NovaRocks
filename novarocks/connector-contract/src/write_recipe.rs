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

//! Bounded pure writer recipes. Sealing proves provider-private semantics
//! without opening a writer, resolving credentials or retaining a runtime handle.

use crate::owned_copy::{CreatedWriteCopy, ObservedCopy, OwnedCopy, PlainCopy};
#[cfg(test)]
use crate::validate_write_field_schema_events;
use crate::{
    ConnectorCodecCategory, ConnectorCodecContractError, ConnectorEncodedPayload, ConnectorError,
    ConnectorErrorKind, ConnectorWriteBinding, ConnectorWriteInputRef, ConnectorWriteInputShape,
    MAX_WRITE_RELATION_DECODED_SCHEMA_BYTES, PureProviderCompileError,
    WRITE_FIELD_ALLOCATION_CHARGE, WriteSchemaVisit, WriterOwnedResourceFacts,
};
use novarocks_type_contract::{
    CompileCheckpoints, CompileControlError, CompilePhase, PureCompileControl,
};
use std::{error::Error, fmt, sync::Arc};

/// Count preflight derived from the existing aggregate schema allocation bound,
/// not from the read recipe's unrelated column limit.
pub const MAX_CONNECTOR_WRITE_INPUT_FIELDS: usize =
    MAX_WRITE_RELATION_DECODED_SCHEMA_BYTES / WRITE_FIELD_ALLOCATION_CHARGE;

pub const MAX_CONNECTOR_WRITER_HANDLE_BYTES: usize = 16 * 1024 * 1024;

#[derive(Clone)]
pub struct ConnectorWriteRecipeDraft {
    binding: ConnectorWriteBinding,
    payload: ConnectorEncodedPayload,
    input: Arc<ConnectorWriteInputShape>,
    _storage: novarocks_type_contract::ValueTypeStorageLifetime,
    charged_bytes: usize,
    original_metadata_namespace: Option<novarocks_type_contract::owned_resources::metadata_materialization::MaterializedFieldNamespace>,
}

impl std::fmt::Debug for ConnectorWriteRecipeDraft {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ConnectorWriteRecipeDraft")
            .field("binding", &self.binding)
            .field("payload", &self.payload)
            .field("input", &self.input)
            .field("charged_bytes", &self.charged_bytes)
            .finish()
    }
}
impl PartialEq for ConnectorWriteRecipeDraft {
    fn eq(&self, other: &Self) -> bool {
        self.binding == other.binding
            && self.payload == other.payload
            && self.input == other.input
            && self.charged_bytes == other.charged_bytes
    }
}
impl Eq for ConnectorWriteRecipeDraft {}

impl ConnectorWriteRecipeDraft {
    pub fn try_new(
        binding: ConnectorWriteBinding,
        payload: ConnectorEncodedPayload,
        input: ConnectorWriteInputShape,
    ) -> Result<Self, ConnectorError> {
        Self::try_new_core(&binding, &payload, &input, &mut PlainCopy)
    }

    /// Construct through the original writer law and sole owned-copy grammar,
    /// borrowing the caller's entry/footer and existing checkpoints. `admit`
    /// must compose these contributions with all other known parent facts and
    /// reject before any callback or next request. It is not an allocator grant.
    /// The source invoice includes full retained backing, not visible lengths.
    /// Source arguments are borrowed, so interruption does not destroy a raw
    /// caller-owned graph before its original owner can observe the outcome.
    pub fn try_new_observed(
        binding: &ConnectorWriteBinding,
        payload: &ConnectorEncodedPayload,
        input: &ConnectorWriteInputShape,
        source_retained_bytes: usize,
        admit: &mut impl FnMut(&WriterOwnedResourceFacts) -> Result<(), CompileControlError>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<Self, PureProviderCompileError<ConnectorError>> {
        let mut observer = ObservedCopy::new(source_retained_bytes, admit, work)?;
        let mut materialization =
            crate::metadata_materialization_copy::MetadataMaterializationCopy::new(&mut observer);
        let mut result = Self::try_new_core(binding, payload, input, &mut materialization)?;
        result.original_metadata_namespace = Some(materialization.finish_namespace()?);
        Ok(result)
    }

    /// Resolve original borrowed field loans through the same writer law and
    /// single owned-copy body as the owned-input entrypoint. No intermediate
    /// owned fields, entry/footer scope, or provider-private seal is created.
    /// The source invoice covers the live loan slices as well as their complete
    /// original Arrow backing and all other retained caller sources.
    pub fn try_new_from_borrowed_input_observed(
        binding: &ConnectorWriteBinding,
        payload: &ConnectorEncodedPayload,
        input: &ConnectorWriteInputRef<'_>,
        source_retained_bytes: usize,
        admit: &mut impl FnMut(&WriterOwnedResourceFacts) -> Result<(), CompileControlError>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<Self, PureProviderCompileError<ConnectorError>> {
        let mut observer = ObservedCopy::new(source_retained_bytes, admit, work)?;
        let mut materialization =
            crate::metadata_materialization_copy::MetadataMaterializationCopy::new(&mut observer);
        let mut result = Self::try_new_core(binding, payload, input, &mut materialization)?;
        result.original_metadata_namespace = Some(materialization.finish_namespace()?);
        Ok(result)
    }

    /// The same complete writer law and copy body with actual independent
    /// temporary, recipe and reachable Field construction responsibility.
    /// Numeric admission remains separate from this genuine factory.
    pub fn try_new_from_borrowed_input_with_factory_in<
        H,
        F: novarocks_type_contract::ValueSchemaCreationFactory<H>,
    >(
        binding: &ConnectorWriteBinding,
        payload: &ConnectorEncodedPayload,
        input: &ConnectorWriteInputRef<'_>,
        source_retained_bytes: usize,
        factory: &F,
        admit: &mut impl FnMut(&WriterOwnedResourceFacts) -> Result<(), CompileControlError>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<Self, crate::WriterOwnedCreationFailure<H>> {
        let mut observer = CreatedWriteCopy::new(source_retained_bytes, factory, admit, work)?;
        let mut materialization =
            crate::metadata_materialization_copy::MetadataMaterializationCopy::new(&mut observer);
        let mut result = Self::try_new_core(binding, payload, input, &mut materialization)?;
        result.original_metadata_namespace = Some(materialization.finish_namespace()?);
        Ok(result)
    }

    fn try_new_core<F: crate::write_input::WriteInputFields, O: OwnedCopy>(
        binding: &ConnectorWriteBinding,
        payload: &ConnectorEncodedPayload,
        input: &crate::write_input::ConnectorWriteInput<F>,
        observer: &mut O,
    ) -> Result<Self, O::Error> {
        // O(1) original-owner counts and requests before touching source fields.
        input.preflight_owned_roles(observer)?;
        observer.source_floor(payload.payload().len())?;
        observer.array::<u8>(payload.payload().len(), 1)?;
        observer.arc::<ConnectorWriteInputShape>()?;
        if observer.source_invoice().is_some() && !payload.payload().is_empty() {
            let shared = novarocks_type_contract::owned_resources::layout::bytes_shared_upper()
                .map_err(|_| observer.arithmetic())?;
            observer.array::<u8>(shared, 1)?;
        }
        observer.work(observer.add(observer.mul(payload.payload().len(), 2)?, 128)?)?;
        if let Some(source) = observer.source_invoice() {
            // All identity spellings are inside the truthful source union.
            // String equality and header validation are opaque library work.
            observer.work(observer.add(observer.mul(source, 8)?, 128)?)?;
        }
        observer.flush()?;
        let same_instance =
            binding.descriptor().instance_id == *binding.catalog_handle().catalog_name();
        observer.step()?;
        if !same_instance {
            return Err(invalid("writer recipe catalog differs from its instance").into());
        }
        observer.flush()?;
        payload
            .header()
            .validate_expected::<ConnectorCodecContractError>(
                &binding.descriptor().provider_id,
                binding.catalog_handle(),
                ConnectorCodecCategory::WriteHandle,
                payload.header().codec_revision(),
            )
            .map_err(|error| invalid(error.to_string()))?;
        observer.step()?;
        if payload.payload().len() > MAX_CONNECTOR_WRITER_HANDLE_BYTES {
            return Err(resource("writer recipe handle exceeds the byte limit").into());
        }
        if input.source_field_count() > MAX_CONNECTOR_WRITE_INPUT_FIELDS {
            return Err(resource("writer recipe input exceeds the field count limit").into());
        }
        input.validate_with(observer)?;
        let mut schema_bytes = 0;
        for field in input.field_refs() {
            observer.work(1)?;
            if field.field().name().is_empty() {
                return Err(invalid("writer recipe input field name is empty").into());
            }
            crate::write_schema::validate_write_bound_field_schema_events::<O::Error>(
                field,
                1,
                &mut schema_bytes,
                |event| {
                    match event {
                        WriteSchemaVisit::BeforeField(field) => {
                            observer.work(128)?;
                            observer.source_floor(observer.add(
                                size_of::<arrow_schema::Field>(),
                                field.name().capacity(),
                            )?)?;
                            if let Some(source) = observer.source_invoice() {
                                let lookup = if field.metadata().is_empty() {
                                    64
                                } else {
                                    let key_bytes =
                                        novarocks_type_contract::NR_LOGICAL_TYPE_KEY.len();
                                    novarocks_type_contract::owned_resources::hashmap::string_operations_work_upper_bound(
                                    source, 1, key_bytes, key_bytes,
                                ).map_err(|error| observer.hash_error(error))?
                                };
                                observer.work(lookup)?;
                            }
                            observer.metadata_iteration(field.metadata().len(), 1)?;
                        }
                        WriteSchemaVisit::BeforeType(_) => {
                            observer.work(64)?;
                            observer.flush()?;
                        }
                        WriteSchemaVisit::Completed => observer.step()?,
                    }
                    Ok(())
                },
            )?;
        }
        // Preserve the original structural retained charge. Actual cumulative
        // request/work upper bounds are a separate parent contribution.
        debug_assert!(schema_bytes <= MAX_WRITE_RELATION_DECODED_SCHEMA_BYTES);
        let charged_bytes = schema_bytes
            .checked_add(payload.payload().len())
            .and_then(|bytes| bytes.checked_add(size_of::<Self>()))
            .and_then(|bytes| {
                bytes.checked_add(size_of::<ConnectorWriteInputShape>() + 2 * size_of::<usize>())
            })
            .and_then(|bytes| {
                bytes.checked_add(
                    input.source_field_count() * size_of::<crate::ConnectorWriteFieldToken>(),
                )
            })
            .ok_or_else(|| resource("writer recipe retained charge overflowed"))?;
        // Count uses the same physical grammar without constructing fields.
        // The actual Arrow Union constructor remains the sole identity judge
        // during materialization, after all original child copies.
        if !observer.materializes() {
            let counted = input.owned_bounded_core(observer)?;
            debug_assert!(counted.is_none());
        }
        observer.begin_copy()?;
        let input = input
            .owned_bounded_core(observer)?
            .ok_or_else(|| invalid("writer owned copy did not materialize its input"))?;
        observer.prefunded_request(
            std::alloc::Layout::array::<u8>(payload.payload().len())
                .map_err(|_| observer.arithmetic())?,
            1,
        )?;
        let mut copied_payload = Vec::new();
        observer.flush()?;
        let reservation = copied_payload.try_reserve_exact(payload.payload().len());
        observer.reserve_exit(reservation)?;
        for segment in payload.payload().chunks(256) {
            copied_payload.extend_from_slice(segment);
            observer.step()?;
        }
        observer.flush()?;
        if !copied_payload.is_empty() {
            let shared = novarocks_type_contract::owned_resources::layout::bytes_shared_upper()
                .map_err(|_| observer.arithmetic())?;
            observer.prefunded_request(
                std::alloc::Layout::array::<u8>(shared).map_err(|_| observer.arithmetic())?,
                1,
            )?;
        }
        let payload = ConnectorEncodedPayload::new(
            payload.header().clone(),
            bytes::Bytes::from(copied_payload),
        );
        observer.step()?;
        observer.flush()?;
        let request = novarocks_type_contract::owned_resources::layout::arc_layout(
            std::alloc::Layout::new::<ConnectorWriteInputShape>(),
        )
        .map_err(|_| observer.arithmetic())?;
        observer.prefunded_request(request, 1)?;
        let input = Arc::new(input);
        observer.step()?;
        Ok(Self {
            binding: binding.clone(),
            payload,
            input,
            _storage: observer.storage_lifetime(),
            charged_bytes,
            original_metadata_namespace: None,
        })
    }

    pub const fn binding(&self) -> &ConnectorWriteBinding {
        &self.binding
    }
    pub const fn payload(&self) -> &ConnectorEncodedPayload {
        &self.payload
    }
    pub fn input(&self) -> &ConnectorWriteInputShape {
        &self.input
    }
    pub fn original_metadata_namespace(&self) -> Option<&novarocks_type_contract::owned_resources::metadata_materialization::MaterializedFieldNamespace>{
        self.original_metadata_namespace.as_ref()
    }

    pub const fn charged_bytes(&self) -> usize {
        self.charged_bytes
    }
}

/// The caller selects the exact installed pure definition. This interface
/// cannot recover or return an execution handle or acquire I/O capabilities.
/// Private decode, canonicalization and copying must observe bounded work;
/// interruption returns Control unchanged, never a Provider diagnostic.
pub trait ConnectorWriteRecipeCompiler: Send + Sync {
    type Error: Error;
    fn compile_private(
        &self,
        draft: &ConnectorWriteRecipeDraft,
        control: &dyn PureCompileControl,
    ) -> Result<ConnectorWriteRecipeDraft, PureProviderCompileError<Self::Error>>;
}

#[derive(Debug)]
pub enum ConnectorWriteRecipeCompileError<E: Error> {
    Contract(ConnectorError),
    Provider(E),
    Control(CompileControlError),
}

impl<E: Error> fmt::Display for ConnectorWriteRecipeCompileError<E> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Contract(error) => fmt::Display::fmt(error, f),
            Self::Provider(error) => fmt::Display::fmt(error, f),
            Self::Control(error) => fmt::Display::fmt(error, f),
        }
    }
}
impl<E: Error + 'static> Error for ConnectorWriteRecipeCompileError<E> {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Contract(error) => Some(error),
            Self::Provider(error) => Some(error),
            Self::Control(error) => Some(error),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConnectorWriteRecipe(ConnectorWriteRecipeDraft);

impl ConnectorWriteRecipe {
    pub fn try_compile_with_provider<C: ConnectorWriteRecipeCompiler + ?Sized>(
        draft: &ConnectorWriteRecipeDraft,
        compiler: &C,
        control: &dyn PureCompileControl,
    ) -> Result<Self, ConnectorWriteRecipeCompileError<C::Error>> {
        let mut work = CompileCheckpoints::try_new(control, CompilePhase::ProviderValidation)
            .map_err(ConnectorWriteRecipeCompileError::Control)?;
        let result = (|| {
            work.flush()
                .map_err(ConnectorWriteRecipeCompileError::Control)?;
            let canonical = compiler
                .compile_private(draft, work.control())
                .map_err(|failure| match failure {
                    PureProviderCompileError::Provider(error) => {
                        ConnectorWriteRecipeCompileError::Provider(error)
                    }
                    PureProviderCompileError::Control(error) => {
                        ConnectorWriteRecipeCompileError::Control(error)
                    }
                })?;
            let same_envelope = canonical.binding == draft.binding
                && canonical.payload.header() == draft.payload.header();
            work.step()
                .map_err(ConnectorWriteRecipeCompileError::Control)?;
            if !same_envelope {
                return Err(ConnectorWriteRecipeCompileError::Contract(invalid(
                    "writer recipe public facts changed during provider canonicalization",
                )));
            }
            let same_backing = Arc::ptr_eq(&canonical.input, &draft.input);
            work.step()
                .map_err(ConnectorWriteRecipeCompileError::Control)?;
            // Both drafts own checked immutable input snapshots. Sharing this
            // exact backing proves identity without copying or rescanning all
            // field trees. A new provider-authored snapshot still faces the
            // complete role/token/field comparison.
            let same_input = same_backing
                || canonical
                    .input
                    .same_layout_observed::<CompileControlError>(&draft.input, || work.step())
                    .map_err(ConnectorWriteRecipeCompileError::Control)?;
            if !same_input {
                return Err(ConnectorWriteRecipeCompileError::Contract(invalid(
                    "writer recipe public facts changed during provider canonicalization",
                )));
            }
            Ok(Self(canonical))
        })();
        work.finish_result(result, ConnectorWriteRecipeCompileError::Control)
    }
    pub const fn draft(&self) -> &ConnectorWriteRecipeDraft {
        &self.0
    }
}

fn invalid(message: impl Into<String>) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::InvalidRequest, message)
}
fn resource(message: &'static str) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::ResourceExhausted, message)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        CatalogHandle, CatalogVersion, ConnectorCodecRevision, ConnectorEnvelopeHeader,
        ConnectorInstanceDescriptor, ConnectorInstanceId, ConnectorProviderId,
        ConnectorWriteFieldBinding, ConnectorWriteFieldToken,
    };
    use arrow_schema::{DataType, Field};

    fn draft(input: ConnectorWriteInputShape) -> ConnectorWriteRecipeDraft {
        let instance = ConnectorInstanceId::try_from_canonical("lake").unwrap();
        let catalog = CatalogHandle::new(instance.clone(), CatalogVersion::from_bytes([7; 32]));
        let provider = ConnectorProviderId::parse("iceberg").unwrap();
        let binding = ConnectorWriteBinding::new(
            ConnectorInstanceDescriptor {
                provider_id: provider.clone(),
                instance_id: instance,
            },
            catalog.clone(),
        );
        let payload = ConnectorEncodedPayload::new(
            ConnectorEnvelopeHeader::new(
                provider,
                catalog,
                ConnectorCodecCategory::WriteHandle,
                ConnectorCodecRevision::try_new(1).unwrap(),
            ),
            bytes::Bytes::from_static(b"private writer"),
        );
        ConnectorWriteRecipeDraft::try_new(binding, payload, input).unwrap()
    }
    fn field(index: u8) -> ConnectorWriteFieldBinding {
        ConnectorWriteFieldBinding::new(
            ConnectorWriteFieldToken::from_bytes([index; 32]),
            Field::new(format!("v{index}"), DataType::Int64, false),
        )
    }
    struct Control;
    impl PureCompileControl for Control {
        fn checkpoint(&self, _: CompilePhase, _: u32) -> Result<(), CompileControlError> {
            Ok(())
        }
    }
    struct Compiler(u8);
    impl ConnectorWriteRecipeCompiler for Compiler {
        type Error = ConnectorError;
        fn compile_private(
            &self,
            draft: &ConnectorWriteRecipeDraft,
            _: &dyn PureCompileControl,
        ) -> Result<ConnectorWriteRecipeDraft, PureProviderCompileError<Self::Error>> {
            let mut input = draft.input().clone();
            let mut binding = draft.binding.clone();
            let mut payload = draft.payload.clone();
            if let ConnectorWriteInputShape::Data { fields } = &mut input {
                match self.0 {
                    1 => fields.reverse(),
                    2 => fields[0] = field(9),
                    3 => {
                        fields[0] = ConnectorWriteFieldBinding::new(
                            fields[0].token(),
                            fields[0]
                                .field()
                                .clone()
                                .with_metadata(std::collections::HashMap::<String, String>::from(
                                    [("logical_type".into(), "json".into())],
                                )),
                        )
                    }
                    4 => {
                        binding = ConnectorWriteBinding::new(
                            binding.descriptor().clone(),
                            CatalogHandle::new(
                                binding.catalog_handle().catalog_name().clone(),
                                CatalogVersion::from_bytes([8; 32]),
                            ),
                        )
                    }
                    5 => {
                        input = ConnectorWriteInputShape::EqualityDelete {
                            equality_fields: fields.clone(),
                        }
                    }
                    _ => {}
                }
            }
            if self.0 == 4 {
                payload = ConnectorEncodedPayload::new(
                    ConnectorEnvelopeHeader::new(
                        binding.descriptor().provider_id.clone(),
                        binding.catalog_handle().clone(),
                        ConnectorCodecCategory::WriteHandle,
                        payload.header().codec_revision(),
                    ),
                    payload.payload().clone(),
                );
            } else {
                payload = ConnectorEncodedPayload::new(
                    payload.header().clone(),
                    bytes::Bytes::from_static(b"canonical writer"),
                );
            }
            ConnectorWriteRecipeDraft::try_new(binding, payload, input)
                .map_err(PureProviderCompileError::Provider)
        }
    }
    #[test]
    fn pure_writer_canonicalization_preserves_all_public_identities() {
        let draft = draft(ConnectorWriteInputShape::Data {
            fields: vec![field(1), field(2)],
        });
        let recipe =
            ConnectorWriteRecipe::try_compile_with_provider(&draft, &Compiler(0), &Control)
                .unwrap();
        assert_eq!(recipe.draft().input(), draft.input());
        assert_eq!(
            recipe.draft().payload().payload().as_ref(),
            b"canonical writer"
        );
        for mutation in 1..=5 {
            assert!(
                matches!(
                    ConnectorWriteRecipe::try_compile_with_provider(
                        &draft,
                        &Compiler(mutation),
                        &Control
                    ),
                    Err(ConnectorWriteRecipeCompileError::Contract(_))
                ),
                "mutation {mutation}"
            );
        }
    }
    #[test]
    fn writer_shape_roles_survive_pure_compilation() {
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
            let draft = draft(input.clone());
            assert_eq!(
                ConnectorWriteRecipe::try_compile_with_provider(&draft, &Compiler(0), &Control)
                    .unwrap()
                    .draft()
                    .input(),
                &input
            );
        }
    }
    #[test]
    fn writer_recipe_refuses_duplicate_identities_and_unbounded_metadata() {
        let valid = draft(ConnectorWriteInputShape::Data {
            fields: vec![field(1)],
        });
        for input in [
            ConnectorWriteInputShape::Data {
                fields: vec![field(1), field(1)],
            },
            ConnectorWriteInputShape::Data {
                fields: vec![ConnectorWriteFieldBinding::new(
                    field(1).token(),
                    Field::new("v", DataType::Int64, false).with_metadata(
                        std::collections::HashMap::<String, String>::from([(
                            "k".into(),
                            "x".repeat(crate::MAX_WRITE_RELATION_METADATA_VALUE_BYTES + 1),
                        )]),
                    ),
                )],
            },
        ] {
            assert!(
                ConnectorWriteRecipeDraft::try_new(
                    valid.binding.clone(),
                    valid.payload.clone(),
                    input
                )
                .is_err()
            );
        }
    }
    #[test]
    fn dictionary_field_ids_and_ordering_are_exact_public_facts() {
        #[allow(deprecated)]
        let field = |id| {
            Field::new_dict(
                "v",
                DataType::Dictionary(Box::new(DataType::Int8), Box::new(DataType::Utf8)),
                false,
                id,
                false,
            )
        };
        let first = ConnectorWriteFieldBinding::new(
            ConnectorWriteFieldToken::from_bytes([1; 32]),
            field(1),
        );
        let second = ConnectorWriteFieldBinding::new(first.token(), field(2));
        assert_ne!(first, second);
    }

    #[test]
    fn recipe_owns_bounded_field_and_metadata_backing() {
        let mut metadata = std::collections::HashMap::with_capacity(10_000);
        let mut value = String::with_capacity(10_000);
        value.push_str("value");
        metadata.insert("key".into(), value);
        let mut fields = Vec::with_capacity(10_000);
        fields.push(ConnectorWriteFieldBinding::new(
            field(1).token(),
            Field::new("v", DataType::Int64, false).with_metadata(metadata),
        ));
        let draft = draft(ConnectorWriteInputShape::Data { fields });
        let ConnectorWriteInputShape::Data { fields } = draft.input() else {
            unreachable!()
        };
        assert_eq!(fields.capacity(), fields.len());
        assert_eq!(fields[0].field().metadata().len(), 1);
        assert_eq!(
            fields[0].field().metadata()["key"].capacity(),
            "value".len()
        );
        assert_eq!(fields[0].field().metadata()["key"], "value");
    }

    #[test]
    fn writer_draft_clones_share_the_exact_owned_input_without_reauthoring_fields() {
        let original = draft(ConnectorWriteInputShape::RowLineage {
            data_fields: vec![field(1)],
            row_identity_fields: vec![field(2)],
        });
        let copied = original.clone();
        assert!(std::ptr::eq(original.input(), copied.input()));
        assert_eq!(original.charged_bytes(), copied.charged_bytes());
        struct ValidatedCopy;
        impl ConnectorWriteRecipeCompiler for ValidatedCopy {
            type Error = ConnectorError;
            fn compile_private(
                &self,
                draft: &ConnectorWriteRecipeDraft,
                _: &dyn PureCompileControl,
            ) -> Result<ConnectorWriteRecipeDraft, PureProviderCompileError<Self::Error>>
            {
                Ok(draft.clone())
            }
        }
        // This fixture tests the neutral seal's immutable ownership only;
        // it does not certify any provider-private bytes as meaningful.
        let sealed =
            ConnectorWriteRecipe::try_compile_with_provider(&original, &ValidatedCopy, &Control)
                .unwrap();
        assert!(std::ptr::eq(original.input(), sealed.draft().input()));
        drop(original);
        assert_eq!(sealed.draft().input(), copied.input());
    }

    #[test]
    fn writer_seal_preserves_original_error_control_prefix_and_success_tail() {
        use std::sync::Mutex;
        struct Trace {
            units: Mutex<Vec<u32>>,
            refusal: Option<(usize, CompileControlError)>,
        }
        impl PureCompileControl for Trace {
            fn checkpoint(
                &self,
                phase: CompilePhase,
                units: u32,
            ) -> Result<(), CompileControlError> {
                assert_eq!(phase, CompilePhase::ProviderValidation);
                let mut trace = self.units.lock().unwrap();
                trace.push(units);
                match &self.refusal {
                    Some((at, cause)) if *at == trace.len() => Err(cause.clone()),
                    _ => Ok(()),
                }
            }
        }
        struct Refusal(Option<CompileControlError>);
        impl ConnectorWriteRecipeCompiler for Refusal {
            type Error = ConnectorError;
            fn compile_private(
                &self,
                _: &ConnectorWriteRecipeDraft,
                _: &dyn PureCompileControl,
            ) -> Result<ConnectorWriteRecipeDraft, PureProviderCompileError<Self::Error>>
            {
                match &self.0 {
                    Some(cause) => Err(PureProviderCompileError::Control(cause.clone())),
                    None => Err(PureProviderCompileError::Provider(invalid(
                        "private refusal",
                    ))),
                }
            }
        }
        let original = draft(ConnectorWriteInputShape::Data {
            fields: vec![field(1)],
        });
        let success = Trace {
            units: Mutex::default(),
            refusal: None,
        };
        ConnectorWriteRecipe::try_compile_with_provider(&original, &Compiler(0), &success).unwrap();
        let successful_trace = success.units.lock().unwrap().clone();
        assert!(*successful_trace.last().unwrap() > 0);
        for cause in [
            CompileControlError::Cancelled,
            CompileControlError::DeadlineExceeded,
            CompileControlError::ResourceExhausted,
        ] {
            let late = Trace {
                units: Mutex::default(),
                refusal: Some((3, cause.clone())),
            };
            let error =
                ConnectorWriteRecipe::try_compile_with_provider(&original, &Refusal(None), &late)
                    .unwrap_err();
            let ConnectorWriteRecipeCompileError::Provider(source) = &error else {
                panic!("late callback replaced provider refusal");
            };
            assert_eq!(source, &invalid("private refusal"));
            assert_eq!(
                error.source().unwrap().downcast_ref::<ConnectorError>(),
                Some(source)
            );
            assert_eq!(*late.units.lock().unwrap(), [0, 0]);
            for at in 1..=2 {
                let early = Trace {
                    units: Mutex::default(),
                    refusal: Some((at, cause.clone())),
                };
                assert!(
                    matches!(ConnectorWriteRecipe::try_compile_with_provider(&original, &Refusal(None), &early), Err(ConnectorWriteRecipeCompileError::Control(actual)) if actual == cause)
                );
                assert_eq!(*early.units.lock().unwrap(), vec![0; at]);
            }
            let primary = Trace {
                units: Mutex::default(),
                refusal: Some((3, cause.clone())),
            };
            let error = ConnectorWriteRecipe::try_compile_with_provider(
                &original,
                &Refusal(Some(cause.clone())),
                &primary,
            )
            .unwrap_err();
            assert!(
                matches!(&error, ConnectorWriteRecipeCompileError::Control(actual) if actual == &cause)
            );
            assert_eq!(
                error
                    .source()
                    .unwrap()
                    .downcast_ref::<CompileControlError>(),
                Some(&cause)
            );
            assert_eq!(*primary.units.lock().unwrap(), [0, 0]);
            let tail = Trace {
                units: Mutex::default(),
                refusal: Some((successful_trace.len(), cause.clone())),
            };
            assert!(
                matches!(ConnectorWriteRecipe::try_compile_with_provider(&original, &Compiler(0), &tail), Err(ConnectorWriteRecipeCompileError::Control(actual)) if actual == cause)
            );
            assert_eq!(*tail.units.lock().unwrap(), successful_trace);
        }
    }
}

#[cfg(test)]
#[path = "write_recipe/owned_tests.rs"]
mod owned_tests;

#[cfg(test)]
#[path = "write_recipe/borrowed_tests.rs"]
mod borrowed_tests;
