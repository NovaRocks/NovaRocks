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

//! Exact borrowed source custody, independent of execution value semantics.
use crate::{ConstantCreationFailure, ConstantError};
use arrow_schema::{Field, FieldRef};
use novarocks_type_contract::owned_resources::{
    layout::arc_layout,
    metadata_materialization::{MaterializedFieldNamespace, MetadataFieldLoan},
};
use novarocks_type_contract::{
    CompileCheckpoints, CompileControlError, ValueTypeStorageLifetime, ValueTypeVisit,
};
use std::{alloc::Layout, sync::Arc};

/// New receipt backing only. Original Field/metadata loans are separate source
/// owners and are not charged again by this constructor.
#[derive(Clone, Copy, Debug, Default)]
pub struct ConstantSourceCreationFacts {
    pub temporary_request_bytes: usize,
    pub retained_request_bytes: usize,
    pub allocation_requests: usize,
}
enum SourceCreation<'a, H> {
    Single(&'a mut dyn novarocks_type_contract::ValueSchemaCreationScope<H>),
    Paired {
        temporary: &'a mut dyn novarocks_type_contract::ValueSchemaCreationScope<H>,
        retained: &'a mut dyn novarocks_type_contract::ValueSchemaCreationScope<H>,
        admit: &'a mut dyn FnMut(
            &ConstantSourceCreationFacts,
        ) -> Result<(), ConstantCreationFailure<H>>,
        facts: ConstantSourceCreationFacts,
    },
}
impl<H> SourceCreation<'_, H> {
    fn storage(&self) -> ValueTypeStorageLifetime {
        match self {
            Self::Single(port) => port.storage_lifetime(),
            Self::Paired { retained, .. } => retained.storage_lifetime(),
        }
    }
    fn request(
        &mut self,
        layout: Layout,
        final_backing: bool,
    ) -> Result<(), ConstantCreationFailure<H>> {
        let port: &mut dyn novarocks_type_contract::ValueSchemaCreationScope<H> = match self {
            Self::Single(port) => &mut **port,
            Self::Paired {
                temporary,
                retained,
                admit,
                facts,
            } => {
                let bytes = if final_backing {
                    &mut facts.retained_request_bytes
                } else {
                    &mut facts.temporary_request_bytes
                };
                *bytes = bytes
                    .checked_add(layout.size())
                    .ok_or(CompileControlError::ResourceExhausted)?;
                facts.allocation_requests = facts
                    .allocation_requests
                    .checked_add(1)
                    .ok_or(CompileControlError::ResourceExhausted)?;
                admit(facts)?;
                if final_backing {
                    &mut **retained
                } else {
                    &mut **temporary
                }
            }
        };
        port.request_layout(layout)
            .map_err(ConstantCreationFailure::Creator)
    }
}
/// The original Field tree and only its reachable positive metadata receipts.
/// The strong root retains actual child owners; Weak receipt addresses never
/// substitute for that payload custody. The invoice is supplied by its author.
#[derive(Clone, Debug)]
pub struct ConstantSourceOwner {
    field: FieldRef,
    metadata: MaterializedFieldNamespace,
    original_source_bytes: usize,
    storage: ValueTypeStorageLifetime,
}
impl ConstantSourceOwner {
    pub fn field_ref(&self) -> &FieldRef {
        &self.field
    }
    pub fn metadata_namespace(&self) -> &MaterializedFieldNamespace {
        &self.metadata
    }
    pub fn original_source_bytes(&self) -> usize {
        self.original_source_bytes
    }
    pub fn storage_lifetime(&self) -> ValueTypeStorageLifetime {
        self.storage.clone()
    }

    /// Retain the same source occurrences, never equal replacement Fields or a
    /// complete decoded table. New receipt-vector storage is observed before
    /// construction; the original source responsibility remains independent.
    pub fn try_new_with_creator_in<H>(
        field: &FieldRef,
        metadata: &MaterializedFieldNamespace,
        original_source_bytes: usize,
        source_storage: ValueTypeStorageLifetime,
        creation: &mut dyn novarocks_type_contract::ValueSchemaCreationScope<H>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<Self, ConstantCreationFailure<H>> {
        Self::try_new_core(
            field,
            metadata,
            original_source_bytes,
            source_storage,
            &mut SourceCreation::Single(creation),
            work,
        )
    }
    pub fn try_new_with_paired_creator_in<H>(
        field: &FieldRef,
        metadata: &MaterializedFieldNamespace,
        original_source_bytes: usize,
        source_storage: ValueTypeStorageLifetime,
        temporary: &mut dyn novarocks_type_contract::ValueSchemaCreationScope<H>,
        retained: &mut dyn novarocks_type_contract::ValueSchemaCreationScope<H>,
        admit: &mut dyn FnMut(
            &ConstantSourceCreationFacts,
        ) -> Result<(), ConstantCreationFailure<H>>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<Self, ConstantCreationFailure<H>> {
        Self::try_new_core(
            field,
            metadata,
            original_source_bytes,
            source_storage,
            &mut SourceCreation::Paired {
                temporary,
                retained,
                admit,
                facts: ConstantSourceCreationFacts::default(),
            },
            work,
        )
    }
    /// The original receipt grammar precedes the actual scope constructors.
    /// Every new scope/header contribution is observed before allocation.
    pub fn try_new_with_factory_in<H, F: novarocks_type_contract::ValueSchemaCreationFactory<H>>(
        field: &FieldRef,
        metadata: &MaterializedFieldNamespace,
        original_source_bytes: usize,
        source_storage: ValueTypeStorageLifetime,
        factory: &F,
        admit: &mut dyn FnMut(
            &ConstantSourceCreationFacts,
        ) -> Result<(), ConstantCreationFailure<H>>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<(Self, ConstantSourceCreationFacts), ConstantCreationFailure<H>> {
        use novarocks_type_contract::ValueSchemaScopeBeginFailure as Begin;
        let (count, payload) = Self::checked_receipts::<H>(field, metadata, work)?;
        let mut scopes = ConstantSourceCreationFacts::default();
        let (mut temporary, mut retained) = {
            let mut begin = |retained: bool| {
                work.flush()?;
                factory
                    .begin_scope_observed(&mut |layout| {
                        let bytes = if retained {
                            &mut scopes.retained_request_bytes
                        } else {
                            &mut scopes.temporary_request_bytes
                        };
                        *bytes = bytes
                            .checked_add(layout.size())
                            .ok_or(CompileControlError::ResourceExhausted)?;
                        scopes.allocation_requests = scopes
                            .allocation_requests
                            .checked_add(1)
                            .ok_or(CompileControlError::ResourceExhausted)?;
                        admit(&scopes)
                    })
                    .map_err(|error| match error {
                        Begin::Observer(error) => error,
                        Begin::Creator(error) => ConstantCreationFailure::Creator(error),
                    })
            };
            (begin(false)?, begin(true)?)
        };
        let scope_prefix = scopes;
        let mut completed = scope_prefix;
        let value = {
            let mut observe = |facts: &ConstantSourceCreationFacts| {
                completed = ConstantSourceCreationFacts {
                    temporary_request_bytes: scope_prefix
                        .temporary_request_bytes
                        .checked_add(facts.temporary_request_bytes)
                        .ok_or(CompileControlError::ResourceExhausted)?,
                    retained_request_bytes: scope_prefix
                        .retained_request_bytes
                        .checked_add(facts.retained_request_bytes)
                        .ok_or(CompileControlError::ResourceExhausted)?,
                    allocation_requests: scope_prefix
                        .allocation_requests
                        .checked_add(facts.allocation_requests)
                        .ok_or(CompileControlError::ResourceExhausted)?,
                };
                admit(&completed)
            };
            Self::materialize_receipts(
                field,
                metadata,
                original_source_bytes,
                source_storage,
                count,
                payload,
                &mut SourceCreation::Paired {
                    temporary: &mut temporary,
                    retained: &mut retained,
                    admit: &mut observe,
                    facts: ConstantSourceCreationFacts::default(),
                },
                work,
            )?
        };
        Ok((value, completed))
    }
    fn try_new_core<H>(
        field: &FieldRef,
        metadata: &MaterializedFieldNamespace,
        original_source_bytes: usize,
        source_storage: ValueTypeStorageLifetime,
        creation: &mut SourceCreation<'_, H>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<Self, ConstantCreationFailure<H>> {
        let (count, payload) = Self::checked_receipts(field, metadata, work)?;
        Self::materialize_receipts(
            field,
            metadata,
            original_source_bytes,
            source_storage,
            count,
            payload,
            creation,
            work,
        )
    }
    /// Publish only the reachable original receipts for a borrowed set of
    /// schema roots. Each supplied backing loan is authored by that exact
    /// Field occurrence; the result retains neither the schema nor its table.
    /// The original one-root constructor uses these same receipt bodies.
    pub fn receipt_namespace_with_factory_in<'a, H, F, I>(
        roots: I,
        metadata: &MaterializedFieldNamespace,
        factory: &F,
        admit: &mut dyn FnMut(
            &ConstantSourceCreationFacts,
        ) -> Result<(), ConstantCreationFailure<H>>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<(MaterializedFieldNamespace, ConstantSourceCreationFacts), ConstantCreationFailure<H>>
    where
        F: novarocks_type_contract::ValueSchemaCreationFactory<H>,
        I: Iterator<Item = (&'a Field, &'a ValueTypeStorageLifetime)> + Clone,
    {
        use novarocks_type_contract::ValueSchemaScopeBeginFailure as Begin;
        let (count, payload) =
            Self::checked_root_receipts(roots.clone().map(|(field, _)| field), metadata, work)?;
        let mut scopes = ConstantSourceCreationFacts::default();
        let (mut temporary, mut retained) = {
            let mut begin = |retained: bool| {
                work.flush()?;
                factory
                    .begin_scope_observed(&mut |layout| {
                        let bytes = if retained {
                            &mut scopes.retained_request_bytes
                        } else {
                            &mut scopes.temporary_request_bytes
                        };
                        *bytes = bytes
                            .checked_add(layout.size())
                            .ok_or(CompileControlError::ResourceExhausted)?;
                        scopes.allocation_requests = scopes
                            .allocation_requests
                            .checked_add(1)
                            .ok_or(CompileControlError::ResourceExhausted)?;
                        admit(&scopes)
                    })
                    .map_err(|error| match error {
                        Begin::Observer(error) => error,
                        Begin::Creator(error) => ConstantCreationFailure::Creator(error),
                    })
            };
            (begin(false)?, begin(true)?)
        };
        let prefix = scopes;
        let mut completed = prefix;
        let namespace = {
            let mut observe = |facts: &ConstantSourceCreationFacts| {
                completed = ConstantSourceCreationFacts {
                    temporary_request_bytes: prefix
                        .temporary_request_bytes
                        .checked_add(facts.temporary_request_bytes)
                        .ok_or(CompileControlError::ResourceExhausted)?,
                    retained_request_bytes: prefix
                        .retained_request_bytes
                        .checked_add(facts.retained_request_bytes)
                        .ok_or(CompileControlError::ResourceExhausted)?,
                    allocation_requests: prefix
                        .allocation_requests
                        .checked_add(facts.allocation_requests)
                        .ok_or(CompileControlError::ResourceExhausted)?,
                };
                admit(&completed)
            };
            let mut creation = SourceCreation::Paired {
                temporary: &mut temporary,
                retained: &mut retained,
                admit: &mut observe,
                facts: ConstantSourceCreationFacts::default(),
            };
            let namespace = Self::materialize_root_receipts(
                roots.clone().map(|(field, _)| field),
                metadata,
                count,
                payload,
                &mut creation,
                work,
            )?;
            let storage = creation.storage().retaining_flat_observed(
                roots.map(|(_, storage)| Ok(storage)),
                |layout| creation.request(layout, true),
                work,
            )?;
            namespace.with_storage_lifetime(storage)
        };
        Ok((namespace, completed))
    }
    fn checked_receipts<H>(
        field: &FieldRef,
        metadata: &MaterializedFieldNamespace,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<(usize, Layout), ConstantCreationFailure<H>> {
        Self::checked_root_receipts(std::iter::once(field.as_ref()), metadata, work)
    }
    fn checked_root_receipts<'a, H>(
        roots: impl Iterator<Item = &'a Field>,
        metadata: &MaterializedFieldNamespace,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<(usize, Layout), ConstantCreationFailure<H>> {
        let mut previous = None;
        for loan in metadata.fields() {
            work.step()?;
            if previous.is_some_and(|address| address > loan.original_address()) {
                return Err(ConstantError::Invalid(
                    "constant source metadata namespace is not ordered",
                )
                .into());
            }
            previous = Some(loan.original_address());
        }
        let mut count = 0usize;
        for field in roots {
            source_fields_in::<ConstantCreationFailure<H>>(field, work, &mut |actual, work| {
                if let Some(loan) =
                    original_loan::<ConstantCreationFailure<H>>(metadata, actual, work)?
                {
                    let source = loan.borrow_original_field().ok_or_else(|| {
                        ConstantError::Invalid("constant source metadata payload has expired")
                    })?;
                    loan.original_backing_observed::<ConstantCreationFailure<H>>(
                        &source,
                        &mut || work.step().map_err(Into::into),
                    )
                    .expect("exact original source occurrence")?;
                    count = count
                        .checked_add(1)
                        .ok_or(CompileControlError::ResourceExhausted)?;
                } else if actual.metadata().as_arc().is_some() {
                    return Err(novarocks_type_contract::owned_resources::metadata_materialization::MetadataMaterializationError::MissingOriginalFieldOrigin.into());
                }
                Ok(())
            })?;
        }
        let payload = Layout::array::<MetadataFieldLoan>(count)
            .map_err(|_| CompileControlError::ResourceExhausted)?;
        Ok((count, payload))
    }
    fn materialize_receipts<H>(
        field: &FieldRef,
        metadata: &MaterializedFieldNamespace,
        original_source_bytes: usize,
        source_storage: ValueTypeStorageLifetime,
        count: usize,
        payload: Layout,
        creation: &mut SourceCreation<'_, H>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<Self, ConstantCreationFailure<H>> {
        let metadata = Self::materialize_root_receipts(
            std::iter::once(field.as_ref()),
            metadata,
            count,
            payload,
            creation,
            work,
        )?;
        let storage = creation
            .storage()
            .retaining_observed(&source_storage, |layout| {
                work.flush()?;
                creation.request(layout, true)
            })?;
        Ok(Self {
            field: field.clone(),
            metadata,
            original_source_bytes,
            storage,
        })
    }
    fn materialize_root_receipts<'a, H>(
        roots: impl Iterator<Item = &'a Field>,
        metadata: &MaterializedFieldNamespace,
        count: usize,
        payload: Layout,
        creation: &mut SourceCreation<'_, H>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<MaterializedFieldNamespace, ConstantCreationFailure<H>> {
        if count != 0 {
            work.flush()?;
            creation.request(payload, false)?;
        }
        work.flush()?;
        let mut loans = Vec::new();
        loans
            .try_reserve_exact(count)
            .map_err(|_| CompileControlError::ResourceExhausted)?;
        work.step()?;
        for field in roots {
            source_fields_in::<ConstantCreationFailure<H>>(field, work, &mut |actual, work| {
                if let Some(loan) =
                    original_loan::<ConstantCreationFailure<H>>(metadata, actual, work)?
                {
                    loans.push(loan.clone());
                }
                Ok(())
            })?;
        }
        work.flush()?;
        creation.request(arc_layout(payload)?, true)?;
        // The unique publication sorts the same receipt addresses in place.
        // std's in-place sort is opaque, with control before and after. Its
        // finite source size is already checked by the borrowed grammar.
        work.flush()?;
        let metadata = MaterializedFieldNamespace::from_original_loans(Arc::from(loans))
            .with_storage_lifetime(creation.storage());
        work.flush()?;
        Ok(metadata)
    }
}

fn original_loan<'a, E: From<CompileControlError>>(
    metadata: &'a MaterializedFieldNamespace,
    field: &Field,
    work: &mut CompileCheckpoints<'_>,
) -> Result<Option<&'a MetadataFieldLoan>, E> {
    let address = field as *const Field as usize;
    let mut low = 0usize;
    let mut high = metadata.fields().len();
    while low < high {
        let mid = low + (high - low) / 2;
        let loan = &metadata.fields()[mid];
        let ordering = loan.original_address().cmp(&address);
        work.step()?;
        match ordering {
            std::cmp::Ordering::Less => low = mid + 1,
            std::cmp::Ordering::Greater => high = mid,
            std::cmp::Ordering::Equal => return Ok(Some(loan)),
        }
    }
    Ok(None)
}

fn source_fields_in<
    E: From<ConstantError> + From<CompileControlError> + From<novarocks_type_contract::ValueTypeError>,
>(
    field: &Field,
    work: &mut CompileCheckpoints<'_>,
    visit: &mut impl FnMut(&Field, &mut CompileCheckpoints<'_>) -> Result<(), E>,
) -> Result<(), E> {
    // Original borrowed grammar uses fixed stack scratch. Initialization is
    // opaque, with control on both sides; it reports no completed visits.
    work.flush()?;
    let mut scratch = [None; novarocks_type_contract::MAX_VALUE_TYPE_NODES];
    work.flush()?;
    visit(field, work)?;
    novarocks_type_contract::validate_arrow_layout_structure_with_scratch_observed::<E>(
        field.data_type(),
        &mut scratch,
        |event| {
            work.step()?;
            if let ValueTypeVisit::Field(child) = event {
                visit(child, work)?;
            }
            Ok(())
        },
    )
}

/// An explicit pure source branch has no invoice or allocator provenance.
/// Every reachable Metadata must have no Arc backing. Arrow 60 canonicalizes
/// removed-entry and cleared maps to None; an empty map retains no allocation.
/// No format annotation is interpreted as a semantic author here.
pub fn validate_pure_borrowed_source_in(
    field: &Field,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), ConstantError> {
    source_fields_in(field, work, &mut |actual, _| {
        if actual.metadata().as_arc().is_some() {
            Err(ConstantError::Invalid(
                "pure constant source metadata requires original creation provenance",
            ))
        } else {
            Ok(())
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ConstantPolicy, ConstantPool};
    use arrow_array::{Array, StringArray};
    use arrow_schema::DataType;
    use novarocks_type_contract::owned_resources::metadata_materialization::{
        MaterializedMetadataMap, MetadataMaterializationError,
    };
    use novarocks_type_contract::{
        CompilePhase, FunctionValueType, PureCompileControl, ValueSchemaCreation,
        ValueTypeStorageHold,
    };
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    struct Control(AtomicBool);
    impl PureCompileControl for Control {
        fn checkpoint(&self, _: CompilePhase, _: u32) -> Result<(), CompileControlError> {
            assert!(!self.0.load(Ordering::SeqCst), "control after host refusal");
            Ok(())
        }
    }
    struct DropHold(Arc<AtomicUsize>);
    impl ValueTypeStorageHold for DropHold {}
    impl Drop for DropHold {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }
    fn held(counter: &Arc<AtomicUsize>) -> ValueTypeStorageLifetime {
        ValueTypeStorageLifetime::held(Arc::new(DropHold(counter.clone())))
    }
    fn source(name: &str) -> (FieldRef, MetadataFieldLoan) {
        let mut map = MaterializedMetadataMap::with_capacity(1);
        map.insert("source-format".into(), "original attributes".into());
        let shared = map
            .into_field(Field::new(name, DataType::Utf8, false))
            .into_shared();
        let loan = shared.loan();
        (shared.into_original_field_ref(), loan)
    }
    fn policy() -> ConstantPolicy {
        ConstantPolicy {
            max_rows: 4096,
            max_array_nodes: 4096,
            max_logical_elements: 65536,
            max_retained_buffer_bytes: 8 * 1024 * 1024,
            max_type_depth: 64,
            max_type_nodes: 4096,
            max_dictionary_depth: 8,
            max_metadata_bytes: 65536,
            max_library_validation_work: 32 * 1024 * 1024,
            max_library_validation_bytes: 32 * 1024 * 1024,
        }
    }

    #[test]
    fn source_custody_keeps_only_reachable_original_fields_and_receipts() {
        let (child, child_loan) = source("child");
        let child_weak = Arc::downgrade(&child);
        let root = Arc::new(Field::new(
            "root",
            DataType::Struct(vec![child.clone()].into()),
            false,
        ));
        let (unrelated, unrelated_loan) = source("unrelated");
        let unrelated_weak = Arc::downgrade(&unrelated);
        let namespace = MaterializedFieldNamespace::from_original_loans(
            vec![child_loan, unrelated_loan].into(),
        );
        let control = Control(AtomicBool::new(false));
        let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Decode).unwrap();
        let mut request = |_: Layout| Ok::<(), ()>(());
        let mut opaque = |_: usize| Ok::<(), ()>(());
        let mut creation =
            ValueSchemaCreation::new(ValueTypeStorageLifetime::pure(), &mut request, &mut opaque);
        let custody = ConstantSourceOwner::try_new_with_creator_in(
            &root,
            &namespace,
            8192,
            ValueTypeStorageLifetime::pure(),
            &mut creation,
            &mut work,
        )
        .unwrap();
        work.finish().unwrap();
        assert!(Arc::ptr_eq(custody.field_ref(), &root));
        assert_eq!(custody.original_source_bytes(), 8192);
        assert_eq!(custody.metadata_namespace().fields().len(), 1);
        drop((root, child, unrelated, namespace));
        assert!(unrelated_weak.upgrade().is_none());
        let child = child_weak.upgrade().unwrap();
        let loan = &custody.metadata_namespace().fields()[0];
        assert!(loan.lends(&child));
        assert!(
            loan.original_backing_observed::<MetadataMaterializationError>(&child, &mut || Ok(()))
                .unwrap()
                .unwrap()
                > 0
        );
        drop(child);
        drop(custody);
        assert!(child_weak.upgrade().is_none());
    }

    #[test]
    fn constant_value_and_array_projection_keep_source_and_creation_until_last_exit() {
        let (field, loan) = source("literal");
        let field_weak = Arc::downgrade(&field);
        let namespace = MaterializedFieldNamespace::from_original_loans(vec![loan].into());
        let original_drops = Arc::new(AtomicUsize::new(0));
        let creation_drops = Arc::new(AtomicUsize::new(0));
        let original_storage = held(&original_drops);
        let creation_storage = held(&creation_drops);
        let mut requests = Vec::new();
        let mut request = |layout: Layout| {
            requests.push(layout);
            Ok::<(), ()>(())
        };
        let mut opaque = |_: usize| Ok::<(), ()>(());
        let mut creation =
            ValueSchemaCreation::new(creation_storage.clone(), &mut request, &mut opaque);
        let control = Control(AtomicBool::new(false));
        let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Decode).unwrap();
        let custody = ConstantSourceOwner::try_new_with_creator_in(
            &field,
            &namespace,
            8192,
            original_storage.clone(),
            &mut creation,
            &mut work,
        )
        .unwrap();
        let data = StringArray::from(vec!["active"]).to_data();
        let payload_pointer = data.buffers()[1].as_ptr();
        let pool = ConstantPool::try_new_with_creator_in(
            field.clone(),
            FunctionValueType::new(DataType::Utf8, false),
            data,
            policy(),
            custody,
            &mut creation,
            &mut |_| Ok(()),
            &mut work,
        )
        .unwrap();
        work.finish().unwrap();
        drop(creation);
        assert!(!requests.is_empty());
        let value = pool.value(0).unwrap();
        let projection = pool.array_projection();
        let backing_weak = Arc::downgrade(&pool.0);
        drop((field, namespace, original_storage, creation_storage, pool));
        assert_eq!(original_drops.load(Ordering::SeqCst), 0);
        assert_eq!(creation_drops.load(Ordering::SeqCst), 0);
        drop(value);
        assert!(
            backing_weak.upgrade().is_none(),
            "projection retained whole pool"
        );
        assert!(field_weak.upgrade().is_some());
        let array = projection
            .array()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(array.value(0), "active");
        assert_eq!(array.value_data().as_ptr(), payload_pointer);
        assert_eq!(original_drops.load(Ordering::SeqCst), 0);
        assert_eq!(creation_drops.load(Ordering::SeqCst), 0);
        drop(projection);
        assert!(field_weak.upgrade().is_none());
        assert_eq!(original_drops.load(Ordering::SeqCst), 1);
        assert_eq!(creation_drops.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn source_creator_preserves_each_original_host_refusal_without_tail_callbacks() {
        let (field, loan) = source("literal");
        let namespace = MaterializedFieldNamespace::from_original_loans(vec![loan].into());
        for refuse_at in 0..2 {
            let failure: Arc<str> = Arc::from("original host refusal");
            let control = Control(AtomicBool::new(false));
            let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Decode).unwrap();
            let mut calls = 0;
            let mut request = |_: Layout| {
                assert!(!control.0.load(Ordering::SeqCst));
                let current = calls;
                calls += 1;
                if current == refuse_at {
                    control.0.store(true, Ordering::SeqCst);
                    Err(failure.clone())
                } else {
                    Ok(())
                }
            };
            let mut opaque = |_: usize| -> Result<(), Arc<str>> {
                panic!("source custody has no opaque heap operation")
            };
            let mut creation = ValueSchemaCreation::new(
                ValueTypeStorageLifetime::pure(),
                &mut request,
                &mut opaque,
            );
            let result = ConstantSourceOwner::try_new_with_creator_in(
                &field,
                &namespace,
                8192,
                ValueTypeStorageLifetime::pure(),
                &mut creation,
                &mut work,
            );
            let Err(ConstantCreationFailure::Creator(actual)) = result else {
                panic!("host refusal was reclassified");
            };
            assert!(Arc::ptr_eq(&actual, &failure));
            drop(creation);
            assert_eq!(calls, refuse_at + 1);
        }
    }

    #[test]
    fn missing_metadata_provenance_is_typed_and_empty_backing_is_explicitly_pure() {
        let mut metadata = std::collections::HashMap::with_capacity(4);
        metadata.insert("removed".into(), "entry".into());
        let field = Arc::new(Field::new("source", DataType::Utf8, false).with_metadata(metadata));
        assert!(!field.metadata().is_empty());
        assert!(field.metadata().as_arc().is_some());
        let namespace = MaterializedFieldNamespace::from_original_loans(Vec::new().into());
        let control = Control(AtomicBool::new(false));
        let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Decode).unwrap();
        let mut request = |_: Layout| -> Result<(), ()> { panic!("allocation before provenance") };
        let mut opaque = |_: usize| -> Result<(), ()> { panic!("opaque before provenance") };
        let mut creation =
            ValueSchemaCreation::new(ValueTypeStorageLifetime::pure(), &mut request, &mut opaque);
        assert!(matches!(
            ConstantSourceOwner::try_new_with_creator_in(
                &field,
                &namespace,
                8192,
                ValueTypeStorageLifetime::pure(),
                &mut creation,
                &mut work
            ),
            Err(ConstantCreationFailure::SourceMetadata(
                MetadataMaterializationError::MissingOriginalFieldOrigin
            ))
        ));
        assert!(validate_pure_borrowed_source_in(&field, &mut work).is_err());
        let pure = Field::new("pure", DataType::Utf8, false);
        validate_pure_borrowed_source_in(&pure, &mut work).unwrap();
        work.finish().unwrap();
    }

    #[test]
    fn emptied_metadata_has_no_backing_and_is_a_pure_borrowed_source() {
        let mut reserved = std::collections::HashMap::<String, String>::with_capacity(4);
        reserved.insert("removed".into(), "entry".into());
        reserved.clear();
        assert!(reserved.capacity() > 0);
        let converted = arrow_schema::Metadata::from(reserved);
        let mut removed = arrow_schema::Metadata::new().with("removed", "entry");
        removed.remove("removed");
        let mut retained = arrow_schema::Metadata::new().with("removed", "entry");
        retained.retain(|_, _| false);
        let mut cleared = arrow_schema::Metadata::new().with("removed", "entry");
        cleared.clear();
        let control = Control(AtomicBool::new(false));
        let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Decode).unwrap();
        for metadata in [converted, removed, retained, cleared] {
            assert!(metadata.is_empty());
            assert!(metadata.as_arc().is_none());
            let field = Field::new("pure", DataType::Utf8, false).with_metadata(metadata);
            validate_pure_borrowed_source_in(&field, &mut work).unwrap();
        }
        work.finish().unwrap();
    }
}
