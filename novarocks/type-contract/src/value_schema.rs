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

//! Ordered semantic occurrences and their exact physical value bindings.
//!
//! Schema operations share immutable fields and types. The caller supplies
//! the lifetime for any new top-level storage; these contracts acquire no
//! execution capacity and infer no semantics from Arrow annotations.

use std::{
    hash::{Hash, Hasher},
    sync::Arc,
};

use arrow_schema::{DataType, Field};

use crate::{
    ExecutionValueTypeError, ValueField, ValueKind, ValueLayout, ValueTypeRef,
    ValueTypeStorageLifetime,
};

#[derive(Debug)]
struct ValueSchemaStorage {
    fields: Box<[ValueField]>,
    expanded_nodes: usize,
    // Drop the fields before releasing the responsibility for this storage.
    _storage: ValueTypeStorageLifetime,
}

/// Immutable, ordered semantic fields. Duplicate occurrences are retained.
#[derive(Clone, Debug)]
pub struct ValueSchema(Arc<ValueSchemaStorage>);

impl ValueSchema {
    /// Actual request for this private holder; occurrence backing is separate.
    pub fn storage_allocation_layout()
    -> Result<std::alloc::Layout, crate::owned_resources::layout::LayoutResourceError> {
        crate::owned_resources::layout::arc_layout(std::alloc::Layout::new::<ValueSchemaStorage>())
    }

    /// Assemble already-validated value trees without adding a schema profile.
    ///
    /// The sum counts repeated occurrences and is checked for arithmetic
    /// overflow. Limits for a particular port or result owner are supplied to
    /// `try_new_with_limits`; the single-value tree limit is not a schema limit.
    /// The caller covers its field vector before constructing it.
    pub fn try_new(
        fields: Vec<ValueField>,
        storage: ValueTypeStorageLifetime,
    ) -> Result<Self, ExecutionValueTypeError> {
        Self::try_new_with_limits(fields, storage, usize::MAX, usize::MAX)
    }

    /// Apply the caller's explicit field and expanded-node profile.
    ///
    /// These numbers validate structure only and grant no allocation capacity.
    pub fn try_new_with_limits(
        fields: Vec<ValueField>,
        storage: ValueTypeStorageLifetime,
        max_fields: usize,
        max_expanded_nodes: usize,
    ) -> Result<Self, ExecutionValueTypeError> {
        if fields.len() > max_fields {
            return Err(ExecutionValueTypeError::Nodes);
        }
        let expanded_nodes = fields.iter().try_fold(0usize, |nodes, field| {
            nodes
                .checked_add(field.value_type().expanded_nodes())
                .filter(|nodes| *nodes <= max_expanded_nodes)
                .ok_or(ExecutionValueTypeError::Nodes)
        })?;
        Ok(Self(Arc::new(ValueSchemaStorage {
            fields: fields.into_boxed_slice(),
            expanded_nodes,
            _storage: storage,
        })))
    }

    pub fn fields(&self) -> &[ValueField] {
        &self.0.fields
    }

    pub fn field(&self, index: usize) -> Option<&ValueField> {
        self.fields().get(index)
    }

    pub fn expanded_nodes(&self) -> usize {
        self.0.expanded_nodes
    }

    pub fn shares_storage(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }

    /// Select exact occurrence indices, including repeats, in output order.
    /// The caller covers the new field vector before this operation allocates.
    pub fn project(
        &self,
        indices: &[usize],
        storage: ValueTypeStorageLifetime,
    ) -> Result<Self, ExecutionValueTypeError> {
        self.project_with_limits(indices, storage, usize::MAX, usize::MAX)
    }

    /// Select occurrences under an explicit caller profile.
    ///
    /// Indices and total nodes are checked before creating the field vector.
    /// The caller still owns its capacity and new top-level storage lifetime.
    pub fn project_with_limits(
        &self,
        indices: &[usize],
        storage: ValueTypeStorageLifetime,
        max_fields: usize,
        max_expanded_nodes: usize,
    ) -> Result<Self, ExecutionValueTypeError> {
        if indices.len() > max_fields {
            return Err(ExecutionValueTypeError::Nodes);
        }
        let expanded_nodes = indices.iter().try_fold(0usize, |nodes, &index| {
            let field = self.field(index).ok_or(ExecutionValueTypeError::Invalid(
                "value schema projection occurrence is out of range",
            ))?;
            nodes
                .checked_add(field.value_type().expanded_nodes())
                .filter(|nodes| *nodes <= max_expanded_nodes)
                .ok_or(ExecutionValueTypeError::Nodes)
        })?;
        let fields = indices
            .iter()
            .map(|&index| self.fields()[index].clone())
            .collect();
        Ok(Self(Arc::new(ValueSchemaStorage {
            fields,
            expanded_nodes,
            _storage: storage,
        })))
    }

    /// Rename one top-level binding; every semantic child remains unchanged.
    pub fn rename_field(
        &self,
        index: usize,
        name: impl Into<Box<str>>,
        storage: ValueTypeStorageLifetime,
    ) -> Result<Self, ExecutionValueTypeError> {
        let original = self.field(index).ok_or(ExecutionValueTypeError::Invalid(
            "value schema rename occurrence is out of range",
        ))?;
        let renamed = ValueField::new(name, original.value_type().clone(), storage.clone());
        let mut fields = self.fields().to_vec();
        fields[index] = renamed;
        Self::try_new(fields, storage)
    }

    /// Change a field's root NULL contract under a new creation lifetime.
    ///
    /// Nested fields, their nullability and their existing owners are shared.
    pub fn with_field_nullable(
        &self,
        index: usize,
        nullable: bool,
        storage: ValueTypeStorageLifetime,
    ) -> Result<Self, ExecutionValueTypeError> {
        let original = self.field(index).ok_or(ExecutionValueTypeError::Invalid(
            "value schema nullability occurrence is out of range",
        ))?;
        let value_type = original
            .value_type()
            .with_nullable(nullable, storage.clone())?;
        let replacement = ValueField::new(original.name(), value_type, storage.clone());
        let mut fields = self.fields().to_vec();
        fields[index] = replacement;
        Self::try_new(fields, storage)
    }
}

impl PartialEq for ValueSchema {
    fn eq(&self, other: &Self) -> bool {
        self.shares_storage(other) || self.fields() == other.fields()
    }
}

impl Eq for ValueSchema {}

impl Hash for ValueSchema {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.fields().hash(state);
    }
}

/// One bound semantic value and its exact, encoding-independent layout.
///
/// Equality and hashing include the physical layout. Logical signatures use
/// `same_semantics` or the borrowed `value_type`, without adopting that layout.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct TypedValueBinding {
    value_type: ValueTypeRef,
    layout: ValueLayout,
}

impl TypedValueBinding {
    /// Observe only the two original strong-handle clones. Shared children and
    /// backing are retained by their immutable owners, with no tree allocation.
    pub fn clone_observed<E>(&self, mut observe: impl FnMut() -> Result<(), E>) -> Result<Self, E> {
        observe()?;
        let value_type = self.value_type.clone();
        observe()?;
        let layout = self.layout.clone();
        Ok(Self { value_type, layout })
    }

    pub fn try_new(
        value_type: ValueTypeRef,
        layout: ValueLayout,
    ) -> Result<Self, ExecutionValueTypeError> {
        Self::try_new_observed(value_type, layout, |_| Ok(()))
    }

    /// Bind without an unobserved recursive comparison. The caller owns the
    /// already constructed nodes and reports every actual node/name byte read.
    pub fn try_new_observed<E: From<ExecutionValueTypeError>>(
        value_type: ValueTypeRef,
        layout: ValueLayout,
        mut observe: impl FnMut(usize) -> Result<(), E>,
    ) -> Result<Self, E> {
        check_layout_observed(&value_type, layout.data_type(), &mut observe)?;
        Ok(Self { value_type, layout })
    }

    /// Borrow a proof from this complete immutable validated binding. Child
    /// projections keep this borrow and create no holder, tree or NULL node.
    pub fn borrowed(&self) -> BorrowedTypedValueBinding<'_> {
        BorrowedTypedValueBinding {
            value_type: &self.value_type,
            data_type: self.layout.data_type(),
        }
    }

    pub fn value_type(&self) -> &ValueTypeRef {
        &self.value_type
    }

    pub fn layout(&self) -> &ValueLayout {
        &self.layout
    }

    /// Retain the exact child semantics and its shared physical backing.
    /// The creating owner admits the new projection holder before this call.
    pub fn child_at(&self, index: usize, storage: ValueTypeStorageLifetime) -> Option<Self> {
        self.child_at_with_construction::<std::convert::Infallible>(index, storage, None)
            .expect("pure typed child projection")
    }
    pub fn child_at_with_construction<E>(
        &self,
        index: usize,
        storage: ValueTypeStorageLifetime,
        observer: Option<&mut dyn FnMut(crate::ValueConstructionRequest) -> Result<(), E>>,
    ) -> Result<Option<Self>, crate::ValueConstructionFailure<E>> {
        let Some(value_type) = self.value_type.child_type_at(index) else {
            return Ok(None);
        };
        let Some(layout) = self
            .layout
            .child_projection_at_with_construction(index, storage, observer)?
        else {
            return Ok(None);
        };
        Ok(Some(Self {
            value_type: value_type.clone(),
            layout,
        }))
    }

    /// Retain a child through the actual new owner's request/control ports.
    /// No new semantic tree is built; the independently retained child keeps
    /// its original node responsibility and its exact physical backing hold.
    pub fn child_at_observed<E: From<crate::owned_resources::layout::LayoutResourceError>>(
        &self,
        index: usize,
        storage: ValueTypeStorageLifetime,
        request: impl FnMut(std::alloc::Layout) -> Result<(), E>,
        mut work: impl FnMut(usize) -> Result<(), E>,
    ) -> Result<Option<Self>, E> {
        work(1)?;
        let Some(value_type) = self.value_type.child_type_at(index) else {
            return Ok(None);
        };
        let Some(layout) = self
            .layout
            .child_projection_at_observed(index, storage, request, &mut work)?
        else {
            return Ok(None);
        };
        work(1)?;
        Ok(Some(Self {
            value_type: value_type.clone(),
            layout,
        }))
    }

    pub fn same_semantics(&self, other: &Self) -> bool {
        self.value_type == other.value_type
    }

    /// Observe logical signature comparison without adopting physical layout.
    pub fn same_semantics_observed<E: From<crate::ValueTypeError>>(
        &self,
        other: &Self,
        observe: impl FnMut() -> Result<(), E>,
    ) -> Result<bool, E> {
        crate::value_types_equal_observed(&self.value_type, &other.value_type, observe)
    }

    /// Cached immutable source work; identity and storage remain separate.
    pub fn comparison_work_upper_bound(&self) -> Result<usize, ExecutionValueTypeError> {
        crate::schema::comparison_work_add(
            self.value_type.comparison_work_upper_bound(),
            self.layout.comparison_work_upper_bound(),
        )
    }

    /// Preserve the existing unit-observer sequence through the sole weighted
    /// body; no immutable identity shortcut suppresses actual observation.
    pub fn exactly_equals_observed<E: From<crate::ValueTypeError>>(
        &self,
        other: &Self,
        mut observe: impl FnMut() -> Result<(), E>,
    ) -> Result<bool, E> {
        self.exactly_equals_weighted_observed(other, |_| observe())
    }
    pub fn exactly_equals_weighted_observed<E: From<crate::ValueTypeError>>(
        &self,
        other: &Self,
        mut observe: impl FnMut(crate::TypeComparisonStep) -> Result<(), E>,
    ) -> Result<bool, E> {
        if !crate::value_types_equal_weighted_observed(
            &self.value_type,
            &other.value_type,
            &mut observe,
        )? {
            return Ok(false);
        }
        // Both immutable layouts passed their canonical construction owner.
        // Reuse the exact borrowed body without reapplying a raw physical policy.
        crate::schema::arrow_validated_data_types_exact_weighted_observed(
            self.layout.data_type(),
            other.layout.data_type(),
            observe,
        )
    }

    pub fn nullable(&self) -> bool {
        self.value_type.nullable()
    }

    pub fn data_type(&self) -> &DataType {
        self.layout.data_type()
    }

    pub fn with_nullable(
        &self,
        nullable: bool,
        storage: ValueTypeStorageLifetime,
    ) -> Result<Self, ExecutionValueTypeError> {
        self.with_nullable_with_construction::<std::convert::Infallible>(nullable, storage, None)
            .map_err(crate::ValueConstructionFailure::into_pure)
    }
    pub fn with_nullable_with_construction<E>(
        &self,
        nullable: bool,
        storage: ValueTypeStorageLifetime,
        observer: Option<&mut dyn FnMut(crate::ValueConstructionRequest) -> Result<(), E>>,
    ) -> Result<Self, crate::ValueConstructionFailure<E>> {
        Self::try_new(
            self.value_type
                .with_nullable_with_construction(nullable, storage, observer)?,
            self.layout.clone(),
        )
        .map_err(Into::into)
    }

    /// Change only the root NULL contract while retaining the exact immutable
    /// physical layout. Validation cannot add a source failure after construction.
    pub fn with_nullable_observed<E: From<crate::owned_resources::layout::LayoutResourceError>>(
        &self,
        nullable: bool,
        storage: ValueTypeStorageLifetime,
        request: impl FnMut(std::alloc::Layout) -> Result<(), E>,
        mut work: impl FnMut(usize) -> Result<(), E>,
    ) -> Result<Self, E> {
        let value_type = self
            .value_type
            .with_nullable_observed(nullable, storage, request, &mut work)?;
        work(1)?;
        Ok(Self {
            value_type,
            layout: self.layout.clone(),
        })
    }
}

enum BindingCompareFailure<E> {
    Caller(E),
    Source(crate::ValueTypeError),
}
impl<E> From<crate::ValueTypeError> for BindingCompareFailure<E> {
    fn from(error: crate::ValueTypeError) -> Self {
        Self::Source(error)
    }
}
fn binding_names_equal<E>(
    a: &str,
    b: &str,
    observe: &mut impl FnMut(usize) -> Result<(), E>,
) -> Result<bool, E> {
    observe(1)?;
    if a.len() != b.len() {
        return Ok(false);
    }
    for (a, b) in a.as_bytes().chunks(1024).zip(b.as_bytes().chunks(1024)) {
        observe(a.len())?;
        if a != b {
            return Ok(false);
        }
    }
    Ok(true)
}
fn check_field_layout_observed<E: From<ExecutionValueTypeError>>(
    value_type: &ValueTypeRef,
    field: &Field,
    observe: &mut impl FnMut(usize) -> Result<(), E>,
) -> Result<(), E> {
    observe(1)?;
    if value_type.nullable() != field.is_nullable() {
        return Err(ExecutionValueTypeError::Invalid(
            "value semantics and layout disagree on child nullability",
        )
        .into());
    }
    check_layout_observed(value_type, field.data_type(), observe)
}
/// Check semantic/layout correspondence without allocating a typed holder.
/// The caller owns normalization, carrier grammar and creation admission; this
/// borrowed check is also used for unreferenced wire field definitions.
pub fn validate_value_layout_observed<E: From<ExecutionValueTypeError>>(
    value_type: &ValueTypeRef,
    data_type: &DataType,
    mut observe: impl FnMut(usize) -> Result<(), E>,
) -> Result<(), E> {
    check_layout_observed(value_type, data_type, &mut observe)
}

fn check_layout_observed<E: From<ExecutionValueTypeError>>(
    value_type: &ValueTypeRef,
    data_type: &DataType,
    observe: &mut impl FnMut(usize) -> Result<(), E>,
) -> Result<(), E> {
    observe(1)?;
    let mismatch = || {
        E::from(ExecutionValueTypeError::Invalid(
            "value semantics and physical layout are incompatible",
        ))
    };
    match (value_type.kind(), data_type) {
        (ValueKind::Scalar(expected), actual) => {
            let equal =
                crate::data_type_layout_matches_carrier_observed::<BindingCompareFailure<E>>(
                    expected,
                    actual,
                    |units| observe(units).map_err(BindingCompareFailure::Caller),
                )
                .map_err(|error| match error {
                    BindingCompareFailure::Caller(error) => error,
                    BindingCompareFailure::Source(error) => {
                        E::from(ExecutionValueTypeError::Logical(error))
                    }
                })?;
            if equal { Ok(()) } else { Err(mismatch()) }
        }
        (ValueKind::String, DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View) => Ok(()),
        (ValueKind::Binary, DataType::Binary | DataType::LargeBinary | DataType::BinaryView) => {
            Ok(())
        }
        (ValueKind::Nominal(logical), actual) => logical
            .validate_carrier(actual)
            .map_err(|e| E::from(ExecutionValueTypeError::Logical(e))),
        (
            ValueKind::List(item),
            DataType::List(field)
            | DataType::LargeList(field)
            | DataType::ListView(field)
            | DataType::LargeListView(field),
        ) => check_field_layout_observed(item, field, observe),
        (
            ValueKind::FixedSizeList { item, length },
            DataType::FixedSizeList(field, actual_length),
        ) if length == actual_length => check_field_layout_observed(item, field, observe),
        (ValueKind::Struct(fields), DataType::Struct(actual)) if fields.len() == actual.len() => {
            for (field, actual) in fields.iter().zip(actual) {
                if !binding_names_equal(field.name(), actual.name(), observe)? {
                    return Err(mismatch());
                }
                check_field_layout_observed(field.value_type(), actual, observe)?;
            }
            Ok(())
        }
        (ValueKind::Map { key, value }, DataType::Map(entries, _)) => {
            if entries.is_nullable() {
                return Err(mismatch());
            }
            let DataType::Struct(fields) = entries.data_type() else {
                return Err(mismatch());
            };
            if fields.len() != 2 {
                return Err(mismatch());
            }
            check_field_layout_observed(key, &fields[0], observe)?;
            check_field_layout_observed(value, &fields[1], observe)
        }
        (ValueKind::Union(fields), DataType::Union(actual, _)) if fields.len() == actual.len() => {
            for (field, (actual_id, actual)) in fields.iter().zip(actual.iter()) {
                observe(1)?;
                if field.type_id != actual_id
                    || !binding_names_equal(field.field.name(), actual.name(), observe)?
                {
                    return Err(mismatch());
                }
                check_field_layout_observed(field.field.value_type(), actual, observe)?;
            }
            Ok(())
        }
        _ => Err(mismatch()),
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::hash_map::DefaultHasher,
        sync::atomic::{AtomicUsize, Ordering},
    };

    use super::*;
    use crate::{MAX_VALUE_TYPE_NODES, ValueLogicalType, ValueTypeStorageHold};

    fn pure() -> ValueTypeStorageLifetime {
        ValueTypeStorageLifetime::pure()
    }

    fn semantic(kind: ValueKind, nullable: bool) -> ValueTypeRef {
        ValueTypeRef::try_new(kind, nullable, pure()).unwrap()
    }

    fn layout(data_type: &DataType) -> ValueLayout {
        ValueLayout::try_new(data_type, pure()).unwrap()
    }

    fn hash<T: Hash>(value: &T) -> u64 {
        let mut hash = DefaultHasher::new();
        value.hash(&mut hash);
        hash.finish()
    }

    struct Hold(Arc<AtomicUsize>);
    impl ValueTypeStorageHold for Hold {}
    impl Drop for Hold {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    fn held(exits: &Arc<AtomicUsize>) -> ValueTypeStorageLifetime {
        ValueTypeStorageLifetime::held(Arc::new(Hold(Arc::clone(exits))))
    }

    #[test]
    fn child_binding_and_fvt_keep_original_layout_responsibility_after_parent_exit() {
        let original_exits = Arc::new(AtomicUsize::new(0));
        let projection_exits = Arc::new(AtomicUsize::new(0));
        let original = held(&original_exits);
        let timezone: Arc<str> = Arc::from("UTC");
        let carrier = DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, Some(timezone));
        let child =
            ValueTypeRef::try_new(ValueKind::Scalar(carrier.clone()), false, original.clone())
                .unwrap();
        let parent_value = ValueTypeRef::try_new(
            ValueKind::Struct(vec![ValueField::new("time", child, original.clone())].into()),
            false,
            original.clone(),
        )
        .unwrap();
        let parent_layout = ValueLayout::try_from_normalized_arrow(
            DataType::Struct(vec![Arc::new(Field::new("time", carrier, false))].into()),
            original.clone(),
        )
        .unwrap();
        let parent = TypedValueBinding::try_new(parent_value, parent_layout).unwrap();
        let projected = parent.child_at(0, held(&projection_exits)).unwrap();
        let fvt = crate::FunctionValueType::try_from_typed_binding(projected.clone()).unwrap();
        drop(original);
        drop(parent);
        drop(projected);
        assert_eq!(original_exits.load(Ordering::SeqCst), 0);
        assert_eq!(projection_exits.load(Ordering::SeqCst), 0);
        assert_eq!(
            fvt.data_type(),
            &DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, Some(Arc::from("UTC")))
        );
        drop(fvt);
        assert_eq!(original_exits.load(Ordering::SeqCst), 1);
        assert_eq!(projection_exits.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn independent_equivalent_schemas_have_equal_content_and_hashes() {
        let schema = || {
            let item = semantic(ValueKind::Nominal(ValueLogicalType::Json), true);
            let value = semantic(ValueKind::List(item), false);
            ValueSchema::try_new(vec![ValueField::new("jsons", value, pure())], pure()).unwrap()
        };
        let first = schema();
        let second = schema();
        assert!(!first.shares_storage(&second));
        assert_eq!(first, second);
        assert_eq!(hash(&first), hash(&second));
    }

    #[test]
    fn projection_preserves_duplicate_occurrences_and_zero_column_shape() {
        let first = ValueField::new("same", semantic(ValueKind::String, false), pure());
        let second = ValueField::new("same", semantic(ValueKind::Binary, true), pure());
        let schema = ValueSchema::try_new(vec![first.clone(), second.clone()], pure()).unwrap();
        let projected = schema.project(&[1, 0, 1], pure()).unwrap();
        assert_eq!(projected.fields(), &[second.clone(), first, second]);
        assert!(
            projected
                .field(0)
                .unwrap()
                .value_type()
                .shares_storage(schema.field(1).unwrap().value_type())
        );
        assert_eq!(projected.expanded_nodes(), 3);
        let empty = projected.project(&[], pure()).unwrap();
        assert!(empty.fields().is_empty());
        assert_eq!(empty.expanded_nodes(), 0);
        assert_eq!(empty, ValueSchema::try_new(vec![], pure()).unwrap());
        assert!(schema.project(&[2], pure()).is_err());
    }

    #[test]
    fn repeated_occurrences_count_against_the_explicit_schema_profile() {
        let item = semantic(ValueKind::String, false);
        let list = semantic(ValueKind::List(item), false);
        let field = ValueField::new("v", list, pure());
        assert_eq!(field.value_type().expanded_nodes(), 2);
        // All individual trees are valid. Their aggregate is not limited by
        // the per-value limit; deployable owners have different schema profiles.
        let field_count = MAX_VALUE_TYPE_NODES + 1;
        let fields = vec![field; field_count];
        let schema = ValueSchema::try_new(fields.clone(), pure()).unwrap();
        assert_eq!(schema.expanded_nodes(), field_count * 2);
        assert_eq!(
            ValueSchema::try_new_with_limits(fields.clone(), pure(), field_count - 1, 65_536)
                .unwrap_err(),
            ExecutionValueTypeError::Nodes
        );
        assert_eq!(
            ValueSchema::try_new_with_limits(fields.clone(), pure(), field_count, 8_192)
                .unwrap_err(),
            ExecutionValueTypeError::Nodes
        );
        let wider = ValueSchema::try_new_with_limits(fields, pure(), field_count, 65_536).unwrap();
        assert_eq!(schema, wider);
        let occurrences = vec![0; field_count];
        assert_eq!(
            wider
                .project(&occurrences, pure())
                .unwrap()
                .expanded_nodes(),
            field_count * 2
        );
        assert!(
            wider
                .project_with_limits(&occurrences, pure(), field_count, 8_192)
                .is_err()
        );
        assert_eq!(
            wider
                .project_with_limits(&occurrences, pure(), field_count, 65_536)
                .unwrap()
                .expanded_nodes(),
            field_count * 2
        );
        assert!(
            wider
                .project_with_limits(&[], pure(), 0, 0)
                .unwrap()
                .fields()
                .is_empty()
        );
    }

    #[test]
    fn rename_and_root_null_extension_preserve_semantic_children() {
        let item = semantic(ValueKind::Nominal(ValueLogicalType::Json), true);
        let list = semantic(ValueKind::List(item.clone()), false);
        let schema = ValueSchema::try_new(
            vec![ValueField::new("before", list.clone(), pure())],
            pure(),
        )
        .unwrap();
        let renamed = schema.rename_field(0, "after", pure()).unwrap();
        assert_eq!(renamed.field(0).unwrap().name(), "after");
        assert_eq!(schema.field(0).unwrap().name(), "before");
        assert!(renamed.field(0).unwrap().value_type().shares_storage(&list));
        let extended = renamed.with_field_nullable(0, true, pure()).unwrap();
        assert!(extended.field(0).unwrap().value_type().nullable());
        let ValueKind::List(child) = extended.field(0).unwrap().value_type().kind() else {
            panic!("list expected");
        };
        assert!(child.shares_storage(&item));
        assert!(schema.rename_field(1, "absent", pure()).is_err());
        assert!(schema.with_field_nullable(1, true, pure()).is_err());
    }

    #[test]
    fn field_and_child_outlive_the_parent_with_their_creation_responsibility() {
        let type_exits = Arc::new(AtomicUsize::new(0));
        let field_exits = Arc::new(AtomicUsize::new(0));
        let schema_exits = Arc::new(AtomicUsize::new(0));
        let child = ValueTypeRef::try_new(ValueKind::String, true, held(&type_exits)).unwrap();
        let parent = ValueTypeRef::try_new(ValueKind::List(child), false, pure()).unwrap();
        let field = ValueField::new("retained_name", parent, held(&field_exits));
        let schema = ValueSchema::try_new(vec![field], held(&schema_exits)).unwrap();
        let field = schema.field(0).unwrap().clone();
        let ValueKind::List(child) = field.value_type().kind() else {
            panic!("list expected");
        };
        let child = child.clone();
        drop(schema);
        assert_eq!(schema_exits.load(Ordering::SeqCst), 1);
        assert_eq!(field_exits.load(Ordering::SeqCst), 0);
        assert_eq!(field.name(), "retained_name");
        drop(field);
        assert_eq!(field_exits.load(Ordering::SeqCst), 1);
        assert_eq!(type_exits.load(Ordering::SeqCst), 0);
        drop(child);
        assert_eq!(type_exits.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn projection_and_binding_retain_only_the_storage_they_reference() {
        let original_exits = Arc::new(AtomicUsize::new(0));
        let projection_exits = Arc::new(AtomicUsize::new(0));
        let layout_exits = Arc::new(AtomicUsize::new(0));
        let responsibility = held(&original_exits);
        let value =
            ValueTypeRef::try_new(ValueKind::String, false, responsibility.clone()).unwrap();
        let field = ValueField::new("s", value, responsibility.clone());
        let schema = ValueSchema::try_new(vec![field], responsibility).unwrap();
        let projection = schema.project(&[0, 0], held(&projection_exits)).unwrap();
        let binding = TypedValueBinding::try_new(
            schema.field(0).unwrap().value_type().clone(),
            ValueLayout::try_new(&DataType::Utf8, held(&layout_exits)).unwrap(),
        )
        .unwrap();
        drop(schema);
        assert_eq!(original_exits.load(Ordering::SeqCst), 0);
        drop(projection);
        assert_eq!(projection_exits.load(Ordering::SeqCst), 1);
        assert_eq!(original_exits.load(Ordering::SeqCst), 0);
        drop(binding);
        assert_eq!(original_exits.load(Ordering::SeqCst), 1);
        assert_eq!(layout_exits.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn binding_separates_logical_semantics_from_exact_layout_identity() {
        let value = semantic(ValueKind::String, true);
        let small = TypedValueBinding::try_new(value.clone(), layout(&DataType::Utf8)).unwrap();
        let large = TypedValueBinding::try_new(value, layout(&DataType::LargeUtf8)).unwrap();
        assert!(small.same_semantics(&large));
        assert_ne!(small, large);
        let equal =
            TypedValueBinding::try_new(semantic(ValueKind::String, true), layout(&DataType::Utf8))
                .unwrap();
        assert_eq!(small, equal);
        assert_eq!(hash(&small), hash(&equal));
        assert!(
            TypedValueBinding::try_new(
                semantic(ValueKind::String, false),
                layout(&DataType::Binary)
            )
            .is_err()
        );
    }

    #[test]
    fn binding_checks_recursive_nullability_names_and_nominal_carriers() {
        let json = semantic(ValueKind::Nominal(ValueLogicalType::Json), true);
        let structure = semantic(
            ValueKind::Struct(vec![ValueField::new("j", json, pure())].into()),
            false,
        );
        let valid = DataType::Struct(vec![Field::new("j", DataType::Utf8, true)].into());
        TypedValueBinding::try_new(structure.clone(), layout(&valid)).unwrap();
        for invalid in [
            DataType::Struct(vec![Field::new("renamed", DataType::Utf8, true)].into()),
            DataType::Struct(vec![Field::new("j", DataType::Utf8, false)].into()),
            DataType::Struct(vec![Field::new("j", DataType::Binary, true)].into()),
        ] {
            assert!(TypedValueBinding::try_new(structure.clone(), layout(&invalid)).is_err());
        }
        let uuid = semantic(ValueKind::Nominal(ValueLogicalType::Uuid), false);
        TypedValueBinding::try_new(uuid.clone(), layout(&DataType::FixedSizeBinary(16))).unwrap();
        assert!(TypedValueBinding::try_new(uuid, layout(&DataType::FixedSizeBinary(15))).is_err());
        let physical = TypedValueBinding::try_new(
            semantic(ValueKind::Scalar(DataType::FixedSizeBinary(16)), false),
            layout(&DataType::FixedSizeBinary(16)),
        )
        .unwrap();
        let nominal = TypedValueBinding::try_new(
            semantic(ValueKind::Nominal(ValueLogicalType::LargeInt), false),
            layout(&DataType::FixedSizeBinary(16)),
        )
        .unwrap();
        assert!(!physical.same_semantics(&nominal));
    }

    #[test]
    fn list_and_map_role_labels_do_not_author_new_semantics() {
        let item = semantic(ValueKind::String, true);
        let list = semantic(ValueKind::List(item), false);
        TypedValueBinding::try_new(
            list,
            layout(&DataType::LargeList(Arc::new(Field::new(
                "element",
                DataType::LargeUtf8,
                true,
            )))),
        )
        .unwrap();
        let map = semantic(
            ValueKind::Map {
                key: semantic(ValueKind::Scalar(DataType::Int32), false),
                value: semantic(ValueKind::Binary, true),
            },
            true,
        );
        let fields = vec![
            Field::new("k", DataType::Int32, false),
            Field::new("v", DataType::LargeBinary, true),
        ];
        TypedValueBinding::try_new(
            map,
            layout(&DataType::Map(
                Arc::new(Field::new("pairs", DataType::Struct(fields.into()), false)),
                false,
            )),
        )
        .unwrap();
    }

    #[test]
    fn root_nullability_changes_keep_layout_owner_and_shared_children() {
        let exits = Arc::new(AtomicUsize::new(0));
        let child = semantic(ValueKind::String, true);
        let binding = TypedValueBinding::try_new(
            semantic(ValueKind::List(child.clone()), false),
            layout(&DataType::List(Arc::new(Field::new(
                "item",
                DataType::Utf8,
                true,
            )))),
        )
        .unwrap();
        let nullable = binding.with_nullable(true, held(&exits)).unwrap();
        drop(binding);
        let ValueKind::List(retained) = nullable.value_type().kind() else {
            panic!("list expected");
        };
        let retained = retained.clone();
        assert!(retained.shares_storage(&child));
        assert_eq!(exits.load(Ordering::SeqCst), 0);
        drop(nullable);
        assert_eq!(exits.load(Ordering::SeqCst), 1);
        assert_eq!(retained, child);
    }

    #[derive(Debug, PartialEq, Eq)]
    enum ConstructionRefusal {
        Stop,
        Layout(crate::owned_resources::layout::LayoutResourceError),
    }
    impl From<crate::owned_resources::layout::LayoutResourceError> for ConstructionRefusal {
        fn from(error: crate::owned_resources::layout::LayoutResourceError) -> Self {
            Self::Layout(error)
        }
    }

    #[test]
    fn child_projection_observes_distinct_lifetime_and_holder_requests_before_allocation() {
        let source_exits = Arc::new(AtomicUsize::new(0));
        let new_exits = Arc::new(AtomicUsize::new(0));
        let source = TypedValueBinding::try_new(
            semantic(ValueKind::List(semantic(ValueKind::String, true)), false),
            ValueLayout::try_new(
                &DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
                held(&source_exits),
            )
            .unwrap(),
        )
        .unwrap();
        let mut requests = Vec::new();
        let child = source
            .child_at_observed(
                0,
                held(&new_exits),
                |layout| {
                    requests.push(layout);
                    Ok::<_, ConstructionRefusal>(())
                },
                |_| Ok(()),
            )
            .unwrap()
            .unwrap();
        assert_eq!(requests.len(), 2);
        assert_eq!(
            requests[1],
            ValueLayout::storage_allocation_layout().unwrap()
        );
        drop(source);
        assert_eq!(source_exits.load(Ordering::SeqCst), 0);
        assert_eq!(child.data_type(), &DataType::Utf8);
        drop(child);
        assert_eq!(source_exits.load(Ordering::SeqCst), 1);
        assert_eq!(new_exits.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn child_projection_refusal_drops_new_lifetime_without_later_requests() {
        let source_exits = Arc::new(AtomicUsize::new(0));
        let new_exits = Arc::new(AtomicUsize::new(0));
        let source = TypedValueBinding::try_new(
            semantic(ValueKind::List(semantic(ValueKind::String, true)), false),
            ValueLayout::try_new(
                &DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
                held(&source_exits),
            )
            .unwrap(),
        )
        .unwrap();
        let mut calls = 0;
        let result = source.child_at_observed(
            0,
            held(&new_exits),
            |_| {
                calls += 1;
                Err::<(), _>(ConstructionRefusal::Stop)
            },
            |_| Ok(()),
        );
        assert_eq!(result.unwrap_err(), ConstructionRefusal::Stop);
        assert_eq!(calls, 1);
        assert_eq!(new_exits.load(Ordering::SeqCst), 1);
        assert_eq!(source_exits.load(Ordering::SeqCst), 0);
        drop(source);
        assert_eq!(source_exits.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn observed_null_extension_copies_only_root_vector_and_retains_child_nodes() {
        let child_exits = Arc::new(AtomicUsize::new(0));
        let original_exits = Arc::new(AtomicUsize::new(0));
        let new_exits = Arc::new(AtomicUsize::new(0));
        let child = ValueTypeRef::try_new(ValueKind::String, true, held(&child_exits)).unwrap();
        let field = ValueField::new("child", child.clone(), pure());
        let source = ValueTypeRef::try_new(
            ValueKind::Struct(vec![field].into()),
            false,
            held(&original_exits),
        )
        .unwrap();
        let mut requests = Vec::new();
        let target = source
            .with_nullable_observed(
                true,
                held(&new_exits),
                |layout| {
                    requests.push(layout);
                    Ok::<_, ConstructionRefusal>(())
                },
                |_| Ok(()),
            )
            .unwrap();
        assert_eq!(
            requests,
            vec![
                std::alloc::Layout::array::<ValueField>(1).unwrap(),
                ValueTypeRef::storage_allocation_layout().unwrap()
            ]
        );
        assert!(target.child_type_at(0).unwrap().shares_storage(&child));
        drop(source);
        assert_eq!(original_exits.load(Ordering::SeqCst), 1);
        drop(child);
        assert_eq!(child_exits.load(Ordering::SeqCst), 0);
        drop(target);
        assert_eq!(child_exits.load(Ordering::SeqCst), 1);
        assert_eq!(new_exits.load(Ordering::SeqCst), 1);
    }
}

/// Borrowed complete value/layout proof, constructible only from an immutable
/// validated binding or one of its structural children. It grants no capacity.
#[derive(Clone, Copy)]
pub struct BorrowedTypedValueBinding<'a> {
    value_type: &'a ValueTypeRef,
    data_type: &'a arrow_schema::DataType,
}
impl<'a> BorrowedTypedValueBinding<'a> {
    pub fn value_type(self) -> &'a ValueTypeRef {
        self.value_type
    }
    pub fn data_type(self) -> &'a arrow_schema::DataType {
        self.data_type
    }
    pub fn child_at(self, index: usize) -> Option<Self> {
        Some(Self {
            value_type: self.value_type.child_type_at(index)?,
            data_type: crate::value_layout::normalized_child_type_at(self.data_type, index)?,
        })
    }
    pub fn exactly_equals_observed<E>(
        self,
        other: Self,
        mut work: impl FnMut() -> Result<(), E>,
    ) -> Result<bool, E> {
        if !crate::function::typed_value::compatible(
            self.value_type,
            other.value_type,
            crate::function::typed_value::Nullability::Exact,
            &mut work,
        )? {
            return Ok(false);
        }
        crate::schema::arrow_validated_data_types_exact_observed(
            self.data_type,
            other.data_type,
            work,
        )
    }
    pub fn same_physical_binding_ignoring_root_nullability_observed<E>(
        self,
        other: Self,
        mut work: impl FnMut() -> Result<(), E>,
    ) -> Result<bool, E> {
        if !crate::function::typed_value::compatible(
            self.value_type,
            other.value_type,
            crate::function::typed_value::Nullability::IgnoreRoot,
            &mut work,
        )? {
            return Ok(false);
        }
        crate::schema::arrow_validated_data_types_exact_observed(
            self.data_type,
            other.data_type,
            work,
        )
    }
}
