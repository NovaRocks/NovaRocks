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
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::fmt;

use crate::ValueTypeRef;
use arrow_schema::{DataType, Field};

const MAX_FUNCTION_IDENTITY_BYTES: usize = 1024;

// Each independently retained identity owns its text and only the lifetime of
// that storage. Neither catalog services nor runtime controls are retained.
struct FunctionIdentityText {
    text: Box<str>,
    // Drop text before releasing its creator responsibility.
    storage: crate::ValueTypeStorageLifetime,
}

macro_rules! stable_identity {
    ($name:ident, $kind:literal, $strict:literal) => {
        #[derive(Clone)]
        pub struct $name(std::sync::Arc<FunctionIdentityText>);

        impl $name {
            /// Actual Arc request for this module's private immutable backing.
            /// The text Box is a separate request of its exact byte length.
            pub fn storage_allocation_layout()
            -> Result<std::alloc::Layout, crate::owned_resources::layout::LayoutResourceError> {
                crate::owned_resources::layout::arc_layout(std::alloc::Layout::new::<
                    FunctionIdentityText,
                >())
            }

            /// Validate the original borrowed grammar without allocating.
            pub fn validate_str(value: &str) -> Result<(), FunctionIdentityError> {
                Self::validate_str_observed(value, || Ok(()))
            }

            /// Observe only the bytes the original identity grammar inspects.
            /// Function/overload identities retain their length-only grammar.
            pub fn validate_str_observed<E: From<FunctionIdentityError>>(
                value: &str,
                mut observe: impl FnMut() -> Result<(), E>,
            ) -> Result<(), E> {
                validate_identity($kind, value)?;
                if $strict {
                    for byte in value.bytes() {
                        let allowed = byte.is_ascii_graphic() && !matches!(byte, b'|' | b',');
                        observe()?;
                        if !allowed {
                            return Err(
                                FunctionIdentityError::InvalidCharacters { kind: $kind }.into()
                            );
                        }
                    }
                }
                Ok(())
            }

            /// Pure declaration author, using ordinary references and no grant.
            pub fn try_new(value: impl AsRef<str>) -> Result<Self, FunctionIdentityError> {
                let value = value.as_ref();
                Self::validate_str(value)?;
                Ok(Self::from_validated_boxed(
                    value.into(),
                    crate::ValueTypeStorageLifetime::pure(),
                ))
            }

            pub fn try_from_boxed(value: Box<str>) -> Result<Self, FunctionIdentityError> {
                Self::try_from_boxed_in(value, crate::ValueTypeStorageLifetime::pure())
            }

            /// Consume the actual author's text without copying it. Runtime
            /// callers admit the text and backing requests before this call.
            pub fn try_from_boxed_in(
                value: Box<str>,
                storage: crate::ValueTypeStorageLifetime,
            ) -> Result<Self, FunctionIdentityError> {
                Self::validate_str(&value)?;
                Ok(Self::from_validated_boxed(value, storage))
            }

            /// Create both actual text and private backing under the original
            /// creator. A callback refusal returns directly, with no footer.
            pub fn try_new_observed<E>(
                value: &str,
                storage: crate::ValueTypeStorageLifetime,
                mut request: impl FnMut(std::alloc::Layout) -> Result<(), E>,
                mut work: impl FnMut(usize) -> Result<(), E>,
            ) -> Result<Self, E>
            where
                E: From<FunctionIdentityError>
                    + From<crate::owned_resources::layout::LayoutResourceError>,
            {
                Self::validate_str_observed(value, || work(1))?;
                let text = std::alloc::Layout::array::<u8>(value.len())
                    .map_err(|_| crate::owned_resources::layout::LayoutResourceError::Arithmetic)?;
                request(text)?;
                // The locked str-to-Box operation copies the bounded original
                // byte span; it is not an exact allocator block-size fact.
                work(value.len())?;
                let value: Box<str> = value.into();
                request(Self::storage_allocation_layout()?)?;
                Ok(Self::from_validated_boxed(value, storage))
            }

            /// The text was already constructed by this creator. Validation
            /// and the sole actual Arc request do not allocate a second label.
            pub fn try_from_boxed_observed<E>(
                value: Box<str>,
                storage: crate::ValueTypeStorageLifetime,
                mut request: impl FnMut(std::alloc::Layout) -> Result<(), E>,
                mut work: impl FnMut(usize) -> Result<(), E>,
            ) -> Result<Self, E>
            where
                E: From<FunctionIdentityError>
                    + From<crate::owned_resources::layout::LayoutResourceError>,
            {
                Self::validate_str_observed(&value, || work(1))?;
                request(Self::storage_allocation_layout()?)?;
                Ok(Self::from_validated_boxed(value, storage))
            }

            fn from_validated_boxed(
                text: Box<str>,
                storage: crate::ValueTypeStorageLifetime,
            ) -> Self {
                Self(std::sync::Arc::new(FunctionIdentityText { text, storage }))
            }

            pub fn as_str(&self) -> &str {
                &self.0.text
            }

            /// Independent identity clones retain the same actual text owner.
            pub fn clone_observed<E>(
                &self,
                mut observe: impl FnMut() -> Result<(), E>,
            ) -> Result<Self, E> {
                observe()?;
                Ok(self.clone())
            }

            pub fn storage_lifetime(&self) -> crate::ValueTypeStorageLifetime {
                self.0.storage.clone()
            }
        }

        impl fmt::Debug for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.debug_tuple(stringify!($name))
                    .field(&self.as_str())
                    .finish()
            }
        }
        impl PartialEq for $name {
            fn eq(&self, other: &Self) -> bool {
                std::sync::Arc::ptr_eq(&self.0, &other.0) || self.as_str() == other.as_str()
            }
        }
        impl Eq for $name {}
        impl PartialOrd for $name {
            fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
                Some(self.cmp(other))
            }
        }
        impl Ord for $name {
            fn cmp(&self, other: &Self) -> std::cmp::Ordering {
                self.as_str().cmp(other.as_str())
            }
        }
        impl std::hash::Hash for $name {
            fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
                std::hash::Hash::hash(self.as_str(), state)
            }
        }
    };
}

stable_identity!(FunctionId, "function", false);
stable_identity!(FunctionOverloadId, "function overload", false);

// Stable identity of an aggregate's serialized intermediate state. Its
// delimiter exclusions preserve the original unambiguous digest grammar.
stable_identity!(AggregateStateFormatId, "aggregate state format", true);

/// Original overload owner's constraints on the logical argument types used
/// to interpret its intermediate state. This does not change any call's exact
/// signature, state/result type, ORDER BY channels, or runtime failure policy.
/// Installed binding validation must check this against the owner declaration.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum AggregateStateArgumentContract {
    /// Every complete argument type must be identical across state phases.
    ExactSignature,
    /// Only logical scalar arguments' root nullability may differ. Nested
    /// nullability, nominal domains, normalized layouts and ORDER BY remain exact.
    ValueRootNullabilityIndependent,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct FunctionValueType {
    binding: crate::TypedValueBinding,
}

#[path = "function/typed_value.rs"]
pub(crate) mod typed_value;

impl FunctionValueType {
    /// Declare a physical value from a metadata-free exact value layout.
    /// Nested annotations are discarded without interpretation. Authors with
    /// semantic children construct and pass the complete ValueTypeRef instead.
    pub fn new(data_type: DataType, nullable: bool) -> Self {
        Self::try_with_logical_type(data_type, nullable, crate::ValueLogicalType::Physical)
            .expect("valid declared physical function value")
    }

    pub fn try_from_typed_binding(
        binding: crate::TypedValueBinding,
    ) -> Result<Self, crate::ValueTypeError> {
        Ok(Self { binding })
    }

    pub fn try_from_value(
        value_type: crate::ValueTypeRef,
        layout: crate::ValueLayout,
    ) -> Result<Self, crate::ValueTypeError> {
        Self::try_from_typed_binding(crate::TypedValueBinding::try_new(value_type, layout)?)
    }

    /// Observe the two actual shared-handle clones; no tree or allocation work.
    pub fn clone_observed<E>(&self, observe: impl FnMut() -> Result<(), E>) -> Result<Self, E> {
        Ok(Self {
            binding: self.binding.clone_observed(observe)?,
        })
    }

    pub fn value_type(&self) -> &crate::ValueTypeRef {
        self.binding.value_type()
    }
    pub fn typed_binding(&self) -> &crate::TypedValueBinding {
        &self.binding
    }
    /// Pure catalog projection of one existing semantic child. Runtime authors
    /// use `child_at_in` after covering the new fixed projection holder.
    pub fn child_at(&self, index: usize) -> Option<Self> {
        self.child_at_in(index, crate::ValueTypeStorageLifetime::pure())
    }
    pub fn child_at_in(
        &self,
        index: usize,
        storage: crate::ValueTypeStorageLifetime,
    ) -> Option<Self> {
        self.binding
            .child_at(index, storage)
            .map(|binding| Self { binding })
    }
    pub fn data_type(&self) -> &DataType {
        self.binding.data_type()
    }
    pub fn nullable(&self) -> bool {
        self.binding.nullable()
    }
    pub fn logical_type(&self) -> crate::ValueLogicalType {
        match self.value_type().kind() {
            crate::ValueKind::Nominal(logical) => *logical,
            _ => crate::ValueLogicalType::Physical,
        }
    }

    pub fn with_nullable(&self, nullable: bool) -> Result<Self, crate::ValueTypeError> {
        self.with_nullable_in(nullable, crate::ValueTypeStorageLifetime::pure())
    }

    pub fn with_nullable_in(
        &self,
        nullable: bool,
        storage: crate::ValueTypeStorageLifetime,
    ) -> Result<Self, crate::ValueTypeError> {
        Self::try_from_typed_binding(self.binding.with_nullable(nullable, storage)?)
    }

    pub fn with_nullable_observed<E: From<crate::owned_resources::layout::LayoutResourceError>>(
        &self,
        nullable: bool,
        storage: crate::ValueTypeStorageLifetime,
        request: impl FnMut(std::alloc::Layout) -> Result<(), E>,
        work: impl FnMut(usize) -> Result<(), E>,
    ) -> Result<Self, E> {
        self.binding
            .with_nullable_observed(nullable, storage, request, work)
            .map(|binding| Self { binding })
    }

    pub fn try_with_logical_type(
        data_type: DataType,
        nullable: bool,
        logical_type: crate::ValueLogicalType,
    ) -> Result<Self, crate::ValueTypeError> {
        Self::try_with_logical_type_with_construction::<std::convert::Infallible>(
            data_type,
            nullable,
            logical_type,
            crate::ValueTypeStorageLifetime::pure(),
            None,
        )
        .map_err(|error| crate::ValueTypeError::from(error.into_pure()))
    }
    pub fn try_with_logical_type_with_construction<E>(
        data_type: DataType,
        nullable: bool,
        logical_type: crate::ValueLogicalType,
        storage: crate::ValueTypeStorageLifetime,
        observer: Option<&mut dyn FnMut(crate::ValueConstructionRequest) -> Result<(), E>>,
    ) -> Result<Self, crate::ValueConstructionFailure<E>> {
        let mut creation = crate::value_construction::ValueConstruction::new(observer);
        let layout = crate::ValueLayout::try_new_with_construction(
            &data_type,
            storage.clone(),
            creation.observer(),
        )?;
        let value = crate::ValueTypeRef::try_from_declared_arrow_with_construction(
            layout.data_type(),
            nullable,
            logical_type,
            storage,
            creation.observer(),
        )?;
        Self::try_from_value(value, layout).map_err(Into::into)
    }
    pub fn child_at_with_construction<E>(
        &self,
        index: usize,
        storage: crate::ValueTypeStorageLifetime,
        observer: Option<&mut dyn FnMut(crate::ValueConstructionRequest) -> Result<(), E>>,
    ) -> Result<Option<Self>, crate::ValueConstructionFailure<E>> {
        self.binding
            .child_at_with_construction(index, storage, observer)
            .map(|binding| binding.map(|binding| Self { binding }))
    }
    pub fn with_nullable_with_construction<E>(
        &self,
        nullable: bool,
        storage: crate::ValueTypeStorageLifetime,
        observer: Option<&mut dyn FnMut(crate::ValueConstructionRequest) -> Result<(), E>>,
    ) -> Result<Self, crate::ValueConstructionFailure<E>> {
        self.binding
            .with_nullable_with_construction(nullable, storage, observer)
            .map(|binding| Self { binding })
    }

    /// Temporary expansion-only import of a positively authored legacy field.
    /// Execution callers must pass an existing complete typed binding instead.
    pub fn try_from_legacy_field_projection(field: &Field) -> Result<Self, crate::ValueTypeError> {
        let storage = crate::ValueTypeStorageLifetime::pure();
        let value = crate::ValueTypeRef::try_from_external_field(field, storage.clone())?;
        let layout = crate::ValueLayout::try_new(field.data_type(), storage)?;
        Self::try_from_value(value, layout)
    }

    /// Temporary declared-root projection for old nested semantic authors.
    pub fn try_from_legacy_declared_projection(
        data_type: DataType,
        nullable: bool,
        logical_type: crate::ValueLogicalType,
    ) -> Result<Self, crate::ValueTypeError> {
        let storage = crate::ValueTypeStorageLifetime::pure();
        let value = crate::ValueTypeRef::try_from_declared_arrow(
            &data_type,
            nullable,
            logical_type,
            storage.clone(),
        )?;
        let layout = crate::ValueLayout::try_new(&data_type, storage)?;
        Self::try_from_value(value, layout)
    }

    pub fn from_legacy_declared_projection(data_type: DataType, nullable: bool) -> Self {
        Self::try_from_legacy_declared_projection(
            data_type,
            nullable,
            crate::ValueLogicalType::Physical,
        )
        .expect("valid temporary legacy function value declaration")
    }

    /// Temporary outward projection for the old production chain. All recursive
    /// nominal semantics come from the typed tree, never from layout metadata.
    pub fn try_to_legacy_field_projection(
        &self,
        name: impl Into<String>,
    ) -> Result<Field, crate::ValueTypeError> {
        typed_value::legacy_field(self.value_type(), self.data_type(), name.into())
    }

    // Expansion aliases remain until the existing Chunk/wire chain flips.
    pub fn try_from_field(field: &Field) -> Result<Self, crate::ValueTypeError> {
        Self::try_from_legacy_field_projection(field)
    }
    pub fn try_to_field(&self, name: impl Into<String>) -> Result<Field, crate::ValueTypeError> {
        self.try_to_legacy_field_projection(name)
    }

    pub fn validate(&self) -> Result<(), crate::ValueTypeError> {
        // Construction checked the complete immutable semantics/layout pair.
        Ok(())
    }

    /// Borrow the exact layout using the legacy resource visitor while the
    /// expansion chain still uses its scratch vocabulary. It cannot rediscover
    /// semantics; those are already checked and held in the immutable binding.
    pub fn validate_with_scratch_observed<'a, E: From<crate::ValueTypeError>>(
        &'a self,
        scratch: &mut crate::owned_resources::type_validation::TypeValidationScratch<'a>,
        observe: impl FnMut(crate::ValueTypeVisit<'a>) -> Result<(), E>,
    ) -> Result<(), E> {
        crate::validate_value_type_structure_with_scratch_observed(
            self.data_type(),
            scratch,
            observe,
        )
    }

    /// Observe the existing logical NULL-widening rule on validated holders.
    pub fn fits_value_type_observed<E>(
        &self,
        expected: &Self,
        mut observe: impl FnMut() -> Result<(), E>,
    ) -> Result<bool, E> {
        typed_value::compatible(
            self.value_type(),
            expected.value_type(),
            typed_value::Nullability::Fits,
            &mut observe,
        )
    }

    /// Match the complete immutable declaration and the installed physical
    /// port, permitting only the owner's existing NULL widening rule.
    /// No raw annotations or unvalidated Arrow grammar enter this comparison.
    pub fn fits_physical_binding_observed<E>(
        &self,
        expected: &Self,
        mut observe: impl FnMut() -> Result<(), E>,
    ) -> Result<bool, E> {
        if !self.fits_value_type_observed(expected, &mut observe)? {
            return Ok(false);
        }
        nested_layout_fits(self.data_type(), expected.data_type(), &mut observe)
    }

    pub fn fits_value_type(&self, expected: &Self) -> bool {
        typed_value::compatible(
            self.value_type(),
            expected.value_type(),
            typed_value::Nullability::Fits,
            &mut || Ok::<(), crate::ValueTypeError>(()),
        )
        .unwrap_or(false)
    }

    pub fn comparison_work_upper_bound(&self) -> Result<usize, crate::ExecutionValueTypeError> {
        self.binding.comparison_work_upper_bound()
    }
    pub fn exactly_equals_weighted_observed<E: From<crate::ValueTypeError>>(
        &self,
        other: &Self,
        observe: impl FnMut(crate::TypeComparisonStep) -> Result<(), E>,
    ) -> Result<bool, E> {
        self.binding
            .exactly_equals_weighted_observed(&other.binding, observe)
    }

    pub fn exactly_equals_observed<E: From<crate::ValueTypeError>>(
        &self,
        other: &Self,
        observe: impl FnMut() -> Result<(), E>,
    ) -> Result<bool, E> {
        self.binding
            .exactly_equals_observed(&other.binding, observe)
    }

    /// Compare complete already validated immutable semantics and exact layout.
    /// Replay exposes only the caller's original observation error.
    pub fn exactly_equals_checked_observed<E>(
        &self,
        other: &Self,
        mut observe: impl FnMut() -> Result<(), E>,
    ) -> Result<bool, E> {
        if !typed_value::compatible(
            self.value_type(),
            other.value_type(),
            typed_value::Nullability::Exact,
            &mut observe,
        )? {
            return Ok(false);
        }
        self.binding
            .layout()
            .equals_observed(other.binding.layout(), observe)
    }

    pub fn same_semantics(&self, other: &Self) -> bool {
        self.binding.same_semantics(&other.binding)
    }
    pub fn same_semantics_observed<E: From<crate::ValueTypeError>>(
        &self,
        other: &Self,
        observe: impl FnMut() -> Result<(), E>,
    ) -> Result<bool, E> {
        self.binding
            .same_semantics_observed(&other.binding, observe)
    }

    pub fn same_value_domain_observed<E: From<crate::ValueTypeError>>(
        &self,
        other: &Self,
        mut observe: impl FnMut() -> Result<(), E>,
    ) -> Result<bool, E> {
        typed_value::compatible(
            self.value_type(),
            other.value_type(),
            typed_value::Nullability::IgnoreRoot,
            &mut observe,
        )
    }

    /// Compare immutable complete semantics and physical binding while ignoring
    /// only root nullability. Nested NULL promises and every layout fact remain.
    /// The observer's original error is the sole failure channel.
    pub fn same_physical_binding_ignoring_root_nullability_observed<E>(
        &self,
        other: &Self,
        mut observe: impl FnMut() -> Result<(), E>,
    ) -> Result<bool, E> {
        if !typed_value::compatible(
            self.value_type(),
            other.value_type(),
            typed_value::Nullability::IgnoreRoot,
            &mut observe,
        )? {
            return Ok(false);
        }
        self.binding
            .layout()
            .equals_observed(other.binding.layout(), observe)
    }
    /// Compare borrowed child semantics without allocating a child binding.
    /// Only the selected child root nullability is ignored.
    pub fn borrowed_value_domains_equal_observed<E: From<crate::ValueTypeError>>(
        left: &ValueTypeRef,
        right: &ValueTypeRef,
        mut observe: impl FnMut() -> Result<(), E>,
    ) -> Result<bool, E> {
        typed_value::compatible(
            left,
            right,
            typed_value::Nullability::IgnoreRoot,
            &mut observe,
        )
    }

    pub fn same_value_domain(&self, other: &Self) -> bool {
        self.same_value_domain_observed(other, || Ok::<(), crate::ValueTypeError>(()))
            .unwrap_or(false)
    }

    /// Logical overload rule that explicitly permits nested NULL widening.
    /// Scalar parameters, nominal identities and semantic field names remain.
    pub fn same_semantics_ignoring_nullability(&self, other: &Self) -> bool {
        typed_value::compatible(
            self.value_type(),
            other.value_type(),
            typed_value::Nullability::IgnoreAll,
            &mut || Ok::<(), crate::ValueTypeError>(()),
        )
        .unwrap_or(false)
    }
}

/// Exact shape of one bound function argument.
///
/// A lambda is executable syntax with its own parameter contract, rather than
/// a scalar value typed as its body.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub enum FunctionArgumentType {
    Value(FunctionValueType),
    Lambda {
        parameter_types: crate::FunctionParameterTypes,
        result_type: FunctionValueType,
    },
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum FunctionKind {
    Scalar,
    Aggregate,
    Window,
    Table,
}

/// Stability of a result within and across workers for the same logical input.
#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum FunctionVolatility {
    #[default]
    Immutable,
    Stable,
    Volatile,
}

impl FunctionVolatility {
    pub const fn is_volatile(self) -> bool {
        matches!(self, Self::Volatile)
    }

    pub const fn is_replica_deterministic(self) -> bool {
        matches!(self, Self::Immutable)
    }
}

/// Whether an implementation may decide which argument expressions to run.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum FunctionArgumentEvaluation {
    Eager,
    ShortCircuit,
}

/// How failures produced by the selected implementation are exposed.
///
/// This does not change failures raised while evaluating argument expressions.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum FunctionFailureBehavior {
    Propagate,
    ReturnsNull,
}

/// Externally visible errors owned by a selected row implementation on legally
/// bound inputs. Child-expression, frozen-contract, resource and lifecycle errors
/// are outside this fact; it does not depend on `FunctionFailureBehavior`.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum FunctionIntrinsicRowError {
    NoRowError,
    MayRaise,
    /// Aggregate/window owners do not evaluate a scalar row implementation.
    NotRowEvaluated,
}

impl FunctionIntrinsicRowError {
    pub const fn is_valid_for_kind(self, kind: FunctionKind) -> bool {
        match kind {
            FunctionKind::Scalar | FunctionKind::Table => !matches!(self, Self::NotRowEvaluated),
            FunctionKind::Aggregate | FunctionKind::Window => {
                matches!(self, Self::NotRowEvaluated)
            }
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum FunctionIdentityError {
    Empty { kind: &'static str },
    TooLong { kind: &'static str, actual: usize },
    InvalidCharacters { kind: &'static str },
}

impl fmt::Display for FunctionIdentityError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Empty { kind } => write!(formatter, "{kind} identity is empty"),
            Self::TooLong { kind, actual } => write!(
                formatter,
                "{kind} identity is {actual} bytes, exceeding {MAX_FUNCTION_IDENTITY_BYTES}"
            ),
            Self::InvalidCharacters { kind } => {
                write!(
                    formatter,
                    "{kind} identity contains non-canonical characters"
                )
            }
        }
    }
}

impl std::error::Error for FunctionIdentityError {}

fn validate_identity(kind: &'static str, value: &str) -> Result<(), FunctionIdentityError> {
    if value.is_empty() {
        return Err(FunctionIdentityError::Empty { kind });
    }
    if value.len() > MAX_FUNCTION_IDENTITY_BYTES {
        return Err(FunctionIdentityError::TooLong {
            kind,
            actual: value.len(),
        });
    }
    Ok(())
}

fn nested_layout_fits<E>(
    actual: &DataType,
    expected: &DataType,
    observe: &mut impl FnMut() -> Result<(), E>,
) -> Result<bool, E> {
    #[allow(deprecated)]
    fn field_fits<E>(
        actual: &Field,
        expected: &Field,
        observe: &mut impl FnMut() -> Result<(), E>,
    ) -> Result<bool, E> {
        observe()?;
        if actual.name() != expected.name()
            || (!expected.is_nullable() && actual.is_nullable())
            || actual.dict_id() != expected.dict_id()
            || actual.dict_is_ordered() != expected.dict_is_ordered()
        {
            return Ok(false);
        }
        fits(actual.data_type(), expected.data_type(), observe)
    }
    fn fits<E>(
        actual: &DataType,
        expected: &DataType,
        observe: &mut impl FnMut() -> Result<(), E>,
    ) -> Result<bool, E> {
        observe()?;
        match (actual, expected) {
            (DataType::List(actual), DataType::List(expected))
            | (DataType::LargeList(actual), DataType::LargeList(expected))
            | (DataType::ListView(actual), DataType::ListView(expected))
            | (DataType::LargeListView(actual), DataType::LargeListView(expected)) => {
                field_fits(actual, expected, observe)
            }
            (DataType::FixedSizeList(actual, al), DataType::FixedSizeList(expected, el)) => {
                if al != el {
                    Ok(false)
                } else {
                    field_fits(actual, expected, observe)
                }
            }
            (DataType::Struct(actual), DataType::Struct(expected)) => {
                if actual.len() != expected.len() {
                    return Ok(false);
                }
                for (actual, expected) in actual.iter().zip(expected) {
                    if !field_fits(actual, expected, observe)? {
                        return Ok(false);
                    }
                }
                Ok(true)
            }
            (DataType::Map(actual, asorted), DataType::Map(expected, esorted)) => {
                if asorted != esorted {
                    Ok(false)
                } else {
                    field_fits(actual, expected, observe)
                }
            }
            (DataType::Union(actual, amode), DataType::Union(expected, emode)) => {
                if amode != emode || actual.len() != expected.len() {
                    return Ok(false);
                }
                for ((ai, actual), (ei, expected)) in actual.iter().zip(expected.iter()) {
                    if ai != ei || !field_fits(actual, expected, observe)? {
                        return Ok(false);
                    }
                }
                Ok(true)
            }
            (DataType::Dictionary(ak, av), DataType::Dictionary(ek, ev)) => {
                Ok(fits(ak, ek, observe)? && fits(av, ev, observe)?)
            }
            (DataType::RunEndEncoded(ar, av), DataType::RunEndEncoded(er, ev)) => {
                Ok(field_fits(ar, er, observe)? && field_fits(av, ev, observe)?)
            }
            (actual, expected) => Ok(actual == expected),
        }
    }
    fits(actual, expected, observe)
}

#[cfg(test)]
mod tests {
    use super::{AggregateStateFormatId, FunctionId, FunctionIdentityError};

    #[test]
    fn observed_value_type_equality_preserves_exact_frozen_identity() {
        use crate::{FunctionValueType, NR_LOGICAL_TYPE_KEY, ValueLogicalType, ValueTypeError};
        use arrow_schema::{DataType, Field};
        #[allow(deprecated)]
        let nested = |dict_id, ordered, nullable, annotation: &str, logical: &str| {
            FunctionValueType::from_legacy_declared_projection(
                DataType::Struct(
                    vec![
                        Field::new_dict(
                            "dictionary",
                            DataType::Dictionary(
                                Box::new(DataType::Int8),
                                Box::new(DataType::Utf8),
                            ),
                            nullable,
                            dict_id,
                            ordered,
                        )
                        .with_metadata(std::collections::HashMap::<
                            String,
                            String,
                        >::from([
                            ("provider".to_owned(), annotation.to_owned()),
                        ])),
                        if logical.is_empty() {
                            Field::new("payload", DataType::Utf8, false)
                        } else {
                            Field::new("payload", DataType::Utf8, false).with_metadata(
                                std::collections::HashMap::<String, String>::from([(
                                    NR_LOGICAL_TYPE_KEY.to_owned(),
                                    logical.to_owned(),
                                )]),
                            )
                        },
                    ]
                    .into(),
                ),
                false,
            )
        };
        let value = nested(7, false, true, "source", "json");
        for other in [
            value.clone(),
            nested(8, false, true, "source", "json"),
            nested(7, true, true, "source", "json"),
            nested(7, false, false, "source", "json"),
            nested(7, false, true, "other", "json"),
            nested(7, false, true, "source", ""),
        ] {
            assert_eq!(
                value
                    .exactly_equals_observed::<ValueTypeError>(&other, || Ok(()))
                    .unwrap(),
                value == other,
            );
        }
        let physical = FunctionValueType::new(DataType::FixedSizeBinary(16), false);
        let uuid = FunctionValueType::try_with_logical_type(
            physical.data_type().clone(),
            false,
            ValueLogicalType::Uuid,
        )
        .unwrap();
        assert!(
            !physical
                .exactly_equals_observed::<ValueTypeError>(&uuid, || Ok(()))
                .unwrap()
        );
        assert!(
            !physical
                .exactly_equals_observed::<ValueTypeError>(
                    &FunctionValueType::from_legacy_declared_projection(
                        physical.data_type().clone(),
                        true
                    ),
                    || Ok(()),
                )
                .unwrap()
        );
    }

    #[test]
    fn observed_value_type_equality_preserves_control_failure_at_entry_and_interior() {
        use crate::{CompileControlError, FunctionValueType, ValueTypeError};
        use arrow_schema::{DataType, Field};
        #[derive(Debug, PartialEq)]
        enum Failure {
            Type(ValueTypeError),
            Control(CompileControlError),
        }
        impl From<ValueTypeError> for Failure {
            fn from(value: ValueTypeError) -> Self {
                Self::Type(value)
            }
        }
        let value = FunctionValueType::from_legacy_declared_projection(
            DataType::Struct(
                (0..320)
                    .map(|index| Field::new(index.to_string(), DataType::Int32, false))
                    .collect(),
            ),
            false,
        );
        let mut visits = 0;
        assert!(
            value
                .exactly_equals_observed::<ValueTypeError>(&value, || {
                    visits += 1;
                    Ok(())
                })
                .unwrap()
        );
        assert!(visits > 960);
        for failure in [
            CompileControlError::Cancelled,
            CompileControlError::DeadlineExceeded,
            CompileControlError::ResourceExhausted,
        ] {
            for stop in [1, 256, visits] {
                let mut observed = 0;
                let result = value.exactly_equals_observed::<Failure>(&value, || {
                    observed += 1;
                    if observed == stop {
                        Err(Failure::Control(failure.clone()))
                    } else {
                        Ok(())
                    }
                });
                assert_eq!(result, Err(Failure::Control(failure.clone())));
                assert_eq!(observed, stop);
            }
        }
    }

    #[test]
    fn source_field_value_domain_is_explicit_and_validated() {
        use crate::{FunctionValueType, NR_LOGICAL_TYPE_KEY, ValueLogicalType};
        use arrow_schema::{DataType, Field};
        let physical = Field::new("bytes", DataType::FixedSizeBinary(16), false);
        assert_eq!(
            FunctionValueType::try_from_field(&physical).unwrap(),
            FunctionValueType::new(DataType::FixedSizeBinary(16), false)
        );
        let uuid =
            physical
                .clone()
                .with_metadata(std::collections::HashMap::<String, String>::from([(
                    NR_LOGICAL_TYPE_KEY.to_owned(),
                    "uuid".to_owned(),
                )]));
        let exact_uuid = FunctionValueType::try_from_field(&uuid).unwrap();
        assert_eq!(exact_uuid.logical_type(), ValueLogicalType::Uuid);
        assert_eq!(
            FunctionValueType::try_from_field(&exact_uuid.try_to_field("roundtrip").unwrap())
                .unwrap(),
            exact_uuid
        );
        assert!(
            FunctionValueType::try_from_field(&uuid.with_data_type(DataType::FixedSizeBinary(15)))
                .is_err()
        );
        let unknown =
            physical.with_metadata(std::collections::HashMap::<String, String>::from([(
                NR_LOGICAL_TYPE_KEY.to_owned(),
                "unknown".to_owned(),
            )]));
        assert!(FunctionValueType::try_from_field(&unknown).is_err());
        let item = Field::new("provider_item", DataType::Utf8, false).with_metadata(
            std::collections::HashMap::<String, String>::from([
                (NR_LOGICAL_TYPE_KEY.to_owned(), "json".to_owned()),
                ("provider.field_id".to_owned(), "91".to_owned()),
            ]),
        );
        let nested = Field::new(
            "values",
            DataType::LargeList(std::sync::Arc::new(item)),
            true,
        );
        assert_eq!(
            FunctionValueType::try_from_field(&nested).unwrap(),
            FunctionValueType::from_legacy_declared_projection(nested.data_type().clone(), true)
        );
    }

    #[test]
    fn stable_function_identity_is_bounded() {
        assert_eq!(
            FunctionId::try_new("builtin/lower/v1").unwrap().as_str(),
            "builtin/lower/v1"
        );
        assert!(matches!(
            FunctionId::try_new(""),
            Err(FunctionIdentityError::Empty { kind: "function" })
        ));
        assert!(matches!(
            FunctionId::try_new("x".repeat(1025)),
            Err(FunctionIdentityError::TooLong { actual: 1025, .. })
        ));
    }

    #[test]
    fn aggregate_state_format_is_canonical_and_unambiguous() {
        assert!(AggregateStateFormatId::try_new("builtin/sum/state-v1").is_ok());
        for invalid in ["state with space", "state|v1", "state,v1"] {
            assert!(matches!(
                AggregateStateFormatId::try_new(invalid),
                Err(FunctionIdentityError::InvalidCharacters { .. })
            ));
        }
    }
}

#[cfg(test)]
#[path = "function/identity_tests.rs"]
mod identity_tests;

#[cfg(test)]
#[path = "function/value_type_scratch_tests.rs"]
mod value_type_scratch_tests;

#[cfg(test)]
#[path = "function/typed_binding_tests.rs"]
mod typed_binding_tests;
