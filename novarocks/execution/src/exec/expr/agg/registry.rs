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

//! Sealed aggregate implementation registry.
//!
//! The process function catalog remains the resolver and metadata authority.
//! This module is the matching executable-implementation authority: it
//! attaches aggregate families to exact catalog overload identities, validates
//! the attachment when the process function set is sealed, and owns all state
//! type erasure. Native compatibility composes both immutable digests. Function
//! bundles implement only the safe typed contracts from `novarocks-functions`.

use std::alloc::Layout;
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::marker::PhantomData;
use std::ptr::NonNull;
use std::sync::Arc;

use arrow::array::ArrayRef;
use arrow::datatypes::DataType;
use novarocks_functions::{
    AggregateBindOptions, AggregateImplementationIdentity, AggregateInputBatch,
    AggregateOverloadIdentity, AggregateStateFormatIdentity, AggregateStateMemoryPolicy,
    EngineFunctionCatalog, EngineFunctionCatalogBuilder, FunctionCatalogError, FunctionKind,
    ResolvedAggregateSignature, TypedAggregateFamily, TypedAggregateKernel,
    TypedAggregateRegistration,
};
use sha2::{Digest, Sha256};

use crate::exec::node::aggregate::{AggFunction, AggTypeSignature};
use crate::runtime::mem_tracker::MemTracker;

use super::functions::AggregateFunction;
use super::{AggSpec, AggStatePtr};

const EXECUTION_IMPLEMENTATION_MANIFEST_DOMAIN: &[u8] =
    b"novarocks.aggregate-execution-implementation-manifest/v1\0";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RetainedMemoryPolicy {
    FixedZero,
    AllocationTracked,
    BoundedRetained { max_retained_bytes_per_state: usize },
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct AggregateImplementationKey {
    canonical_name: Box<str>,
    overload: AggregateOverloadIdentity,
}

impl AggregateImplementationKey {
    fn try_new(
        canonical_name: impl AsRef<str>,
        overload: AggregateOverloadIdentity,
    ) -> Result<Self, ExecutionFunctionSetError> {
        Ok(Self {
            canonical_name: canonicalize_function_name(canonical_name.as_ref())?,
            overload,
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ExecutionFunctionSetError {
    Catalog(FunctionCatalogError),
    InvalidCanonicalName {
        name: Box<str>,
    },
    DuplicateAggregateImplementation {
        canonical_name: Box<str>,
        overload: AggregateOverloadIdentity,
    },
    AggregateMetadataWithoutOverloads {
        canonical_name: Box<str>,
    },
    AggregateMetadataWithoutImplementation {
        canonical_name: Box<str>,
        overload: AggregateOverloadIdentity,
    },
    AggregateImplementationWithoutMetadata {
        canonical_name: Box<str>,
        overload: AggregateOverloadIdentity,
    },
    BuiltinAggregateImplementationWithoutMetadata {
        canonical_name: Box<str>,
    },
    AggregateImplementationStateFormatMismatch {
        canonical_name: Box<str>,
        overload: AggregateOverloadIdentity,
        metadata: AggregateStateFormatIdentity,
        implementation: AggregateStateFormatIdentity,
    },
    UnknownAggregateImplementation {
        canonical_name: Box<str>,
        overload: AggregateOverloadIdentity,
    },
    ResolvedAggregateDrift {
        canonical_name: Box<str>,
        planned: Box<ResolvedAggregateSignature>,
        local: Box<ResolvedAggregateSignature>,
    },
    PrepareAggregate {
        canonical_name: Box<str>,
        overload: AggregateOverloadIdentity,
        message: Box<str>,
    },
}

impl fmt::Display for ExecutionFunctionSetError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Catalog(error) => error.fmt(formatter),
            Self::InvalidCanonicalName { name } => {
                write!(
                    formatter,
                    "invalid canonical aggregate function name `{name}`"
                )
            }
            Self::DuplicateAggregateImplementation {
                canonical_name,
                overload,
            } => write!(
                formatter,
                "duplicate aggregate implementation `{canonical_name}` overload `{}`",
                overload.as_str()
            ),
            Self::AggregateMetadataWithoutOverloads { canonical_name } => write!(
                formatter,
                "aggregate metadata `{canonical_name}` has no exact overload declarations"
            ),
            Self::AggregateMetadataWithoutImplementation {
                canonical_name,
                overload,
            } => write!(
                formatter,
                "aggregate metadata `{canonical_name}` overload `{}` has no execution implementation",
                overload.as_str()
            ),
            Self::AggregateImplementationWithoutMetadata {
                canonical_name,
                overload,
            } => write!(
                formatter,
                "aggregate implementation `{canonical_name}` overload `{}` has no catalog metadata",
                overload.as_str()
            ),
            Self::BuiltinAggregateImplementationWithoutMetadata { canonical_name } => write!(
                formatter,
                "builtin aggregate implementation `{canonical_name}` has no catalog metadata"
            ),
            Self::AggregateImplementationStateFormatMismatch {
                canonical_name,
                overload,
                metadata,
                implementation,
            } => write!(
                formatter,
                "aggregate implementation `{canonical_name}` overload `{}` state format mismatch: metadata=`{}`, implementation=`{}`",
                overload.as_str(),
                metadata.as_str(),
                implementation.as_str()
            ),
            Self::UnknownAggregateImplementation {
                canonical_name,
                overload,
            } => write!(
                formatter,
                "aggregate implementation `{canonical_name}` overload `{}` is not installed",
                overload.as_str()
            ),
            Self::ResolvedAggregateDrift {
                canonical_name,
                planned,
                local,
            } => write!(
                formatter,
                "aggregate `{canonical_name}` resolved signature drift: planned={planned:?}, local={local:?}"
            ),
            Self::PrepareAggregate {
                canonical_name,
                overload,
                message,
            } => write!(
                formatter,
                "prepare aggregate `{canonical_name}` overload `{}`: {message}",
                overload.as_str()
            ),
        }
    }
}

impl std::error::Error for ExecutionFunctionSetError {}

impl From<FunctionCatalogError> for ExecutionFunctionSetError {
    fn from(value: FunctionCatalogError) -> Self {
        Self::Catalog(value)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum PreparedAggregateError {
    StatePointerCount { expected: usize, actual: usize },
    Input(String),
    CreateState(String),
    Update(String),
    Merge(String),
    BuildIntermediate(String),
    BuildFinal(String),
}

impl fmt::Display for PreparedAggregateError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::StatePointerCount { expected, actual } => write!(
                formatter,
                "aggregate state pointer count {actual} does not match input row count {expected}"
            ),
            Self::Input(message) => write!(formatter, "aggregate input: {message}"),
            Self::CreateState(message) => write!(formatter, "create aggregate state: {message}"),
            Self::Update(message) => write!(formatter, "update aggregate state: {message}"),
            Self::Merge(message) => write!(formatter, "merge aggregate state: {message}"),
            Self::BuildIntermediate(message) => {
                write!(formatter, "build aggregate intermediate output: {message}")
            }
            Self::BuildFinal(message) => {
                write!(formatter, "build aggregate final output: {message}")
            }
        }
    }
}

impl std::error::Error for PreparedAggregateError {}

pub struct ExecutionFunctionSetBuilder {
    catalog: EngineFunctionCatalogBuilder,
    aggregate_implementations: BTreeMap<AggregateImplementationKey, AggregateImplementationBinding>,
}

struct AggregateImplementationBinding {
    family: Arc<dyn ErasedAggregateFamily>,
    implementation: AggregateImplementationIdentity,
    expected_state_format: AggregateStateFormatIdentity,
}

impl Default for ExecutionFunctionSetBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl ExecutionFunctionSetBuilder {
    pub fn new() -> Self {
        Self {
            catalog: EngineFunctionCatalogBuilder::new(),
            aggregate_implementations: BTreeMap::new(),
        }
    }

    /// Metadata-only contributions use this builder. Scalar functions need no
    /// aggregate implementation. Aggregate metadata is checked bidirectionally
    /// against the implementation index by [`Self::seal`].
    pub fn catalog_builder_mut(&mut self) -> &mut EngineFunctionCatalogBuilder {
        &mut self.catalog
    }

    pub fn catalog_builder(&self) -> &EngineFunctionCatalogBuilder {
        &self.catalog
    }

    /// Installs one safe typed aggregate family and its catalog metadata as one
    /// operation. The provider never sees Execution's erased state interface.
    pub fn register_typed_aggregate<F>(
        &mut self,
        registration: TypedAggregateRegistration<F>,
    ) -> Result<(), ExecutionFunctionSetError>
    where
        F: TypedAggregateFamily,
    {
        let (definition, implementation, family) = registration.into_parts();
        let canonical_name = definition.canonical_name().to_string().into_boxed_str();
        let overloads = definition
            .aggregate_overloads()
            .iter()
            .map(|overload| (overload.identity.clone(), overload.state_format.clone()))
            .collect::<Vec<_>>();
        let adapter: Arc<dyn ErasedAggregateFamily> = Arc::new(TypedFamilyAdapter { family });

        self.ensure_implementation_keys_available(
            &canonical_name,
            &overloads
                .iter()
                .map(|(identity, _)| identity.clone())
                .collect::<Vec<_>>(),
        )?;
        self.catalog.register(definition)?;
        self.insert_aggregate_implementation(&canonical_name, overloads, implementation, adapter);
        Ok(())
    }

    /// Execution-private migration entry for existing builtins. It accepts an
    /// already-erased family only inside this crate; raw state contracts never
    /// cross the Execution crate boundary. Catalog metadata must be registered
    /// separately and `seal` still enforces exact one-to-one coverage.
    fn register_legacy_aggregate_family(
        &mut self,
        canonical_name: impl AsRef<str>,
        overloads: impl IntoIterator<Item = (AggregateOverloadIdentity, AggregateStateFormatIdentity)>,
        implementation: AggregateImplementationIdentity,
        family: Arc<dyn ErasedAggregateFamily>,
    ) -> Result<(), ExecutionFunctionSetError> {
        let canonical_name = canonical_name.as_ref();
        let overloads = overloads.into_iter().collect::<Vec<_>>();
        self.ensure_implementation_keys_available(
            canonical_name,
            &overloads
                .iter()
                .map(|(identity, _)| identity.clone())
                .collect::<Vec<_>>(),
        )?;
        self.insert_aggregate_implementation(canonical_name, overloads, implementation, family);
        Ok(())
    }

    pub(in crate::exec::expr::agg) fn register_legacy_aggregate(
        &mut self,
        canonical_name: impl AsRef<str>,
        overloads: impl IntoIterator<Item = AggregateOverloadIdentity>,
        implementation: AggregateImplementationIdentity,
        expected_state_format: AggregateStateFormatIdentity,
        function: &'static dyn AggregateFunction,
    ) -> Result<(), ExecutionFunctionSetError> {
        self.register_legacy_aggregate_family(
            canonical_name,
            overloads
                .into_iter()
                .map(|overload| (overload, expected_state_format.clone())),
            implementation,
            Arc::new(LegacyAggregateFamilyAdapter { function }),
        )
    }

    fn ensure_implementation_keys_available(
        &self,
        canonical_name: &str,
        overloads: &[AggregateOverloadIdentity],
    ) -> Result<(), ExecutionFunctionSetError> {
        let mut pending = BTreeSet::new();
        for overload in overloads {
            let key = AggregateImplementationKey::try_new(canonical_name, overload.clone())?;
            if self.aggregate_implementations.contains_key(&key) || !pending.insert(key.clone()) {
                return Err(
                    ExecutionFunctionSetError::DuplicateAggregateImplementation {
                        canonical_name: key.canonical_name,
                        overload: key.overload,
                    },
                );
            }
        }
        Ok(())
    }

    fn insert_aggregate_implementation(
        &mut self,
        canonical_name: &str,
        overloads: Vec<(AggregateOverloadIdentity, AggregateStateFormatIdentity)>,
        implementation: AggregateImplementationIdentity,
        family: Arc<dyn ErasedAggregateFamily>,
    ) {
        for (overload, expected_state_format) in overloads {
            self.aggregate_implementations.insert(
                AggregateImplementationKey::try_new(canonical_name, overload)
                    .expect("aggregate implementation name was validated before insertion"),
                AggregateImplementationBinding {
                    family: Arc::clone(&family),
                    implementation: implementation.clone(),
                    expected_state_format,
                },
            );
        }
    }

    pub fn seal(self) -> Result<SealedExecutionFunctionSet, ExecutionFunctionSetError> {
        let catalog = Arc::new(self.catalog.seal()?);
        let mut metadata_keys = BTreeSet::new();
        for definition in catalog
            .definitions()
            .iter()
            .filter(|definition| definition.kind() == FunctionKind::Aggregate)
        {
            if definition.aggregate_overloads().is_empty() {
                return Err(
                    ExecutionFunctionSetError::AggregateMetadataWithoutOverloads {
                        canonical_name: definition.canonical_name().into(),
                    },
                );
            }
            for overload in definition.aggregate_overloads() {
                let key = AggregateImplementationKey::try_new(
                    definition.canonical_name(),
                    overload.identity.clone(),
                )?;
                metadata_keys.insert(key.clone());
                let Some(implementation) = self.aggregate_implementations.get(&key) else {
                    return Err(
                        ExecutionFunctionSetError::AggregateMetadataWithoutImplementation {
                            canonical_name: key.canonical_name,
                            overload: key.overload,
                        },
                    );
                };
                if implementation.expected_state_format != overload.state_format {
                    return Err(
                        ExecutionFunctionSetError::AggregateImplementationStateFormatMismatch {
                            canonical_name: key.canonical_name,
                            overload: key.overload,
                            metadata: overload.state_format.clone(),
                            implementation: implementation.expected_state_format.clone(),
                        },
                    );
                }
            }
        }
        for key in self.aggregate_implementations.keys() {
            if !metadata_keys.contains(key) {
                return Err(
                    ExecutionFunctionSetError::AggregateImplementationWithoutMetadata {
                        canonical_name: key.canonical_name.clone(),
                        overload: key.overload.clone(),
                    },
                );
            }
        }
        let implementation_manifest_digest =
            digest_aggregate_implementation_manifest(&self.aggregate_implementations);
        Ok(SealedExecutionFunctionSet {
            catalog,
            aggregate_implementations: self.aggregate_implementations,
            implementation_manifest_digest,
        })
    }
}

pub struct SealedExecutionFunctionSet {
    catalog: Arc<EngineFunctionCatalog>,
    aggregate_implementations: BTreeMap<AggregateImplementationKey, AggregateImplementationBinding>,
    implementation_manifest_digest: [u8; 32],
}

impl fmt::Debug for SealedExecutionFunctionSet {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SealedExecutionFunctionSet")
            .field("catalog_digest", &self.catalog.digest())
            .field(
                "implementation_manifest_digest",
                &self.implementation_manifest_digest,
            )
            .field(
                "aggregate_implementation_count",
                &self.aggregate_implementations.len(),
            )
            .finish()
    }
}

impl SealedExecutionFunctionSet {
    pub fn catalog(&self) -> &Arc<EngineFunctionCatalog> {
        &self.catalog
    }

    pub const fn implementation_manifest_digest(&self) -> [u8; 32] {
        self.implementation_manifest_digest
    }

    /// Prepares one exact aggregate kernel after verifying that the local
    /// catalog resolves the same signature frozen by planning.
    pub(crate) fn prepare_resolved_aggregate(
        &self,
        canonical_name: &str,
        context: &AggregatePrepareContext<'_>,
    ) -> Result<PreparedAggregateKernel, ExecutionFunctionSetError> {
        let selected = context.selected;
        let local = self
            .catalog
            .resolve_selected_aggregate_update_trusted(
                canonical_name,
                &selected.overload,
                &selected.argument_types,
            )
            .map_err(|error| ExecutionFunctionSetError::PrepareAggregate {
                canonical_name: canonical_name.into(),
                overload: selected.overload.clone(),
                message: error.to_string().into(),
            })?;
        if &local != selected {
            return Err(ExecutionFunctionSetError::ResolvedAggregateDrift {
                canonical_name: canonical_name.into(),
                planned: Box::new(selected.clone()),
                local: Box::new(local),
            });
        }
        let key = AggregateImplementationKey::try_new(canonical_name, selected.overload.clone())?;
        let implementation = self.aggregate_implementations.get(&key).ok_or_else(|| {
            ExecutionFunctionSetError::UnknownAggregateImplementation {
                canonical_name: key.canonical_name.clone(),
                overload: key.overload.clone(),
            }
        })?;
        if implementation.expected_state_format != selected.state_format {
            return Err(
                ExecutionFunctionSetError::AggregateImplementationStateFormatMismatch {
                    canonical_name: key.canonical_name,
                    overload: key.overload,
                    metadata: selected.state_format.clone(),
                    implementation: implementation.expected_state_format.clone(),
                },
            );
        }
        implementation
            .family
            .prepare(context)
            .map(|kernel| PreparedAggregateKernel { kernel })
            .map_err(|message| ExecutionFunctionSetError::PrepareAggregate {
                canonical_name: key.canonical_name,
                overload: key.overload,
                message: message.into(),
            })
    }
}

fn digest_aggregate_implementation_manifest(
    implementations: &BTreeMap<AggregateImplementationKey, AggregateImplementationBinding>,
) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(EXECUTION_IMPLEMENTATION_MANIFEST_DOMAIN);
    hasher.update(
        u32::try_from(implementations.len())
            .expect("aggregate implementation count fits u32")
            .to_be_bytes(),
    );
    for (key, implementation) in implementations {
        digest_manifest_text(&mut hasher, &key.canonical_name);
        digest_manifest_text(&mut hasher, key.overload.as_str());
        digest_manifest_text(&mut hasher, implementation.implementation.as_str());
        digest_manifest_text(&mut hasher, implementation.expected_state_format.as_str());
    }
    hasher.finalize().into()
}

fn digest_manifest_text(hasher: &mut Sha256, value: &str) {
    hasher.update(
        u32::try_from(value.len())
            .expect("aggregate implementation manifest text length fits u32")
            .to_be_bytes(),
    );
    hasher.update(value.as_bytes());
}

/// All values needed while binding one aggregate kernel. The typed provider
/// contract receives only `selected` and `options`; legacy execution details
/// remain private to the compatibility adapter.
pub(crate) struct AggregatePrepareContext<'a> {
    pub(crate) selected: &'a ResolvedAggregateSignature,
    pub(crate) options: &'a AggregateBindOptions,
    pub(crate) legacy: Option<LegacyAggregateBindContext<'a>>,
}

#[cfg(test)]
impl<'a> AggregatePrepareContext<'a> {
    pub(crate) fn typed(
        selected: &'a ResolvedAggregateSignature,
        options: &'a AggregateBindOptions,
    ) -> Self {
        Self {
            selected,
            options,
            legacy: None,
        }
    }
}

#[derive(Clone, Copy)]
pub(crate) struct LegacyAggregateBindContext<'a> {
    pub(crate) function: &'a AggFunction,
    pub(crate) evaluated_input_type: Option<&'a DataType>,
    pub(crate) input_is_intermediate: bool,
}

#[derive(Clone)]
pub(crate) struct PreparedAggregateKernel {
    kernel: Arc<dyn ErasedAggregateKernel>,
}

impl fmt::Debug for PreparedAggregateKernel {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PreparedAggregateKernel")
            .field("state_layout", &self.state_layout())
            .finish_non_exhaustive()
    }
}

impl PreparedAggregateKernel {
    pub(crate) fn state_layout(&self) -> Layout {
        self.kernel.state_layout()
    }

    pub(crate) fn retained_memory_policy(&self) -> RetainedMemoryPolicy {
        self.kernel.retained_memory_policy()
    }

    /// `base + offset` must denote a properly aligned, uninitialized slot of
    /// at least `state_layout().size()` bytes and must be dropped exactly once
    /// after successful initialization.
    pub(crate) unsafe fn init_state(
        &self,
        base: AggStatePtr,
        offset: usize,
    ) -> Result<(), PreparedAggregateError> {
        unsafe { self.kernel.init_state(base, offset, None) }
    }

    pub(crate) unsafe fn init_state_with_tracker(
        &self,
        base: AggStatePtr,
        offset: usize,
        tracker: Arc<MemTracker>,
    ) -> Result<(), PreparedAggregateError> {
        unsafe { self.kernel.init_state(base, offset, Some(tracker)) }
    }

    /// `base + offset` must contain a state initialized by this kernel.
    pub(crate) unsafe fn drop_state(&self, base: AggStatePtr, offset: usize) {
        unsafe { self.kernel.drop_state(base, offset) }
    }

    /// Every address must contain a state initialized by this kernel. Repeated
    /// addresses are valid: the adapter borrows one state at a time.
    pub(crate) unsafe fn update_batch(
        &self,
        offset: usize,
        state_ptrs: &[AggStatePtr],
        input: AggregateInputBatch<'_>,
    ) -> Result<(), PreparedAggregateError> {
        unsafe { self.kernel.update_batch(offset, state_ptrs, input) }
    }

    /// Every address must contain a state initialized by this kernel. Repeated
    /// addresses are valid: the adapter borrows one state at a time.
    pub(crate) unsafe fn merge_batch(
        &self,
        offset: usize,
        state_ptrs: &[AggStatePtr],
        input: AggregateInputBatch<'_>,
    ) -> Result<(), PreparedAggregateError> {
        unsafe { self.kernel.merge_batch(offset, state_ptrs, input) }
    }

    pub(crate) unsafe fn build_intermediate(
        &self,
        offset: usize,
        group_states: &[AggStatePtr],
    ) -> Result<ArrayRef, PreparedAggregateError> {
        unsafe { self.kernel.build_intermediate(offset, group_states) }
    }

    pub(crate) unsafe fn build_final(
        &self,
        offset: usize,
        group_states: &[AggStatePtr],
    ) -> Result<ArrayRef, PreparedAggregateError> {
        unsafe { self.kernel.build_final(offset, group_states) }
    }

    pub(crate) unsafe fn retained_bytes(&self, base: AggStatePtr, offset: usize) -> usize {
        unsafe { self.kernel.retained_bytes(base, offset) }
    }
}

pub(crate) trait ErasedAggregateFamily: Send + Sync {
    fn prepare(
        &self,
        context: &AggregatePrepareContext<'_>,
    ) -> Result<Arc<dyn ErasedAggregateKernel>, String>;
}

pub(crate) trait ErasedAggregateKernel: Send + Sync {
    fn state_layout(&self) -> Layout;

    fn retained_memory_policy(&self) -> RetainedMemoryPolicy;

    unsafe fn init_state(
        &self,
        base: AggStatePtr,
        offset: usize,
        tracker: Option<Arc<MemTracker>>,
    ) -> Result<(), PreparedAggregateError>;

    unsafe fn drop_state(&self, base: AggStatePtr, offset: usize);

    unsafe fn update_batch(
        &self,
        offset: usize,
        state_ptrs: &[AggStatePtr],
        input: AggregateInputBatch<'_>,
    ) -> Result<(), PreparedAggregateError>;

    unsafe fn merge_batch(
        &self,
        offset: usize,
        state_ptrs: &[AggStatePtr],
        input: AggregateInputBatch<'_>,
    ) -> Result<(), PreparedAggregateError>;

    unsafe fn build_intermediate(
        &self,
        offset: usize,
        group_states: &[AggStatePtr],
    ) -> Result<ArrayRef, PreparedAggregateError>;

    unsafe fn build_final(
        &self,
        offset: usize,
        group_states: &[AggStatePtr],
    ) -> Result<ArrayRef, PreparedAggregateError>;

    unsafe fn retained_bytes(&self, base: AggStatePtr, offset: usize) -> usize;
}

struct TypedFamilyAdapter<F> {
    family: Arc<F>,
}

impl<F> ErasedAggregateFamily for TypedFamilyAdapter<F>
where
    F: TypedAggregateFamily,
{
    fn prepare(
        &self,
        context: &AggregatePrepareContext<'_>,
    ) -> Result<Arc<dyn ErasedAggregateKernel>, String> {
        self.family
            .prepare(context.selected, context.options)
            .map(|kernel| Arc::new(TypedKernelAdapter { kernel }) as Arc<dyn ErasedAggregateKernel>)
            .map_err(|error| error.to_string())
    }
}

struct LegacyAggregateFamilyAdapter {
    function: &'static dyn AggregateFunction,
}

impl ErasedAggregateFamily for LegacyAggregateFamilyAdapter {
    fn prepare(
        &self,
        context: &AggregatePrepareContext<'_>,
    ) -> Result<Arc<dyn ErasedAggregateKernel>, String> {
        let legacy = context
            .legacy
            .ok_or_else(|| "legacy aggregate bind context is required".to_string())?;
        let planned_types = legacy
            .function
            .types
            .as_ref()
            .ok_or_else(|| "aggregate type signature is required".to_string())?;
        let planned_output = planned_types
            .output_type
            .as_ref()
            .ok_or_else(|| "aggregate output_type signature is required".to_string())?;
        let output_matches_catalog = planned_output == &context.selected.output_type;
        let output_matches_local_intermediate =
            !legacy.input_is_intermediate && planned_output == &context.selected.intermediate_type;
        if !output_matches_catalog && !output_matches_local_intermediate {
            return Err(format!(
                "aggregate output type drift: planned={planned_output:?}, catalog final={:?}, catalog intermediate={:?}",
                context.selected.output_type, context.selected.intermediate_type
            ));
        }
        if let Some(planned_intermediate) = planned_types.intermediate_type.as_ref()
            && planned_intermediate != &context.selected.intermediate_type
        {
            return Err(format!(
                "aggregate intermediate type drift: planned={planned_intermediate:?}, catalog={:?}",
                context.selected.intermediate_type
            ));
        }
        if let Some(planned_input) = planned_types.input_arg_type.as_ref()
            && context.selected.argument_types.first() != Some(planned_input)
        {
            return Err(format!(
                "aggregate input type drift: planned={planned_input:?}, catalog first argument={:?}",
                context.selected.argument_types.first()
            ));
        }

        // The catalog selection is the only executable type authority. The
        // legacy implementation adapter receives a normalized copy so its
        // historical optional type carrier cannot override that authority.
        let mut exact_function = legacy.function.clone();
        exact_function.types = Some(AggTypeSignature {
            intermediate_type: Some(context.selected.intermediate_type.clone()),
            output_type: Some(context.selected.output_type.clone()),
            input_arg_type: context.selected.argument_types.first().cloned(),
        });
        let mut spec = self.function.build_spec_from_type(
            &exact_function,
            legacy.evaluated_input_type,
            legacy.input_is_intermediate,
        )?;
        spec.input_arg_type = context.selected.argument_types.first().cloned();
        if spec.intermediate_type != context.selected.intermediate_type
            || spec.output_type != context.selected.output_type
        {
            return Err(format!(
                "legacy aggregate type drift: implementation intermediate={:?}, output={:?}; catalog intermediate={:?}, output={:?}",
                spec.intermediate_type,
                spec.output_type,
                context.selected.intermediate_type,
                context.selected.output_type
            ));
        }
        Ok(Arc::new(LegacyKernelAdapter {
            function: self.function,
            spec,
        }))
    }
}

struct LegacyKernelAdapter {
    function: &'static dyn AggregateFunction,
    spec: AggSpec,
}

impl ErasedAggregateKernel for LegacyKernelAdapter {
    fn state_layout(&self) -> Layout {
        let (size, align) = self.function.state_layout_for(&self.spec.kind);
        Layout::from_size_align(size, align).expect("legacy aggregate state layout must be valid")
    }

    fn retained_memory_policy(&self) -> RetainedMemoryPolicy {
        self.function.retained_memory_policy(&self.spec)
    }

    unsafe fn init_state(
        &self,
        base: AggStatePtr,
        offset: usize,
        tracker: Option<Arc<MemTracker>>,
    ) -> Result<(), PreparedAggregateError> {
        let pointer = (base as *mut u8).wrapping_add(offset);
        self.function
            .init_state_with_tracker(&self.spec, pointer, tracker)
            .map_err(PreparedAggregateError::CreateState)
    }

    unsafe fn drop_state(&self, base: AggStatePtr, offset: usize) {
        let pointer = (base as *mut u8).wrapping_add(offset);
        self.function.drop_state(&self.spec, pointer);
    }

    unsafe fn update_batch(
        &self,
        offset: usize,
        state_ptrs: &[AggStatePtr],
        input: AggregateInputBatch<'_>,
    ) -> Result<(), PreparedAggregateError> {
        validate_state_pointer_count(state_ptrs, input.row_count())?;
        let values = input.array_ref().cloned();
        let view = self
            .function
            .build_input_view(&self.spec, &values)
            .map_err(PreparedAggregateError::Input)?;
        self.function
            .update_batch(&self.spec, offset, state_ptrs, &view)
            .map_err(PreparedAggregateError::Update)
    }

    unsafe fn merge_batch(
        &self,
        offset: usize,
        state_ptrs: &[AggStatePtr],
        input: AggregateInputBatch<'_>,
    ) -> Result<(), PreparedAggregateError> {
        validate_state_pointer_count(state_ptrs, input.row_count())?;
        let values = input.array_ref().cloned();
        let view = self
            .function
            .build_merge_view(&self.spec, &values)
            .map_err(PreparedAggregateError::Input)?;
        self.function
            .merge_batch(&self.spec, offset, state_ptrs, &view)
            .map_err(PreparedAggregateError::Merge)
    }

    unsafe fn build_intermediate(
        &self,
        offset: usize,
        group_states: &[AggStatePtr],
    ) -> Result<ArrayRef, PreparedAggregateError> {
        self.function
            .build_array(&self.spec, offset, group_states, true)
            .map_err(PreparedAggregateError::BuildIntermediate)
    }

    unsafe fn build_final(
        &self,
        offset: usize,
        group_states: &[AggStatePtr],
    ) -> Result<ArrayRef, PreparedAggregateError> {
        self.function
            .build_array(&self.spec, offset, group_states, false)
            .map_err(PreparedAggregateError::BuildFinal)
    }

    unsafe fn retained_bytes(&self, base: AggStatePtr, offset: usize) -> usize {
        let pointer = (base as *const u8).wrapping_add(offset);
        self.function.retained_bytes(&self.spec, pointer)
    }
}

struct TypedKernelAdapter<K> {
    kernel: K,
}

impl<K> ErasedAggregateKernel for TypedKernelAdapter<K>
where
    K: TypedAggregateKernel,
{
    fn state_layout(&self) -> Layout {
        Layout::new::<K::State>()
    }

    fn retained_memory_policy(&self) -> RetainedMemoryPolicy {
        match self.kernel.memory_policy() {
            AggregateStateMemoryPolicy::FixedZero => RetainedMemoryPolicy::FixedZero,
            AggregateStateMemoryPolicy::BoundedRetained {
                max_retained_bytes_per_state,
            } => RetainedMemoryPolicy::BoundedRetained {
                max_retained_bytes_per_state,
            },
        }
    }

    unsafe fn init_state(
        &self,
        base: AggStatePtr,
        offset: usize,
        _tracker: Option<Arc<MemTracker>>,
    ) -> Result<(), PreparedAggregateError> {
        let state = self
            .kernel
            .create_state()
            .map_err(|error| PreparedAggregateError::CreateState(error.to_string()))?;
        let slot = state_slot::<K::State>(base, offset);
        unsafe { slot.as_ptr().write(state) };
        Ok(())
    }

    unsafe fn drop_state(&self, base: AggStatePtr, offset: usize) {
        let slot = state_slot::<K::State>(base, offset);
        unsafe { slot.as_ptr().drop_in_place() };
    }

    unsafe fn update_batch(
        &self,
        offset: usize,
        state_ptrs: &[AggStatePtr],
        input: AggregateInputBatch<'_>,
    ) -> Result<(), PreparedAggregateError> {
        validate_state_pointer_count(state_ptrs, input.row_count())?;
        let prepared = self
            .kernel
            .prepare_update(&input)
            .map_err(|error| PreparedAggregateError::Input(error.to_string()))?;
        for (row, base) in state_ptrs.iter().copied().enumerate() {
            // A grouped batch may repeat a state address. Create exactly one
            // mutable reference for this call and let it expire before moving
            // to the next row; never manufacture an aliased mutable slice.
            let state = unsafe { &mut *state_slot::<K::State>(base, offset).as_ptr() };
            self.kernel
                .update_row(state, &prepared, row)
                .map_err(|error| PreparedAggregateError::Update(error.to_string()))?;
        }
        Ok(())
    }

    unsafe fn merge_batch(
        &self,
        offset: usize,
        state_ptrs: &[AggStatePtr],
        input: AggregateInputBatch<'_>,
    ) -> Result<(), PreparedAggregateError> {
        validate_state_pointer_count(state_ptrs, input.row_count())?;
        let prepared = self
            .kernel
            .prepare_merge(&input)
            .map_err(|error| PreparedAggregateError::Input(error.to_string()))?;
        for (row, base) in state_ptrs.iter().copied().enumerate() {
            let state = unsafe { &mut *state_slot::<K::State>(base, offset).as_ptr() };
            self.kernel
                .merge_row(state, &prepared, row)
                .map_err(|error| PreparedAggregateError::Merge(error.to_string()))?;
        }
        Ok(())
    }

    unsafe fn build_intermediate(
        &self,
        offset: usize,
        group_states: &[AggStatePtr],
    ) -> Result<ArrayRef, PreparedAggregateError> {
        let states = TypedStateIter::<K::State>::new(group_states, offset);
        self.kernel
            .build_intermediate(states)
            .map_err(|error| PreparedAggregateError::BuildIntermediate(error.to_string()))
    }

    unsafe fn build_final(
        &self,
        offset: usize,
        group_states: &[AggStatePtr],
    ) -> Result<ArrayRef, PreparedAggregateError> {
        let states = TypedStateIter::<K::State>::new(group_states, offset);
        self.kernel
            .build_final(states)
            .map_err(|error| PreparedAggregateError::BuildFinal(error.to_string()))
    }

    unsafe fn retained_bytes(&self, base: AggStatePtr, offset: usize) -> usize {
        let state = unsafe { &*state_slot::<K::State>(base, offset).as_ptr() };
        self.kernel.retained_bytes(state)
    }
}

fn validate_state_pointer_count(
    state_ptrs: &[AggStatePtr],
    row_count: usize,
) -> Result<(), PreparedAggregateError> {
    if state_ptrs.len() != row_count {
        return Err(PreparedAggregateError::StatePointerCount {
            expected: row_count,
            actual: state_ptrs.len(),
        });
    }
    Ok(())
}

fn canonicalize_function_name(name: &str) -> Result<Box<str>, ExecutionFunctionSetError> {
    let canonical_name = name.to_ascii_lowercase();
    let valid = !canonical_name.is_empty()
        && canonical_name.bytes().all(|byte| {
            byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(byte, b'_' | b'$')
        });
    if !valid {
        return Err(ExecutionFunctionSetError::InvalidCanonicalName { name: name.into() });
    }
    Ok(canonical_name.into_boxed_str())
}

fn state_slot<S>(base: AggStatePtr, offset: usize) -> NonNull<S> {
    let pointer = (base as *mut u8).wrapping_add(offset).cast::<S>();
    NonNull::new(pointer).expect("aggregate state base must be non-null")
}

struct TypedStateIter<'state, S> {
    bases: std::slice::Iter<'state, AggStatePtr>,
    offset: usize,
    _state: PhantomData<&'state S>,
}

impl<'state, S> TypedStateIter<'state, S> {
    fn new(group_states: &'state [AggStatePtr], offset: usize) -> Self {
        Self {
            bases: group_states.iter(),
            offset,
            _state: PhantomData,
        }
    }
}

impl<'state, S> Iterator for TypedStateIter<'state, S> {
    type Item = &'state S;

    fn next(&mut self) -> Option<Self::Item> {
        let base = *self.bases.next()?;
        // SAFETY: construction and use are confined to the erased adapter. Its
        // caller guarantees that every slot contains an initialized `S` for
        // the duration of this iterator. Immutable aliases are permitted.
        Some(unsafe { &*state_slot::<S>(base, self.offset).as_ptr() })
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.bases.size_hint()
    }
}

impl<S> ExactSizeIterator for TypedStateIter<'_, S> {
    fn len(&self) -> usize {
        self.bases.len()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    use arrow::array::{Array, Int64Array};
    use arrow::datatypes::DataType;
    use novarocks_functions::{
        AggregateOverloadDeclaration, AggregateOverloadMetadata, AggregateStateFormatIdentity,
        FunctionDefinition, FunctionResolutionError, FunctionSignatureResolver, FunctionVisibility,
        FunctionVolatility, ResolvedFunctionSignature,
    };

    use super::*;
    use crate::exec::expr::agg::AggStateArena;

    const FUNCTION_NAME: &str = "$typed_sum";
    const OVERLOAD_ID: &str = "typed-sum/i64/v1";
    const STATE_FORMAT: &str = "typed-sum-state/i64/v1";

    #[derive(Debug)]
    struct TestError(&'static str);

    impl fmt::Display for TestError {
        fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            formatter.write_str(self.0)
        }
    }

    struct SumState {
        value: i64,
        drops: Arc<AtomicUsize>,
    }

    impl Drop for SumState {
        fn drop(&mut self) {
            self.drops.fetch_add(1, Ordering::SeqCst);
        }
    }

    struct SumKernel {
        drops: Arc<AtomicUsize>,
    }

    impl TypedAggregateKernel for SumKernel {
        type State = SumState;
        type PreparedUpdateBatch<'batch> = &'batch Int64Array;
        type PreparedMergeBatch<'batch> = &'batch Int64Array;
        type Error = TestError;

        fn memory_policy(&self) -> AggregateStateMemoryPolicy {
            AggregateStateMemoryPolicy::FixedZero
        }

        fn create_state(&self) -> Result<Self::State, Self::Error> {
            Ok(SumState {
                value: 0,
                drops: Arc::clone(&self.drops),
            })
        }

        fn prepare_update<'batch>(
            &self,
            input: &'batch AggregateInputBatch<'batch>,
        ) -> Result<Self::PreparedUpdateBatch<'batch>, Self::Error> {
            input
                .values()
                .and_then(|values| values.as_any().downcast_ref::<Int64Array>())
                .ok_or(TestError("expected Int64 update input"))
        }

        fn update_row<'batch>(
            &self,
            state: &mut Self::State,
            prepared: &Self::PreparedUpdateBatch<'batch>,
            row: usize,
        ) -> Result<(), Self::Error> {
            if !prepared.is_null(row) {
                state.value += prepared.value(row);
            }
            Ok(())
        }

        fn prepare_merge<'batch>(
            &self,
            input: &'batch AggregateInputBatch<'batch>,
        ) -> Result<Self::PreparedMergeBatch<'batch>, Self::Error> {
            self.prepare_update(input)
        }

        fn merge_row<'batch>(
            &self,
            state: &mut Self::State,
            prepared: &Self::PreparedMergeBatch<'batch>,
            row: usize,
        ) -> Result<(), Self::Error> {
            self.update_row(state, prepared, row)
        }

        fn build_intermediate<'state, I>(&self, states: I) -> Result<ArrayRef, Self::Error>
        where
            Self::State: 'state,
            I: ExactSizeIterator<Item = &'state Self::State>,
        {
            Ok(Arc::new(Int64Array::from_iter_values(
                states.map(|state| state.value),
            )))
        }

        fn build_final<'state, I>(&self, states: I) -> Result<ArrayRef, Self::Error>
        where
            Self::State: 'state,
            I: ExactSizeIterator<Item = &'state Self::State>,
        {
            self.build_intermediate(states)
        }

        fn retained_bytes(&self, _state: &Self::State) -> usize {
            0
        }
    }

    struct SumFamily {
        overloads: Box<[AggregateOverloadDeclaration]>,
        binds: Arc<AtomicUsize>,
        drops: Arc<AtomicUsize>,
        saw_distinct: Arc<AtomicBool>,
    }

    impl SumFamily {
        fn new(binds: Arc<AtomicUsize>, drops: Arc<AtomicUsize>) -> Self {
            Self::new_with_probe(binds, drops, Arc::new(AtomicBool::new(false)))
        }

        fn new_with_probe(
            binds: Arc<AtomicUsize>,
            drops: Arc<AtomicUsize>,
            saw_distinct: Arc<AtomicBool>,
        ) -> Self {
            Self {
                overloads: vec![overload_declaration()].into_boxed_slice(),
                binds,
                drops,
                saw_distinct,
            }
        }
    }

    impl TypedAggregateFamily for SumFamily {
        type Kernel = SumKernel;
        type PrepareError = TestError;

        fn overloads(&self) -> &[AggregateOverloadDeclaration] {
            &self.overloads
        }

        fn resolve_signature(
            &self,
            argument_types: &[DataType],
        ) -> Result<ResolvedAggregateSignature, FunctionResolutionError> {
            if argument_types != [DataType::Int64] {
                return Err(FunctionResolutionError::NoMatchingSignature {
                    candidates: 1,
                    binding_enforced: true,
                });
            }
            Ok(resolved_signature())
        }

        fn prepare(
            &self,
            _selected: &ResolvedAggregateSignature,
            options: &AggregateBindOptions,
        ) -> Result<Self::Kernel, Self::PrepareError> {
            self.binds.fetch_add(1, Ordering::SeqCst);
            self.saw_distinct
                .store(options.distinct(), Ordering::SeqCst);
            Ok(SumKernel {
                drops: Arc::clone(&self.drops),
            })
        }
    }

    fn overload_identity() -> AggregateOverloadIdentity {
        AggregateOverloadIdentity::try_new(OVERLOAD_ID).unwrap()
    }

    fn overload_declaration() -> AggregateOverloadDeclaration {
        AggregateOverloadDeclaration::try_new(OVERLOAD_ID, "(i64)", "i64", "i64", STATE_FORMAT)
            .unwrap()
    }

    fn overload_metadata() -> AggregateOverloadMetadata {
        AggregateOverloadMetadata::try_new(
            OVERLOAD_ID,
            [DataType::Int64],
            DataType::Int64,
            DataType::Int64,
            STATE_FORMAT,
        )
        .unwrap()
    }

    fn resolved_signature() -> ResolvedAggregateSignature {
        ResolvedAggregateSignature {
            overload: overload_identity(),
            argument_types: vec![DataType::Int64],
            intermediate_type: DataType::Int64,
            output_type: DataType::Int64,
            state_format: AggregateStateFormatIdentity::try_new(STATE_FORMAT).unwrap(),
        }
    }

    fn typed_registration(
        binds: Arc<AtomicUsize>,
        drops: Arc<AtomicUsize>,
    ) -> TypedAggregateRegistration<SumFamily> {
        typed_registration_with_identity("typed-sum/exec-v1", binds, drops)
    }

    fn typed_registration_with_identity(
        implementation: &str,
        binds: Arc<AtomicUsize>,
        drops: Arc<AtomicUsize>,
    ) -> TypedAggregateRegistration<SumFamily> {
        TypedAggregateRegistration::try_new(
            FUNCTION_NAME,
            FunctionVisibility::Hidden,
            FunctionVolatility::Immutable,
            AggregateImplementationIdentity::try_new(implementation).unwrap(),
            SumFamily::new(binds, drops),
        )
        .unwrap()
    }

    struct ScalarResolver;

    impl FunctionSignatureResolver for ScalarResolver {
        fn resolve(
            &self,
            argument_types: &[DataType],
        ) -> Result<ResolvedFunctionSignature, FunctionResolutionError> {
            Ok(ResolvedFunctionSignature {
                return_type: DataType::Int64,
                argument_types: argument_types.to_vec(),
                enforce_argument_binding: true,
            })
        }
    }

    fn add_scalar_metadata(builder: &mut ExecutionFunctionSetBuilder) {
        builder
            .catalog_builder_mut()
            .register(
                FunctionDefinition::try_new(
                    "scalar_for_test",
                    FunctionKind::Scalar,
                    FunctionVisibility::Public,
                    FunctionVolatility::Immutable,
                    ["(i64)->i64"],
                    Arc::new(ScalarResolver),
                )
                .unwrap(),
            )
            .unwrap();
    }

    #[test]
    fn seal_rejects_aggregate_metadata_without_implementation() {
        let mut builder = ExecutionFunctionSetBuilder::new();
        builder
            .catalog_builder_mut()
            .register(
                FunctionDefinition::try_new_exact_aggregate(
                    "missing_impl",
                    FunctionVisibility::Public,
                    FunctionVolatility::Immutable,
                    [overload_metadata()],
                )
                .unwrap(),
            )
            .unwrap();

        assert!(matches!(
            builder.seal(),
            Err(ExecutionFunctionSetError::AggregateMetadataWithoutImplementation { .. })
        ));
    }

    #[test]
    fn seal_rejects_metadata_only_extra_overload() {
        let extra_overload = AggregateOverloadMetadata::try_new(
            "typed-sum/i32/v1",
            [DataType::Int32],
            DataType::Int64,
            DataType::Int64,
            STATE_FORMAT,
        )
        .unwrap();
        let mut builder = ExecutionFunctionSetBuilder::new();
        builder
            .catalog_builder_mut()
            .register(
                FunctionDefinition::try_new_exact_aggregate(
                    FUNCTION_NAME,
                    FunctionVisibility::Hidden,
                    FunctionVolatility::Immutable,
                    [overload_metadata(), extra_overload],
                )
                .unwrap(),
            )
            .unwrap();
        let family: Arc<dyn ErasedAggregateFamily> = Arc::new(TypedFamilyAdapter {
            family: Arc::new(SumFamily::new(
                Arc::new(AtomicUsize::new(0)),
                Arc::new(AtomicUsize::new(0)),
            )),
        });
        builder
            .register_legacy_aggregate_family(
                FUNCTION_NAME,
                [(
                    overload_identity(),
                    AggregateStateFormatIdentity::try_new(STATE_FORMAT).unwrap(),
                )],
                AggregateImplementationIdentity::try_new("typed-sum/exec-v1").unwrap(),
                family,
            )
            .unwrap();

        assert!(matches!(
            builder.seal(),
            Err(ExecutionFunctionSetError::AggregateMetadataWithoutImplementation {
                overload,
                ..
            }) if overload.as_str() == "typed-sum/i32/v1"
        ));
    }

    #[test]
    fn seal_rejects_implementation_without_metadata_and_duplicate_implementation() {
        let binds = Arc::new(AtomicUsize::new(0));
        let drops = Arc::new(AtomicUsize::new(0));
        let family: Arc<dyn ErasedAggregateFamily> = Arc::new(TypedFamilyAdapter {
            family: Arc::new(SumFamily::new(binds, drops)),
        });
        let mut builder = ExecutionFunctionSetBuilder::new();
        add_scalar_metadata(&mut builder);
        builder
            .register_legacy_aggregate_family(
                FUNCTION_NAME,
                [(overload_identity(), resolved_signature().state_format)],
                AggregateImplementationIdentity::try_new("typed-sum/exec-v1").unwrap(),
                Arc::clone(&family),
            )
            .unwrap();
        assert!(matches!(
            builder.register_legacy_aggregate_family(
                FUNCTION_NAME,
                [(overload_identity(), resolved_signature().state_format)],
                AggregateImplementationIdentity::try_new("typed-sum/exec-v1").unwrap(),
                family,
            ),
            Err(ExecutionFunctionSetError::DuplicateAggregateImplementation { .. })
        ));
        assert!(matches!(
            builder.seal(),
            Err(ExecutionFunctionSetError::AggregateImplementationWithoutMetadata { .. })
        ));
    }

    #[test]
    fn seal_rejects_implementation_state_format_drift() {
        let binds = Arc::new(AtomicUsize::new(0));
        let drops = Arc::new(AtomicUsize::new(0));
        let family: Arc<dyn ErasedAggregateFamily> = Arc::new(TypedFamilyAdapter {
            family: Arc::new(SumFamily::new(binds, drops)),
        });
        let mut builder = ExecutionFunctionSetBuilder::new();
        builder
            .catalog_builder_mut()
            .register(
                FunctionDefinition::try_new_exact_aggregate(
                    FUNCTION_NAME,
                    FunctionVisibility::Hidden,
                    FunctionVolatility::Immutable,
                    [overload_metadata()],
                )
                .unwrap(),
            )
            .unwrap();
        builder
            .register_legacy_aggregate_family(
                FUNCTION_NAME,
                [(
                    overload_identity(),
                    AggregateStateFormatIdentity::try_new("typed-sum-state/v2").unwrap(),
                )],
                AggregateImplementationIdentity::try_new("typed-sum/exec-v2").unwrap(),
                family,
            )
            .unwrap();

        assert!(matches!(
            builder.seal(),
            Err(
                ExecutionFunctionSetError::AggregateImplementationStateFormatMismatch {
                    metadata,
                    implementation,
                    ..
                }
            ) if metadata.as_str() == STATE_FORMAT
                && implementation.as_str() == "typed-sum-state/v2"
        ));
    }

    #[test]
    fn implementation_manifest_digest_tracks_only_executable_contracts() {
        let build = |implementation: &str, with_scalar: bool| {
            let mut builder = ExecutionFunctionSetBuilder::new();
            builder
                .register_typed_aggregate(typed_registration_with_identity(
                    implementation,
                    Arc::new(AtomicUsize::new(0)),
                    Arc::new(AtomicUsize::new(0)),
                ))
                .unwrap();
            if with_scalar {
                add_scalar_metadata(&mut builder);
            }
            builder.seal().unwrap()
        };

        let baseline = build("typed-sum/exec-v1", false);
        let catalog_only = build("typed-sum/exec-v1", true);
        let implementation_only = build("typed-sum/exec-v2", false);

        assert_ne!(baseline.catalog().digest(), catalog_only.catalog().digest());
        assert_eq!(
            baseline.implementation_manifest_digest(),
            catalog_only.implementation_manifest_digest()
        );
        assert_eq!(
            baseline.catalog().digest(),
            implementation_only.catalog().digest()
        );
        assert_ne!(
            baseline.implementation_manifest_digest(),
            implementation_only.implementation_manifest_digest()
        );
    }

    #[test]
    fn hidden_typed_family_binds_only_through_trusted_resolution() {
        let binds = Arc::new(AtomicUsize::new(0));
        let drops = Arc::new(AtomicUsize::new(0));
        let mut builder = ExecutionFunctionSetBuilder::new();
        builder
            .register_typed_aggregate(typed_registration(binds.clone(), drops))
            .unwrap();
        let functions = builder.seal().unwrap();

        assert!(matches!(
            functions
                .catalog()
                .resolve_aggregate_user(FUNCTION_NAME, &[DataType::Int64]),
            Err(FunctionResolutionError::HiddenFunction)
        ));
        let selected = functions
            .catalog()
            .resolve_aggregate_trusted(FUNCTION_NAME, &[DataType::Int64])
            .unwrap();
        functions
            .prepare_resolved_aggregate(
                FUNCTION_NAME,
                &AggregatePrepareContext::typed(&selected, &AggregateBindOptions::default()),
            )
            .unwrap();
        assert_eq!(binds.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn typed_adapter_updates_repeated_state_pointer_and_drops_once() {
        let binds = Arc::new(AtomicUsize::new(0));
        let drops = Arc::new(AtomicUsize::new(0));
        let mut builder = ExecutionFunctionSetBuilder::new();
        builder
            .register_typed_aggregate(typed_registration(binds, Arc::clone(&drops)))
            .unwrap();
        let functions = builder.seal().unwrap();
        let selected = functions
            .catalog()
            .resolve_aggregate_trusted(FUNCTION_NAME, &[DataType::Int64])
            .unwrap();
        let kernel = functions
            .prepare_resolved_aggregate(
                FUNCTION_NAME,
                &AggregatePrepareContext::typed(&selected, &AggregateBindOptions::default()),
            )
            .unwrap();
        let layout = kernel.state_layout();
        let mut arena = AggStateArena::new(layout.size());
        let base = arena.alloc(layout.size(), layout.align());
        unsafe { kernel.init_state(base, 0).unwrap() };

        let values: ArrayRef = Arc::new(Int64Array::from(vec![1, 2, 3]));
        let input = AggregateInputBatch::try_new(Some(&values), 3).unwrap();
        unsafe { kernel.update_batch(0, &[base, base, base], input).unwrap() };
        let result = unsafe { kernel.build_final(0, &[base]).unwrap() };
        let result = result.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(result.value(0), 6);
        assert_eq!(unsafe { kernel.retained_bytes(base, 0) }, 0);

        unsafe { kernel.drop_state(base, 0) };
        assert_eq!(drops.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn family_bind_is_not_repeated_for_groups_or_batches() {
        let binds = Arc::new(AtomicUsize::new(0));
        let drops = Arc::new(AtomicUsize::new(0));
        let mut builder = ExecutionFunctionSetBuilder::new();
        builder
            .register_typed_aggregate(typed_registration(Arc::clone(&binds), Arc::clone(&drops)))
            .unwrap();
        let functions = builder.seal().unwrap();
        let kernel = functions
            .prepare_resolved_aggregate(
                FUNCTION_NAME,
                &AggregatePrepareContext::typed(
                    &resolved_signature(),
                    &AggregateBindOptions::default(),
                ),
            )
            .unwrap();
        assert_eq!(binds.load(Ordering::SeqCst), 1);

        let layout = kernel.state_layout();
        let mut arena = AggStateArena::new(layout.size() * 2);
        let first = arena.alloc(layout.size(), layout.align());
        let second = arena.alloc(layout.size(), layout.align());
        unsafe {
            kernel.init_state(first, 0).unwrap();
            kernel.init_state(second, 0).unwrap();
        }
        let first_values: ArrayRef = Arc::new(Int64Array::from(vec![1, 2]));
        let second_values: ArrayRef = Arc::new(Int64Array::from(vec![3, 4]));
        unsafe {
            kernel
                .update_batch(
                    0,
                    &[first, second],
                    AggregateInputBatch::try_new(Some(&first_values), 2).unwrap(),
                )
                .unwrap();
            kernel
                .merge_batch(
                    0,
                    &[first, second],
                    AggregateInputBatch::try_new(Some(&second_values), 2).unwrap(),
                )
                .unwrap();
        }
        assert_eq!(binds.load(Ordering::SeqCst), 1);

        unsafe {
            kernel.drop_state(first, 0);
            kernel.drop_state(second, 0);
        }
        assert_eq!(drops.load(Ordering::SeqCst), 2);
    }

    #[test]
    fn kernel_entry_executes_only_the_prepared_kernel() {
        let binds = Arc::new(AtomicUsize::new(0));
        let drops = Arc::new(AtomicUsize::new(0));
        let mut builder = ExecutionFunctionSetBuilder::new();
        builder
            .register_typed_aggregate(typed_registration(Arc::clone(&binds), Arc::clone(&drops)))
            .unwrap();
        let functions = builder.seal().unwrap();
        let selected = functions
            .catalog()
            .resolve_aggregate_trusted(FUNCTION_NAME, &[DataType::Int64])
            .unwrap();
        let kernels = crate::exec::expr::agg::build_kernel_set(
            &functions,
            &[AggFunction {
                name: FUNCTION_NAME.to_string(),
                ..Default::default()
            }],
            &[Some(DataType::Int64)],
            &[selected],
        )
        .unwrap();
        assert_eq!(binds.load(Ordering::SeqCst), 1);

        let kernel = &kernels.entries[0];
        let mut arena = AggStateArena::new(kernels.layout.total_size);
        let base = arena.alloc(kernels.layout.total_size, kernel.state_align());
        kernel.init_state(base).unwrap();
        let values: ArrayRef = Arc::new(Int64Array::from(vec![2, 5]));
        kernel
            .update_batch(
                &[base, base],
                AggregateInputBatch::try_new(Some(&values), 2).unwrap(),
            )
            .unwrap();
        let result = kernel.build_array(&[base], false).unwrap();
        let result = result.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(result.value(0), 7);
        kernel.drop_state(base);

        assert_eq!(binds.load(Ordering::SeqCst), 1);
        assert_eq!(drops.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn safe_bind_options_reach_typed_family() {
        let binds = Arc::new(AtomicUsize::new(0));
        let drops = Arc::new(AtomicUsize::new(0));
        let saw_distinct = Arc::new(AtomicBool::new(false));
        let family =
            SumFamily::new_with_probe(Arc::clone(&binds), drops, Arc::clone(&saw_distinct));
        let registration = TypedAggregateRegistration::try_new(
            FUNCTION_NAME,
            FunctionVisibility::Hidden,
            FunctionVolatility::Immutable,
            AggregateImplementationIdentity::try_new("typed-sum/exec-v1").unwrap(),
            family,
        )
        .unwrap();
        let mut builder = ExecutionFunctionSetBuilder::new();
        builder.register_typed_aggregate(registration).unwrap();
        let functions = builder.seal().unwrap();
        let options = AggregateBindOptions::try_new(true, &[false], &[true], Some(1024)).unwrap();
        functions
            .prepare_resolved_aggregate(
                FUNCTION_NAME,
                &AggregatePrepareContext::typed(&resolved_signature(), &options),
            )
            .unwrap();

        assert_eq!(binds.load(Ordering::SeqCst), 1);
        assert!(saw_distinct.load(Ordering::SeqCst));
    }
}
