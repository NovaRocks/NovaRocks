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

//! Generic, provider-side adapters for the transport-neutral read runtime.
//!
//! This is the sole SPI location that recovers a concrete value from an
//! opaque handle.  A connector implements the typed traits below; FE and BE
//! use only `ConnectorRead*` trait objects and therefore cannot downcast a
//! provider payload or acquire a typed accessor after installation.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Debug;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

use futures::Stream;
use futures::future::BoxFuture;

use super::page_source::{
    ConnectorPreparationControl, ConnectorPreparationProgress, ConnectorPreparationStart,
    ConnectorPreparedPageSource,
};

use super::page_stream::{ConnectorPageStream, ConnectorPollBudget, OwnedConnectorPageStream};
use super::runtime::{
    ConnectorAdmittedReadProviderFactory, ConnectorReadBinding, ConnectorReadChangeWindow,
    ConnectorReadColumnBinding, ConnectorReadColumnHandle, ConnectorReadConstraint,
    ConnectorReadDynamicFilter, ConnectorReadDynamicFilterSnapshot, ConnectorReadFilterApplication,
    ConnectorReadLimitApplication, ConnectorReadMetadata, ConnectorReadPageSourceProvider,
    ConnectorReadRelationKind, ConnectorReadRelationVersion, ConnectorReadSplit,
    ConnectorReadSplitFacts, ConnectorReadSplitManager, ConnectorReadSplitSource,
    ConnectorReadSystemTablePlan, ConnectorReadTableHandle, ConnectorReadTransactionHandle,
    binding_error, column_handle, column_value, map_constraint, map_tuple_domain, split_handle,
    split_value, table_handle, table_value, transaction_handle, transaction_value, type_error,
};
use super::{
    Assignment, BoundsMatch, ColumnHandle, ColumnValueBounds, ConnectorExpression,
    ConnectorMvTargetPartitionSelection, ConnectorReadDistribution, ConnectorReadOrderingKey,
    ConnectorReadProperties, ConnectorReadPublicSchema, ConnectorReadStaticFacts, ConnectorSession,
    ConnectorSplit, ConnectorSplitBatch, Constraint, DynamicFilter, DynamicFilterSnapshot,
    PageSourceMetrics, SchemaTableName, SourcePage, SystemTableUnitHint, TupleDomain,
};
use crate::connector::{
    CatalogHandle, ConnectorError, ConnectorExecutionResources, ConnectorInstanceDescriptor,
    ConnectorPinnedFileSet, ConnectorRequestContext,
};

/// One concrete provider type family.  The associated values never escape the
/// adapter: their only cross-role representation is an opaque SPI handle.
pub trait ProviderReadRuntime: Send + Sync + 'static {
    type Table: Debug + Send + Sync + 'static;
    type Column: ColumnHandle;
    type Transaction: Clone + Debug + Send + Sync + 'static;
    type Split: ConnectorSplit;

    fn descriptor(&self) -> &ConnectorInstanceDescriptor;
    fn catalog_handle(&self) -> &CatalogHandle;

    /// The transaction frozen with this exact provider binding.  It is copied
    /// only into an opaque relation at the provider-side creation boundary.
    fn transaction(&self) -> Self::Transaction;
}

#[derive(Clone, Debug)]
pub struct ProviderReadColumnBinding<C> {
    name: Arc<str>,
    column: C,
    hidden: bool,
}

impl<C> ProviderReadColumnBinding<C> {
    pub fn new(name: impl AsRef<str>, column: C, hidden: bool) -> Self {
        Self {
            name: Arc::from(name.as_ref()),
            column,
            hidden,
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub const fn column(&self) -> &C {
        &self.column
    }

    pub const fn hidden(&self) -> bool {
        self.hidden
    }
}

#[derive(Clone, Debug)]
pub struct ProviderReadFilterApplication<T, C: Ord + Clone + Debug> {
    handle: T,
    remaining_constraint: Constraint<C>,
    remaining_expression: Option<ConnectorExpression>,
}

impl<T, C: Ord + Clone + Debug> ProviderReadFilterApplication<T, C> {
    pub const fn new(
        handle: T,
        remaining_constraint: Constraint<C>,
        remaining_expression: Option<ConnectorExpression>,
    ) -> Self {
        Self {
            handle,
            remaining_constraint,
            remaining_expression,
        }
    }

    pub const fn handle(&self) -> &T {
        &self.handle
    }

    pub fn into_handle(self) -> T {
        self.handle
    }

    pub const fn remaining_constraint(&self) -> &Constraint<C> {
        &self.remaining_constraint
    }

    pub const fn remaining_expression(&self) -> Option<&ConnectorExpression> {
        self.remaining_expression.as_ref()
    }
}

pub type ProviderReadFilterResult<T, C> =
    Result<Option<ProviderReadFilterApplication<T, C>>, ConnectorError>;

#[derive(Clone, Debug)]
pub struct ProviderReadLimitApplication<T> {
    handle: T,
    limit_guaranteed: bool,
}

impl<T> ProviderReadLimitApplication<T> {
    pub const fn new(handle: T, limit_guaranteed: bool) -> Self {
        Self {
            handle,
            limit_guaranteed,
        }
    }

    pub const fn handle(&self) -> &T {
        &self.handle
    }

    pub fn into_handle(self) -> T {
        self.handle
    }

    pub const fn limit_guaranteed(&self) -> bool {
        self.limit_guaranteed
    }
}

#[derive(Clone, Debug)]
pub struct ProviderReadSystemTablePlan<T> {
    handle: T,
    unit_hint: SystemTableUnitHint,
}

impl<T> ProviderReadSystemTablePlan<T> {
    pub const fn new(handle: T, unit_hint: SystemTableUnitHint) -> Self {
        Self { handle, unit_hint }
    }

    pub const fn handle(&self) -> &T {
        &self.handle
    }

    pub fn into_handle(self) -> T {
        self.handle
    }

    pub const fn unit_hint(&self) -> SystemTableUnitHint {
        self.unit_hint
    }
}

/// No value of this type can be manufactured for a provider without an
/// exact frozen-source producer. Its adoption port remains unsupported.
#[derive(Debug)]
pub enum UnsupportedFrozenReadSource {}

/// No preparation value exists for a provider which does not implement the
/// frozen-source preparation contract.
pub enum UnsupportedPreparedSourceRead {}

pub struct ProviderPreparedSourceRead<T> {
    source: T,
    admission: Option<crate::connector::ConnectorChangeWindowAdmission>,
    to_metadata: Arc<crate::connector::ConnectorTableMetadata>,
}

impl<T> ProviderPreparedSourceRead<T> {
    pub const fn snapshot(
        source: T,
        to_metadata: Arc<crate::connector::ConnectorTableMetadata>,
    ) -> Self {
        Self {
            source,
            admission: None,
            to_metadata,
        }
    }

    pub const fn change_window(
        source: T,
        admission: crate::connector::ConnectorChangeWindowAdmission,
        to_metadata: Arc<crate::connector::ConnectorTableMetadata>,
    ) -> Self {
        Self {
            source,
            admission: Some(admission),
            to_metadata,
        }
    }
}

pub struct ProviderReadOpenedRelation<T> {
    handle: T,
    unit_hint: Option<SystemTableUnitHint>,
}
impl<T> ProviderReadOpenedRelation<T> {
    pub const fn new(handle: T, unit_hint: Option<SystemTableUnitHint>) -> Self {
        Self { handle, unit_hint }
    }
    pub fn into_parts(self) -> (T, Option<SystemTableUnitHint>) {
        (self.handle, self.unit_hint)
    }
}

pub trait ProviderReadMetadata: ProviderReadRuntime {
    /// Completed resource-free facts own their retained data independently of
    /// the preparing request/provider. Projection borrows a current exact
    /// binding; a receipt does not keep the old request's live capabilities.
    type PreparedSourceRead: Send + Sync + 'static;

    fn prepare_source_read<'a>(
        &'a self,
        _session: &'a ConnectorSession,
        _request: &'a super::runtime::ConnectorReadSourcePreparationRequest,
        _cpu: &'a Arc<dyn crate::connector::ProviderMetadataCpuPort>,
        _original: &'a crate::connector::ConnectorOriginalResultScope,
        _owner: &'a mut Option<crate::connector::ConnectorProcessLocalPayload>,
    ) -> BoxFuture<'a, Result<ProviderPreparedSourceRead<Self::PreparedSourceRead>, ConnectorError>>
    {
        Box::pin(async {
            Err(ConnectorError::new(
                crate::connector::ConnectorErrorKind::Unsupported,
                "provider does not prepare frozen source reads",
            ))
        })
    }

    fn project_prepared_source_read(
        &self,
        _session: &ConnectorSession,
        _request: &super::runtime::ConnectorReadSourcePreparationRequest,
        _source: &Self::PreparedSourceRead,
        _projection: super::runtime::ConnectorReadSourceProjection,
    ) -> Result<Self::Table, ConnectorError> {
        Err(ConnectorError::new(
            crate::connector::ConnectorErrorKind::Unsupported,
            "provider does not project frozen source reads",
        ))
    }

    type PreparedRelation: Send + Sync + 'static;
    fn prepare_relation<'a>(
        &'a self,
        session: &'a ConnectorSession,
        request: &'a super::runtime::ConnectorReadRelationPreparationRequest,
    ) -> BoxFuture<'a, Result<Option<Self::PreparedRelation>, ConnectorError>>;
    fn create_prepared_relation(
        &self,
        prepared: Self::PreparedRelation,
    ) -> Result<Option<ProviderReadOpenedRelation<Self::Table>>, ConnectorError>;

    type FrozenSource: Debug + Send + Sync + 'static;

    /// Source validation belongs to the provider. In particular a write
    /// incarnation must not be interpreted as a read CatalogHandle version.
    fn adopt_frozen_source(
        &self,
        _session: &ConnectorSession,
        _owner: &crate::connector::ConnectorProviderBindingKey,
        _source: &Self::FrozenSource,
    ) -> Result<Self::Table, ConnectorError> {
        Err(ConnectorError::new(
            crate::connector::ConnectorErrorKind::Unsupported,
            "provider read generation does not adopt frozen write sources",
        ))
    }
    fn get_table_handle(
        &self,
        session: &ConnectorSession,
        name: &SchemaTableName,
        version: ConnectorReadRelationVersion,
        reference: Option<&str>,
    ) -> Result<Option<Self::Table>, ConnectorError>;

    fn get_mv_target_handle(
        &self,
        _session: &ConnectorSession,
        _name: &SchemaTableName,
        _object_id: &crate::connector::ConnectorTableObjectId,
        _snapshot_id: Option<i64>,
    ) -> Result<Option<Self::Table>, ConnectorError> {
        Err(ConnectorError::new(
            crate::connector::ConnectorErrorKind::Unsupported,
            "provider read generation does not support frozen MV target reads",
        ))
    }

    fn get_pinned_file_set_handle(
        &self,
        session: &ConnectorSession,
        name: &SchemaTableName,
        pinned: &ConnectorPinnedFileSet,
    ) -> Result<Option<Self::Table>, ConnectorError>;

    fn get_mv_target_partition_handle(
        &self,
        _session: &ConnectorSession,
        _name: &SchemaTableName,
        _selection: &ConnectorMvTargetPartitionSelection,
    ) -> Result<Option<Self::Table>, ConnectorError> {
        Err(ConnectorError::new(
            crate::connector::ConnectorErrorKind::Unsupported,
            "provider read generation does not support MV target partition selection",
        ))
    }

    fn get_column_bindings(
        &self,
        session: &ConnectorSession,
        table: &Self::Table,
    ) -> Result<Vec<ProviderReadColumnBinding<Self::Column>>, ConnectorError>;

    fn final_static_facts(
        &self,
        _session: &ConnectorSession,
        _table: &Self::Table,
    ) -> Result<ConnectorReadStaticFacts<Self::Column>, ConnectorError> {
        Err(ConnectorError::new(
            crate::connector::ConnectorErrorKind::Unsupported,
            "provider read generation does not publish final static facts",
        ))
    }

    /// One public field per column, in the given order, authored by the same
    /// field derivation this provider's pure read compiler checks a frozen
    /// public schema against. A provider has exactly one such author; it never
    /// takes a field name from a SQL alias or a type from an engine type.
    fn read_public_schema(
        &self,
        _session: &ConnectorSession,
        _table: &Self::Table,
        _columns: &[&Self::Column],
        _creation: &mut super::ConnectorReadSchemaCreation<'_>,
    ) -> Result<
        ConnectorReadPublicSchema,
        novarocks_connector_contract::PureProviderCompileError<ConnectorError>,
    > {
        Err(ConnectorError::new(
            crate::connector::ConnectorErrorKind::Unsupported,
            "provider read generation does not publish a public read schema",
        )
        .into())
    }

    fn apply_filter(
        &self,
        session: &ConnectorSession,
        table: &Self::Table,
        constraint: &Constraint<Self::Column>,
    ) -> ProviderReadFilterResult<Self::Table, Self::Column>;

    fn apply_projection(
        &self,
        session: &ConnectorSession,
        table: &Self::Table,
        assignments: &[Assignment<Self::Column>],
    ) -> Result<Option<Self::Table>, ConnectorError>;

    fn apply_limit(
        &self,
        session: &ConnectorSession,
        table: &Self::Table,
        limit: u64,
    ) -> Result<Option<ProviderReadLimitApplication<Self::Table>>, ConnectorError>;

    fn get_system_table_plan(
        &self,
        session: &ConnectorSession,
        name: &SchemaTableName,
    ) -> Result<Option<ProviderReadSystemTablePlan<Self::Table>>, ConnectorError>;

    fn get_system_table_plan_for_request(
        &self,
        _session: &ConnectorSession,
        _name: &SchemaTableName,
        _request: &super::ConnectorReadMetadataRequest,
    ) -> Result<Option<ProviderReadSystemTablePlan<Self::Table>>, ConnectorError> {
        Err(ConnectorError::new(
            crate::connector::ConnectorErrorKind::Unsupported,
            "provider read generation does not support typed metadata requests",
        ))
    }

    fn get_change_window_plan(
        &self,
        session: &ConnectorSession,
        name: &SchemaTableName,
        window: ConnectorReadChangeWindow,
    ) -> Result<Option<Self::Table>, ConnectorError>;

    fn get_table_execute_plan(
        &self,
        session: &ConnectorSession,
        name: &SchemaTableName,
        procedure: super::runtime::ConnectorReadTableExecuteProcedure,
    ) -> Result<Option<Self::Table>, ConnectorError>;
}

pub trait ProviderReadSplitSource<P: ProviderReadRuntime>: Send {
    fn profile_snapshot(&self) -> super::SplitSourceProfile {
        super::SplitSourceProfile::default()
    }

    fn initial_dynamic_filter_wait_request(&self) -> std::time::Duration {
        std::time::Duration::ZERO
    }

    fn next_batch(
        &mut self,
        max_size: usize,
        dynamic_filter: &DynamicFilterSnapshot<P::Column>,
    ) -> Result<ConnectorSplitBatch<P::Split>, ConnectorError>;

    fn is_finished(&self) -> bool;
    fn close(&mut self) -> Result<(), ConnectorError>;
}

/// Original owned split inputs. Sharing an opaque table keeps its actual
/// concrete value without imposing Clone or re-creating Provider facts.
pub struct ProviderReadSplitOpenRequest<P: ProviderReadRuntime> {
    pub session: ConnectorSession,
    pub table: Arc<P::Table>,
    pub columns: Vec<Assignment<P::Column>>,
    pub dynamic_filter_columns: BTreeSet<P::Column>,
    pub constraint: Constraint<P::Column>,
    pub preparation: Option<super::runtime::ConnectorReadSplitPreparation>,
}

/// Driver returned after the original split-open source has been installed.
pub type ProviderReadSplitOpenFuture<P> =
    BoxFuture<'static, Result<Box<dyn ProviderReadSplitSource<P>>, ConnectorError>>;

pub trait ProviderReadSplitManager: ProviderReadRuntime {
    /// Prepared-source providers install their closed original Source here,
    /// synchronously into the caller-owned slot before returning its driver.
    /// Ordinary reads retain their existing borrowed planning implementation.
    fn begin_get_splits(
        self: Arc<Self>,
        request: ProviderReadSplitOpenRequest<Self>,
        original_owner: &mut Option<crate::connector::ConnectorProcessLocalPayload>,
    ) -> Result<ProviderReadSplitOpenFuture<Self>, ConnectorError>
    where
        Self: Sized,
    {
        if original_owner.is_some() {
            return Err(ConnectorError::new(
                crate::connector::ConnectorErrorKind::InvalidRequest,
                "split opening owner is occupied",
            ));
        }
        Ok(Box::pin(async move {
            self.get_splits(
                &request.session,
                &request.table,
                &request.columns,
                &request.dynamic_filter_columns,
                &request.constraint,
            )
            .await
        }))
    }

    fn get_splits<'a>(
        &'a self,
        session: &'a ConnectorSession,
        table: &'a Self::Table,
        columns: &'a [Assignment<Self::Column>],
        dynamic_filter_columns: &'a BTreeSet<Self::Column>,
        constraint: &'a Constraint<Self::Column>,
    ) -> BoxFuture<'a, Result<Box<dyn ProviderReadSplitSource<Self>>, ConnectorError>>
    where
        Self: Sized;
}

pub trait ProviderReadPageSourceProvider<P: ProviderReadRuntime>: Send + Sync {
    /// Opens one split as a page stream the host polls with `budget`, the
    /// CPU budget it refills every turn; nested streams share it. Opening
    /// must not wait for I/O: the stream opens its input when first polled.
    fn create_page_stream(
        &self,
        session: &ConnectorSession,
        table: &P::Table,
        split: &P::Split,
        scheduled_split_sequence_id: u64,
        columns: &[Assignment<P::Column>],
        dynamic_filter: &Arc<dyn DynamicFilter<P::Column>>,
        budget: &ConnectorPollBudget,
    ) -> Result<OwnedConnectorPageStream, ConnectorError>;

    /// Return `Unsupported` when this provider has no future-split preparation
    /// path. This must not open a page stream or construct a decoder.
    fn prepare_page_source(
        &self,
        _session: &ConnectorSession,
        _table: &P::Table,
        _split: &P::Split,
        _scheduled_split_sequence_id: u64,
        _columns: &[Assignment<P::Column>],
        _dynamic_filter: &Arc<dyn DynamicFilter<P::Column>>,
    ) -> Result<ProviderPreparationStart<P>, ConnectorError> {
        Ok(ProviderPreparationStart::Unsupported)
    }
}

pub enum ProviderPreparationStart<P: ProviderReadRuntime> {
    Unsupported,
    Prepared(Box<dyn ProviderPreparedPageSource<P>>),
}

/// Concrete provider input remains hidden behind the generic adapter.
pub trait ProviderPreparedPageSource<P: ProviderReadRuntime>: Send {
    fn advance(
        &mut self,
        remaining_input_bytes: u64,
    ) -> Result<ConnectorPreparationProgress, ConnectorError>;

    fn retained_input_bytes(&self) -> u64;

    fn control(&self) -> Arc<dyn ConnectorPreparationControl>;

    /// Transfers prepared input to a page stream polled with `budget`; see
    /// [`ConnectorPreparedPageSource::promote`].
    fn promote(
        self: Box<Self>,
        dynamic_filter: &Arc<dyn DynamicFilter<P::Column>>,
        budget: &ConnectorPollBudget,
    ) -> Result<OwnedConnectorPageStream, ConnectorError>;
}

/// Concrete backend factory contract. The provider receives an admitted
/// resource capability separately from operation control and authorized I/O.
pub trait ProviderAdmittedReadFactory<P: ProviderReadRuntime>: Send + Sync {
    fn create_page_source_provider(
        &self,
        request: &ConnectorRequestContext,
        resources: ConnectorExecutionResources,
        options: super::ConnectorPageSourceProviderOptions,
    ) -> Result<Arc<dyn ProviderReadPageSourceProvider<P>>, ConnectorError>;
}

/// The single generic bridge from provider-owned types to role-visible SPI.
pub struct ReadRuntimeAdapter<P> {
    provider: Arc<P>,
    binding: ConnectorReadBinding,
}

impl<P> Clone for ReadRuntimeAdapter<P> {
    fn clone(&self) -> Self {
        Self {
            provider: self.provider.clone(),
            binding: self.binding.clone(),
        }
    }
}

impl<P: ProviderReadRuntime> ReadRuntimeAdapter<P> {
    pub fn new(provider: Arc<P>) -> Self {
        assert_eq!(
            provider.descriptor().instance_id,
            *provider.catalog_handle().catalog_name(),
            "provider read runtime descriptor and catalog handle must name the same catalog"
        );
        let binding = ConnectorReadBinding::new(
            provider.descriptor().clone(),
            provider.catalog_handle().clone(),
        );
        Self { provider, binding }
    }

    pub const fn binding(&self) -> &ConnectorReadBinding {
        &self.binding
    }

    /// Wrap a provider transaction at a provider-owned codec or service
    /// boundary. Roles receive only the opaque result.
    pub fn wrap_transaction(&self, transaction: P::Transaction) -> ConnectorReadTransactionHandle {
        transaction_handle(self.binding.clone(), transaction)
    }

    /// Pair a provider-bound table with this adapter's frozen transaction.
    ///
    /// Frontend code uses the returned neutral relation through planning and
    /// lets the exact installed codec serialize it at fragment egress.  It
    /// cannot observe or construct a transaction payload itself.
    pub fn relation(
        &self,
        kind: ConnectorReadRelationKind,
        table: ConnectorReadTableHandle,
    ) -> Result<super::runtime::ConnectorReadRelation, ConnectorError> {
        self.table(&table)?;
        Ok(super::runtime::ConnectorReadRelation::new(
            kind,
            table,
            self.wrap_transaction(self.provider.transaction()),
        ))
    }

    /// Wrap a provider table at a provider-owned codec or service boundary.
    pub fn wrap_table(&self, table: P::Table) -> ConnectorReadTableHandle {
        table_handle(self.binding.clone(), table)
    }

    /// Wrap a provider column at a provider-owned codec or service boundary.
    pub fn wrap_column(&self, column: P::Column) -> ConnectorReadColumnHandle {
        column_handle(self.binding.clone(), column)
    }

    /// Wrap a provider split at a provider-owned codec or service boundary.
    pub fn wrap_split(&self, split: P::Split) -> ConnectorReadSplit {
        let facts = ConnectorReadSplitFacts::new(
            split.is_remotely_accessible(),
            split.addresses().to_vec(),
            split.affinity_key(),
            split.split_weight(),
            split.retained_size_in_bytes(),
        );
        split_handle(self.binding.clone(), facts, split)
    }

    /// Recover a provider table only through this adapter's exact binding.
    ///
    /// A codec keeps the adapter privately.  No installed role service exposes
    /// an adapter, an erased payload, or a generic downcast operation.
    pub fn table<'a>(
        &self,
        handle: &'a ConnectorReadTableHandle,
    ) -> Result<&'a P::Table, ConnectorError> {
        if handle.binding() != &self.binding {
            return Err(binding_error());
        }
        table_value(handle).ok_or_else(type_error)
    }

    /// Recover a provider column only through this adapter's exact binding.
    pub fn column<'a>(
        &self,
        handle: &'a ConnectorReadColumnHandle,
    ) -> Result<&'a P::Column, ConnectorError> {
        if handle.binding() != &self.binding {
            return Err(binding_error());
        }
        column_value(handle).ok_or_else(type_error)
    }

    /// Recover a provider split only through this adapter's exact binding.
    pub fn split<'a>(
        &self,
        handle: &'a ConnectorReadSplit,
    ) -> Result<&'a P::Split, ConnectorError> {
        if handle.binding() != &self.binding {
            return Err(binding_error());
        }
        split_value(handle).ok_or_else(type_error)
    }

    /// Recover a provider transaction only through this adapter's exact binding.
    pub fn transaction<'a>(
        &self,
        handle: &'a ConnectorReadTransactionHandle,
    ) -> Result<&'a P::Transaction, ConnectorError> {
        if handle.binding() != &self.binding {
            return Err(binding_error());
        }
        transaction_value(handle).ok_or_else(type_error)
    }

    fn typed_constraint(
        &self,
        constraint: &ConnectorReadConstraint,
    ) -> Result<Constraint<P::Column>, ConnectorError> {
        map_constraint(constraint, |column| self.column(column).cloned())
    }

    fn typed_assignments(
        &self,
        assignments: &[Assignment<ConnectorReadColumnHandle>],
    ) -> Result<Vec<Assignment<P::Column>>, ConnectorError> {
        assignments
            .iter()
            .map(|assignment| {
                Assignment::try_new(
                    assignment.variable(),
                    self.column(assignment.column())?.clone(),
                    assignment.value_type(),
                )
            })
            .collect()
    }

    fn typed_columns(
        &self,
        columns: &BTreeSet<ConnectorReadColumnHandle>,
    ) -> Result<BTreeSet<P::Column>, ConnectorError> {
        columns
            .iter()
            .map(|column| self.column(column).cloned())
            .collect()
    }

    fn role_constraint(&self, constraint: &Constraint<P::Column>) -> ConnectorReadConstraint {
        let summary = constraint
            .summary()
            .transform_keys(|column| Some(self.wrap_column(column.clone())));
        let assignments = constraint
            .assignments()
            .iter()
            .map(|(name, column)| (name.clone(), self.wrap_column(column.clone())))
            .collect();
        Constraint::try_new(summary, constraint.expression().clone(), assignments)
            .expect("provider returned a valid constraint")
    }
}

impl<P: ProviderReadMetadata> ReadRuntimeAdapter<P> {
    /// Called only at a provider-owned source producer. FE retains the opaque
    /// result; only the matching installed adapter recovers the typed value.
    pub fn freeze_source(
        owner: crate::connector::ConnectorProviderBindingKey,
        source: P::FrozenSource,
    ) -> super::runtime::ConnectorFrozenReadSource {
        super::runtime::frozen_read_source(owner, source)
    }

    fn bridge_filter(
        &self,
        session: &ConnectorSession,
        table: &ConnectorReadTableHandle,
        constraint: &ConnectorReadConstraint,
    ) -> Result<Option<ConnectorReadFilterApplication>, ConnectorError> {
        let table = self.table(table)?;
        let constraint = self.typed_constraint(constraint)?;
        self.provider
            .apply_filter(session, table, &constraint)
            .map(|result| {
                result.map(|result| {
                    let remaining_constraint = self.role_constraint(result.remaining_constraint());
                    let remaining_expression = result.remaining_expression().cloned();
                    let handle = result.into_handle();
                    ConnectorReadFilterApplication::new(
                        self.wrap_table(handle),
                        remaining_constraint,
                        remaining_expression,
                    )
                })
            })
    }

    fn bridge_projection(
        &self,
        session: &ConnectorSession,
        table: &ConnectorReadTableHandle,
        assignments: &[Assignment<ConnectorReadColumnHandle>],
    ) -> Result<Option<ConnectorReadTableHandle>, ConnectorError> {
        let table = self.table(table)?;
        let assignments = self.typed_assignments(assignments)?;
        self.provider
            .apply_projection(session, table, &assignments)
            .map(|value| value.map(|table| self.wrap_table(table)))
    }

    fn bridge_limit(
        &self,
        session: &ConnectorSession,
        table: &ConnectorReadTableHandle,
        limit: u64,
    ) -> Result<Option<ConnectorReadLimitApplication>, ConnectorError> {
        let table = self.table(table)?;
        self.provider
            .apply_limit(session, table, limit)
            .map(|result| {
                result.map(|result| {
                    let limit_guaranteed = result.limit_guaranteed();
                    let handle = result.into_handle();
                    ConnectorReadLimitApplication::new(self.wrap_table(handle), limit_guaranteed)
                })
            })
    }
}

struct BoundRelationPreparation<P: ProviderReadMetadata> {
    facts: P::PreparedRelation,
    provider: Arc<P>,
}

struct BoundSourcePreparation<P: ProviderReadMetadata> {
    facts: P::PreparedSourceRead,
}

impl<P: ProviderReadMetadata> ConnectorReadMetadata for ReadRuntimeAdapter<P> {
    fn prepare_source_read<'a>(
        &'a self,
        session: &'a ConnectorSession,
        request: &'a super::runtime::ConnectorReadSourcePreparationRequest,
        cpu: &'a Arc<dyn crate::connector::ProviderMetadataCpuPort>,
        original: &'a crate::connector::ConnectorOriginalResultScope,
        owner: &'a mut Option<crate::connector::ConnectorProcessLocalPayload>,
    ) -> super::runtime::ConnectorPreparedSourceReadFuture<'a> {
        Box::pin(async move {
            if request.observed_table().owner() != &self.provider.descriptor().instance_id {
                return Err(binding_error());
            }
            let prepared = self
                .provider
                .prepare_source_read(session, request, cpu, original, owner)
                .await?;
            let is_window = matches!(
                request.selection(),
                super::runtime::ConnectorReadSourceSelection::ChangeWindow(_)
            );
            if is_window != prepared.admission.is_some() {
                return Err(ConnectorError::new(
                    crate::connector::ConnectorErrorKind::InvalidRequest,
                    "provider source preparation decision differs from its selection",
                ));
            }
            Ok(super::runtime::ConnectorPreparedSourceRead::new(
                self.binding.clone(),
                request.clone(),
                prepared.admission,
                prepared.to_metadata,
                BoundSourcePreparation::<P> {
                    facts: prepared.source,
                },
            ))
        })
    }

    fn project_prepared_source_read(
        &self,
        session: &ConnectorSession,
        source: &super::runtime::ConnectorPreparedSourceRead,
        projection: super::runtime::ConnectorReadSourceProjection,
    ) -> Result<ConnectorReadTableHandle, ConnectorError> {
        if source.binding() != &self.binding {
            return Err(binding_error());
        }
        if !source.permits(projection) {
            return Err(ConnectorError::new(
                crate::connector::ConnectorErrorKind::InvalidRequest,
                "prepared source does not permit the requested read projection",
            ));
        }
        let prepared = source
            .facts::<BoundSourcePreparation<P>>()
            .ok_or_else(type_error)?;
        self.provider
            .project_prepared_source_read(session, source.request(), &prepared.facts, projection)
            .map(|table| self.wrap_table(table))
    }

    fn prepare_relation<'a>(
        &'a self,
        session: &'a ConnectorSession,
        request: &'a super::runtime::ConnectorReadRelationPreparationRequest,
    ) -> super::runtime::ConnectorReadRelationPreparationFuture<'a> {
        Box::pin(async move {
            self.provider
                .prepare_relation(session, request)
                .await
                .map(|facts| {
                    facts.map(|facts| {
                        super::runtime::ConnectorReadRelationPreparation::new(
                            self.binding.clone(),
                            BoundRelationPreparation::<P> {
                                facts,
                                provider: Arc::clone(&self.provider),
                            },
                        )
                    })
                })
        })
    }
    fn create_prepared_relation(
        &self,
        prepared: super::runtime::ConnectorReadRelationPreparation,
    ) -> Result<Option<super::runtime::ConnectorReadOpenedRelation>, ConnectorError> {
        let original = prepared.into_facts::<BoundRelationPreparation<P>>(&self.binding)?;
        if !Arc::ptr_eq(&original.provider, &self.provider) {
            return Err(binding_error());
        }
        self.provider
            .create_prepared_relation(original.facts)
            .map(|relation| {
                relation.map(|relation| {
                    let (handle, hint) = relation.into_parts();
                    super::runtime::ConnectorReadOpenedRelation::new(self.wrap_table(handle), hint)
                })
            })
    }

    fn negotiate(
        &self,
        session: &ConnectorSession,
        negotiation: &crate::connector::read_stack::negotiation::ReadNegotiation,
    ) -> Result<crate::connector::read_stack::negotiation::ReadNegotiated, ConnectorError> {
        use crate::connector::read_stack::negotiation::{
            ReadNegotiated, ReadPushdownDisposition, ReadPushdownOp, ReadPushdownOutcome,
        };

        // Operations apply in the order they were offered, each against the
        // handle the previous one produced. A declined operation leaves the
        // handle where it was, so the rest of the list still means what the
        // caller intended.
        let mut handle = negotiation.handle.clone();
        let mut outcomes = Vec::with_capacity(negotiation.ops.len());
        let mut changed = false;
        for op in &negotiation.ops {
            let outcome = match op {
                ReadPushdownOp::Projection { assignments } => {
                    match self.bridge_projection(session, &handle, assignments)? {
                        Some(narrowed) => {
                            handle = narrowed;
                            changed = true;
                            ReadPushdownOutcome::exact()
                        }
                        None => ReadPushdownOutcome::declined(),
                    }
                }
                ReadPushdownOp::Filter { constraint } => {
                    match self.bridge_filter(session, &handle, constraint)? {
                        Some(application) => {
                            let remaining = application.remaining_constraint().clone();
                            // A provider that hands back nothing to evaluate has
                            // guaranteed the predicate; anything left over is a
                            // pruning answer with the residual named.
                            let exact = remaining.summary().is_all()
                                && remaining.expression().is_constant_true();
                            handle = application.into_handle();
                            changed = true;
                            ReadPushdownOutcome {
                                disposition: if exact {
                                    ReadPushdownDisposition::Exact
                                } else {
                                    ReadPushdownDisposition::PruningOnly
                                },
                                residual: Some(remaining),
                            }
                        }
                        None => ReadPushdownOutcome::declined(),
                    }
                }
                ReadPushdownOp::Limit { rows } => {
                    match self.bridge_limit(session, &handle, *rows)? {
                        Some(application) => {
                            let guaranteed = application.limit_guaranteed();
                            handle = application.into_handle();
                            changed = true;
                            ReadPushdownOutcome {
                                disposition: if guaranteed {
                                    ReadPushdownDisposition::Exact
                                } else {
                                    ReadPushdownDisposition::PruningOnly
                                },
                                residual: None,
                            }
                        }
                        None => ReadPushdownOutcome::declined(),
                    }
                }
            };
            outcomes.push(outcome);
        }
        Ok(ReadNegotiated {
            handle,
            outcomes,
            changed,
        })
    }

    fn binding(&self) -> &ConnectorReadBinding {
        &self.binding
    }

    fn relation(
        &self,
        kind: ConnectorReadRelationKind,
        table: ConnectorReadTableHandle,
    ) -> Result<super::runtime::ConnectorReadRelation, ConnectorError> {
        ReadRuntimeAdapter::relation(self, kind, table)
    }

    fn get_table_handle(
        &self,
        session: &ConnectorSession,
        name: &SchemaTableName,
        version: ConnectorReadRelationVersion,
        reference: Option<&str>,
    ) -> Result<Option<ConnectorReadTableHandle>, ConnectorError> {
        self.provider
            .get_table_handle(session, name, version, reference)
            .map(|value| value.map(|table| self.wrap_table(table)))
    }

    fn adopt_frozen_source(
        &self,
        session: &ConnectorSession,
        source: &super::runtime::ConnectorFrozenReadSource,
    ) -> Result<ConnectorReadTableHandle, ConnectorError> {
        if source.owner().instance_id != self.provider.descriptor().instance_id {
            return Err(binding_error());
        }
        let typed = super::runtime::frozen_read_source_value::<P::FrozenSource>(source)
            .ok_or_else(type_error)?;
        self.provider
            .adopt_frozen_source(session, source.owner(), typed)
            .map(|table| self.wrap_table(table))
    }

    fn get_mv_target_handle(
        &self,
        session: &ConnectorSession,
        name: &SchemaTableName,
        object_id: &crate::connector::ConnectorTableObjectId,
        snapshot_id: Option<i64>,
    ) -> Result<Option<ConnectorReadTableHandle>, ConnectorError> {
        self.provider
            .get_mv_target_handle(session, name, object_id, snapshot_id)
            .map(|value| value.map(|table| self.wrap_table(table)))
    }

    fn get_pinned_file_set_handle(
        &self,
        session: &ConnectorSession,
        name: &SchemaTableName,
        pinned: &ConnectorPinnedFileSet,
    ) -> Result<Option<ConnectorReadTableHandle>, ConnectorError> {
        self.provider
            .get_pinned_file_set_handle(session, name, pinned)
            .map(|value| value.map(|table| self.wrap_table(table)))
    }

    fn get_mv_target_partition_handle(
        &self,
        session: &ConnectorSession,
        name: &SchemaTableName,
        selection: &ConnectorMvTargetPartitionSelection,
    ) -> Result<Option<ConnectorReadTableHandle>, ConnectorError> {
        self.provider
            .get_mv_target_partition_handle(session, name, selection)
            .map(|value| value.map(|table| self.wrap_table(table)))
    }

    fn get_column_bindings(
        &self,
        session: &ConnectorSession,
        table: &ConnectorReadTableHandle,
    ) -> Result<Vec<ConnectorReadColumnBinding>, ConnectorError> {
        let table = self.table(table)?;
        self.provider
            .get_column_bindings(session, table)
            .map(|columns| {
                columns
                    .into_iter()
                    .map(|column| {
                        let ProviderReadColumnBinding {
                            name,
                            column,
                            hidden,
                        } = column;
                        ConnectorReadColumnBinding::new(name, self.wrap_column(column), hidden)
                    })
                    .collect()
            })
    }

    fn freeze(
        &self,
        session: &ConnectorSession,
        request: &crate::connector::read_stack::negotiation::ReadFreezeRequest,
    ) -> Result<
        crate::connector::read_stack::negotiation::ConnectorReadFrozen<ConnectorReadColumnHandle>,
        ConnectorError,
    > {
        let table = self.table(&request.handle)?;
        let facts = self.provider.final_static_facts(session, table)?;
        let distribution = match facts.properties().distribution() {
            ConnectorReadDistribution::Unconstrained => ConnectorReadDistribution::Unconstrained,
            ConnectorReadDistribution::Singleton => ConnectorReadDistribution::Singleton,
            ConnectorReadDistribution::RoundRobin => ConnectorReadDistribution::RoundRobin,
            ConnectorReadDistribution::Hash {
                keys,
                partition_space,
                admissible,
                algorithm,
            } => ConnectorReadDistribution::Hash {
                keys: keys
                    .iter()
                    .cloned()
                    .map(|column| self.wrap_column(column))
                    .collect::<Vec<_>>()
                    .into(),
                partition_space: *partition_space,
                admissible: *admissible,
                algorithm: *algorithm,
            },
            ConnectorReadDistribution::BucketShuffle {
                keys,
                partition_space,
                bucket_count,
                hash,
                layout,
                ordinal_domain_evidence,
            } => ConnectorReadDistribution::BucketShuffle {
                keys: keys
                    .iter()
                    .cloned()
                    .map(|column| self.wrap_column(column))
                    .collect::<Vec<_>>()
                    .into(),
                partition_space: *partition_space,
                bucket_count: *bucket_count,
                hash: *hash,
                layout: *layout,
                ordinal_domain_evidence: *ordinal_domain_evidence,
            },
        };
        let ordering = facts
            .properties()
            .ordering()
            .iter()
            .map(|key| {
                ConnectorReadOrderingKey::new(
                    self.wrap_column(key.column().clone()),
                    key.direction(),
                    key.null_ordering(),
                )
            })
            .collect::<Vec<_>>();
        let properties = ConnectorReadProperties::try_new(distribution, ordering)?;
        let facts = ConnectorReadStaticFacts::try_new(
            facts.input_version().clone(),
            facts.selection_digest(),
            properties,
            facts.artifact_coverage().clone(),
            Arc::<[u8]>::from(facts.coverage_evidence()),
        )?;
        // Carry back what was frozen, so the caller can establish it is the
        // read it asked to commit before it can reach the facts at all.
        Ok(
            crate::connector::read_stack::negotiation::ConnectorReadFrozen::new(
                facts,
                request.relation_kind,
                request.handle.binding().clone(),
            ),
        )
    }

    fn read_public_schema(
        &self,
        session: &ConnectorSession,
        table: &ConnectorReadTableHandle,
        columns: &[ConnectorReadColumnHandle],
        creation: &mut super::ConnectorReadSchemaCreation<'_>,
    ) -> Result<
        ConnectorReadPublicSchema,
        novarocks_connector_contract::PureProviderCompileError<ConnectorError>,
    > {
        creation.work(1)?;
        let table = self.table(table)?;
        let layout = std::alloc::Layout::array::<&P::Column>(columns.len()).map_err(|_| {
            ConnectorError::new(
                crate::connector::ConnectorErrorKind::ResourceExhausted,
                "public schema source reference extent exceeds address space",
            )
        })?;
        if layout.size() != 0 {
            creation.request(layout)?;
        }
        let mut borrowed = Vec::new();
        borrowed.try_reserve_exact(columns.len()).map_err(|_| {
            ConnectorError::new(
                crate::connector::ConnectorErrorKind::ResourceExhausted,
                "public schema source reference allocation was refused",
            )
        })?;
        for column in columns {
            creation.work(1)?;
            borrowed.push(self.column(column)?);
        }
        let schema = self
            .provider
            .read_public_schema(session, table, &borrowed, creation)?;
        // The fields are positional facts of the columns asked about. An answer
        // of any other arity cannot be paired back with them, so it is refused
        // here rather than left for a later ordinal check to misattribute.
        if schema.schema().fields().len() != columns.len() {
            return Err(ConnectorError::new(
                crate::connector::ConnectorErrorKind::InvalidRequest,
                format!(
                    "provider published {} public read fields for {} columns",
                    schema.schema().fields().len(),
                    columns.len()
                ),
            )
            .into());
        }
        creation.work(1)?;
        if schema.execution_schema().is_none() || schema.source_schema_facts().is_none() {
            return Err(ConnectorError::new(
                crate::connector::ConnectorErrorKind::InvalidRequest,
                "provider public schema lacks complete execution/source declarations",
            )
            .into());
        }
        Ok(schema)
    }

    fn get_system_table_plan(
        &self,
        session: &ConnectorSession,
        name: &SchemaTableName,
    ) -> Result<Option<ConnectorReadSystemTablePlan>, ConnectorError> {
        self.provider
            .get_system_table_plan(session, name)
            .map(|result| {
                result.map(|result| {
                    let unit_hint = result.unit_hint();
                    let handle = result.into_handle();
                    ConnectorReadSystemTablePlan::new(self.wrap_table(handle), unit_hint)
                })
            })
    }

    fn get_system_table_plan_for_request(
        &self,
        session: &ConnectorSession,
        name: &SchemaTableName,
        request: &super::ConnectorReadMetadataRequest,
    ) -> Result<Option<ConnectorReadSystemTablePlan>, ConnectorError> {
        self.provider
            .get_system_table_plan_for_request(session, name, request)
            .map(|result| {
                result.map(|result| {
                    let unit_hint = result.unit_hint();
                    let handle = result.into_handle();
                    ConnectorReadSystemTablePlan::new(self.wrap_table(handle), unit_hint)
                })
            })
    }

    fn get_change_window_plan(
        &self,
        session: &ConnectorSession,
        name: &SchemaTableName,
        window: ConnectorReadChangeWindow,
    ) -> Result<Option<ConnectorReadTableHandle>, ConnectorError> {
        self.provider
            .get_change_window_plan(session, name, window)
            .map(|value| value.map(|table| self.wrap_table(table)))
    }

    fn get_table_execute_plan(
        &self,
        session: &ConnectorSession,
        name: &SchemaTableName,
        procedure: super::runtime::ConnectorReadTableExecuteProcedure,
    ) -> Result<Option<ConnectorReadTableHandle>, ConnectorError> {
        self.provider
            .get_table_execute_plan(session, name, procedure)
            .map(|value| value.map(|table| self.wrap_table(table)))
    }
}

impl<P: ProviderReadSplitManager> ConnectorReadSplitManager for ReadRuntimeAdapter<P> {
    fn binding(&self) -> &ConnectorReadBinding {
        &self.binding
    }

    fn get_splits(
        &self,
        request: super::runtime::ConnectorReadSplitOpenRequest,
        original_owner: &mut Option<crate::connector::ConnectorProcessLocalPayload>,
    ) -> super::runtime::ConnectorReadSplitOpenFuture {
        let adapter = self.clone();
        // Typed input and the Provider Source are received before an async
        // borrower can poll or unwind. A pending driver never owns this slot.
        let opening = (|| {
            if original_owner.is_some() {
                return Err(ConnectorError::new(
                    crate::connector::ConnectorErrorKind::InvalidRequest,
                    "split opening owner is occupied",
                ));
            }
            let (session, table, columns, dynamic_filter_columns, constraint, preparation) =
                request.into_parts();
            adapter.table(&table)?;
            let table =
                super::runtime::shared_table_value::<P::Table>(&table).ok_or_else(type_error)?;
            let request = ProviderReadSplitOpenRequest::<P> {
                session,
                table,
                columns: adapter.typed_assignments(&columns)?,
                dynamic_filter_columns: adapter.typed_columns(&dynamic_filter_columns)?,
                constraint: adapter.typed_constraint(&constraint)?,
                preparation,
            };
            Arc::clone(&adapter.provider).begin_get_splits(request, original_owner)
        })();
        Box::pin(async move {
            let source = opening?.await?;
            Ok(Box::new(AdapterSplitSource { source, adapter })
                as Box<dyn ConnectorReadSplitSource>)
        })
    }
}

struct AdapterSplitSource<P: ProviderReadRuntime> {
    source: Box<dyn ProviderReadSplitSource<P>>,
    adapter: ReadRuntimeAdapter<P>,
}

impl<P: ProviderReadRuntime> ConnectorReadSplitSource for AdapterSplitSource<P> {
    fn profile_snapshot(&self) -> super::SplitSourceProfile {
        self.source.profile_snapshot()
    }

    fn initial_dynamic_filter_wait_request(&self) -> std::time::Duration {
        self.source.initial_dynamic_filter_wait_request()
    }

    fn next_batch(
        &mut self,
        max_size: usize,
        dynamic_filter: &ConnectorReadDynamicFilterSnapshot,
    ) -> Result<ConnectorSplitBatch<ConnectorReadSplit>, ConnectorError> {
        let domain = map_tuple_domain(dynamic_filter.current_predicate(), |column| {
            self.adapter.column(column).cloned()
        })?;
        let dynamic_filter = DynamicFilterSnapshot::new(domain, dynamic_filter.is_complete());
        self.source
            .next_batch(max_size, &dynamic_filter)
            .map(|batch| {
                let no_more_splits = batch.no_more_splits();
                ConnectorSplitBatch::new(
                    batch
                        .into_splits()
                        .into_iter()
                        .map(|split| self.adapter.wrap_split(split))
                        .collect(),
                    no_more_splits,
                )
            })
    }

    fn is_finished(&self) -> bool {
        self.source.is_finished()
    }

    fn close(&mut self) -> Result<(), ConnectorError> {
        self.source.close()
    }
}

/// A live filter mapping.  Any mapping violation is latched and converted to a
/// connector error at the next Result-returning source boundary.
pub struct ProviderDynamicFilter<P: ProviderReadRuntime> {
    inner: Arc<ConnectorReadDynamicFilter>,
    covered: BTreeSet<P::Column>,
    reverse: BTreeMap<P::Column, ConnectorReadColumnHandle>,
    adapter: ReadRuntimeAdapter<P>,
    error: Mutex<Option<ConnectorError>>,
}

impl<P: ProviderReadRuntime> ProviderDynamicFilter<P> {
    fn new(
        adapter: ReadRuntimeAdapter<P>,
        inner: Arc<ConnectorReadDynamicFilter>,
    ) -> Result<Self, ConnectorError> {
        let mut covered = BTreeSet::new();
        let mut reverse = BTreeMap::new();
        for column in inner.columns_covered() {
            let mapped = adapter.column(column)?.clone();
            if reverse.insert(mapped.clone(), column.clone()).is_some() {
                return Err(ConnectorError::new(
                    crate::connector::ConnectorErrorKind::InvalidRequest,
                    "connector dynamic filter maps multiple columns to one provider column",
                ));
            }
            covered.insert(mapped);
        }
        Ok(Self {
            inner,
            covered,
            reverse,
            adapter,
            error: Mutex::new(None),
        })
    }

    fn latch(&self, error: ConnectorError) {
        let mut guard = self
            .error
            .lock()
            .expect("dynamic filter error latch is not poisoned");
        if guard.is_none() {
            *guard = Some(error);
        }
    }

    fn check(&self) -> Result<(), ConnectorError> {
        self.error
            .lock()
            .expect("dynamic filter error latch is not poisoned")
            .clone()
            .map_or(Ok(()), Err)
    }
}

impl<P: ProviderReadRuntime> DynamicFilter<P::Column> for ProviderDynamicFilter<P> {
    fn columns_covered(&self) -> &BTreeSet<P::Column> {
        &self.covered
    }

    fn current_predicate(&self) -> TupleDomain<P::Column> {
        match map_tuple_domain(&self.inner.current_predicate(), |column| {
            self.adapter.column(column).cloned()
        }) {
            Ok(domain) => domain,
            Err(error) => {
                self.latch(error);
                TupleDomain::all()
            }
        }
    }

    fn is_complete(&self) -> bool {
        self.inner.is_complete()
    }

    fn is_awaitable(&self) -> bool {
        self.inner.is_awaitable()
    }

    fn is_blocked(&self) -> bool {
        self.inner.is_blocked()
    }

    fn bounds_may_match(&self, column: &P::Column, bounds: &ColumnValueBounds) -> BoundsMatch {
        match self.reverse.get(column) {
            Some(column) => self.inner.bounds_may_match(column, bounds),
            None => {
                self.latch(ConnectorError::new(
                    crate::connector::ConnectorErrorKind::InvalidRequest,
                    "provider dynamic filter queried a column outside its covered set",
                ));
                BoundsMatch::Unknown
            }
        }
    }
}

/// A provider page stream behind the adapter's dynamic-filter binding checks,
/// made before and after every provider callback. A failed check ends the
/// stream with its error; the host then closes it.
struct CheckedPageStream<P: ProviderReadRuntime> {
    stream: OwnedConnectorPageStream,
    dynamic_filter: Arc<ProviderDynamicFilter<P>>,
}

impl<P: ProviderReadRuntime> Stream for CheckedPageStream<P> {
    type Item = Result<SourcePage, ConnectorError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        if let Err(error) = this.dynamic_filter.check() {
            return Poll::Ready(Some(Err(error)));
        }
        match this.stream.as_mut().poll_next(cx) {
            Poll::Ready(Some(Ok(page))) => {
                Poll::Ready(Some(this.dynamic_filter.check().map(|()| page)))
            }
            other => other,
        }
    }
}

impl<P: ProviderReadRuntime> ConnectorPageStream for CheckedPageStream<P> {
    fn metrics(&self) -> PageSourceMetrics {
        self.stream.metrics()
    }

    fn memory_usage_bytes(&self) -> u64 {
        self.stream.memory_usage_bytes()
    }

    fn advance_successor_preparation(
        self: Pin<&mut Self>,
        remaining_input_bytes: u64,
        remaining_candidates: usize,
    ) -> Result<ConnectorPreparationProgress, ConnectorError> {
        let this = self.get_mut();
        let stop_successors = |stream: &OwnedConnectorPageStream| {
            if let Some(control) = stream.successor_preparation_control() {
                control.request_stop();
            }
        };
        if let Err(error) = this.dynamic_filter.check() {
            stop_successors(&this.stream);
            return Err(error);
        }
        let retained_before = this.stream.successor_preparation_input_bytes();
        let candidates_before = this.stream.successor_preparation_candidate_count();
        let progress = match this
            .stream
            .as_mut()
            .advance_successor_preparation(remaining_input_bytes, remaining_candidates)
        {
            Ok(progress) => progress,
            Err(error) => {
                stop_successors(&this.stream);
                return Err(error);
            }
        };
        if this
            .stream
            .successor_preparation_input_bytes()
            .saturating_sub(retained_before)
            > remaining_input_bytes
            || this
                .stream
                .successor_preparation_candidate_count()
                .saturating_sub(candidates_before)
                > remaining_candidates
        {
            stop_successors(&this.stream);
            return Err(ConnectorError::new(
                crate::connector::ConnectorErrorKind::Internal,
                "provider successor preparation exceeded its supplied capacity",
            ));
        }
        if let Err(error) = this.dynamic_filter.check() {
            stop_successors(&this.stream);
            return Err(error);
        }
        Ok(progress)
    }

    fn successor_preparation_input_bytes(&self) -> u64 {
        self.stream.successor_preparation_input_bytes()
    }

    fn successor_preparation_candidate_count(&self) -> usize {
        self.stream.successor_preparation_candidate_count()
    }

    fn successor_preparation_control(&self) -> Option<Arc<dyn ConnectorPreparationControl>> {
        self.stream.successor_preparation_control()
    }

    fn close(self: Pin<Box<Self>>) -> BoxFuture<'static, Result<(), ConnectorError>> {
        let this = *Pin::into_inner(self);
        this.stream.close()
    }
}

struct AdapterPageSourceProvider<P: ProviderReadRuntime> {
    provider: Arc<dyn ProviderReadPageSourceProvider<P>>,
    adapter: ReadRuntimeAdapter<P>,
}

struct AdapterPreparedPageSource<P: ProviderReadRuntime> {
    prepared: Box<dyn ProviderPreparedPageSource<P>>,
    adapter: ReadRuntimeAdapter<P>,
    binding: ConnectorReadBinding,
    preparation_filter: Arc<ProviderDynamicFilter<P>>,
}

impl<P: ProviderReadRuntime> ConnectorPreparedPageSource for AdapterPreparedPageSource<P> {
    fn advance(
        &mut self,
        remaining_input_bytes: u64,
    ) -> Result<ConnectorPreparationProgress, ConnectorError> {
        if self.adapter.binding() != &self.binding {
            self.prepared.control().request_stop();
            return Err(binding_error());
        }
        if let Err(error) = self.preparation_filter.check() {
            self.prepared.control().request_stop();
            return Err(error);
        }
        let retained_before = self.prepared.retained_input_bytes();
        let progress = match self.prepared.advance(remaining_input_bytes) {
            Ok(progress) => progress,
            Err(error) => {
                self.prepared.control().request_stop();
                return Err(error);
            }
        };
        if self
            .prepared
            .retained_input_bytes()
            .saturating_sub(retained_before)
            > remaining_input_bytes
        {
            self.prepared.control().request_stop();
            return Err(ConnectorError::new(
                crate::connector::ConnectorErrorKind::Internal,
                "provider preparation exceeded the supplied input capacity",
            ));
        }
        if let Err(error) = self.preparation_filter.check() {
            self.prepared.control().request_stop();
            return Err(error);
        }
        Ok(progress)
    }

    fn retained_input_bytes(&self) -> u64 {
        self.prepared.retained_input_bytes()
    }

    fn control(&self) -> Arc<dyn ConnectorPreparationControl> {
        self.prepared.control()
    }

    fn promote(
        self: Box<Self>,
        dynamic_filter: &Arc<ConnectorReadDynamicFilter>,
        budget: &ConnectorPollBudget,
    ) -> Result<OwnedConnectorPageStream, ConnectorError> {
        if self.adapter.binding() != &self.binding {
            self.prepared.control().request_stop();
            return Err(binding_error());
        }
        if let Err(error) = self.preparation_filter.check() {
            self.prepared.control().request_stop();
            return Err(error);
        }
        let live_filter =
            match ProviderDynamicFilter::new(self.adapter.clone(), dynamic_filter.clone()) {
                Ok(filter) => Arc::new(filter),
                Err(error) => {
                    self.prepared.control().request_stop();
                    return Err(error);
                }
            };
        if let Err(error) = live_filter.check() {
            self.prepared.control().request_stop();
            return Err(error);
        }
        let typed_filter: Arc<dyn DynamicFilter<P::Column>> = live_filter.clone();
        let control = self.prepared.control();
        let stream = match self.prepared.promote(&typed_filter, budget) {
            Ok(stream) => stream,
            Err(error) => {
                control.request_stop();
                return Err(error);
            }
        };
        if let Err(error) = live_filter.check() {
            // Closing seals the stream's operations; the task source observes
            // their exit.
            drop(stream.close());
            return Err(error);
        }
        Ok(Box::pin(CheckedPageStream {
            stream,
            dynamic_filter: live_filter,
        }))
    }
}

impl<P: ProviderReadRuntime> ConnectorReadPageSourceProvider for AdapterPageSourceProvider<P> {
    fn create_page_stream(
        &self,
        session: &ConnectorSession,
        table: &ConnectorReadTableHandle,
        split: &ConnectorReadSplit,
        scheduled_split_sequence_id: u64,
        columns: &[Assignment<ConnectorReadColumnHandle>],
        dynamic_filter: &Arc<ConnectorReadDynamicFilter>,
        budget: &ConnectorPollBudget,
    ) -> Result<OwnedConnectorPageStream, ConnectorError> {
        let table = self.adapter.table(table)?;
        let split = self.adapter.split(split)?;
        let columns = self.adapter.typed_assignments(columns)?;
        let dynamic_filter = Arc::new(ProviderDynamicFilter::new(
            self.adapter.clone(),
            dynamic_filter.clone(),
        )?);
        dynamic_filter.check()?;
        let typed_filter: Arc<dyn DynamicFilter<P::Column>> = dynamic_filter.clone();
        let stream = self.provider.create_page_stream(
            session,
            table,
            split,
            scheduled_split_sequence_id,
            &columns,
            &typed_filter,
            budget,
        )?;
        if let Err(error) = dynamic_filter.check() {
            // Closing seals the stream's operations; the task source observes
            // their exit.
            drop(stream.close());
            return Err(error);
        }
        Ok(Box::pin(CheckedPageStream {
            stream,
            dynamic_filter,
        }))
    }

    fn prepare_page_source(
        &self,
        session: &ConnectorSession,
        table: &ConnectorReadTableHandle,
        split: &ConnectorReadSplit,
        scheduled_split_sequence_id: u64,
        columns: &[Assignment<ConnectorReadColumnHandle>],
        dynamic_filter: &Arc<ConnectorReadDynamicFilter>,
    ) -> Result<ConnectorPreparationStart, ConnectorError> {
        let table = self.adapter.table(table)?;
        let split = self.adapter.split(split)?;
        let columns = self.adapter.typed_assignments(columns)?;
        let preparation_filter = Arc::new(ProviderDynamicFilter::new(
            self.adapter.clone(),
            dynamic_filter.clone(),
        )?);
        preparation_filter.check()?;
        let typed_filter: Arc<dyn DynamicFilter<P::Column>> = preparation_filter.clone();
        match self.provider.prepare_page_source(
            session,
            table,
            split,
            scheduled_split_sequence_id,
            &columns,
            &typed_filter,
        )? {
            ProviderPreparationStart::Unsupported => {
                preparation_filter.check()?;
                Ok(ConnectorPreparationStart::Unsupported)
            }
            ProviderPreparationStart::Prepared(prepared) => {
                if let Err(error) = preparation_filter.check() {
                    prepared.control().request_stop();
                    return Err(error);
                }
                Ok(ConnectorPreparationStart::Prepared(Box::new(
                    AdapterPreparedPageSource {
                        prepared,
                        adapter: self.adapter.clone(),
                        binding: self.adapter.binding().clone(),
                        preparation_filter,
                    },
                )))
            }
        }
    }
}

/// Combines a provider-owned execution factory with the separately-owned
/// metadata type adapter. It is constructed by the concrete connector's
/// exact-key bundle factory and never exposed as a typed accessor to a role.
pub struct ProviderReadFactoryAdapter<P: ProviderReadRuntime, F: ProviderAdmittedReadFactory<P>> {
    factory: Arc<F>,
    adapter: ReadRuntimeAdapter<P>,
}

impl<P: ProviderReadRuntime, F: ProviderAdmittedReadFactory<P>> ProviderReadFactoryAdapter<P, F> {
    pub fn new(adapter: ReadRuntimeAdapter<P>, factory: Arc<F>) -> Self {
        Self { factory, adapter }
    }
}

impl<P, F> ConnectorAdmittedReadProviderFactory for ProviderReadFactoryAdapter<P, F>
where
    P: ProviderReadRuntime,
    F: ProviderAdmittedReadFactory<P>,
{
    fn binding(&self) -> &ConnectorReadBinding {
        self.adapter.binding()
    }

    fn create_page_source_provider(
        &self,
        request: &ConnectorRequestContext,
        resources: ConnectorExecutionResources,
        options: super::ConnectorPageSourceProviderOptions,
    ) -> Result<Arc<dyn ConnectorReadPageSourceProvider>, ConnectorError> {
        Ok(Arc::new(AdapterPageSourceProvider {
            provider: ProviderAdmittedReadFactory::create_page_source_provider(
                self.factory.as_ref(),
                request,
                resources,
                options,
            )?,
            adapter: ReadRuntimeAdapter::clone(&self.adapter),
        }))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};
    use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
    use std::time::SystemTime;

    use super::*;
    use crate::connector::read_stack::{
        ConnectorReadAttemptAccessMint, ConnectorReadAttemptAccessReacquirer,
        ConnectorReadAttemptAccessSealer, ConnectorReadAttemptAccessSource,
        ConnectorReadAttemptRuntime, ConnectorReadRequestControl, ConnectorValue,
        PageSourceMetrics, SourcePage,
    };
    use crate::connector::{ConnectorErrorKind, ConnectorInstanceId, ConnectorProviderId};

    #[derive(Clone, Debug)]
    struct Table;
    #[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
    struct Column(u8);
    #[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
    struct WrongColumn;
    #[derive(Debug)]
    struct Split;

    impl ColumnHandle for Column {}
    impl ColumnHandle for WrongColumn {}
    impl ConnectorSplit for Split {
        fn retained_size_in_bytes(&self) -> u64 {
            0
        }
    }

    const PUBLIC_SCHEMA_EXTRA_FIELD: u8 = 99;

    struct SourceDropWitness(Arc<AtomicU64>);
    impl Drop for SourceDropWitness {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    struct ProbeSourceFacts {
        ordinal: usize,
        _retention: Option<SourceDropWitness>,
    }

    struct Probe {
        // Representative provider-private request inventory. It must not be
        // retained merely because a completed fact receipt has aliases.
        _source_request_inventory: Option<SourceDropWitness>,
        descriptor: ConnectorInstanceDescriptor,
        catalog_handle: CatalogHandle,
        public_schema_calls: AtomicU64,
        preparation_gate: std::sync::Mutex<Option<futures::channel::oneshot::Receiver<()>>>,
        prepared_calls: AtomicU64,
        created_calls: AtomicU64,
        split_gate: std::sync::Mutex<Option<futures::channel::oneshot::Receiver<()>>>,
        split_calls: AtomicU64,
    }

    impl Probe {
        fn new() -> Self {
            Self {
                _source_request_inventory: None,
                descriptor: ConnectorInstanceDescriptor {
                    provider_id: ConnectorProviderId::parse("probe").expect("provider ID"),
                    instance_id: ConnectorInstanceId::parse("catalog").expect("instance ID"),
                },
                catalog_handle: CatalogHandle::new(
                    ConnectorInstanceId::parse("catalog").expect("instance ID"),
                    crate::connector::CatalogVersion::from_bytes([3; 32]),
                ),
                public_schema_calls: AtomicU64::new(0),
                preparation_gate: std::sync::Mutex::new(None),
                prepared_calls: AtomicU64::new(0),
                created_calls: AtomicU64::new(0),
                split_gate: std::sync::Mutex::new(None),
                split_calls: AtomicU64::new(0),
            }
        }
    }

    impl ProviderReadRuntime for Probe {
        type Table = Table;
        type Column = Column;
        type Transaction = ();
        type Split = Split;

        fn descriptor(&self) -> &ConnectorInstanceDescriptor {
            &self.descriptor
        }

        fn catalog_handle(&self) -> &CatalogHandle {
            &self.catalog_handle
        }

        fn transaction(&self) -> Self::Transaction {}
    }

    impl ProviderReadMetadata for Probe {
        type PreparedSourceRead = ProbeSourceFacts;

        fn project_prepared_source_read(
            &self,
            _session: &ConnectorSession,
            _request: &super::super::ConnectorReadSourcePreparationRequest,
            source: &Self::PreparedSourceRead,
            _projection: super::super::ConnectorReadSourceProjection,
        ) -> Result<Self::Table, ConnectorError> {
            assert_eq!(source.ordinal, 41);
            self.created_calls.fetch_add(1, Ordering::SeqCst);
            Ok(Table)
        }

        type PreparedRelation = (
            Self::Table,
            Option<crate::connector::read_stack::SystemTableUnitHint>,
        );
        fn prepare_relation<'a>(
            &'a self,
            session: &'a crate::connector::read_stack::ConnectorSession,
            request: &'a crate::connector::read_stack::ConnectorReadRelationPreparationRequest,
        ) -> futures::future::BoxFuture<
            'a,
            Result<Option<Self::PreparedRelation>, crate::connector::ConnectorError>,
        > {
            Box::pin(async move {
                self.prepared_calls.fetch_add(1, Ordering::SeqCst);
                let gate = self.preparation_gate.lock().unwrap().take();
                if let Some(gate) = gate {
                    gate.await.expect("preparation gate");
                }
                use crate::connector::read_stack::ConnectorReadRelationPreparationKind as K;
                let name = request.name();
                match request.kind() {
                    K::Table { version, reference } => self
                        .get_table_handle(session, name, *version, reference.as_deref())
                        .map(|v| v.map(|v| (v, None))),
                    K::Metadata { request } => match request {
                        Some(request) => {
                            self.get_system_table_plan_for_request(session, name, request)
                        }
                        None => self.get_system_table_plan(session, name),
                    }
                    .map(|v| {
                        v.map(|v| {
                            let hint = v.unit_hint();
                            (v.into_handle(), Some(hint))
                        })
                    }),
                    K::Delta { window } => self
                        .get_change_window_plan(session, name, *window)
                        .map(|v| v.map(|v| (v, None))),
                    K::MvTarget {
                        object_id,
                        snapshot_id,
                    } => self
                        .get_mv_target_handle(session, name, object_id, *snapshot_id)
                        .map(|v| v.map(|v| (v, None))),
                    K::MvTargetPartition { selection } => self
                        .get_mv_target_partition_handle(session, name, selection)
                        .map(|v| v.map(|v| (v, None))),
                    K::PinnedFileSet { pinned } => self
                        .get_pinned_file_set_handle(session, name, pinned)
                        .map(|v| v.map(|v| (v, None))),
                    K::TableExecute { procedure } => self
                        .get_table_execute_plan(session, name, procedure.clone())
                        .map(|v| v.map(|v| (v, None))),
                }
            })
        }
        fn create_prepared_relation(
            &self,
            prepared: Self::PreparedRelation,
        ) -> Result<
            Option<crate::connector::read_stack::adapter::ProviderReadOpenedRelation<Self::Table>>,
            crate::connector::ConnectorError,
        > {
            self.created_calls.fetch_add(1, Ordering::SeqCst);
            Ok(Some(
                crate::connector::read_stack::adapter::ProviderReadOpenedRelation::new(
                    prepared.0, prepared.1,
                ),
            ))
        }

        type FrozenSource = UnsupportedFrozenReadSource;

        fn get_table_handle(
            &self,
            _session: &ConnectorSession,
            _name: &SchemaTableName,
            _version: ConnectorReadRelationVersion,
            _reference: Option<&str>,
        ) -> Result<Option<Self::Table>, ConnectorError> {
            Ok(Some(Table))
        }

        fn get_pinned_file_set_handle(
            &self,
            _session: &ConnectorSession,
            _name: &SchemaTableName,
            _pinned: &ConnectorPinnedFileSet,
        ) -> Result<Option<Self::Table>, ConnectorError> {
            Ok(Some(Table))
        }

        fn get_column_bindings(
            &self,
            _session: &ConnectorSession,
            _table: &Self::Table,
        ) -> Result<Vec<ProviderReadColumnBinding<Self::Column>>, ConnectorError> {
            Ok(Vec::new())
        }

        /// One field per column named after it; the column numbered
        /// `PUBLIC_SCHEMA_EXTRA_FIELD` answers with a field too many.
        fn read_public_schema(
            &self,
            _session: &ConnectorSession,
            _table: &Self::Table,
            columns: &[&Self::Column],
            creation: &mut super::super::ConnectorReadSchemaCreation<'_>,
        ) -> Result<
            ConnectorReadPublicSchema,
            novarocks_connector_contract::PureProviderCompileError<ConnectorError>,
        > {
            self.public_schema_calls.fetch_add(1, Ordering::SeqCst);
            let mut fields = columns
                .iter()
                .map(|column| {
                    arrow::datatypes::Field::new(
                        format!("c{}", column.0),
                        arrow::datatypes::DataType::Int64,
                        true,
                    )
                })
                .collect::<Vec<_>>();
            if columns
                .iter()
                .any(|column| column.0 == PUBLIC_SCHEMA_EXTRA_FIELD)
            {
                fields.push(arrow::datatypes::Field::new(
                    "extra",
                    arrow::datatypes::DataType::Int64,
                    true,
                ));
            }
            let schema = Arc::new(arrow::datatypes::Schema::new(fields));
            let storage = creation.storage();
            let bindings = schema
                .fields()
                .iter()
                .map(|_| {
                    // Explicit fixture declaration; source annotations do not
                    // author even this test provider's value identity.
                    let semantic = novarocks_type_contract::ValueTypeRef::try_new(
                        novarocks_type_contract::ValueKind::Scalar(
                            arrow::datatypes::DataType::Int64,
                        ),
                        true,
                        storage.clone(),
                    )
                    .unwrap();
                    let layout = novarocks_type_contract::ValueLayout::try_from_normalized_arrow(
                        arrow::datatypes::DataType::Int64,
                        storage.clone(),
                    )
                    .unwrap();
                    novarocks_type_contract::FunctionValueType::try_from_value(semantic, layout)
                        .unwrap()
                })
                .collect::<Box<[_]>>();
            let execution = novarocks_connector_contract::ConnectorReadExecutionSchema::try_new(
                schema.clone(),
                bindings,
                storage.clone(),
            )?;
            let shape = novarocks_connector_contract::ConnectorSourceSchemaShape::try_new(
                schema,
                storage.clone(),
            )?;
            let source = novarocks_connector_contract::ConnectorSourceSchemaFacts::try_new(
                shape,
                std::collections::HashMap::new(),
                Box::new([]),
                storage.clone(),
            )?;
            let (request, work) = creation.observers();
            ConnectorReadPublicSchema::try_new_typed_observed(
                &execution,
                &source,
                storage,
                |layout| {
                    request(layout)
                        .map_err(novarocks_connector_contract::PureProviderCompileError::Control)
                },
                |units| {
                    work(units)
                        .map_err(novarocks_connector_contract::PureProviderCompileError::Control)
                },
            )
        }

        fn apply_filter(
            &self,
            _session: &ConnectorSession,
            _table: &Self::Table,
            _constraint: &Constraint<Self::Column>,
        ) -> ProviderReadFilterResult<Self::Table, Self::Column> {
            Ok(None)
        }

        fn apply_projection(
            &self,
            _session: &ConnectorSession,
            _table: &Self::Table,
            _assignments: &[Assignment<Self::Column>],
        ) -> Result<Option<Self::Table>, ConnectorError> {
            Ok(None)
        }

        fn apply_limit(
            &self,
            _session: &ConnectorSession,
            _table: &Self::Table,
            _limit: u64,
        ) -> Result<Option<ProviderReadLimitApplication<Self::Table>>, ConnectorError> {
            Ok(None)
        }

        fn get_system_table_plan(
            &self,
            _session: &ConnectorSession,
            _name: &SchemaTableName,
        ) -> Result<Option<ProviderReadSystemTablePlan<Self::Table>>, ConnectorError> {
            Ok(None)
        }

        fn get_change_window_plan(
            &self,
            _session: &ConnectorSession,
            _name: &SchemaTableName,
            _window: ConnectorReadChangeWindow,
        ) -> Result<Option<Self::Table>, ConnectorError> {
            Ok(None)
        }

        fn get_table_execute_plan(
            &self,
            _session: &ConnectorSession,
            _name: &SchemaTableName,
            _procedure: crate::connector::read_stack::ConnectorReadTableExecuteProcedure,
        ) -> Result<Option<Self::Table>, ConnectorError> {
            Ok(None)
        }
    }

    impl ProviderReadSplitManager for Probe {
        fn get_splits<'a>(
            &'a self,
            _session: &'a ConnectorSession,
            _table: &'a Self::Table,
            _columns: &'a [Assignment<Self::Column>],
            _dynamic_filter_columns: &'a BTreeSet<Self::Column>,
            _constraint: &'a Constraint<Self::Column>,
        ) -> futures::future::BoxFuture<
            'a,
            Result<Box<dyn ProviderReadSplitSource<Self>>, ConnectorError>,
        > {
            Box::pin(async move {
                self.split_calls.fetch_add(1, Ordering::SeqCst);
                let gate = self.split_gate.lock().unwrap().take();
                if let Some(gate) = gate {
                    gate.await.expect("split open gate");
                }
                Err(ConnectorError::new(
                    crate::connector::ConnectorErrorKind::Unsupported,
                    "attempt-access test provider does not enumerate splits",
                ))
            })
        }
    }

    struct BadFilter {
        covered: BTreeSet<ConnectorReadColumnHandle>,
        bad: ConnectorReadColumnHandle,
    }

    impl DynamicFilter<ConnectorReadColumnHandle> for BadFilter {
        fn columns_covered(&self) -> &BTreeSet<ConnectorReadColumnHandle> {
            &self.covered
        }

        fn current_predicate(&self) -> TupleDomain<ConnectorReadColumnHandle> {
            TupleDomain::with_column_domains(BTreeMap::from([(
                self.bad.clone(),
                crate::connector::read_stack::Domain::single_value(ConnectorValue::BigInt(9))
                    .expect("valid test domain"),
            )]))
            .expect("bounded test predicate")
        }

        fn is_complete(&self) -> bool {
            false
        }

        fn is_awaitable(&self) -> bool {
            true
        }
    }

    struct TestPageStream {
        dynamic_filter: Option<Arc<dyn DynamicFilter<Column>>>,
        close_called: Arc<AtomicBool>,
    }

    impl Stream for TestPageStream {
        type Item = Result<SourcePage, ConnectorError>;

        fn poll_next(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
            if let Some(dynamic_filter) = &self.dynamic_filter {
                let _ = dynamic_filter.current_predicate();
            }
            Poll::Ready(Some(Ok(SourcePage::zero_channel(1))))
        }
    }

    impl ConnectorPageStream for TestPageStream {
        fn metrics(&self) -> PageSourceMetrics {
            PageSourceMetrics::default()
        }

        fn memory_usage_bytes(&self) -> u64 {
            0
        }

        fn close(self: Pin<Box<Self>>) -> BoxFuture<'static, Result<(), ConnectorError>> {
            self.close_called.store(true, Ordering::SeqCst);
            Box::pin(async { Ok(()) })
        }
    }

    struct TestProvider {
        trigger_during_create: bool,
        close_called: Arc<AtomicBool>,
    }

    impl ProviderReadPageSourceProvider<Probe> for TestProvider {
        fn create_page_stream(
            &self,
            _session: &ConnectorSession,
            _table: &Table,
            _split: &Split,
            _scheduled_split_sequence_id: u64,
            _columns: &[Assignment<Column>],
            dynamic_filter: &Arc<dyn DynamicFilter<Column>>,
            _budget: &ConnectorPollBudget,
        ) -> Result<OwnedConnectorPageStream, ConnectorError> {
            if self.trigger_during_create {
                let _ = dynamic_filter.current_predicate();
            }
            Ok(Box::pin(TestPageStream {
                dynamic_filter: (!self.trigger_during_create).then(|| dynamic_filter.clone()),
                close_called: self.close_called.clone(),
            }))
        }
    }

    fn session() -> ConnectorSession {
        ConnectorSession::try_new("q", "u", "UTC", "en_US", SystemTime::UNIX_EPOCH)
            .expect("session")
    }

    fn prepared_source_metadata(
        adapter: &ReadRuntimeAdapter<Probe>,
    ) -> Arc<crate::connector::ConnectorTableMetadata> {
        use crate::connector::{
            ConnectorTableDefinitionFacts, ConnectorTableIdentity, ConnectorTableMetadata,
            ConnectorTableMetadataHeader, ConnectorTablePlanningFacts,
        };
        let storage = novarocks_type_contract::ValueTypeStorageLifetime::pure();
        let schema = Arc::new(arrow::datatypes::Schema::empty());
        let execution = novarocks_connector_contract::ConnectorReadExecutionSchema::try_new(
            schema.clone(),
            Vec::new().into_boxed_slice(),
            storage.clone(),
        )
        .unwrap();
        let mut request = |_| Ok(());
        let mut work = |_| Ok(());
        let mut creation =
            super::super::ConnectorReadSchemaCreation::new(storage, &mut request, &mut work);
        Arc::new(
            ConnectorTableMetadata::try_new_typed(
                ConnectorTableMetadataHeader {
                    identity: ConnectorTableIdentity {
                        instance_id: adapter.provider.descriptor.instance_id.clone(),
                        namespace: "db".into(),
                        table: "source".into(),
                    },
                    planning_facts: ConnectorTablePlanningFacts::empty(),
                    definition_facts: ConnectorTableDefinitionFacts::empty(),
                    version: None,
                    statistics_data_version: None,
                    table: crate::connector::ConnectorTableHandle::try_new(
                        adapter.provider.descriptor.instance_id.clone(),
                        bytes::Bytes::from_static(b"prepared-source"),
                    )
                    .unwrap(),
                },
                schema,
                execution,
                &mut creation,
            )
            .unwrap(),
        )
    }

    fn prepared_source(
        adapter: &ReadRuntimeAdapter<Probe>,
        admission: Option<crate::connector::ConnectorChangeWindowAdmission>,
    ) -> super::super::ConnectorPreparedSourceRead {
        prepared_source_with_facts(
            adapter,
            admission,
            ProbeSourceFacts {
                ordinal: 41,
                _retention: None,
            },
        )
    }

    fn prepared_source_with_facts(
        adapter: &ReadRuntimeAdapter<Probe>,
        admission: Option<crate::connector::ConnectorChangeWindowAdmission>,
        facts: ProbeSourceFacts,
    ) -> super::super::ConnectorPreparedSourceRead {
        use crate::connector::read_stack::{
            ConnectorReadChangeWindow, ConnectorReadSourcePreparationRequest,
        };
        let object = crate::connector::ConnectorTableObjectId::try_new(bytes::Bytes::from_static(
            b"source-object",
        ))
        .unwrap();
        let revision = |snapshot| {
            crate::connector::ConnectorExactSemanticRevision::try_from_table_object_and_snapshot(
                adapter.provider.descriptor.provider_id.clone(),
                &object,
                Some(snapshot),
            )
            .unwrap()
        };
        let name = SchemaTableName::try_new("db", "source").unwrap();
        let observation = crate::connector::ConnectorProcessLocalPayload::new(
            crate::connector::ConnectorProviderBindingKey {
                instance_id: adapter.provider.descriptor.instance_id.clone(),
                incarnation: crate::connector::ProviderBindingEpoch::from_bytes([1; 16]),
            },
            Arc::new(41_i64),
        );
        let request = if admission.is_some() {
            ConnectorReadSourcePreparationRequest::change_window(
                name,
                prepared_source_metadata(adapter).table.clone(),
                observation,
                object.clone(),
                ConnectorReadChangeWindow::new(40, 41),
                revision(40),
                revision(41),
            )
        } else {
            ConnectorReadSourcePreparationRequest::snapshot(
                name,
                prepared_source_metadata(adapter).table.clone(),
                observation,
                object.clone(),
                41,
                revision(41),
            )
        };
        super::super::ConnectorPreparedSourceRead::new(
            adapter.binding.clone(),
            request,
            admission,
            prepared_source_metadata(adapter),
            BoundSourcePreparation::<Probe> { facts },
        )
    }

    #[test]
    fn prepared_source_projects_only_the_capabilities_of_its_closed_decision() {
        use crate::connector::ConnectorChangeWindowAdmission as A;
        use crate::connector::read_stack::ConnectorReadSourceProjection as P;
        let provider = Arc::new(Probe::new());
        let adapter = ReadRuntimeAdapter::new(Arc::clone(&provider));
        let incremental = A::Incremental {
            has_inserts: true,
            has_deletes: false,
            partition_impact: crate::connector::ConnectorChangeWindowPartitionImpact::Unpartitioned,
        };
        for decision in [
            None,
            Some(A::MetadataOnly),
            Some(A::FullRebuild(
                crate::connector::ConnectorChangeWindowFullRebuildReason::LineageBroken {
                    from_snapshot_id: 40,
                },
            )),
            Some(incremental),
        ] {
            let source = prepared_source(&adapter, decision.clone());
            for projection in [P::To, P::From, P::Delta] {
                let before = provider.created_calls.load(Ordering::SeqCst);
                let result = adapter.project_prepared_source_read(&session(), &source, projection);
                let permitted = projection == P::To
                    || matches!(decision, Some(A::Incremental { .. }))
                    || (projection == P::Delta && matches!(decision, Some(A::MetadataOnly)));
                assert_eq!(result.is_ok(), permitted);
                assert_eq!(
                    provider.created_calls.load(Ordering::SeqCst),
                    before + u64::from(permitted),
                );
                if let Err(error) = result {
                    assert_eq!(error.kind(), ConnectorErrorKind::InvalidRequest);
                }
            }
        }
        assert_eq!(provider.prepared_calls.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn prepared_source_rejects_foreign_bindings_and_payloads_before_projection() {
        use crate::connector::read_stack::ConnectorReadSourceProjection as P;
        let provider = Arc::new(Probe::new());
        let adapter = ReadRuntimeAdapter::new(Arc::clone(&provider));
        let mut foreign = Probe::new();
        foreign.catalog_handle = CatalogHandle::new(
            foreign.descriptor.instance_id.clone(),
            crate::connector::CatalogVersion::from_bytes([4; 32]),
        );
        let foreign = ReadRuntimeAdapter::new(Arc::new(foreign));
        let source = prepared_source(&adapter, None);
        assert!(
            foreign
                .project_prepared_source_read(&session(), &source, P::To)
                .is_err()
        );
        let wrong_type = super::super::ConnectorPreparedSourceRead::new(
            adapter.binding.clone(),
            source.request().clone(),
            None,
            prepared_source_metadata(&adapter),
            41_usize,
        );
        assert!(
            adapter
                .project_prepared_source_read(&session(), &wrong_type, P::To)
                .is_err()
        );
        assert_eq!(provider.created_calls.load(Ordering::SeqCst), 0);
        assert_eq!(foreign.provider.created_calls.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn prepared_source_projects_into_another_request_of_the_same_exact_binding() {
        use crate::connector::read_stack::ConnectorReadSourceProjection;
        let original = ReadRuntimeAdapter::new(Arc::new(Probe::new()));
        let request = ReadRuntimeAdapter::new(Arc::new(Probe::new()));
        let source = prepared_source(&original, None);
        let alias = source.clone();
        assert!(std::ptr::eq(source.to_metadata(), alias.to_metadata()));
        request
            .project_prepared_source_read(&session(), &source, ConnectorReadSourceProjection::To)
            .unwrap();
        assert_eq!(original.provider.created_calls.load(Ordering::SeqCst), 0);
        assert_eq!(request.provider.created_calls.load(Ordering::SeqCst), 1);
        assert_eq!(request.provider.prepared_calls.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn prepared_source_aliases_retain_facts_without_the_original_request_inventory() {
        use crate::connector::read_stack::ConnectorReadSourceProjection;
        let inventory_drops = Arc::new(AtomicU64::new(0));
        let facts_drops = Arc::new(AtomicU64::new(0));
        let provider = Arc::new(Probe {
            _source_request_inventory: Some(SourceDropWitness(inventory_drops.clone())),
            ..Probe::new()
        });
        let old_request = Arc::downgrade(&provider);
        let adapter = ReadRuntimeAdapter::new(provider.clone());
        let source = prepared_source_with_facts(
            &adapter,
            None,
            ProbeSourceFacts {
                ordinal: 41,
                _retention: Some(SourceDropWitness(facts_drops.clone())),
            },
        );
        let alias = source.clone();
        drop(adapter);
        drop(provider);
        assert!(old_request.upgrade().is_none());
        assert_eq!(inventory_drops.load(Ordering::SeqCst), 1);
        assert_eq!(facts_drops.load(Ordering::SeqCst), 0);

        // A different request of the same exact binding consumes retained
        // facts after the preparing provider and its inventory actually drop.
        let request = ReadRuntimeAdapter::new(Arc::new(Probe::new()));
        request
            .project_prepared_source_read(&session(), &alias, ConnectorReadSourceProjection::To)
            .unwrap();
        assert_eq!(request.provider.created_calls.load(Ordering::SeqCst), 1);
        assert_eq!(request.provider.prepared_calls.load(Ordering::SeqCst), 0);
        drop(source);
        assert_eq!(facts_drops.load(Ordering::SeqCst), 0);
        drop(alias);
        assert_eq!(facts_drops.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn frozen_mv_target_requires_provider_support_without_generic_fallback() {
        let provider = Arc::new(Probe::new());
        let name = SchemaTableName::try_new("db", "orders").unwrap();
        let object = crate::connector::ConnectorTableObjectId::try_new(bytes::Bytes::from_static(
            b"opaque-target-object",
        ))
        .unwrap();
        let direct = ProviderReadMetadata::get_mv_target_handle(
            provider.as_ref(),
            &session(),
            &name,
            &object,
            Some(41),
        )
        .unwrap_err();
        assert_eq!(direct.kind(), ConnectorErrorKind::Unsupported);
        let adapter = ReadRuntimeAdapter::new(provider);
        for snapshot in [Some(41), None] {
            let erased = ConnectorReadMetadata::get_mv_target_handle(
                &adapter,
                &session(),
                &name,
                &object,
                snapshot,
            )
            .unwrap_err();
            assert_eq!(erased.kind(), ConnectorErrorKind::Unsupported);
        }
        assert!(
            ConnectorReadMetadata::get_table_handle(
                &adapter,
                &session(),
                &name,
                ConnectorReadRelationVersion::SnapshotId(41),
                None,
            )
            .unwrap()
            .is_some()
        );
    }

    fn adapter_and_filter() -> (
        ReadRuntimeAdapter<Probe>,
        Arc<ConnectorReadDynamicFilter>,
        ConnectorReadTableHandle,
        ConnectorReadSplit,
    ) {
        let adapter = ReadRuntimeAdapter::new(Arc::new(Probe::new()));
        let valid = adapter.wrap_column(Column(1));
        let bad = column_handle(adapter.binding().clone(), WrongColumn);
        let filter: Arc<ConnectorReadDynamicFilter> = Arc::new(BadFilter {
            covered: BTreeSet::from([valid]),
            bad,
        });
        let table = adapter.wrap_table(Table);
        let split = adapter.wrap_split(Split);
        (adapter, filter, table, split)
    }

    fn test_session() -> ConnectorSession {
        ConnectorSession::try_new("q", "u", "UTC", "en_US", SystemTime::UNIX_EPOCH)
            .expect("session")
    }

    fn current_relation_request(
        name: SchemaTableName,
    ) -> crate::connector::read_stack::ConnectorReadRelationPreparationRequest {
        use crate::connector::read_stack::{
            ConnectorReadRelationPreparationKind as K, ConnectorReadRelationPreparationRequest as R,
        };
        R::new(
            name,
            K::Table {
                version: ConnectorReadRelationVersion::Current,
                reference: None,
            },
        )
    }

    #[tokio::test]
    async fn relation_preparation_waits_before_the_exact_author_creates_a_handle() {
        use futures::FutureExt;
        let (release, gate) = futures::channel::oneshot::channel();
        let probe = Probe::new();
        *probe.preparation_gate.lock().unwrap() = Some(gate);
        let provider = Arc::new(probe);
        let adapter = ReadRuntimeAdapter::new(Arc::clone(&provider));
        let session = test_session();
        let name = SchemaTableName::try_new("db", "t").unwrap();
        let request = current_relation_request(name);
        let mut preparing = adapter.prepare_relation(&session, &request);
        assert!(preparing.as_mut().now_or_never().is_none());
        assert_eq!(provider.prepared_calls.load(Ordering::SeqCst), 1);
        assert_eq!(provider.created_calls.load(Ordering::SeqCst), 0);
        release.send(()).unwrap();
        let prepared = preparing.await.unwrap().unwrap();
        assert_eq!(prepared.binding(), adapter.binding());
        let (table, _) = adapter
            .create_prepared_relation(prepared)
            .unwrap()
            .unwrap()
            .into_parts();
        assert_eq!(table.binding(), adapter.binding());
        assert_eq!(provider.created_calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn prepared_relation_cannot_cross_equal_catalog_bindings_with_distinct_authors() {
        let original = Arc::new(Probe::new());
        let replacement = Arc::new(Probe::new());
        let adapter = ReadRuntimeAdapter::new(Arc::clone(&original));
        let other = ReadRuntimeAdapter::new(Arc::clone(&replacement));
        assert_eq!(adapter.binding(), other.binding());
        let session = test_session();
        let name = SchemaTableName::try_new("db", "t").unwrap();
        let request = current_relation_request(name);
        let prepared = adapter
            .prepare_relation(&session, &request)
            .await
            .unwrap()
            .unwrap();
        let error = other.create_prepared_relation(prepared).err().unwrap();
        assert_eq!(error.kind(), binding_error().kind());
        assert_eq!(original.created_calls.load(Ordering::SeqCst), 0);
        assert_eq!(replacement.created_calls.load(Ordering::SeqCst), 0);
        let prepared = adapter
            .prepare_relation(&session, &request)
            .await
            .unwrap()
            .unwrap();
        adapter.create_prepared_relation(prepared).unwrap().unwrap();
        assert_eq!(original.created_calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn full_relation_preparation_keeps_original_author_and_selection() {
        use crate::connector::read_stack::{
            ConnectorReadRelationPreparationKind as K, ConnectorReadRelationPreparationRequest as R,
        };
        let provider = Arc::new(Probe::new());
        let adapter = ReadRuntimeAdapter::new(Arc::clone(&provider));
        let other = ReadRuntimeAdapter::new(Arc::new(Probe::new()));
        let session = test_session();
        let request = R::new(
            SchemaTableName::try_new("db", "t").unwrap(),
            K::Table {
                version: ConnectorReadRelationVersion::SnapshotId(41),
                reference: Some("branch".into()),
            },
        );
        let prepared = adapter
            .prepare_relation(&session, &request)
            .await
            .unwrap()
            .unwrap();
        let error = other.create_prepared_relation(prepared).err().unwrap();
        assert_eq!(error.kind(), binding_error().kind());
        assert_eq!(provider.created_calls.load(Ordering::SeqCst), 0);
        let prepared = adapter
            .prepare_relation(&session, &request)
            .await
            .unwrap()
            .unwrap();
        let (table, hint) = adapter
            .create_prepared_relation(prepared)
            .unwrap()
            .unwrap()
            .into_parts();
        assert_eq!(table.binding(), adapter.binding());
        assert!(hint.is_none());
        assert_eq!(provider.prepared_calls.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn owned_split_future_keeps_the_same_author_after_borrowing_wait_is_cancelled() {
        use crate::connector::read_stack::ConnectorReadSplitOpenRequest;
        use futures::FutureExt;
        let (release, gate) = futures::channel::oneshot::channel();
        let provider = Arc::new(Probe::new());
        *provider.split_gate.lock().unwrap() = Some(gate);
        let original = Arc::downgrade(&provider);
        let adapter = ReadRuntimeAdapter::new(Arc::clone(&provider));
        let table = adapter.wrap_table(Table);
        let mut original_owner = None;
        let mut opening = adapter.get_splits(
            ConnectorReadSplitOpenRequest::new(
                test_session(),
                table,
                Vec::new(),
                BTreeSet::new(),
                Constraint::of_summary(TupleDomain::all()),
            ),
            &mut original_owner,
        );
        assert!(opening.as_mut().now_or_never().is_none());
        assert_eq!(provider.split_calls.load(Ordering::SeqCst), 1);
        drop(adapter);
        drop(provider);
        assert!(original.upgrade().is_some());
        // Dropping the temporary borrowing poll did not drop the stable F.
        release.send(()).unwrap();
        let error = opening.await.err().unwrap();
        assert_eq!(error.kind(), ConnectorErrorKind::Unsupported);
        assert!(original.upgrade().is_none());
    }

    #[tokio::test]
    async fn occupied_split_owner_refuses_before_provider_and_preserves_original() {
        let provider = Arc::new(Probe::new());
        let adapter = ReadRuntimeAdapter::new(provider.clone());
        let retained = Arc::new(41_i64);
        let weak = Arc::downgrade(&retained);
        let mut owner = Some(crate::connector::ConnectorProcessLocalPayload::new(
            crate::connector::ConnectorProviderBindingKey {
                instance_id: provider.descriptor.instance_id.clone(),
                incarnation: crate::connector::ProviderBindingEpoch::from_bytes([1; 16]),
            },
            retained,
        ));
        let error = adapter
            .get_splits(
                super::super::ConnectorReadSplitOpenRequest::new(
                    test_session(),
                    adapter.wrap_table(Table),
                    Vec::new(),
                    BTreeSet::new(),
                    Constraint::of_summary(TupleDomain::all()),
                ),
                &mut owner,
            )
            .await
            .err()
            .unwrap();
        assert_eq!(
            error.kind(),
            crate::connector::ConnectorErrorKind::InvalidRequest
        );
        assert_eq!(provider.split_calls.load(Ordering::SeqCst), 0);
        assert_eq!(
            **owner.as_ref().unwrap().value::<i64>("catalog").unwrap(),
            41
        );
        assert!(weak.upgrade().is_some());
        drop(owner);
        assert!(weak.upgrade().is_none());
    }

    fn probe_public_schema(
        adapter: &ReadRuntimeAdapter<Probe>,
        table: &ConnectorReadTableHandle,
        columns: &[ConnectorReadColumnHandle],
    ) -> Result<
        ConnectorReadPublicSchema,
        novarocks_connector_contract::PureProviderCompileError<ConnectorError>,
    > {
        let mut request = |_| Ok(());
        let mut work = |_| Ok(());
        let mut creation = super::super::ConnectorReadSchemaCreation::new(
            novarocks_type_contract::ValueTypeStorageLifetime::pure(),
            &mut request,
            &mut work,
        );
        ConnectorReadMetadata::read_public_schema(
            adapter,
            &test_session(),
            table,
            columns,
            &mut creation,
        )
    }

    #[test]
    fn public_read_schema_answers_the_frozen_columns_in_assignment_order() {
        let adapter = ReadRuntimeAdapter::new(Arc::new(Probe::new()));
        let table = adapter.wrap_table(Table);
        let columns = [2, 1, 2].map(|id| adapter.wrap_column(Column(id)));
        let schema = probe_public_schema(&adapter, &table, &columns).expect("public schema");
        assert_eq!(
            schema
                .schema()
                .fields()
                .iter()
                .map(|field| field.name().as_str())
                .collect::<Vec<_>>(),
            ["c2", "c1", "c2"]
        );
        assert_eq!(
            schema.logical_types(),
            [novarocks_type_contract::ValueLogicalType::Physical; 3]
        );
    }

    #[test]
    fn public_read_schema_refuses_an_answer_that_cannot_be_paired_with_its_columns() {
        let adapter = ReadRuntimeAdapter::new(Arc::new(Probe::new()));
        let table = adapter.wrap_table(Table);
        let columns = [adapter.wrap_column(Column(PUBLIC_SCHEMA_EXTRA_FIELD))];
        let error = probe_public_schema(&adapter, &table, &columns)
            .expect_err("one field too many is refused");
        assert!(
            matches!(error, novarocks_connector_contract::PureProviderCompileError::Provider(error)
            if error.kind() == ConnectorErrorKind::InvalidRequest)
        );
    }

    #[test]
    fn public_read_schema_refuses_a_column_of_another_generation() {
        let adapter = ReadRuntimeAdapter::new(Arc::new(Probe::new()));
        let mut other = Probe::new();
        other.catalog_handle = CatalogHandle::new(
            ConnectorInstanceId::parse("catalog").expect("instance ID"),
            crate::connector::CatalogVersion::from_bytes([4; 32]),
        );
        let other = ReadRuntimeAdapter::new(Arc::new(other));
        let table = adapter.wrap_table(Table);
        let foreign = [other.wrap_column(Column(1))];
        let error = probe_public_schema(&adapter, &table, &foreign)
            .expect_err("a column of another admitted generation is refused");
        assert!(
            matches!(error, novarocks_connector_contract::PureProviderCompileError::Provider(error)
            if error.kind() == ConnectorErrorKind::InvalidRequest)
        );
    }

    #[test]
    fn public_read_schema_preserves_original_creation_refusal_before_provider_entry() {
        use novarocks_connector_contract::PureProviderCompileError;
        use novarocks_type_contract::{CompileControlError, ValueTypeStorageLifetime};
        for cause in [
            CompileControlError::Cancelled,
            CompileControlError::DeadlineExceeded,
            CompileControlError::ResourceExhausted,
        ] {
            for stop_at_request in [false, true] {
                let probe = Arc::new(Probe::new());
                let adapter = ReadRuntimeAdapter::new(probe.clone());
                let table = adapter.wrap_table(Table);
                let columns = [adapter.wrap_column(Column(1))];
                let requests = std::cell::RefCell::new(Vec::new());
                let steps = std::cell::RefCell::new(Vec::new());
                let mut request = |layout| {
                    requests.borrow_mut().push(layout);
                    Err(cause.clone())
                };
                let mut work = |units| {
                    steps.borrow_mut().push(units);
                    if stop_at_request {
                        Ok(())
                    } else {
                        Err(cause.clone())
                    }
                };
                let mut creation = super::super::ConnectorReadSchemaCreation::new(
                    ValueTypeStorageLifetime::pure(),
                    &mut request,
                    &mut work,
                );
                let result = ConnectorReadMetadata::read_public_schema(
                    &adapter,
                    &test_session(),
                    &table,
                    &columns,
                    &mut creation,
                );
                assert!(
                    matches!(result, Err(PureProviderCompileError::Control(actual)) if actual == cause)
                );
                assert_eq!(probe.public_schema_calls.load(Ordering::SeqCst), 0);
                assert_eq!(*steps.borrow(), [1]);
                if stop_at_request {
                    assert_eq!(
                        *requests.borrow(),
                        [std::alloc::Layout::array::<&Column>(1).unwrap()]
                    );
                } else {
                    assert!(requests.borrow().is_empty());
                }
            }
        }
    }

    #[test]
    fn relation_freezes_the_provider_transaction_without_exposing_payloads() {
        let adapter = ReadRuntimeAdapter::new(Arc::new(Probe::new()));
        let relation = adapter
            .relation(ConnectorReadRelationKind::Table, adapter.wrap_table(Table))
            .expect("same-binding table forms a relation");
        assert_eq!(relation.kind(), ConnectorReadRelationKind::Table);
        assert_eq!(
            adapter
                .transaction(relation.transaction())
                .expect("transaction"),
            &()
        );
    }

    #[test]
    fn static_attempt_access_requires_explicit_mode_and_exact_binding() {
        let adapter = ReadRuntimeAdapter::new(Arc::new(Probe::new()));
        let table = adapter.wrap_table(Table);
        let unsupported = ConnectorReadRequestControl::unsupported_attempt_access(
            Arc::new(adapter.clone()),
            Arc::new(adapter.clone()),
        );
        let error = match unsupported.seal_attempt_access(&table) {
            Ok(_) => panic!("absence of an explicit access mode must fail closed"),
            Err(error) => error,
        };
        assert_eq!(error.kind(), ConnectorErrorKind::Unsupported);

        let static_control = ConnectorReadRequestControl::static_attempt_access(
            Arc::new(adapter.clone()),
            Arc::new(adapter.clone()),
            adapter.binding().clone(),
        );
        let source = static_control
            .seal_attempt_access(&table)
            .expect("static access is explicitly declared");
        let attempt = crate::connector::ConnectorAttemptContext::from_admitted_request(
            crate::connector::ConnectorRequestContext::try_new(
                std::time::Instant::now() + std::time::Duration::from_secs(1),
                crate::connector::ConnectorStopOwner::new().view(),
                crate::connector::MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES,
                crate::connector::MAX_CONNECTOR_TOTAL_PAYLOAD_BYTES,
            )
            .expect("attempt request context"),
        );
        futures::executor::block_on(source.for_attempt(&attempt))
            .expect("same static binding rebinds for a new attempt");

        let foreign_adapter = ReadRuntimeAdapter::new(Arc::new(Probe {
            public_schema_calls: AtomicU64::new(0),
            descriptor: ConnectorInstanceDescriptor {
                provider_id: ConnectorProviderId::parse("probe").expect("provider ID"),
                instance_id: ConnectorInstanceId::parse("other").expect("instance ID"),
            },
            catalog_handle: CatalogHandle::new(
                ConnectorInstanceId::parse("other").expect("instance ID"),
                crate::connector::CatalogVersion::from_bytes([4; 32]),
            ),

            ..Probe::new()
        }));
        let error = match static_control.seal_attempt_access(&foreign_adapter.wrap_table(Table)) {
            Ok(_) => panic!("a foreign generation cannot enter the static access source"),
            Err(error) => error,
        };
        assert_eq!(error.kind(), ConnectorErrorKind::InvalidRequest);
    }

    #[test]
    fn provider_attempt_access_rejects_foreign_returned_generation() {
        struct ReturnRuntime(ConnectorReadAttemptRuntime);
        impl ConnectorReadAttemptAccessReacquirer for ReturnRuntime {
            fn for_attempt<'a>(
                &'a self,
                _request: &'a crate::connector::ConnectorAttemptContext,
            ) -> futures::future::BoxFuture<'a, Result<ConnectorReadAttemptRuntime, ConnectorError>>
            {
                Box::pin(async move { Ok(self.0.clone()) })
            }
        }

        struct SealWith(Arc<dyn ConnectorReadAttemptAccessReacquirer>);
        impl ConnectorReadAttemptAccessSealer for SealWith {
            fn seal(
                &self,
                _frozen: &ConnectorReadTableHandle,
                mint: ConnectorReadAttemptAccessMint,
            ) -> Result<ConnectorReadAttemptAccessSource, ConnectorError> {
                Ok(mint.seal(Arc::clone(&self.0)))
            }
        }

        let adapter = ReadRuntimeAdapter::new(Arc::new(Probe::new()));
        let foreign_adapter = ReadRuntimeAdapter::new(Arc::new(Probe {
            public_schema_calls: AtomicU64::new(0),
            descriptor: ConnectorInstanceDescriptor {
                provider_id: ConnectorProviderId::parse("probe").expect("provider ID"),
                instance_id: ConnectorInstanceId::parse("foreign").expect("instance ID"),
            },
            catalog_handle: CatalogHandle::new(
                ConnectorInstanceId::parse("foreign").expect("instance ID"),
                crate::connector::CatalogVersion::from_bytes([9; 32]),
            ),

            ..Probe::new()
        }));
        let foreign = ConnectorReadAttemptRuntime::new(Arc::new(foreign_adapter.clone()));
        let control = ConnectorReadRequestControl::provider_reacquire_attempt_access(
            Arc::new(adapter.clone()),
            Arc::new(adapter.clone()),
            Arc::new(SealWith(Arc::new(ReturnRuntime(foreign)))),
        );
        let source = control
            .seal_attempt_access(&adapter.wrap_table(Table))
            .expect("same-generation handle seals");
        let request = crate::connector::ConnectorAttemptContext::from_admitted_request(
            crate::connector::ConnectorRequestContext::try_new(
                std::time::Instant::now() + std::time::Duration::from_secs(1),
                crate::connector::ConnectorStopOwner::new().view(),
                crate::connector::MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES,
                crate::connector::MAX_CONNECTOR_TOTAL_PAYLOAD_BYTES,
            )
            .expect("attempt request context"),
        );
        let error = match futures::executor::block_on(source.for_attempt(&request)) {
            Ok(_) => panic!("reacquirer must not replace the sealed generation"),
            Err(error) => error,
        };
        assert_eq!(error.kind(), ConnectorErrorKind::InvalidRequest);
    }

    #[test]
    fn provider_sealer_cannot_replay_a_source_from_an_earlier_seal() {
        struct ReturnRuntime(ConnectorReadAttemptRuntime);
        impl ConnectorReadAttemptAccessReacquirer for ReturnRuntime {
            fn for_attempt<'a>(
                &'a self,
                _request: &'a crate::connector::ConnectorAttemptContext,
            ) -> futures::future::BoxFuture<'a, Result<ConnectorReadAttemptRuntime, ConnectorError>>
            {
                Box::pin(async move { Ok(self.0.clone()) })
            }
        }

        struct ReplaySource {
            prior: Mutex<Option<ConnectorReadAttemptAccessSource>>,
            reacquirer: Arc<dyn ConnectorReadAttemptAccessReacquirer>,
        }
        impl ConnectorReadAttemptAccessSealer for ReplaySource {
            fn seal(
                &self,
                _frozen: &ConnectorReadTableHandle,
                mint: ConnectorReadAttemptAccessMint,
            ) -> Result<ConnectorReadAttemptAccessSource, ConnectorError> {
                let mut prior = self.prior.lock().expect("prior source lock");
                if let Some(source) = prior.as_ref() {
                    return Ok(source.clone());
                }
                let source = mint.seal(Arc::clone(&self.reacquirer));
                *prior = Some(source.clone());
                Ok(source)
            }
        }

        let adapter = ReadRuntimeAdapter::new(Arc::new(Probe::new()));
        let returned = ConnectorReadAttemptRuntime::new(Arc::new(adapter.clone()));
        let control = ConnectorReadRequestControl::provider_reacquire_attempt_access(
            Arc::new(adapter.clone()),
            Arc::new(adapter.clone()),
            Arc::new(ReplaySource {
                prior: Mutex::new(None),
                reacquirer: Arc::new(ReturnRuntime(returned)),
            }),
        );
        control
            .seal_attempt_access(&adapter.wrap_table(Table))
            .expect("first seal owns its mint");
        let error = match control.seal_attempt_access(&adapter.wrap_table(Table)) {
            Ok(_) => panic!("a prior source must not satisfy another seal"),
            Err(error) => error,
        };
        assert_eq!(error.kind(), ConnectorErrorKind::InvalidRequest);
    }

    #[test]
    fn attempt_access_rejects_mixed_metadata_and_split_generations() {
        let adapter = ReadRuntimeAdapter::new(Arc::new(Probe::new()));
        let foreign_adapter = ReadRuntimeAdapter::new(Arc::new(Probe {
            public_schema_calls: AtomicU64::new(0),
            descriptor: ConnectorInstanceDescriptor {
                provider_id: ConnectorProviderId::parse("probe").expect("provider ID"),
                instance_id: ConnectorInstanceId::parse("foreign").expect("instance ID"),
            },
            catalog_handle: CatalogHandle::new(
                ConnectorInstanceId::parse("foreign").expect("instance ID"),
                crate::connector::CatalogVersion::from_bytes([8; 32]),
            ),

            ..Probe::new()
        }));
        let control = ConnectorReadRequestControl::static_attempt_access(
            Arc::new(adapter.clone()),
            Arc::new(foreign_adapter),
            adapter.binding().clone(),
        );
        let error = match control.seal_attempt_access(&adapter.wrap_table(Table)) {
            Ok(_) => panic!("mixed request services must fail closed"),
            Err(error) => error,
        };
        assert_eq!(error.kind(), ConnectorErrorKind::InvalidRequest);
    }

    #[test]
    fn creation_time_filter_contract_error_closes_the_unpublished_stream() {
        let (adapter, filter, table, split) = adapter_and_filter();
        let close_called = Arc::new(AtomicBool::new(false));
        let provider = AdapterPageSourceProvider {
            provider: Arc::new(TestProvider {
                trigger_during_create: true,
                close_called: close_called.clone(),
            }),
            adapter,
        };

        let error = match provider.create_page_stream(
            &session(),
            &table,
            &split,
            1,
            &[],
            &filter,
            &ConnectorPollBudget::new(),
        ) {
            Ok(_) => panic!("latching during creation must reject the stream"),
            Err(error) => error,
        };
        assert_eq!(error.kind(), ConnectorErrorKind::InvalidRequest);
        assert!(close_called.load(Ordering::SeqCst));
    }

    #[test]
    fn poll_time_filter_contract_error_wins_over_the_same_poll_page() {
        use futures::StreamExt;

        let (adapter, filter, table, split) = adapter_and_filter();
        let close_called = Arc::new(AtomicBool::new(false));
        let provider = AdapterPageSourceProvider {
            provider: Arc::new(TestProvider {
                trigger_during_create: false,
                close_called: close_called.clone(),
            }),
            adapter,
        };
        let mut stream = provider
            .create_page_stream(
                &session(),
                &table,
                &split,
                1,
                &[],
                &filter,
                &ConnectorPollBudget::new(),
            )
            .expect("stream is valid before its first provider callback");

        let error = futures::executor::block_on(stream.next())
            .expect("an item")
            .expect_err("latched error must win over a same-poll page");
        assert_eq!(error.kind(), ConnectorErrorKind::InvalidRequest);
        futures::executor::block_on(stream.close()).expect("the host closes the ended stream");
        assert!(close_called.load(Ordering::SeqCst));
    }

    #[derive(Default)]
    struct TestPreparationControl {
        paused: AtomicBool,
        stopped: AtomicBool,
        retained: AtomicU64,
        changed: futures::task::AtomicWaker,
    }

    impl ConnectorPreparationControl for TestPreparationControl {
        fn request_pause(&self) {
            self.paused.store(true, Ordering::SeqCst);
        }

        fn request_resume(&self) {
            self.paused.store(false, Ordering::SeqCst);
        }

        fn request_reclaim(&self) {
            self.retained.store(0, Ordering::SeqCst);
        }

        fn request_stop(&self) {
            self.stopped.store(true, Ordering::SeqCst);
            self.retained.store(0, Ordering::SeqCst);
            self.changed.wake();
        }

        fn retained_input_bytes(&self) -> u64 {
            self.retained.load(Ordering::SeqCst)
        }

        fn is_drained(&self) -> bool {
            self.stopped.load(Ordering::SeqCst)
        }

        fn wait_drained(
            &self,
        ) -> std::pin::Pin<
            Box<
                dyn std::future::Future<
                        Output = crate::connector::read_stack::ConnectorPreparationExit,
                    > + Send
                    + '_,
            >,
        > {
            Box::pin(futures::future::poll_fn(move |cx| {
                self.changed.register(cx.waker());
                if self.is_drained() {
                    std::task::Poll::Ready(
                        crate::connector::read_stack::ConnectorPreparationExit::Exited,
                    )
                } else {
                    std::task::Poll::Pending
                }
            }))
        }
    }

    struct TestPrepared {
        control: Arc<TestPreparationControl>,
        filter: Arc<dyn DynamicFilter<Column>>,
        close_called: Arc<AtomicBool>,
    }

    impl ProviderPreparedPageSource<Probe> for TestPrepared {
        fn advance(
            &mut self,
            remaining_input_bytes: u64,
        ) -> Result<ConnectorPreparationProgress, ConnectorError> {
            let _ = self.filter.current_predicate();
            if remaining_input_bytes < 4 {
                return Ok(ConnectorPreparationProgress::Deferred);
            }
            self.control.retained.store(4, Ordering::SeqCst);
            Ok(ConnectorPreparationProgress::Ready)
        }

        fn retained_input_bytes(&self) -> u64 {
            self.control.retained_input_bytes()
        }

        fn control(&self) -> Arc<dyn ConnectorPreparationControl> {
            self.control.clone()
        }

        fn promote(
            self: Box<Self>,
            dynamic_filter: &Arc<dyn DynamicFilter<Column>>,
            _budget: &ConnectorPollBudget,
        ) -> Result<OwnedConnectorPageStream, ConnectorError> {
            let _ = dynamic_filter.current_predicate();
            Ok(Box::pin(TestPageStream {
                dynamic_filter: None,
                close_called: self.close_called.clone(),
            }))
        }
    }

    struct TestPreparationProvider {
        control: Arc<TestPreparationControl>,
        close_called: Arc<AtomicBool>,
        created: Arc<AtomicBool>,
        supports_preparation: bool,
    }

    impl ProviderReadPageSourceProvider<Probe> for TestPreparationProvider {
        fn create_page_stream(
            &self,
            _session: &ConnectorSession,
            _table: &Table,
            _split: &Split,
            _scheduled_split_sequence_id: u64,
            _columns: &[Assignment<Column>],
            _dynamic_filter: &Arc<dyn DynamicFilter<Column>>,
            _budget: &ConnectorPollBudget,
        ) -> Result<OwnedConnectorPageStream, ConnectorError> {
            self.created.store(true, Ordering::SeqCst);
            Ok(Box::pin(TestPageStream {
                dynamic_filter: None,
                close_called: self.close_called.clone(),
            }))
        }

        fn prepare_page_source(
            &self,
            _session: &ConnectorSession,
            _table: &Table,
            _split: &Split,
            _scheduled_split_sequence_id: u64,
            _columns: &[Assignment<Column>],
            dynamic_filter: &Arc<dyn DynamicFilter<Column>>,
        ) -> Result<ProviderPreparationStart<Probe>, ConnectorError> {
            if !self.supports_preparation {
                return Ok(ProviderPreparationStart::Unsupported);
            }
            Ok(ProviderPreparationStart::Prepared(Box::new(TestPrepared {
                control: self.control.clone(),
                filter: dynamic_filter.clone(),
                close_called: self.close_called.clone(),
            })))
        }
    }

    #[test]
    fn unsupported_preparation_never_opens_a_page_stream() {
        let (adapter, filter, table, split) = adapter_and_filter();
        let created = Arc::new(AtomicBool::new(false));
        let provider = AdapterPageSourceProvider {
            provider: Arc::new(TestPreparationProvider {
                control: Arc::default(),
                close_called: Arc::new(AtomicBool::new(false)),
                created: created.clone(),
                supports_preparation: false,
            }),
            adapter,
        };
        assert!(matches!(
            provider
                .prepare_page_source(&session(), &table, &split, 1, &[], &filter)
                .expect("unsupported is an explicit verdict"),
            ConnectorPreparationStart::Unsupported
        ));
        assert!(!created.load(Ordering::SeqCst));
    }

    #[test]
    fn prepared_input_obeys_capacity_and_live_filter_on_promotion() {
        let adapter = ReadRuntimeAdapter::new(Arc::new(Probe::new()));
        let column = adapter.wrap_column(Column(1));
        let initial_filter: Arc<ConnectorReadDynamicFilter> =
            Arc::new(crate::connector::read_stack::CompleteAllDynamicFilter::new(
                BTreeSet::from([column.clone()]),
            ));
        let later_filter: Arc<ConnectorReadDynamicFilter> = Arc::new(BadFilter {
            covered: BTreeSet::from([column]),
            bad: column_handle(adapter.binding().clone(), WrongColumn),
        });
        let table = adapter.wrap_table(Table);
        let split = adapter.wrap_split(Split);
        let control = Arc::new(TestPreparationControl::default());
        let close_called = Arc::new(AtomicBool::new(false));
        let created = Arc::new(AtomicBool::new(false));
        let provider = AdapterPageSourceProvider {
            provider: Arc::new(TestPreparationProvider {
                control: control.clone(),
                close_called: close_called.clone(),
                created: created.clone(),
                supports_preparation: true,
            }),
            adapter,
        };
        let ConnectorPreparationStart::Prepared(mut prepared) = provider
            .prepare_page_source(&session(), &table, &split, 1, &[], &initial_filter)
            .expect("preparation starts")
        else {
            panic!("provider supports preparation");
        };
        assert!(!created.load(Ordering::SeqCst));
        assert_eq!(
            prepared.advance(3).expect("bounded step"),
            ConnectorPreparationProgress::Deferred
        );
        assert_eq!(prepared.retained_input_bytes(), 0);
        assert_eq!(
            prepared.advance(4).expect("bounded step"),
            ConnectorPreparationProgress::Ready
        );
        assert_eq!(prepared.control().retained_input_bytes(), 4);
        let error = match prepared.promote(&later_filter, &ConnectorPollBudget::new()) {
            Ok(_) => panic!("invalid live filter cannot publish a stream"),
            Err(error) => error,
        };
        assert_eq!(error.kind(), ConnectorErrorKind::InvalidRequest);
        assert!(close_called.load(Ordering::SeqCst));
    }

    struct TestSuccessorStream {
        control: Arc<TestPreparationControl>,
        candidates: usize,
        over_budget: bool,
    }

    impl Stream for TestSuccessorStream {
        type Item = Result<SourcePage, ConnectorError>;

        fn poll_next(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
            Poll::Pending
        }
    }

    impl ConnectorPageStream for TestSuccessorStream {
        fn advance_successor_preparation(
            self: Pin<&mut Self>,
            remaining_input_bytes: u64,
            remaining_candidates: usize,
        ) -> Result<ConnectorPreparationProgress, ConnectorError> {
            let this = self.get_mut();
            if remaining_input_bytes < 2 || remaining_candidates == 0 {
                return Ok(ConnectorPreparationProgress::Deferred);
            }
            let bytes = if this.over_budget { 3 } else { 2 };
            this.control.retained.store(bytes, Ordering::SeqCst);
            this.candidates += 1;
            Ok(ConnectorPreparationProgress::Ready)
        }

        fn successor_preparation_input_bytes(&self) -> u64 {
            self.control.retained_input_bytes()
        }

        fn successor_preparation_candidate_count(&self) -> usize {
            self.candidates
        }

        fn successor_preparation_control(&self) -> Option<Arc<dyn ConnectorPreparationControl>> {
            Some(self.control.clone())
        }

        fn metrics(&self) -> PageSourceMetrics {
            PageSourceMetrics::default()
        }

        fn memory_usage_bytes(&self) -> u64 {
            0
        }

        fn close(self: Pin<Box<Self>>) -> BoxFuture<'static, Result<(), ConnectorError>> {
            Box::pin(async { Ok(()) })
        }
    }

    #[test]
    fn checked_stream_forwards_successor_control_and_enforces_capacity() {
        let adapter = ReadRuntimeAdapter::new(Arc::new(Probe::new()));
        let filter: Arc<ConnectorReadDynamicFilter> = Arc::new(
            crate::connector::read_stack::CompleteAllDynamicFilter::new(BTreeSet::new()),
        );
        let checked_filter =
            Arc::new(ProviderDynamicFilter::new(adapter, filter).expect("valid filter binding"));
        let control = Arc::new(TestPreparationControl::default());
        let mut stream = Box::pin(CheckedPageStream::<Probe> {
            stream: Box::pin(TestSuccessorStream {
                control: control.clone(),
                candidates: 0,
                over_budget: false,
            }),
            dynamic_filter: checked_filter.clone(),
        });
        assert_eq!(
            stream
                .as_mut()
                .advance_successor_preparation(1, 1)
                .expect("deferred"),
            ConnectorPreparationProgress::Deferred
        );
        stream
            .successor_preparation_control()
            .expect("independent control")
            .request_pause();
        assert!(control.paused.load(Ordering::SeqCst));
        stream
            .successor_preparation_control()
            .expect("independent control")
            .request_resume();
        assert_eq!(
            stream
                .as_mut()
                .advance_successor_preparation(2, 1)
                .expect("ready"),
            ConnectorPreparationProgress::Ready
        );
        assert_eq!(stream.successor_preparation_input_bytes(), 2);
        assert_eq!(stream.successor_preparation_candidate_count(), 1);

        let violating_control = Arc::new(TestPreparationControl::default());
        let mut violating = Box::pin(CheckedPageStream::<Probe> {
            stream: Box::pin(TestSuccessorStream {
                control: violating_control.clone(),
                candidates: 0,
                over_budget: true,
            }),
            dynamic_filter: checked_filter,
        });
        assert_eq!(
            violating
                .as_mut()
                .advance_successor_preparation(2, 1)
                .expect_err("over-budget input must fail")
                .kind(),
            ConnectorErrorKind::Internal
        );
        assert!(violating_control.stopped.load(Ordering::SeqCst));
    }
}
