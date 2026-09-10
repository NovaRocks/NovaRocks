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
use std::sync::{Arc, Mutex};

use super::runtime::{
    ConnectorReadBinding, ConnectorReadChangeWindow, ConnectorReadColumnBinding,
    ConnectorReadColumnHandle, ConnectorReadConstraint, ConnectorReadDynamicFilter,
    ConnectorReadDynamicFilterSnapshot, ConnectorReadFilterApplication,
    ConnectorReadLimitApplication, ConnectorReadMetadata, ConnectorReadPageSourceProvider,
    ConnectorReadProviderFactory, ConnectorReadRelationKind, ConnectorReadRelationVersion,
    ConnectorReadSplit, ConnectorReadSplitFacts, ConnectorReadSplitManager,
    ConnectorReadSplitSource, ConnectorReadSystemTablePlan, ConnectorReadSystemTableProvider,
    ConnectorReadTableHandle, ConnectorReadTransactionHandle, binding_error, column_handle,
    column_value, map_constraint, map_tuple_domain, split_handle, split_value, table_handle,
    table_value, transaction_handle, transaction_value, type_error,
};
use super::{
    Assignment, BoundsMatch, ColumnHandle, ColumnValueBounds, ConnectorExpression,
    ConnectorPageSource, ConnectorSession, ConnectorSplit, ConnectorSplitBatch, Constraint,
    DynamicFilter, DynamicFilterSnapshot, PageSourceMetrics, SchemaTableName, SourcePage,
    SystemTableDistribution, TupleDomain,
};
use crate::connector::{
    CatalogHandle, ConnectorError, ConnectorInstanceDescriptor, ConnectorPinnedFileSet,
    ConnectorRequestContext,
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
    distribution: SystemTableDistribution,
}

impl<T> ProviderReadSystemTablePlan<T> {
    pub const fn new(handle: T, distribution: SystemTableDistribution) -> Self {
        Self {
            handle,
            distribution,
        }
    }

    pub const fn handle(&self) -> &T {
        &self.handle
    }

    pub fn into_handle(self) -> T {
        self.handle
    }

    pub const fn distribution(&self) -> SystemTableDistribution {
        self.distribution
    }
}

pub trait ProviderReadMetadata: ProviderReadRuntime {
    fn get_table_handle(
        &self,
        session: &ConnectorSession,
        name: &SchemaTableName,
        version: ConnectorReadRelationVersion,
        reference: Option<&str>,
    ) -> Result<Option<Self::Table>, ConnectorError>;

    fn get_pinned_file_set_handle(
        &self,
        session: &ConnectorSession,
        name: &SchemaTableName,
        pinned: &ConnectorPinnedFileSet,
    ) -> Result<Option<Self::Table>, ConnectorError>;

    fn get_column_bindings(
        &self,
        session: &ConnectorSession,
        table: &Self::Table,
    ) -> Result<Vec<ProviderReadColumnBinding<Self::Column>>, ConnectorError>;

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

pub trait ProviderReadSplitManager: ProviderReadRuntime {
    fn get_splits(
        &self,
        session: &ConnectorSession,
        table: &Self::Table,
        columns: &[Assignment<Self::Column>],
        dynamic_filter_columns: &BTreeSet<Self::Column>,
        constraint: &Constraint<Self::Column>,
    ) -> Result<Box<dyn ProviderReadSplitSource<Self>>, ConnectorError>
    where
        Self: Sized;
}

pub trait ProviderReadPageSourceProvider<P: ProviderReadRuntime>: Send + Sync {
    fn create_page_source(
        &self,
        session: &ConnectorSession,
        table: &P::Table,
        split: &P::Split,
        scheduled_split_sequence_id: u64,
        columns: &[Assignment<P::Column>],
        dynamic_filter: &Arc<dyn DynamicFilter<P::Column>>,
    ) -> Result<Box<dyn ConnectorPageSource>, ConnectorError>;
}

pub trait ProviderReadSystemTableProvider<P: ProviderReadRuntime>: Send + Sync {
    fn create_system_page_source(
        &self,
        session: &ConnectorSession,
        table: &P::Table,
        columns: &[Assignment<P::Column>],
    ) -> Result<Box<dyn ConnectorPageSource>, ConnectorError>;
}

/// Worker-side factory whose access resources may differ from the metadata
/// runtime that owns table/column/split values.  `P` is the latter exact type
/// family, so every page source still receives only handles decoded by the
/// matching adapter.
pub trait ProviderReadFactory<P: ProviderReadRuntime>: Send + Sync {
    fn create_page_source_provider(
        &self,
        request: &ConnectorRequestContext,
        options: super::ConnectorPageSourceProviderOptions,
    ) -> Result<Arc<dyn ProviderReadPageSourceProvider<P>>, ConnectorError>;

    fn create_system_table_provider(
        &self,
        request: &ConnectorRequestContext,
    ) -> Result<Arc<dyn ProviderReadSystemTableProvider<P>>, ConnectorError>;
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

impl<P: ProviderReadMetadata> ConnectorReadMetadata for ReadRuntimeAdapter<P> {
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

    fn apply_filter(
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

    fn apply_projection(
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

    fn apply_limit(
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

    fn get_system_table_plan(
        &self,
        session: &ConnectorSession,
        name: &SchemaTableName,
    ) -> Result<Option<ConnectorReadSystemTablePlan>, ConnectorError> {
        self.provider
            .get_system_table_plan(session, name)
            .map(|result| {
                result.map(|result| {
                    let distribution = result.distribution();
                    let handle = result.into_handle();
                    ConnectorReadSystemTablePlan::new(self.wrap_table(handle), distribution)
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
        session: &ConnectorSession,
        table: &ConnectorReadTableHandle,
        columns: &[Assignment<ConnectorReadColumnHandle>],
        dynamic_filter_columns: &BTreeSet<ConnectorReadColumnHandle>,
        constraint: &ConnectorReadConstraint,
    ) -> Result<Box<dyn ConnectorReadSplitSource>, ConnectorError> {
        let table = self.table(table)?;
        let columns = self.typed_assignments(columns)?;
        let dynamic_filter_columns = self.typed_columns(dynamic_filter_columns)?;
        let constraint = self.typed_constraint(constraint)?;
        let source = self.provider.get_splits(
            session,
            table,
            &columns,
            &dynamic_filter_columns,
            &constraint,
        )?;
        Ok(Box::new(AdapterSplitSource {
            source,
            adapter: self.clone(),
        }))
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

struct CheckedPageSource<P: ProviderReadRuntime> {
    source: Box<dyn ConnectorPageSource>,
    dynamic_filter: Arc<ProviderDynamicFilter<P>>,
}

impl<P: ProviderReadRuntime> ConnectorPageSource for CheckedPageSource<P> {
    fn next_source_page(&mut self) -> Result<Option<SourcePage>, ConnectorError> {
        self.dynamic_filter.check()?;
        let page = self.source.next_source_page()?;
        if let Err(error) = self.dynamic_filter.check() {
            let _ = self.source.close();
            return Err(error);
        }
        Ok(page)
    }

    fn is_finished(&self) -> bool {
        self.source.is_finished()
    }

    fn is_blocked(&self) -> bool {
        self.source.is_blocked()
    }

    fn metrics(&self) -> PageSourceMetrics {
        self.source.metrics()
    }

    fn memory_usage_bytes(&self) -> u64 {
        self.source.memory_usage_bytes()
    }

    fn close(&mut self) -> Result<(), ConnectorError> {
        self.source.close()
    }
}

struct AdapterPageSourceProvider<P: ProviderReadRuntime> {
    provider: Arc<dyn ProviderReadPageSourceProvider<P>>,
    adapter: ReadRuntimeAdapter<P>,
}

impl<P: ProviderReadRuntime> ConnectorReadPageSourceProvider for AdapterPageSourceProvider<P> {
    fn create_page_source(
        &self,
        session: &ConnectorSession,
        table: &ConnectorReadTableHandle,
        split: &ConnectorReadSplit,
        scheduled_split_sequence_id: u64,
        columns: &[Assignment<ConnectorReadColumnHandle>],
        dynamic_filter: &Arc<ConnectorReadDynamicFilter>,
    ) -> Result<Box<dyn ConnectorPageSource>, ConnectorError> {
        let table = self.adapter.table(table)?;
        let split = self.adapter.split(split)?;
        let columns = self.adapter.typed_assignments(columns)?;
        let dynamic_filter = Arc::new(ProviderDynamicFilter::new(
            self.adapter.clone(),
            dynamic_filter.clone(),
        )?);
        dynamic_filter.check()?;
        let typed_filter: Arc<dyn DynamicFilter<P::Column>> = dynamic_filter.clone();
        let mut source = self.provider.create_page_source(
            session,
            table,
            split,
            scheduled_split_sequence_id,
            &columns,
            &typed_filter,
        )?;
        if let Err(error) = dynamic_filter.check() {
            let _ = source.close();
            return Err(error);
        }
        Ok(Box::new(CheckedPageSource {
            source,
            dynamic_filter,
        }))
    }
}

struct AdapterSystemTableProvider<P: ProviderReadRuntime> {
    provider: Arc<dyn ProviderReadSystemTableProvider<P>>,
    adapter: ReadRuntimeAdapter<P>,
}

impl<P: ProviderReadRuntime> ConnectorReadSystemTableProvider for AdapterSystemTableProvider<P> {
    fn create_system_page_source(
        &self,
        session: &ConnectorSession,
        table: &ConnectorReadTableHandle,
        columns: &[Assignment<ConnectorReadColumnHandle>],
    ) -> Result<Box<dyn ConnectorPageSource>, ConnectorError> {
        let table = self.adapter.table(table)?;
        let columns = self.adapter.typed_assignments(columns)?;
        self.provider
            .create_system_page_source(session, table, &columns)
    }
}

/// Combines a provider-owned execution factory with the separately-owned
/// metadata type adapter. It is constructed by the concrete connector's
/// exact-key bundle factory and never exposed as a typed accessor to a role.
pub struct ProviderReadFactoryAdapter<P: ProviderReadRuntime, F: ProviderReadFactory<P>> {
    factory: Arc<F>,
    adapter: ReadRuntimeAdapter<P>,
}

impl<P: ProviderReadRuntime, F: ProviderReadFactory<P>> ProviderReadFactoryAdapter<P, F> {
    pub fn new(adapter: ReadRuntimeAdapter<P>, factory: Arc<F>) -> Self {
        Self { factory, adapter }
    }
}

impl<P: ProviderReadRuntime, F: ProviderReadFactory<P>> ConnectorReadProviderFactory
    for ProviderReadFactoryAdapter<P, F>
{
    fn create_page_source_provider(
        &self,
        request: &ConnectorRequestContext,
        options: super::ConnectorPageSourceProviderOptions,
    ) -> Result<Arc<dyn ConnectorReadPageSourceProvider>, ConnectorError> {
        Ok(Arc::new(AdapterPageSourceProvider {
            provider: self.factory.create_page_source_provider(request, options)?,
            adapter: ReadRuntimeAdapter::clone(&self.adapter),
        }))
    }

    fn create_system_table_provider(
        &self,
        request: &ConnectorRequestContext,
    ) -> Result<Arc<dyn ConnectorReadSystemTableProvider>, ConnectorError> {
        Ok(Arc::new(AdapterSystemTableProvider {
            provider: self.factory.create_system_table_provider(request)?,
            adapter: ReadRuntimeAdapter::clone(&self.adapter),
        }))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};
    use std::sync::atomic::{AtomicBool, Ordering};
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

    struct Probe {
        descriptor: ConnectorInstanceDescriptor,
        catalog_handle: CatalogHandle,
    }

    impl Probe {
        fn new() -> Self {
            Self {
                descriptor: ConnectorInstanceDescriptor {
                    provider_id: ConnectorProviderId::parse("probe").expect("provider ID"),
                    instance_id: ConnectorInstanceId::parse("catalog").expect("instance ID"),
                },
                catalog_handle: CatalogHandle::new(
                    ConnectorInstanceId::parse("catalog").expect("instance ID"),
                    crate::connector::CatalogVersion::from_bytes([3; 32]),
                ),
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
        fn get_splits(
            &self,
            _session: &ConnectorSession,
            _table: &Self::Table,
            _columns: &[Assignment<Self::Column>],
            _dynamic_filter_columns: &BTreeSet<Self::Column>,
            _constraint: &Constraint<Self::Column>,
        ) -> Result<Box<dyn ProviderReadSplitSource<Self>>, ConnectorError> {
            unimplemented!("attempt-access tests do not enumerate splits")
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

    struct TestPageSource {
        dynamic_filter: Option<Arc<dyn DynamicFilter<Column>>>,
        close_called: Arc<AtomicBool>,
    }

    impl ConnectorPageSource for TestPageSource {
        fn next_source_page(&mut self) -> Result<Option<SourcePage>, ConnectorError> {
            if let Some(dynamic_filter) = &self.dynamic_filter {
                let _ = dynamic_filter.current_predicate();
            }
            Ok(Some(SourcePage::zero_channel(1)))
        }

        fn is_finished(&self) -> bool {
            false
        }

        fn metrics(&self) -> PageSourceMetrics {
            PageSourceMetrics::default()
        }

        fn memory_usage_bytes(&self) -> u64 {
            0
        }

        fn close(&mut self) -> Result<(), ConnectorError> {
            self.close_called.store(true, Ordering::SeqCst);
            Ok(())
        }
    }

    struct TestProvider {
        trigger_during_create: bool,
        close_called: Arc<AtomicBool>,
    }

    impl ProviderReadPageSourceProvider<Probe> for TestProvider {
        fn create_page_source(
            &self,
            _session: &ConnectorSession,
            _table: &Table,
            _split: &Split,
            _scheduled_split_sequence_id: u64,
            _columns: &[Assignment<Column>],
            dynamic_filter: &Arc<dyn DynamicFilter<Column>>,
        ) -> Result<Box<dyn ConnectorPageSource>, ConnectorError> {
            if self.trigger_during_create {
                let _ = dynamic_filter.current_predicate();
            }
            Ok(Box::new(TestPageSource {
                dynamic_filter: (!self.trigger_during_create).then(|| dynamic_filter.clone()),
                close_called: self.close_called.clone(),
            }))
        }
    }

    fn session() -> ConnectorSession {
        ConnectorSession::try_new("q", "u", "UTC", "en_US", SystemTime::UNIX_EPOCH)
            .expect("session")
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
        struct Active;
        impl crate::connector::ConnectorCancellation for Active {
            fn is_cancelled(&self) -> bool {
                false
            }
        }

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
                Arc::new(Active),
                crate::connector::MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES,
                crate::connector::MAX_CONNECTOR_TOTAL_PAYLOAD_BYTES,
            )
            .expect("attempt request context"),
        );
        source
            .for_attempt(&attempt)
            .expect("same static binding rebinds for a new attempt");

        let foreign_adapter = ReadRuntimeAdapter::new(Arc::new(Probe {
            descriptor: ConnectorInstanceDescriptor {
                provider_id: ConnectorProviderId::parse("probe").expect("provider ID"),
                instance_id: ConnectorInstanceId::parse("other").expect("instance ID"),
            },
            catalog_handle: CatalogHandle::new(
                ConnectorInstanceId::parse("other").expect("instance ID"),
                crate::connector::CatalogVersion::from_bytes([4; 32]),
            ),
        }));
        let error = match static_control.seal_attempt_access(&foreign_adapter.wrap_table(Table)) {
            Ok(_) => panic!("a foreign generation cannot enter the static access source"),
            Err(error) => error,
        };
        assert_eq!(error.kind(), ConnectorErrorKind::InvalidRequest);
    }

    #[test]
    fn provider_attempt_access_rejects_foreign_returned_generation() {
        struct Active;
        impl crate::connector::ConnectorCancellation for Active {
            fn is_cancelled(&self) -> bool {
                false
            }
        }

        struct ReturnRuntime(ConnectorReadAttemptRuntime);
        impl ConnectorReadAttemptAccessReacquirer for ReturnRuntime {
            fn for_attempt(
                &self,
                _request: &crate::connector::ConnectorAttemptContext,
            ) -> Result<ConnectorReadAttemptRuntime, ConnectorError> {
                Ok(self.0.clone())
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
            descriptor: ConnectorInstanceDescriptor {
                provider_id: ConnectorProviderId::parse("probe").expect("provider ID"),
                instance_id: ConnectorInstanceId::parse("foreign").expect("instance ID"),
            },
            catalog_handle: CatalogHandle::new(
                ConnectorInstanceId::parse("foreign").expect("instance ID"),
                crate::connector::CatalogVersion::from_bytes([9; 32]),
            ),
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
                Arc::new(Active),
                crate::connector::MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES,
                crate::connector::MAX_CONNECTOR_TOTAL_PAYLOAD_BYTES,
            )
            .expect("attempt request context"),
        );
        let error = match source.for_attempt(&request) {
            Ok(_) => panic!("reacquirer must not replace the sealed generation"),
            Err(error) => error,
        };
        assert_eq!(error.kind(), ConnectorErrorKind::InvalidRequest);
    }

    #[test]
    fn provider_sealer_cannot_replay_a_source_from_an_earlier_seal() {
        struct ReturnRuntime(ConnectorReadAttemptRuntime);
        impl ConnectorReadAttemptAccessReacquirer for ReturnRuntime {
            fn for_attempt(
                &self,
                _request: &crate::connector::ConnectorAttemptContext,
            ) -> Result<ConnectorReadAttemptRuntime, ConnectorError> {
                Ok(self.0.clone())
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
            descriptor: ConnectorInstanceDescriptor {
                provider_id: ConnectorProviderId::parse("probe").expect("provider ID"),
                instance_id: ConnectorInstanceId::parse("foreign").expect("instance ID"),
            },
            catalog_handle: CatalogHandle::new(
                ConnectorInstanceId::parse("foreign").expect("instance ID"),
                crate::connector::CatalogVersion::from_bytes([8; 32]),
            ),
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
    fn creation_time_filter_contract_error_closes_the_unpublished_source() {
        let (adapter, filter, table, split) = adapter_and_filter();
        let close_called = Arc::new(AtomicBool::new(false));
        let provider = AdapterPageSourceProvider {
            provider: Arc::new(TestProvider {
                trigger_during_create: true,
                close_called: close_called.clone(),
            }),
            adapter,
        };

        let error = match provider.create_page_source(&session(), &table, &split, 1, &[], &filter) {
            Ok(_) => panic!("latching during creation must reject the source"),
            Err(error) => error,
        };
        assert_eq!(error.kind(), ConnectorErrorKind::InvalidRequest);
        assert!(close_called.load(Ordering::SeqCst));
    }

    #[test]
    fn page_time_filter_contract_error_wins_over_the_same_call_page() {
        let (adapter, filter, table, split) = adapter_and_filter();
        let close_called = Arc::new(AtomicBool::new(false));
        let provider = AdapterPageSourceProvider {
            provider: Arc::new(TestProvider {
                trigger_during_create: false,
                close_called: close_called.clone(),
            }),
            adapter,
        };
        let mut source = provider
            .create_page_source(&session(), &table, &split, 1, &[], &filter)
            .expect("source is valid before its first provider callback");

        let error = source
            .next_source_page()
            .expect_err("latched error must win over a same-call page");
        assert_eq!(error.kind(), ConnectorErrorKind::InvalidRequest);
        assert!(close_called.load(Ordering::SeqCst));
    }
}
