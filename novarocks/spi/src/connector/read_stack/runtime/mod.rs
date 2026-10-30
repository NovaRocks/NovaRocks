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

//! Transport-neutral connector read runtime contracts.
//!
//! The public values in this module deliberately reveal only a binding and
//! neutral scheduling or planning facts.  Their provider payloads are private
//! to this crate.  A provider supplies concrete associated types to the
//! generic adapter in the sibling module; FE and BE consume only these values
//! and the service traits below.

use std::any::Any;
use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};
use std::fmt::{Debug, Formatter};
use std::marker::PhantomData;
use std::sync::Arc;

use crate::connector::read_stack::ConnectorMvTargetPartitionSelection;
use crate::connector::read_stack::page_source::ConnectorPreparationStart;
use crate::connector::read_stack::{
    Assignment, ColumnHandle, ConnectorExpression, ConnectorPollBudget, ConnectorSession,
    ConnectorSplitBatch, Constraint, DynamicFilter, DynamicFilterSnapshot, HostAddress,
    OwnedConnectorPageStream, SchemaTableName, SplitWeight, SystemTableUnitHint, TupleDomain,
};
use crate::connector::{
    ConnectorAttemptContext, ConnectorError, ConnectorExecutionResources, ConnectorPinnedFileSet,
    ConnectorPlanningContext, ConnectorRequestContext,
};

pub use novarocks_connector_contract::{ConnectorReadBinding, ConnectorReadRelationKind};

mod source_preparation;
pub use source_preparation::{
    ConnectorPreparedSourceRead, ConnectorPreparedSourceReadFuture,
    ConnectorReadSourcePreparationRequest, ConnectorReadSourceProjection,
    ConnectorReadSourceSelection, ConnectorReadSplitPreparation,
};

#[derive(Clone)]
struct OpaquePayload(Arc<dyn Any + Send + Sync>);

impl OpaquePayload {
    fn new<T: Send + Sync + 'static>(value: T) -> Self {
        Self(Arc::new(value))
    }

    fn downcast_ref<T: 'static>(&self) -> Option<&T> {
        self.0.downcast_ref()
    }
}

macro_rules! opaque_handle {
    ($(#[$meta:meta])* $name:ident) => {
        $(#[$meta])*
        #[derive(Clone)]
        pub struct $name {
            binding: ConnectorReadBinding,
            #[allow(dead_code)] // Transaction recovery is introduced by the codec boundary in T30.
            payload: OpaquePayload,
        }

        impl $name {
            pub const fn binding(&self) -> &ConnectorReadBinding {
                &self.binding
            }
        }

        impl Debug for $name {
            fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
                formatter
                    .debug_struct(stringify!($name))
                    .field("binding", &self.binding)
                    .finish_non_exhaustive()
            }
        }
    };
}

opaque_handle!(
    /// A provider-bound table handle whose payload is intentionally inaccessible.
    ///
    /// ```compile_fail
    /// use novarocks_spi::connector::read_stack::ConnectorReadTableHandle;
    ///
    /// fn leak(handle: ConnectorReadTableHandle) {
    ///     let _ = handle.payload;
    /// }
    /// ```
    ConnectorReadTableHandle
);
opaque_handle!(ConnectorReadTransactionHandle);

/// Provider-owned source waiting for adoption by an admitted read generation.
///
/// This process-local receipt is not a read binding or a native wire handle.
/// Roles can retain it but cannot recover its provider-private source facts.
#[derive(Clone)]
pub struct ConnectorFrozenReadSource {
    owner: crate::connector::ConnectorProviderBindingKey,
    payload: OpaquePayload,
}

impl ConnectorFrozenReadSource {
    pub const fn owner(&self) -> &crate::connector::ConnectorProviderBindingKey {
        &self.owner
    }
}

impl Debug for ConnectorFrozenReadSource {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ConnectorFrozenReadSource")
            .field("owner", &self.owner)
            .finish_non_exhaustive()
    }
}

pub(crate) fn frozen_read_source<T: Send + Sync + 'static>(
    owner: crate::connector::ConnectorProviderBindingKey,
    value: T,
) -> ConnectorFrozenReadSource {
    ConnectorFrozenReadSource {
        owner,
        payload: OpaquePayload::new(value),
    }
}

pub(crate) fn frozen_read_source_value<T: 'static>(
    source: &ConnectorFrozenReadSource,
) -> Option<&T> {
    source.payload.downcast_ref()
}

#[derive(Clone)]
pub struct ConnectorReadSplit {
    binding: ConnectorReadBinding,
    facts: ConnectorReadSplitFacts,
    payload: OpaquePayload,
}

impl ConnectorReadSplit {
    pub const fn binding(&self) -> &ConnectorReadBinding {
        &self.binding
    }

    pub const fn facts(&self) -> &ConnectorReadSplitFacts {
        &self.facts
    }
}

impl Debug for ConnectorReadSplit {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ConnectorReadSplit")
            .field("binding", &self.binding)
            .field("facts", &self.facts)
            .finish_non_exhaustive()
    }
}

/// A column's equality and ordering delegate to its provider's concrete
/// `ColumnHandle`, without exposing that payload outside the adapter.
#[derive(Clone)]
pub struct ConnectorReadColumnHandle {
    binding: ConnectorReadBinding,
    payload: OpaquePayload,
    comparison: Arc<dyn ErasedColumnComparison>,
}

/// Private provider-erased ordering for an opaque column payload. It is never
/// derived from wire bytes, pointer identity, or a role-visible token.
trait ErasedColumnComparison: Send + Sync {
    fn compare(&self, left: &OpaquePayload, right: &OpaquePayload) -> Option<Ordering>;
}

struct TypedColumnComparison<C>(PhantomData<fn() -> C>);

impl<C: ColumnHandle> ErasedColumnComparison for TypedColumnComparison<C> {
    fn compare(&self, left: &OpaquePayload, right: &OpaquePayload) -> Option<Ordering> {
        Some(left.downcast_ref::<C>()?.cmp(right.downcast_ref::<C>()?))
    }
}

impl ConnectorReadColumnHandle {
    pub const fn binding(&self) -> &ConnectorReadBinding {
        &self.binding
    }
}

impl Debug for ConnectorReadColumnHandle {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ConnectorReadColumnHandle")
            .field("binding", &self.binding)
            .finish_non_exhaustive()
    }
}

impl PartialEq for ConnectorReadColumnHandle {
    fn eq(&self, other: &Self) -> bool {
        self.binding == other.binding
            && self
                .comparison
                .compare(&self.payload, &other.payload)
                .is_some_and(Ordering::is_eq)
    }
}

impl Eq for ConnectorReadColumnHandle {}

impl PartialOrd for ConnectorReadColumnHandle {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for ConnectorReadColumnHandle {
    fn cmp(&self, other: &Self) -> Ordering {
        (
            self.binding.descriptor().provider_id.as_str(),
            self.binding.descriptor().instance_id.as_str(),
            self.binding.catalog_handle().version().as_bytes(),
        )
            .cmp(&(
                other.binding.descriptor().provider_id.as_str(),
                other.binding.descriptor().instance_id.as_str(),
                other.binding.catalog_handle().version().as_bytes(),
            ))
            .then_with(|| {
                self.comparison
                    .compare(&self.payload, &other.payload)
                    .expect("same read binding must carry one concrete column family")
            })
    }
}

impl ColumnHandle for ConnectorReadColumnHandle {}

/// Neutral scheduling facts carried alongside a provider split.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConnectorReadSplitFacts {
    remotely_accessible: bool,
    addresses: Arc<[HostAddress]>,
    affinity_key: Option<Arc<str>>,
    split_weight: SplitWeight,
    retained_size_in_bytes: u64,
}

impl ConnectorReadSplitFacts {
    pub fn new(
        remotely_accessible: bool,
        addresses: Vec<HostAddress>,
        affinity_key: Option<impl AsRef<str>>,
        split_weight: SplitWeight,
        retained_size_in_bytes: u64,
    ) -> Self {
        Self {
            remotely_accessible,
            addresses: Arc::from(addresses),
            affinity_key: affinity_key.map(|value| Arc::from(value.as_ref())),
            split_weight,
            retained_size_in_bytes,
        }
    }

    pub const fn remotely_accessible(&self) -> bool {
        self.remotely_accessible
    }

    pub fn addresses(&self) -> &[HostAddress] {
        &self.addresses
    }

    pub fn affinity_key(&self) -> Option<&str> {
        self.affinity_key.as_deref()
    }

    pub const fn split_weight(&self) -> SplitWeight {
        self.split_weight
    }

    pub const fn retained_size_in_bytes(&self) -> u64 {
        self.retained_size_in_bytes
    }
}

pub type ConnectorReadConstraint = Constraint<ConnectorReadColumnHandle>;
pub type ConnectorReadDynamicFilterSnapshot = DynamicFilterSnapshot<ConnectorReadColumnHandle>;
pub type ConnectorReadDynamicFilter = dyn DynamicFilter<ConnectorReadColumnHandle>;
pub type ConnectorReadAssignment = Assignment<ConnectorReadColumnHandle>;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConnectorReadColumnBinding {
    name: Arc<str>,
    column: ConnectorReadColumnHandle,
    hidden: bool,
}

impl ConnectorReadColumnBinding {
    pub fn new(name: impl AsRef<str>, column: ConnectorReadColumnHandle, hidden: bool) -> Self {
        Self {
            name: Arc::from(name.as_ref()),
            column,
            hidden,
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub const fn column(&self) -> &ConnectorReadColumnHandle {
        &self.column
    }

    pub const fn is_hidden(&self) -> bool {
        self.hidden
    }
}

#[derive(Clone, Debug)]
pub struct ConnectorReadFilterApplication {
    handle: ConnectorReadTableHandle,
    remaining_constraint: ConnectorReadConstraint,
    remaining_expression: Option<ConnectorExpression>,
}

impl ConnectorReadFilterApplication {
    pub const fn new(
        handle: ConnectorReadTableHandle,
        remaining_constraint: ConnectorReadConstraint,
        remaining_expression: Option<ConnectorExpression>,
    ) -> Self {
        Self {
            handle,
            remaining_constraint,
            remaining_expression,
        }
    }

    pub const fn handle(&self) -> &ConnectorReadTableHandle {
        &self.handle
    }

    pub fn into_handle(self) -> ConnectorReadTableHandle {
        self.handle
    }

    pub const fn remaining_constraint(&self) -> &ConnectorReadConstraint {
        &self.remaining_constraint
    }

    pub const fn remaining_expression(&self) -> Option<&ConnectorExpression> {
        self.remaining_expression.as_ref()
    }
}

#[derive(Clone, Debug)]
pub struct ConnectorReadLimitApplication {
    handle: ConnectorReadTableHandle,
    limit_guaranteed: bool,
}

impl ConnectorReadLimitApplication {
    pub const fn new(handle: ConnectorReadTableHandle, limit_guaranteed: bool) -> Self {
        Self {
            handle,
            limit_guaranteed,
        }
    }

    pub const fn handle(&self) -> &ConnectorReadTableHandle {
        &self.handle
    }

    pub fn into_handle(self) -> ConnectorReadTableHandle {
        self.handle
    }

    pub const fn limit_guaranteed(&self) -> bool {
        self.limit_guaranteed
    }
}

#[derive(Clone, Debug)]
pub struct ConnectorReadSystemTablePlan {
    handle: ConnectorReadTableHandle,
    unit_hint: SystemTableUnitHint,
}

impl ConnectorReadSystemTablePlan {
    pub const fn new(handle: ConnectorReadTableHandle, unit_hint: SystemTableUnitHint) -> Self {
        Self { handle, unit_hint }
    }

    pub const fn handle(&self) -> &ConnectorReadTableHandle {
        &self.handle
    }

    pub fn into_handle(self) -> ConnectorReadTableHandle {
        self.handle
    }

    pub const fn unit_hint(&self) -> SystemTableUnitHint {
        self.unit_hint
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ConnectorReadRelationVersion {
    Current,
    SnapshotId(i64),
    Reference,
}

#[derive(Clone, Debug)]
pub struct ConnectorReadRelation {
    kind: ConnectorReadRelationKind,
    table: ConnectorReadTableHandle,
    transaction: ConnectorReadTransactionHandle,
}

impl ConnectorReadRelation {
    pub const fn new(
        kind: ConnectorReadRelationKind,
        table: ConnectorReadTableHandle,
        transaction: ConnectorReadTransactionHandle,
    ) -> Self {
        Self {
            kind,
            table,
            transaction,
        }
    }

    pub const fn kind(&self) -> ConnectorReadRelationKind {
        self.kind
    }

    pub const fn table(&self) -> &ConnectorReadTableHandle {
        &self.table
    }

    pub const fn transaction(&self) -> &ConnectorReadTransactionHandle {
        &self.transaction
    }

    pub fn into_parts(
        self,
    ) -> (
        ConnectorReadRelationKind,
        ConnectorReadTableHandle,
        ConnectorReadTransactionHandle,
    ) {
        (self.kind, self.table, self.transaction)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ConnectorReadChangeWindow {
    from_snapshot_id: i64,
    to_snapshot_id: i64,
}

impl ConnectorReadChangeWindow {
    pub const fn new(from_snapshot_id: i64, to_snapshot_id: i64) -> Self {
        Self {
            from_snapshot_id,
            to_snapshot_id,
        }
    }

    pub const fn from_snapshot_id(&self) -> i64 {
        self.from_snapshot_id
    }

    pub const fn to_snapshot_id(&self) -> i64 {
        self.to_snapshot_id
    }
}

#[derive(Clone, Debug)]
pub struct ConnectorReadFrozenRewriteGroup {
    operation_id: crate::connector::ConnectorWriteOperationId,
    artifact_digest_hex: Arc<str>,
    group_digest_hex: Arc<str>,
    content: crate::connector::ConnectorProcessLocalPayload,
}
impl PartialEq for ConnectorReadFrozenRewriteGroup {
    fn eq(&self, other: &Self) -> bool {
        self.operation_id == other.operation_id
            && self.artifact_digest_hex == other.artifact_digest_hex
            && self.group_digest_hex == other.group_digest_hex
    }
}
impl Eq for ConnectorReadFrozenRewriteGroup {}
impl ConnectorReadFrozenRewriteGroup {
    pub fn from_cohort(
        group: &crate::connector::ConnectorFrozenRewriteGroup,
        group_digest: [u8; 32],
    ) -> Self {
        Self {
            operation_id: group.operation_id(),
            artifact_digest_hex: Arc::from(
                group
                    .artifact_digest()
                    .iter()
                    .map(|b| format!("{b:02x}"))
                    .collect::<String>(),
            ),
            group_digest_hex: Arc::from(
                group_digest
                    .iter()
                    .map(|b| format!("{b:02x}"))
                    .collect::<String>(),
            ),
            content: group.content().clone(),
        }
    }
    pub const fn operation_id(&self) -> crate::connector::ConnectorWriteOperationId {
        self.operation_id
    }
    pub fn artifact_digest_hex(&self) -> &str {
        &self.artifact_digest_hex
    }
    pub fn group_digest_hex(&self) -> &str {
        &self.group_digest_hex
    }
    pub fn content(&self) -> &crate::connector::ConnectorProcessLocalPayload {
        &self.content
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ConnectorReadTableExecuteProcedure {
    RewritePositionDeleteFiles(ConnectorReadFrozenRewriteGroup),
}

/// Original frozen selection, carried unchanged through physical observation.
#[derive(Clone, Debug)]
pub enum ConnectorReadRelationPreparationKind {
    Table {
        version: ConnectorReadRelationVersion,
        reference: Option<String>,
    },
    Metadata {
        request: Option<super::ConnectorReadMetadataRequest>,
    },
    Delta {
        window: ConnectorReadChangeWindow,
    },
    MvTarget {
        object_id: crate::connector::ConnectorTableObjectId,
        snapshot_id: Option<i64>,
    },
    MvTargetPartition {
        selection: super::ConnectorMvTargetPartitionSelection,
    },
    PinnedFileSet {
        pinned: crate::connector::ConnectorPinnedFileSet,
    },
    TableExecute {
        procedure: ConnectorReadTableExecuteProcedure,
    },
}

#[derive(Clone, Debug)]
pub struct ConnectorReadRelationPreparationRequest {
    name: SchemaTableName,
    kind: ConnectorReadRelationPreparationKind,
}
impl ConnectorReadRelationPreparationRequest {
    pub const fn new(name: SchemaTableName, kind: ConnectorReadRelationPreparationKind) -> Self {
        Self { name, kind }
    }
    pub const fn name(&self) -> &SchemaTableName {
        &self.name
    }
    pub const fn kind(&self) -> &ConnectorReadRelationPreparationKind {
        &self.kind
    }
}

pub struct ConnectorReadOpenedRelation {
    handle: ConnectorReadTableHandle,
    unit_hint: Option<super::SystemTableUnitHint>,
}
impl ConnectorReadOpenedRelation {
    pub(crate) const fn new(
        handle: ConnectorReadTableHandle,
        unit_hint: Option<super::SystemTableUnitHint>,
    ) -> Self {
        Self { handle, unit_hint }
    }
    pub fn into_parts(self) -> (ConnectorReadTableHandle, Option<super::SystemTableUnitHint>) {
        (self.handle, self.unit_hint)
    }
}

/// One completed Provider relation observation, consumed by its exact original
/// adapter. No schema observer, executable callback, grant, or pending F lives
/// in this carrier. The original Provider/SDK backing has independent custody.
pub struct ConnectorReadRelationPreparation {
    binding: ConnectorReadBinding,
    facts: Box<dyn Any + Send + Sync>,
}
impl ConnectorReadRelationPreparation {
    pub const fn binding(&self) -> &ConnectorReadBinding {
        &self.binding
    }
    pub(crate) fn new<T: Any + Send + Sync>(binding: ConnectorReadBinding, facts: T) -> Self {
        Self {
            binding,
            facts: Box::new(facts),
        }
    }
    pub(crate) fn into_facts<T: Any + Send + Sync>(
        self,
        binding: &ConnectorReadBinding,
    ) -> Result<T, ConnectorError> {
        if &self.binding != binding {
            return Err(binding_error());
        }
        self.facts
            .downcast::<T>()
            .map(|facts| *facts)
            .map_err(|_| type_error())
    }
}
impl Debug for ConnectorReadRelationPreparation {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ConnectorReadRelationPreparation")
            .field("binding", &self.binding)
            .finish_non_exhaustive()
    }
}
pub type ConnectorReadRelationPreparationFuture<'a> = std::pin::Pin<
    Box<
        dyn std::future::Future<
                Output = Result<Option<ConnectorReadRelationPreparation>, ConnectorError>,
            > + Send
            + 'a,
    >,
>;

/// Coordinator-facing read services. They are transport neutral and never
/// expose the concrete payload of a returned handle.
pub trait ConnectorReadMetadata: Send + Sync {
    fn prepare_source_read<'a>(
        &'a self,
        _session: &'a ConnectorSession,
        _request: &'a ConnectorReadSourcePreparationRequest,
        _cpu: &'a Arc<dyn crate::connector::ProviderMetadataCpuPort>,
        _original: &'a crate::connector::ConnectorOriginalResultScope,
        _owner: &'a mut Option<crate::connector::ConnectorProcessLocalPayload>,
    ) -> ConnectorPreparedSourceReadFuture<'a> {
        Box::pin(async {
            Err(ConnectorError::new(
                crate::connector::ConnectorErrorKind::Unsupported,
                "read generation does not prepare frozen source reads",
            ))
        })
    }

    fn project_prepared_source_read(
        &self,
        _session: &ConnectorSession,
        _source: &ConnectorPreparedSourceRead,
        _projection: ConnectorReadSourceProjection,
    ) -> Result<ConnectorReadTableHandle, ConnectorError> {
        Err(ConnectorError::new(
            crate::connector::ConnectorErrorKind::Unsupported,
            "read generation does not project frozen source reads",
        ))
    }

    fn prepare_relation<'a>(
        &'a self,
        _session: &'a ConnectorSession,
        _request: &'a ConnectorReadRelationPreparationRequest,
    ) -> ConnectorReadRelationPreparationFuture<'a> {
        Box::pin(async {
            Err(ConnectorError::new(
                crate::connector::ConnectorErrorKind::Unsupported,
                "read generation does not support asynchronous relation preparation",
            ))
        })
    }
    fn create_prepared_relation(
        &self,
        _prepared: ConnectorReadRelationPreparation,
    ) -> Result<Option<ConnectorReadOpenedRelation>, ConnectorError> {
        Err(ConnectorError::new(
            crate::connector::ConnectorErrorKind::Unsupported,
            "read generation does not support prepared relation creation",
        ))
    }

    /// Exact installed read generation served by this metadata view.
    fn binding(&self) -> &ConnectorReadBinding;

    /// Freeze an opaque table with the transaction of its exact installed
    /// provider binding.  Roles can retain the resulting relation but cannot
    /// inspect or manufacture its transaction payload.
    fn relation(
        &self,
        kind: ConnectorReadRelationKind,
        table: ConnectorReadTableHandle,
    ) -> Result<ConnectorReadRelation, ConnectorError>;

    fn get_table_handle(
        &self,
        session: &ConnectorSession,
        name: &SchemaTableName,
        version: ConnectorReadRelationVersion,
        reference: Option<&str>,
    ) -> Result<Option<ConnectorReadTableHandle>, ConnectorError>;

    /// Freeze the complete MV target at its admitted object and snapshot.
    /// A missing snapshot is not permission to select another version. `None`
    /// requires the target to remain empty. This is separate from historical
    /// table reads so providers can use their current-ref observation scope.
    fn get_mv_target_handle(
        &self,
        _session: &ConnectorSession,
        _name: &SchemaTableName,
        _object_id: &crate::connector::ConnectorTableObjectId,
        _snapshot_id: Option<i64>,
    ) -> Result<Option<ConnectorReadTableHandle>, ConnectorError> {
        Err(ConnectorError::new(
            crate::connector::ConnectorErrorKind::Unsupported,
            "provider read generation does not support frozen MV target reads",
        ))
    }

    /// Adopt a provider-frozen source without reopening or widening it.
    /// The installed provider must validate its source owner and observation.
    fn adopt_frozen_source(
        &self,
        _session: &ConnectorSession,
        _source: &ConnectorFrozenReadSource,
    ) -> Result<ConnectorReadTableHandle, ConnectorError> {
        Err(ConnectorError::new(
            crate::connector::ConnectorErrorKind::Unsupported,
            "provider read generation does not adopt frozen write sources",
        ))
    }

    fn get_pinned_file_set_handle(
        &self,
        session: &ConnectorSession,
        name: &SchemaTableName,
        pinned: &ConnectorPinnedFileSet,
    ) -> Result<Option<ConnectorReadTableHandle>, ConnectorError>;

    /// Freeze a Known MV target partition selection on a provider-validated
    /// exact target and snapshot. Providers that cannot prove file selection
    /// must return a snapshot-pinned unrestricted handle.
    fn get_mv_target_partition_handle(
        &self,
        _session: &ConnectorSession,
        _name: &SchemaTableName,
        _selection: &ConnectorMvTargetPartitionSelection,
    ) -> Result<Option<ConnectorReadTableHandle>, ConnectorError> {
        Err(ConnectorError::new(
            crate::connector::ConnectorErrorKind::Unsupported,
            "provider read generation does not support MV target partition selection",
        ))
    }

    fn get_column_bindings(
        &self,
        session: &ConnectorSession,
        table: &ConnectorReadTableHandle,
    ) -> Result<Vec<ConnectorReadColumnBinding>, ConnectorError>;

    /// Commit the negotiated read and publish its immutable facts.
    ///
    /// This is the counterpart to `negotiate` and the point where a read stops
    /// being negotiable. It happens once, enumerates no splits and returns no
    /// runtime capability. The answer carries what it froze so the caller can
    /// check it is the read it asked to commit; the facts are not reachable
    /// until it has.
    fn freeze(
        &self,
        _session: &ConnectorSession,
        _request: &super::negotiation::ReadFreezeRequest,
    ) -> Result<super::negotiation::ConnectorReadFrozen<ConnectorReadColumnHandle>, ConnectorError>
    {
        Err(ConnectorError::new(
            crate::connector::ConnectorErrorKind::Unsupported,
            "connector read generation does not publish final static facts",
        ))
    }

    /// The exact public schema of a frozen read's assignment columns.
    ///
    /// Asked at freeze, with the frozen handle and the assignment columns in
    /// assignment order, repeats included. The answer has one field per column
    /// and is an immutable fact of that read: the provider's pure read compiler
    /// later checks the frozen public schema against this same author, so a
    /// caller carries it unchanged and never derives one from SQL names or
    /// engine types. Like `freeze`, this enumerates no splits and returns no
    /// runtime capability.
    fn read_public_schema(
        &self,
        _session: &ConnectorSession,
        _table: &ConnectorReadTableHandle,
        _columns: &[ConnectorReadColumnHandle],
        _creation: &mut super::ConnectorReadSchemaCreation<'_>,
    ) -> Result<
        super::ConnectorReadPublicSchema,
        novarocks_connector_contract::PureProviderCompileError<ConnectorError>,
    > {
        Err(ConnectorError::new(
            crate::connector::ConnectorErrorKind::Unsupported,
            "connector read generation does not publish a public read schema",
        )
        .into())
    }

    /// Offer a provider an ordered list of pushdowns and learn what it takes on.
    ///
    /// This is the one way a caller asks that question. It is pure and
    /// repeatable: the same offer against the same handle answers the same way
    /// and commits to nothing, so it can be asked while a decision is still
    /// open. Committing is a separate call - see `final_static_facts` - which
    /// happens once and freezes what was negotiated.
    fn negotiate(
        &self,
        session: &ConnectorSession,
        negotiation: &super::negotiation::ReadNegotiation,
    ) -> Result<super::negotiation::ReadNegotiated, ConnectorError>;

    fn get_system_table_plan(
        &self,
        session: &ConnectorSession,
        name: &SchemaTableName,
    ) -> Result<Option<ConnectorReadSystemTablePlan>, ConnectorError>;

    /// Resolve a metadata relation while preserving its typed kind and version
    /// request. The legacy name-only method remains separate until its callers
    /// are retired; implementations must never infer missing request fields.
    fn get_system_table_plan_for_request(
        &self,
        _session: &ConnectorSession,
        _name: &SchemaTableName,
        _request: &super::ConnectorReadMetadataRequest,
    ) -> Result<Option<ConnectorReadSystemTablePlan>, ConnectorError> {
        Err(ConnectorError::new(
            crate::connector::ConnectorErrorKind::Unsupported,
            "connector read generation does not support typed metadata requests",
        ))
    }

    fn get_change_window_plan(
        &self,
        session: &ConnectorSession,
        name: &SchemaTableName,
        window: ConnectorReadChangeWindow,
    ) -> Result<Option<ConnectorReadTableHandle>, ConnectorError>;

    fn get_table_execute_plan(
        &self,
        session: &ConnectorSession,
        name: &SchemaTableName,
        procedure: ConnectorReadTableExecuteProcedure,
    ) -> Result<Option<ConnectorReadTableHandle>, ConnectorError>;
}

/// One request-bound FE view of an exact connector read control generation.
/// The opaque request context never crosses a wire; it lets a provider bind
/// response-vended object-store access while preserving the generation-scoped
/// codec and registration lease.
#[derive(Clone)]
pub struct ConnectorReadRequestControl {
    metadata: Arc<dyn ConnectorReadMetadata>,
    splits: Arc<dyn ConnectorReadSplitManager>,
    binding: ConnectorReadBinding,
    attempt_access: ConnectorReadAttemptAccessMode,
}

#[derive(Clone)]
enum ConnectorReadAttemptAccessMode {
    Unsupported,
    Static(ConnectorReadBinding),
    Provider(Arc<dyn ConnectorReadAttemptAccessSealer>),
}

impl ConnectorReadRequestControl {
    /// Construct a request view that explicitly rejects attempt replacement.
    pub fn unsupported_attempt_access(
        metadata: Arc<dyn ConnectorReadMetadata>,
        splits: Arc<dyn ConnectorReadSplitManager>,
    ) -> Self {
        let binding = metadata.binding().clone();
        Self {
            metadata,
            splits,
            binding,
            attempt_access: ConnectorReadAttemptAccessMode::Unsupported,
        }
    }

    /// Construct a request view whose installed metadata and split services are
    /// sufficient for every attempt. This is an explicit provider contract;
    /// absence of a request factory does not imply static access.
    pub fn static_attempt_access(
        metadata: Arc<dyn ConnectorReadMetadata>,
        splits: Arc<dyn ConnectorReadSplitManager>,
        binding: ConnectorReadBinding,
    ) -> Self {
        Self {
            metadata,
            splits,
            binding: binding.clone(),
            attempt_access: ConnectorReadAttemptAccessMode::Static(binding),
        }
    }

    /// Construct a request view whose provider validates the final handle and
    /// reacquires the next attempt's request-scoped access.
    pub fn provider_reacquire_attempt_access(
        metadata: Arc<dyn ConnectorReadMetadata>,
        splits: Arc<dyn ConnectorReadSplitManager>,
        sealer: Arc<dyn ConnectorReadAttemptAccessSealer>,
    ) -> Self {
        let binding = metadata.binding().clone();
        Self {
            metadata,
            splits,
            binding,
            attempt_access: ConnectorReadAttemptAccessMode::Provider(sealer),
        }
    }

    pub const fn binding(&self) -> &ConnectorReadBinding {
        &self.binding
    }

    pub fn metadata(&self) -> Arc<dyn ConnectorReadMetadata> {
        Arc::clone(&self.metadata)
    }

    pub fn splits(&self) -> Arc<dyn ConnectorReadSplitManager> {
        Arc::clone(&self.splits)
    }

    /// Seal the final, already-negotiated handle into a provider-owned
    /// per-attempt access source. Providers without request-vended access use
    /// an exact static rebind to these same generation services.
    pub fn seal_attempt_access(
        &self,
        frozen: &ConnectorReadTableHandle,
    ) -> Result<ConnectorReadAttemptAccessSource, ConnectorError> {
        if self.metadata.binding() != &self.binding
            || self.splits.binding() != &self.binding
            || frozen.binding() != &self.binding
        {
            return Err(ConnectorError::new(
                crate::connector::ConnectorErrorKind::InvalidRequest,
                "Connector read attempt access received capabilities from different bindings",
            ));
        }
        match &self.attempt_access {
            ConnectorReadAttemptAccessMode::Provider(sealer) => {
                let seal = Arc::new(());
                let source = sealer.seal(
                    frozen,
                    ConnectorReadAttemptAccessMint {
                        frozen: frozen.clone(),
                        seal: Arc::clone(&seal),
                    },
                )?;
                if !Arc::ptr_eq(&seal, &source.seal) {
                    return Err(ConnectorError::new(
                        crate::connector::ConnectorErrorKind::InvalidRequest,
                        "Connector read attempt sealer returned a source from another seal",
                    ));
                }
                Ok(source)
            }
            ConnectorReadAttemptAccessMode::Static(binding) => {
                Ok(ConnectorReadAttemptAccessSource::new_private(
                    ConnectorReadAttemptAccessMint {
                        frozen: frozen.clone(),
                        seal: Arc::new(()),
                    },
                    Arc::new(StaticConnectorReadAttemptAccess {
                        splits: Arc::clone(&self.splits),
                        binding: binding.clone(),
                    }),
                ))
            }
            ConnectorReadAttemptAccessMode::Unsupported => Err(ConnectorError::new(
                crate::connector::ConnectorErrorKind::Unsupported,
                "Connector read generation does not support per-attempt access reacquisition",
            )),
        }
    }
}

/// Provider-owned factory sealed against one exact, final read handle.
pub trait ConnectorReadAttemptAccessSealer: Send + Sync {
    fn seal(
        &self,
        frozen: &ConnectorReadTableHandle,
        mint: ConnectorReadAttemptAccessMint,
    ) -> Result<ConnectorReadAttemptAccessSource, ConnectorError>;
}

/// Authority issued only after a request control verifies one exact final
/// handle and installed generation. The provider may attach its private
/// reacquirer but cannot replace the frozen handle.
pub struct ConnectorReadAttemptAccessMint {
    frozen: ConnectorReadTableHandle,
    seal: Arc<()>,
}

impl ConnectorReadAttemptAccessMint {
    pub fn seal(
        self,
        reacquirer: Arc<dyn ConnectorReadAttemptAccessReacquirer>,
    ) -> ConnectorReadAttemptAccessSource {
        ConnectorReadAttemptAccessSource::new_private(self, reacquirer)
    }
}

/// Reacquires only request-scoped access for one immutable scan. It cannot
/// renegotiate projection, predicate, limit, snapshot, or relation identity.
pub trait ConnectorReadAttemptAccessReacquirer: Send + Sync {
    /// Fact sealed with this exact read, not inferred from its relation kind.
    /// Prepared-source reads need the original metadata CPU/scope packet;
    /// ordinary reads retain their existing execution preparation contract.
    fn requires_metadata_split_preparation(&self) -> bool {
        false
    }

    fn for_attempt<'a>(
        &'a self,
        request: &'a ConnectorAttemptContext,
    ) -> futures::future::BoxFuture<'a, Result<ConnectorReadAttemptRuntime, ConnectorError>>;
}

/// Attempt-local execution services for one already-negotiated scan.
///
/// Metadata is deliberately absent: reacquisition can refresh authorization
/// and build split sources for the frozen handle, but it cannot call planning
/// operations or return a replacement handle.
#[derive(Clone)]
pub struct ConnectorReadAttemptRuntime {
    splits: Arc<dyn ConnectorReadSplitManager>,
    binding: ConnectorReadBinding,
}

impl ConnectorReadAttemptRuntime {
    pub fn new(splits: Arc<dyn ConnectorReadSplitManager>) -> Self {
        let binding = splits.binding().clone();
        Self { splits, binding }
    }

    pub const fn binding(&self) -> &ConnectorReadBinding {
        &self.binding
    }

    pub fn splits(&self) -> Arc<dyn ConnectorReadSplitManager> {
        Arc::clone(&self.splits)
    }
}

/// Opaque process-local access source paired with the exact final handle it
/// was sealed from. No credential or provider payload is exposed.
#[derive(Clone)]
pub struct ConnectorReadAttemptAccessSource {
    frozen: ConnectorReadTableHandle,
    seal: Arc<()>,
    reacquirer: Arc<dyn ConnectorReadAttemptAccessReacquirer>,
}

impl ConnectorReadAttemptAccessSource {
    fn new_private(
        mint: ConnectorReadAttemptAccessMint,
        reacquirer: Arc<dyn ConnectorReadAttemptAccessReacquirer>,
    ) -> Self {
        Self {
            frozen: mint.frozen,
            seal: mint.seal,
            reacquirer,
        }
    }

    pub const fn frozen(&self) -> &ConnectorReadTableHandle {
        &self.frozen
    }

    pub fn requires_metadata_split_preparation(&self) -> bool {
        self.reacquirer.requires_metadata_split_preparation()
    }

    pub(crate) async fn for_attempt(
        &self,
        request: &ConnectorAttemptContext,
    ) -> Result<ConnectorReadAttemptRuntime, ConnectorError> {
        check_attempt_request_active(request)?;
        let runtime = self.reacquirer.for_attempt(request).await?;
        if runtime.binding() != self.frozen.binding()
            || runtime.splits.binding() != self.frozen.binding()
        {
            return Err(ConnectorError::new(
                crate::connector::ConnectorErrorKind::InvalidRequest,
                "Connector read attempt reacquirer returned another binding",
            ));
        }
        Ok(runtime)
    }
}

struct StaticConnectorReadAttemptAccess {
    splits: Arc<dyn ConnectorReadSplitManager>,
    binding: ConnectorReadBinding,
}

impl ConnectorReadAttemptAccessReacquirer for StaticConnectorReadAttemptAccess {
    fn for_attempt<'a>(
        &'a self,
        request: &'a ConnectorAttemptContext,
    ) -> futures::future::BoxFuture<'a, Result<ConnectorReadAttemptRuntime, ConnectorError>> {
        Box::pin(async move {
            check_attempt_request_active(request)?;
            if self.splits.binding() != &self.binding {
                return Err(ConnectorError::new(
                    crate::connector::ConnectorErrorKind::InvalidRequest,
                    "Static Connector attempt access changed its installed binding",
                ));
            }
            Ok(ConnectorReadAttemptRuntime::new(Arc::clone(&self.splits)))
        })
    }
}

fn check_attempt_request_active(request: &ConnectorAttemptContext) -> Result<(), ConnectorError> {
    let request = request.request();
    if request.is_cancelled() {
        return Err(ConnectorError::new(
            crate::connector::ConnectorErrorKind::Cancelled,
            "Connector read attempt access was cancelled",
        ));
    }
    if std::time::Instant::now() >= request.deadline() {
        return Err(ConnectorError::new(
            crate::connector::ConnectorErrorKind::DeadlineExceeded,
            "Connector read attempt access deadline elapsed",
        ));
    }
    Ok(())
}

/// Connector-owned factory for a request-bound coordinator read view. The
/// factory is retained by the exact installed control; a role can request a
/// view but cannot inspect or construct provider payloads.
pub trait ConnectorReadRequestControlFactory: Send + Sync {
    fn planning_io_geometry(
        &self,
    ) -> Result<crate::connector::ConnectorMetadataMaintenanceIoGeometry, ConnectorError> {
        Err(ConnectorError::new(
            crate::connector::ConnectorErrorKind::Unsupported,
            "read factory has no closed I/O geometry",
        ))
    }

    /// Pure selected-route facts from this exact installed read generation.
    /// Called after acquiring the original paid proposal position and before
    /// request-control construction or any physical preparation. A callback
    /// only appends facts to that same proposal; it grants no effect authority.
    fn visit_planning_dependencies(
        &self,
        _visit: &mut dyn FnMut(&crate::connector::SharedDependency) -> Result<(), ConnectorError>,
    ) -> Result<(), ConnectorError> {
        Err(ConnectorError::new(
            crate::connector::ConnectorErrorKind::Unsupported,
            "read generation has no selected planning dependency author",
        ))
    }

    fn for_planning(
        &self,
        request: &ConnectorPlanningContext,
    ) -> Result<ConnectorReadRequestControl, ConnectorError>;
}

pub trait ConnectorReadSplitSource: Send {
    fn profile_snapshot(&self) -> super::SplitSourceProfile {
        super::SplitSourceProfile::default()
    }

    /// A connector may ask the coordinator to wait briefly for an initial
    /// dynamic-filter snapshot before it expands its first file.  The
    /// coordinator owns the actual cap and fairness policy; zero means this
    /// source must start immediately.
    fn initial_dynamic_filter_wait_request(&self) -> std::time::Duration {
        std::time::Duration::ZERO
    }

    fn next_batch(
        &mut self,
        max_size: usize,
        dynamic_filter: &ConnectorReadDynamicFilterSnapshot,
    ) -> Result<ConnectorSplitBatch<ConnectorReadSplit>, ConnectorError>;

    fn is_finished(&self) -> bool;

    fn close(&mut self) -> Result<(), ConnectorError>;
}

/// Owned original open input. The stable initializer retains the returned F;
/// cancellation of a borrowing waiter does not discard its source construction.
pub struct ConnectorReadSplitOpenRequest {
    session: ConnectorSession,
    columns: Vec<ConnectorReadAssignment>,
    dynamic_filter_columns: BTreeSet<ConnectorReadColumnHandle>,
    constraint: ConnectorReadConstraint,
    table: ConnectorReadTableHandle,
    preparation: Option<ConnectorReadSplitPreparation>,
}
impl ConnectorReadSplitOpenRequest {
    pub const fn new(
        session: ConnectorSession,
        table: ConnectorReadTableHandle,
        columns: Vec<ConnectorReadAssignment>,
        dynamic_filter_columns: BTreeSet<ConnectorReadColumnHandle>,
        constraint: ConnectorReadConstraint,
    ) -> Self {
        Self {
            session,
            table,
            columns,
            dynamic_filter_columns,
            constraint,
            preparation: None,
        }
    }
    pub fn with_source_preparation(mut self, preparation: ConnectorReadSplitPreparation) -> Self {
        self.preparation = Some(preparation);
        self
    }
    pub(crate) fn into_parts(
        self,
    ) -> (
        ConnectorSession,
        ConnectorReadTableHandle,
        Vec<ConnectorReadAssignment>,
        BTreeSet<ConnectorReadColumnHandle>,
        ConnectorReadConstraint,
        Option<ConnectorReadSplitPreparation>,
    ) {
        (
            self.session,
            self.table,
            self.columns,
            self.dynamic_filter_columns,
            self.constraint,
            self.preparation,
        )
    }
}
pub type ConnectorReadSplitOpenFuture =
    futures::future::BoxFuture<'static, Result<Box<dyn ConnectorReadSplitSource>, ConnectorError>>;

pub trait ConnectorReadSplitManager: Send + Sync {
    fn binding(&self) -> &ConnectorReadBinding;
    /// Install any Provider opening source into the caller's stable slot
    /// before returning its borrowing Future. The caller retains that source
    /// through raw receipt, actual CPU/IO exit and foreign Future Drop.
    fn get_splits(
        &self,
        request: ConnectorReadSplitOpenRequest,
        original_owner: &mut Option<crate::connector::ConnectorProcessLocalPayload>,
    ) -> ConnectorReadSplitOpenFuture;
}

/// A role-local lease that keeps one exact read-control registration alive.
///
/// The connector that owns a control generation retains the strong lease with
/// its generation metadata.  Role registries retain only a weak reference, so
/// dropping the generation can remove exactly its local slot without turning
/// registry retirement into a Host or RPC operation.
///
/// This is deliberately a marker: it carries no provider payload, wire value,
/// or role authority.
pub trait ConnectorReadRegistrationLease: Send + Sync {}

pub trait ConnectorReadPageSourceProvider: Send + Sync {
    /// Opens one split as a page stream the host polls with `budget`, the
    /// CPU budget it refills every turn. Opening must not wait for I/O: the
    /// stream opens its input when it is first polled.
    fn create_page_stream(
        &self,
        session: &ConnectorSession,
        table: &ConnectorReadTableHandle,
        split: &ConnectorReadSplit,
        scheduled_split_sequence_id: u64,
        columns: &[ConnectorReadAssignment],
        dynamic_filter: &Arc<ConnectorReadDynamicFilter>,
        budget: &ConnectorPollBudget,
    ) -> Result<OwnedConnectorPageStream, ConnectorError>;

    /// Optionally prepare an already scheduled future split without opening
    /// its page stream or advancing the scan's consumption position.
    fn prepare_page_source(
        &self,
        _session: &ConnectorSession,
        _table: &ConnectorReadTableHandle,
        _split: &ConnectorReadSplit,
        _scheduled_split_sequence_id: u64,
        _columns: &[ConnectorReadAssignment],
        _dynamic_filter: &Arc<ConnectorReadDynamicFilter>,
    ) -> Result<ConnectorPreparationStart, ConnectorError> {
        Ok(ConnectorPreparationStart::Unsupported)
    }
}

/// The backend factory contract after task admission. All reads
/// require the exact task's resource capability as an independent argument;
/// metadata callers cannot obtain one from a request context.
///
/// ```compile_fail
/// use novarocks_spi::connector::read_stack::ConnectorAdmittedReadProviderFactory;
/// use novarocks_spi::connector::ConnectorRequestContext;
/// let factory: &dyn ConnectorAdmittedReadProviderFactory = todo!();
/// let request: &ConnectorRequestContext = todo!();
/// let _ = factory.create_page_source_provider(request);
/// ```
///
/// ```compile_fail
/// use novarocks_spi::connector::read_stack::ConnectorAdmittedReadProviderFactory;
/// use novarocks_spi::connector::ConnectorRequestContext;
/// let factory: &dyn ConnectorAdmittedReadProviderFactory = todo!();
/// let request: &ConnectorRequestContext = todo!();
/// let options: novarocks_spi::connector::read_stack::ConnectorPageSourceProviderOptions = todo!();
/// let _ = factory.create_page_source_provider(request, request.clone(), options);
/// ```
pub trait ConnectorAdmittedReadProviderFactory: Send + Sync {
    /// Exact catalog generation whose task-owned provider this factory opens.
    fn binding(&self) -> &ConnectorReadBinding;

    fn create_page_source_provider(
        &self,
        request: &ConnectorRequestContext,
        resources: ConnectorExecutionResources,
        options: super::ConnectorPageSourceProviderOptions,
    ) -> Result<Arc<dyn ConnectorReadPageSourceProvider>, ConnectorError>;
}

// These helpers are intentionally crate-private.  Concrete connectors use the
// generic adapter; no caller can obtain an `Any` payload or arbitrarily
// downcast an installed handle.
pub(crate) fn table_handle<T: Send + Sync + 'static>(
    binding: ConnectorReadBinding,
    value: T,
) -> ConnectorReadTableHandle {
    ConnectorReadTableHandle {
        binding,
        payload: OpaquePayload::new(value),
    }
}

pub(crate) fn transaction_handle<T: Send + Sync + 'static>(
    binding: ConnectorReadBinding,
    value: T,
) -> ConnectorReadTransactionHandle {
    ConnectorReadTransactionHandle {
        binding,
        payload: OpaquePayload::new(value),
    }
}

pub(crate) fn column_handle<T: ColumnHandle>(
    binding: ConnectorReadBinding,
    value: T,
) -> ConnectorReadColumnHandle {
    ConnectorReadColumnHandle {
        binding,
        payload: OpaquePayload::new(value),
        comparison: Arc::new(TypedColumnComparison::<T>(PhantomData)),
    }
}

pub(crate) fn split_handle<T: Send + Sync + 'static>(
    binding: ConnectorReadBinding,
    facts: ConnectorReadSplitFacts,
    value: T,
) -> ConnectorReadSplit {
    ConnectorReadSplit {
        binding,
        facts,
        payload: OpaquePayload::new(value),
    }
}

pub(crate) fn table_value<T: 'static>(handle: &ConnectorReadTableHandle) -> Option<&T> {
    handle.payload.downcast_ref()
}

pub(crate) fn shared_table_value<T: Send + Sync + 'static>(
    handle: &ConnectorReadTableHandle,
) -> Option<Arc<T>> {
    handle.payload.0.clone().downcast::<T>().ok()
}

pub(crate) fn transaction_value<T: 'static>(handle: &ConnectorReadTransactionHandle) -> Option<&T> {
    handle.payload.downcast_ref()
}

pub(crate) fn column_value<T: 'static>(handle: &ConnectorReadColumnHandle) -> Option<&T> {
    handle.payload.downcast_ref()
}

pub(crate) fn split_value<T: 'static>(handle: &ConnectorReadSplit) -> Option<&T> {
    handle.payload.downcast_ref()
}

pub(crate) fn binding_error() -> ConnectorError {
    ConnectorError::new(
        crate::connector::ConnectorErrorKind::InvalidRequest,
        "connector read handle belongs to another binding",
    )
}

pub(crate) fn type_error() -> ConnectorError {
    ConnectorError::new(
        crate::connector::ConnectorErrorKind::InvalidRequest,
        "connector read handle has an incompatible concrete type",
    )
}

pub(crate) fn map_constraint<C: Ord + Clone + Debug>(
    constraint: &ConnectorReadConstraint,
    mut map: impl FnMut(&ConnectorReadColumnHandle) -> Result<C, ConnectorError>,
) -> Result<Constraint<C>, ConnectorError> {
    let summary = map_tuple_domain(constraint.summary(), &mut map)?;
    let assignments = constraint
        .assignments()
        .iter()
        .map(|(name, column)| map(column).map(|column| (name.clone(), column)))
        .collect::<Result<BTreeMap<_, _>, _>>()?;
    Constraint::try_new(summary, constraint.expression().clone(), assignments)
}

pub(crate) fn map_tuple_domain<C: Ord + Clone + Debug>(
    domain: &TupleDomain<ConnectorReadColumnHandle>,
    mut map: impl FnMut(&ConnectorReadColumnHandle) -> Result<C, ConnectorError>,
) -> Result<TupleDomain<C>, ConnectorError> {
    let Some(domains) = domain.domains() else {
        return Ok(TupleDomain::none());
    };
    let mapped = domains
        .iter()
        .map(|(column, domain)| map(column).map(|column| (column, domain.clone())))
        .collect::<Result<BTreeMap<_, _>, _>>()?;
    TupleDomain::with_column_domains(mapped)
}
