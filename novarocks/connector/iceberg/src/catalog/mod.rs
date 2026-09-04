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

//! The provider-private Iceberg catalog owner.
//!
//! Design: ADR-0118 (docs/adr/ADR-0118-iceberg-provider-private-catalog-owner.md)
//!
//! [`NovaRocksCatalog`] is the only place Iceberg catalog semantics are
//! decided. Everything above it — metadata, DDL, CTAS, write, maintenance,
//! cleanup — asks this trait and never inspects a catalog kind, an optional
//! concrete client, or a capability slot.
//!
//! # Why the methods take owned requests
//!
//! The neutral SPI is synchronous, so every catalog call is polled through
//! `IcebergCatalogRuntime::block_on`, which spawns a bridge thread and
//! therefore requires `Future + Send + 'static`. Call sites satisfy that by
//! moving an `Arc<dyn NovaRocksCatalog>` and an owned request into an
//! `async move` block:
//!
//! ```ignore
//! let catalog = Arc::clone(context.novarocks_catalog());
//! runtime.block_on(async move { catalog.load_table(request).await })
//! ```
//!
//! The block owns everything it touches, so it is `'static` even though the
//! trait methods borrow `&self`. Requests must not borrow for the same reason.
//!
//! # What is deliberately absent
//!
//! There is no `rename_table` or `rename_view`: the neutral
//! `ConnectorCatalogMutationOperation` has no rename operation, so such methods
//! would be surface no caller reaches and no test can exercise. The trait
//! covers the operations the provider actually performs, and grows only when a
//! real caller appears.
//!
//! There is no `register_table` either. Every catalog could forward one, but
//! nothing called it: production anchors an already-written metadata file
//! through `anchor_written_metadata`, which is the operation the provider
//! actually performs. A trait method whose only content is a forward to a
//! primitive no caller wants is a second way to do one thing.

pub(crate) mod delegate;
pub(crate) mod dispatch;
pub(crate) mod error;
pub(crate) mod factory;
pub(crate) mod hadoop;
pub(crate) mod hive;
pub(crate) mod rest;

use std::fmt::Debug;
use std::sync::Arc;

use async_trait::async_trait;
use novarocks_spi::connector::ConnectorError;

use self::error::{CatalogOutcome, CatalogUnsupported};

/// A namespace this catalog can be asked about.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub(crate) struct CatalogNamespaceName {
    pub(crate) namespace: Arc<str>,
}

impl CatalogNamespaceName {
    pub(crate) fn new(namespace: impl Into<Arc<str>>) -> Self {
        Self {
            namespace: namespace.into(),
        }
    }
}

impl std::fmt::Display for CatalogNamespaceName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.namespace)
    }
}

/// A table or view this catalog can be asked about.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub(crate) struct CatalogTableName {
    pub(crate) namespace: Arc<str>,
    pub(crate) name: Arc<str>,
}

impl CatalogTableName {
    pub(crate) fn new(namespace: impl Into<Arc<str>>, name: impl Into<Arc<str>>) -> Self {
        Self {
            namespace: namespace.into(),
            name: name.into(),
        }
    }

    pub(crate) fn canonical(&self) -> Arc<str> {
        Arc::from(format!("{}.{}", self.namespace, self.name))
    }
}

impl std::fmt::Display for CatalogTableName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}.{}", self.namespace, self.name)
    }
}

/// What a table creation is ultimately for.
///
/// This is the reason admission is a per-request question rather than a
/// per-catalog flag. A Hadoop catalog can publish an empty table atomically
/// (ADR-0077) but cannot satisfy a standard staged CTAS; a REST catalog can
/// create a table yet still be unable to run CTAS safely when it has no
/// enumerable staging root. One boolean cannot say either thing.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum CatalogCreateIntent {
    /// `CREATE TABLE` with no rows. The catalog owns the whole frontier.
    EmptyTable,
    /// `CREATE TABLE AS SELECT`. A source runs and a writer dispatches between
    /// admission and publication, so the catalog must prove the whole path
    /// before the first side effect.
    CreateTableAsSelect,
}

impl CatalogCreateIntent {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::EmptyTable => "empty-table",
            Self::CreateTableAsSelect => "ctas",
        }
    }
}

/// The result of asking a catalog to begin a transaction.
///
/// The `Ready` arm is what separates this from a plain outcome: admission
/// succeeded and the caller now holds the single frontier for this operation.
/// The other three arms are terminal, and `Unsupported` among them is the only
/// one that promises nothing was attempted.
#[derive(Debug)]
pub(crate) enum CatalogTransactionStart {
    Ready(Box<transaction::Transaction>),
    /// Refused before any external side effect. Safe to report to the user as
    /// "this catalog cannot do that", and safe to clean up caller-owned state.
    Unsupported(CatalogUnsupported),
    /// Admission failed with proof that nothing external was mutated.
    KnownUncommitted {
        failure: novarocks_spi::connector::ConnectorMutationFailure,
    },
    /// Admission itself may have dispatched a request — a REST staged create
    /// is a real catalog mutation. Callers must not retry or clean up.
    // Not constructed yet: no wired constructor dispatches during admission.
    #[allow(dead_code)]
    CommitUnknown {
        failure: novarocks_spi::connector::ConnectorMutationFailure,
        evidence: error::CatalogCommitEvidence,
    },
}

impl CatalogTransactionStart {
    /// True when admission failed and left the caller owning everything it
    /// touched, so caller-owned temporary state is safe to release.
    ///
    /// This answers a question only the terminal arms have: a `Ready`
    /// transaction has not failed at all, and releasing its state is
    /// [`transaction::Transaction::abort`]'s business, which refuses once the
    /// outcome is unknown.
    // No production caller yet: this is the update-commit half of the
    // lifecycle, which is wired when `commit/**` migrates off `vendored_client`.
    #[allow(dead_code)]
    pub(crate) fn permits_cleanup(&self) -> bool {
        matches!(self, Self::Unsupported(_) | Self::KnownUncommitted { .. })
    }
}

/// A drop that committed, with the exact identity captured before the drop.
///
/// The identity is captured ahead of the catalog request precisely because it
/// is unreadable afterwards, and post-commit cleanup may only ever act on
/// exact identity. See ADR-0118 on the catalog/filesystem split.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct CatalogDropTableReceipt {
    pub(crate) table_uuid: Option<Arc<str>>,
    pub(crate) table_location: Option<Arc<str>>,
    pub(crate) metadata_location: Option<Arc<str>>,
    /// The table's own last-updated timestamp, in lake time, read before the
    /// drop. Age evidence for later collection; this owner never compares it.
    pub(crate) last_updated_ms: i64,
}

/// Inputs for a conditional create.
#[derive(Debug)]
pub(crate) struct ConditionalCreateRequest {
    pub(crate) namespace: CatalogNamespaceName,
    pub(crate) creation: crate::iceberg::TableCreation,
    /// Caller-owned operation identity, stamped into the attempt so a later
    /// adjudication can recognise this exact attempt rather than any create.
    pub(crate) operation_id: Arc<str>,
}

/// A prepared-but-unpublished conditional create.
///
/// Opaque above this module: the concrete attempt state belongs to whichever
/// implementation prepared it.
#[derive(Debug)]
pub(crate) struct ConditionalCreateAttempt {
    pub(crate) facts: ConditionalCreateFacts,
    inner: ConditionalCreateAttemptState,
}

#[derive(Debug)]
enum ConditionalCreateAttemptState {
    Hadoop(Box<crate::hadoop_catalog::HadoopCreateAttempt>),
}

impl ConditionalCreateAttempt {
    pub(crate) fn hadoop(
        attempt: crate::hadoop_catalog::HadoopCreateAttempt,
        facts: ConditionalCreateFacts,
    ) -> Self {
        Self {
            facts,
            inner: ConditionalCreateAttemptState::Hadoop(Box::new(attempt)),
        }
    }

    pub(crate) fn into_hadoop(self) -> Option<crate::hadoop_catalog::HadoopCreateAttempt> {
        match self.inner {
            ConditionalCreateAttemptState::Hadoop(attempt) => Some(*attempt),
        }
    }
}

/// Facts a prepared attempt exposes so its caller can build publication
/// evidence before the attempt is dispatched.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ConditionalCreateFacts {
    pub(crate) operation_id: Arc<str>,
    pub(crate) table_uuid: Arc<str>,
    pub(crate) metadata_location: Arc<str>,
    pub(crate) metadata_digest: Arc<str>,
}

/// What a published conditional create produced.
// Produced only by `publish_conditional_create`; see the note there.
#[allow(dead_code)]
#[derive(Clone, Debug)]
pub(crate) struct ConditionalCreateReceipt {
    pub(crate) facts: ConditionalCreateFacts,
    pub(crate) already_existed: bool,
    pub(crate) authoritative_table_uuid: Arc<str>,
    pub(crate) authoritative_metadata_digest: Arc<str>,
    /// Metadata location the published table now resolves to.
    pub(crate) published_metadata_location: Option<Arc<str>>,
    /// A failure that happened after the create committed. It never downgrades
    /// the commit; it is finalization state.
    pub(crate) finalization_failure: Option<Arc<str>>,
}

/// Identity a later adjudication compares against.
#[derive(Clone, Debug)]
pub(crate) struct ConditionalCreateEvidence {
    pub(crate) namespace: Arc<str>,
    pub(crate) table: Arc<str>,
    pub(crate) expected_table_uuid: Arc<str>,
    pub(crate) metadata_location: Arc<str>,
    pub(crate) metadata_digest: Arc<str>,
}

/// Read-only verdict on a conditional create.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum ConditionalCreateVerdict {
    /// Exact positive: this attempt's metadata is present.
    Committed {
        finalization_failure: Option<Arc<str>>,
    },
    /// Nothing is there. Not proof the request never landed elsewhere.
    Absent,
    /// Something is there, but it is not this attempt's.
    Foreign,
}

/// What a staged create produced, keeping every dispatch distinction the
/// underlying protocol makes.
///
/// The arms mirror the REST client's typed staged-create result rather than
/// collapsing it: `Conflict` is a definite server rejection, `KnownUncommitted`
/// proves the request never went out, and `CommitUnknown` means it may have.
/// Flattening these is how a lost response becomes a duplicate publication.
pub(crate) enum StagedCreateStart {
    Staged {
        /// The table response must not select a FileIO before a vended
        /// delegation, when present, has entered the query-attempt collector.
        table: crate::loaded_table::IcebergLoadedTableMaterialization,
        initialization_updates: Vec<crate::iceberg::TableUpdate>,
        /// Response-local vended material.  It is intentionally not part of
        /// the table or staged-write payload; the caller must hand it to the
        /// query-attempt lease owner or refuse the operation.
        access_delegation: crate::loaded_table::IcebergAccessDelegation,
    },
    Conflict(String),
    KnownUncommitted(String),
    CommitUnknown(String),
    /// This catalog has no staged-create protocol. Proven zero side effect.
    Unsupported(CatalogUnsupported),
}

/// What committing a staged create produced.
#[derive(Debug)]
pub(crate) enum StagedCommitResult {
    Committed(crate::iceberg::table::Table),
    Conflict(String),
    KnownUncommitted(String),
    CommitUnknown(String),
    /// The server answered, but the answer does not prove the publication. The
    /// create may well have landed, so this is not an uncommitted result.
    CommittedResponseInvalid(String),
    Unsupported(CatalogUnsupported),
}

/// The single semantic owner of Iceberg catalog behavior.
///
/// Implementations are chosen once per control generation by
/// [`factory::NovaRocksCatalogFactory`] and are never downcast, kind-checked,
/// or unwrapped by callers.
///
/// # Read methods
///
/// Reads have no publication frontier. They must keep `NotFound`,
/// `Unsupported`, and `Unavailable` distinguishable: an implementation that
/// cannot answer returns `Unsupported` and never fabricates `false` or an empty
/// collection. That fiction is what this owner exists to remove.
///
/// # Mutation methods
///
/// Direct mutations own exactly one external frontier with no caller-owned gap
/// in front of it, so they answer with [`CatalogOutcome`] directly. Operations
/// that do have such a gap — a source to run, a writer to dispatch, staged
/// objects to publish — go through a transaction constructor instead, so that
/// admission completes before the first side effect.
#[async_trait]
pub(crate) trait NovaRocksCatalog: Debug + Send + Sync + 'static {
    /// Human-readable implementation name, for diagnostics and log lines only.
    ///
    /// This must never be consulted to decide whether an operation is
    /// supported. Ask the operation.
    // Identity for assertions; nothing in production needs to ask.
    #[allow(dead_code)]
    fn implementation_name(&self) -> &'static str;

    /// The vendored client this owner wraps.
    ///
    /// Migration seam, removed with the last unmigrated caller. It exists so a
    /// generation holds exactly *one* catalog while the operation families move
    /// across one at a time. Building a second client for the legacy handle
    /// instead would give two clients with separate in-memory state that
    /// disagree about the same lake — a dropped table still visible through the
    /// other handle, for instance.
    fn vendored_client(&self) -> Arc<dyn crate::iceberg::Catalog>;

    /// The provider-private REST client that may refresh an already-admitted
    /// vended credential scope. Only the REST implementation owns such a
    /// capability; callers never downcast or construct a second catalog.
    fn vended_credential_refresh_catalog(
        &self,
    ) -> Option<Arc<crate::iceberg_catalog_rest::RestCatalog>> {
        None
    }

    // ---- A. Reads -------------------------------------------------------

    async fn list_namespaces(&self) -> Result<Vec<String>, ConnectorError>;

    async fn namespace_exists(
        &self,
        namespace: CatalogNamespaceName,
    ) -> Result<bool, ConnectorError>;

    async fn list_tables(
        &self,
        namespace: CatalogNamespaceName,
    ) -> Result<Vec<String>, ConnectorError>;

    async fn table_exists(&self, table: CatalogTableName) -> Result<bool, ConnectorError>;

    async fn load_table(
        &self,
        table: CatalogTableName,
    ) -> Result<crate::loaded_table::IcebergLoadedTable, ConnectorError>;

    /// Whether the view exists.
    ///
    /// An implementation whose catalog format cannot hold views returns
    /// `Unsupported`, not `Ok(false)`. Callers that need "is this name a view?"
    /// as a hint may map `Unsupported` to "not a view" themselves; callers that
    /// need an authoritative enumeration must surface the refusal.
    async fn view_exists(&self, view: CatalogTableName) -> Result<bool, ConnectorError>;

    /// Enumerate views. See [`NovaRocksCatalog::view_exists`] on why this is
    /// not allowed to answer with an empty vector when it cannot answer.
    async fn list_views(
        &self,
        namespace: CatalogNamespaceName,
    ) -> Result<Vec<String>, ConnectorError>;

    async fn load_view(
        &self,
        view: CatalogTableName,
    ) -> Result<crate::iceberg::spec::ViewMetadata, ConnectorError>;

    // ---- B. Direct mutations -------------------------------------------

    async fn create_namespace(
        &self,
        namespace: CatalogNamespaceName,
    ) -> CatalogOutcome<CatalogNamespaceName>;

    async fn drop_namespace(
        &self,
        namespace: CatalogNamespaceName,
    ) -> CatalogOutcome<CatalogNamespaceName>;

    /// Drop a table from the catalog.
    ///
    /// This removes catalog visibility only. Object deletion is the filesystem
    /// owner's job and may only proceed on `KnownCommitted`; see ADR-0118.
    async fn drop_table(&self, table: CatalogTableName) -> CatalogOutcome<CatalogDropTableReceipt>;

    /// Make a write's freshly published metadata reachable through this
    /// catalog.
    ///
    /// Catalogs that track metadata locations themselves have nothing to do and
    /// answer with a no-op. A filesystem catalog, whose entry *is* the metadata
    /// pointer, has to anchor it.
    ///
    /// This exists so the write path stops asking "is this a remote catalog?".
    /// That question was answered outside the factory, by a helper that also
    /// swallowed the errors from the namespace it created along the way.
    async fn anchor_written_metadata(
        &self,
        table: CatalogTableName,
        metadata_location: Arc<str>,
    ) -> CatalogOutcome<CatalogTableName>;

    // ---- C. Transaction constructors ------------------------------------

    /// Begin a transaction against an existing table.
    // No production caller yet: this is the update-commit half of the
    // lifecycle, which is wired when `commit/**` migrates off `vendored_client`.
    #[allow(dead_code)]
    async fn new_transaction(
        &self,
        request: transaction::TransactionRequest,
    ) -> CatalogTransactionStart;

    /// Create an invisible target for a staged publication.
    ///
    /// The staged table is not reachable through the catalog until it is
    /// committed, which is what makes a CTAS absent-or-complete. A catalog with
    /// no such protocol answers `Unsupported`, and never by creating a visible
    /// empty table to fill in afterwards.
    async fn stage_create_table(
        &self,
        namespace: CatalogNamespaceName,
        creation: crate::iceberg::TableCreation,
    ) -> StagedCreateStart;

    /// Publish a staged create with exactly one assert-create request.
    async fn commit_staged_table(
        &self,
        commit: crate::iceberg::TableCommit,
        request_file_io: crate::iceberg::io::FileIO,
    ) -> StagedCommitResult;

    /// Stage a conditional create against storage, without publishing it.
    ///
    /// A filesystem catalog's create is not a catalog call: its linearization
    /// point is a conditional write of the canonical metadata (ADR-0077). That
    /// primitive has no equivalent on the generic catalog trait, so it lives
    /// here rather than being reached for through a concrete client -- which is
    /// how a catalog kind used to escape the factory.
    ///
    /// Preparing is local: it builds metadata and sends nothing.
    async fn prepare_conditional_create(
        &self,
        request: ConditionalCreateRequest,
    ) -> CatalogOutcome<ConditionalCreateAttempt>;

    /// Publish a prepared conditional create with exactly one storage request.
    // Reached only by tests, which is the only way the lost-response path can
    // be exercised at all: production publishes through `ConditionalCreateDispatch`.
    #[allow(dead_code)]
    async fn publish_conditional_create(
        &self,
        attempt: ConditionalCreateAttempt,
    ) -> CatalogOutcome<ConditionalCreateReceipt>;

    /// Read-only adjudication of a conditional create whose outcome is unknown.
    ///
    /// Exact-positive only: an absent target proves nothing, and this must never
    /// write or delete.
    async fn adjudicate_conditional_create(
        &self,
        evidence: ConditionalCreateEvidence,
    ) -> Result<ConditionalCreateVerdict, ConnectorError>;

    /// Decide whether a create with this intent can be admitted.
    ///
    /// This is not a capability table. It is the same decision
    /// [`NovaRocksCatalog::new_create_table_transaction`] makes, reachable by
    /// callers that must refuse before they build a table definition — a CTAS
    /// has to be turned away before its source runs, and building the
    /// definition first would already be work done on a request that cannot
    /// succeed. Implementations answer it from the same inputs, and the
    /// constructor calls it, so the two cannot drift apart.
    fn admit_create(&self, intent: CatalogCreateIntent) -> Result<(), CatalogUnsupported>;

    /// Begin a transaction that creates a table.
    ///
    /// The request carries [`CatalogCreateIntent`], and implementations may
    /// accept one intent while refusing another on the same catalog.
    async fn new_create_table_transaction(
        &self,
        request: transaction::CreateTableTransactionRequest,
    ) -> CatalogTransactionStart;

    /// Begin a transaction that creates a table or replaces it in place.
    // No production caller yet: this is the update-commit half of the
    // lifecycle, which is wired when `commit/**` migrates off `vendored_client`.
    #[allow(dead_code)]
    async fn new_create_or_replace_table_transaction(
        &self,
        request: transaction::CreateTableTransactionRequest,
    ) -> CatalogTransactionStart;
}

pub(crate) mod transaction;

/// Begin a publication against an existing table.
///
/// Every catalog shares this: the frontier is one `update_table`, and the
/// updates arrive after the writer runs. Admission here is the identity and
/// name check, which is exactly the work that must happen before a source
/// executes.
// No production caller yet: see the note on `new_transaction`.
#[allow(dead_code)]
fn start_update_table_transaction(
    delegate: &delegate::CatalogDelegate,
    request: transaction::TransactionRequest,
) -> CatalogTransactionStart {
    let ident = match delegate::table_ident(&request.target) {
        Ok(ident) => ident,
        Err(error) => {
            return CatalogTransactionStart::KnownUncommitted {
                failure: novarocks_spi::connector::ConnectorMutationFailure::new(
                    novarocks_spi::connector::ConnectorMutationFailureKind::InvalidRequest,
                    error.to_string(),
                ),
            };
        }
    };
    let mut evidence = error::CatalogCommitEvidence::for_target(request.target.canonical())
        .with_target_ref(Arc::clone(&request.target_ref))
        .with_base_snapshot_id(request.base_snapshot_id);
    if let Some(uuid) = &request.expected_table_uuid {
        evidence = evidence.with_target_uuid(Arc::clone(uuid));
    }
    let dispatch = Arc::new(dispatch::UpdateTableDispatch::new(
        Arc::clone(delegate.client()),
        ident,
        Arc::clone(&request.target_ref),
        request.marker.clone(),
    ));
    CatalogTransactionStart::Ready(Box::new(transaction::Transaction::new(
        request.identity,
        request.target,
        transaction::TransactionShape::Existing,
        evidence,
        dispatch,
    )))
}

/// Begin a publication that creates a table through one catalog request.
fn start_create_table_transaction(
    delegate: &delegate::CatalogDelegate,
    request: transaction::CreateTableTransactionRequest,
) -> CatalogTransactionStart {
    let namespace = CatalogNamespaceName::new(Arc::clone(&request.target.namespace));
    let namespace_ident = match delegate::namespace_ident(&namespace) {
        Ok(ident) => ident,
        Err(error) => {
            return CatalogTransactionStart::KnownUncommitted {
                failure: novarocks_spi::connector::ConnectorMutationFailure::new(
                    novarocks_spi::connector::ConnectorMutationFailureKind::InvalidRequest,
                    error.to_string(),
                ),
            };
        }
    };
    let table_ident = match delegate::table_ident(&request.target) {
        Ok(ident) => ident,
        Err(error) => {
            return CatalogTransactionStart::KnownUncommitted {
                failure: novarocks_spi::connector::ConnectorMutationFailure::new(
                    novarocks_spi::connector::ConnectorMutationFailureKind::InvalidRequest,
                    error.to_string(),
                ),
            };
        }
    };
    let evidence = error::CatalogCommitEvidence::for_target(request.target.canonical());
    let dispatch = Arc::new(dispatch::CreateTableDispatch::new(
        Arc::clone(delegate.client()),
        namespace_ident,
        request.creation,
        table_ident,
    ));
    CatalogTransactionStart::Ready(Box::new(transaction::Transaction::new(
        request.identity,
        request.target,
        transaction::TransactionShape::Create(request.intent),
        evidence,
        dispatch,
    )))
}
