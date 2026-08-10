use arrow::array::ArrayRef;

use crate::runtime::endpoint::RuntimeEndpoint;
use crate::runtime::query_context::QueryId;
use novarocks_types::SlotId;

use super::FragmentIoError;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LookupKind {
    PrimaryKey,
    Lake,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LookupTarget {
    backend_id: i32,
    endpoint: Option<RuntimeEndpoint>,
}

impl LookupTarget {
    pub fn new(backend_id: i32, endpoint: Option<RuntimeEndpoint>) -> Self {
        Self {
            backend_id,
            endpoint,
        }
    }

    pub const fn backend_id(&self) -> i32 {
        self.backend_id
    }

    pub fn endpoint(&self) -> Option<&RuntimeEndpoint> {
        self.endpoint.as_ref()
    }
}

#[derive(Clone)]
pub struct LookupColumn {
    slot_id: SlotId,
    values: ArrayRef,
}

impl LookupColumn {
    pub fn new(slot_id: SlotId, values: ArrayRef) -> Self {
        Self { slot_id, values }
    }

    pub const fn slot_id(&self) -> SlotId {
        self.slot_id
    }

    pub fn values(&self) -> &ArrayRef {
        &self.values
    }
}

#[derive(Clone)]
pub struct LookupRequest {
    query_id: QueryId,
    lookup_node_id: i32,
    tuple_id: i32,
    kind: LookupKind,
    target: LookupTarget,
    columns: Vec<LookupColumn>,
}

impl LookupRequest {
    pub fn new(
        query_id: QueryId,
        lookup_node_id: i32,
        tuple_id: i32,
        kind: LookupKind,
        target: LookupTarget,
        columns: Vec<LookupColumn>,
    ) -> Self {
        Self {
            query_id,
            lookup_node_id,
            tuple_id,
            kind,
            target,
            columns,
        }
    }

    pub const fn query_id(&self) -> QueryId {
        self.query_id
    }

    pub const fn lookup_node_id(&self) -> i32 {
        self.lookup_node_id
    }

    pub const fn tuple_id(&self) -> i32 {
        self.tuple_id
    }

    pub const fn kind(&self) -> LookupKind {
        self.kind
    }

    pub fn target(&self) -> &LookupTarget {
        &self.target
    }

    pub fn columns(&self) -> &[LookupColumn] {
        &self.columns
    }
}

#[derive(Clone)]
pub struct LookupBatch {
    columns: Vec<LookupColumn>,
}

impl LookupBatch {
    pub fn new(columns: Vec<LookupColumn>) -> Self {
        Self { columns }
    }

    pub fn columns(&self) -> &[LookupColumn] {
        &self.columns
    }
}

pub trait FragmentLookupClient: Send + Sync + 'static {
    fn lookup(&self, request: LookupRequest) -> Result<LookupBatch, FragmentIoError>;
}

/// Explicit failure used by pipeline-only callers that cannot execute a
/// protocol lookup. Real fragment admission must inject a role adapter.
#[derive(Debug, Default)]
pub struct UnavailableFragmentLookupClient;

impl FragmentLookupClient for UnavailableFragmentLookupClient {
    fn lookup(&self, _request: LookupRequest) -> Result<LookupBatch, FragmentIoError> {
        Err(FragmentIoError::new(
            super::FragmentIoOperation::Lookup,
            super::FragmentIoErrorKind::Unavailable,
            "fragment lookup client is not configured",
        ))
    }
}
