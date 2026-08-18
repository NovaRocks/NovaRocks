//! Frontend-local lifecycle transport ports.
use crate::query_execution::lifecycle_plan::QueryLifecycleTarget;
use novarocks_protocol::lifecycle::{
    QueryAbortRequest, QueryControlAttach, QueryControlCommand, QueryControlEvent, QueryInitAck,
    QueryInitRequest, QueryStageAck, QueryStageRequest, QueryStartAck, QueryStartRequest,
    QueryTerminationAck,
};
use std::sync::Arc;
use std::time::Duration;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum QueryLifecycleTransportErrorKind {
    DeadlineExceeded,
    StreamClosed,
    Backpressure,
    InvalidResponse,
    Unavailable,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct QueryLifecycleTransportError {
    kind: QueryLifecycleTransportErrorKind,
    detail: String,
}
impl QueryLifecycleTransportError {
    pub(crate) fn new(kind: QueryLifecycleTransportErrorKind, detail: impl Into<String>) -> Self {
        Self {
            kind,
            detail: detail.into(),
        }
    }
    pub(crate) const fn kind(&self) -> QueryLifecycleTransportErrorKind {
        self.kind
    }
    pub(crate) fn detail(&self) -> &str {
        &self.detail
    }
    pub(crate) const fn is_unknown_init_outcome(&self) -> bool {
        matches!(
            self.kind,
            QueryLifecycleTransportErrorKind::DeadlineExceeded
                | QueryLifecycleTransportErrorKind::StreamClosed
        )
    }
    pub(crate) const fn is_unknown_stage_or_start_outcome(&self) -> bool {
        self.is_unknown_init_outcome()
    }
}
impl std::fmt::Display for QueryLifecycleTransportError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}: {}", self.kind, self.detail)
    }
}
impl std::error::Error for QueryLifecycleTransportError {}
pub(crate) trait QueryControlSession: Send + Sync + 'static {
    fn send(&self, command: QueryControlCommand) -> Result<(), QueryLifecycleTransportError>;
    fn recv_timeout(
        &self,
        timeout: Duration,
    ) -> Result<QueryControlEvent, QueryLifecycleTransportError>;
}
pub(crate) trait QueryLifecycleTransport: Send + Sync + 'static {
    fn init_query(
        &self,
        target: QueryLifecycleTarget,
        request: QueryInitRequest,
        timeout: Duration,
    ) -> Result<QueryInitAck, QueryLifecycleTransportError>;
    fn attach_control(
        &self,
        target: QueryLifecycleTarget,
        attach: QueryControlAttach,
        timeout: Duration,
    ) -> Result<Arc<dyn QueryControlSession>, QueryLifecycleTransportError>;
    fn stage_fragments(
        &self,
        target: QueryLifecycleTarget,
        request: &QueryStageRequest,
        timeout: Duration,
    ) -> Result<QueryStageAck, QueryLifecycleTransportError> {
        let _ = (target, request, timeout);
        Err(QueryLifecycleTransportError::new(
            QueryLifecycleTransportErrorKind::Unavailable,
            "StageFragments is not supported by this lifecycle transport",
        ))
    }
    fn start_prepared_query(
        &self,
        target: QueryLifecycleTarget,
        request: &QueryStartRequest,
        timeout: Duration,
    ) -> Result<QueryStartAck, QueryLifecycleTransportError> {
        let _ = (target, request, timeout);
        Err(QueryLifecycleTransportError::new(
            QueryLifecycleTransportErrorKind::Unavailable,
            "StartPreparedQuery is not supported by this lifecycle transport",
        ))
    }
    fn abort_query(
        &self,
        target: QueryLifecycleTarget,
        request: QueryAbortRequest,
        timeout: Duration,
    ) -> Result<QueryTerminationAck, QueryLifecycleTransportError>;
}
