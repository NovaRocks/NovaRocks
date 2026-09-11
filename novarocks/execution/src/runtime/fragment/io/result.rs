use std::sync::Arc;

use crate::exec::chunk::Chunk;
use crate::runtime::observable::Observable;
use novarocks_execution_contract::TaskIdentity;
use novarocks_types::{FieldRenderSchema, PrimitiveType, SlotId, UniqueId};

use super::FragmentIoError;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ResultAbort {
    PrepareRollback,
    NeverStarted,
    Failed(String),
    Cancelled(String),
}

/// Byte credit reserved by the result owner before the pipeline transfers a
/// chunk into the result stream.
///
/// The result owner decides when the reservation is released. A queue keeps
/// the credit beside the retained payload and drops it only after consumer
/// progress, acknowledgement, cancellation, or terminal cleanup releases
/// those bytes.
pub struct ResultWriteCredit {
    bytes: usize,
    release: Option<Box<dyn Fn(usize) + Send + Sync + 'static>>,
}

impl ResultWriteCredit {
    pub fn new(bytes: usize, release: impl Fn(usize) + Send + Sync + 'static) -> Self {
        Self {
            bytes,
            release: Some(Box::new(release)),
        }
    }

    pub const fn bytes(&self) -> usize {
        self.bytes
    }

    /// Releases an unused suffix of this reservation before the retained
    /// payload takes ownership of the remaining bytes.
    pub fn shrink_to(&mut self, retained_bytes: usize) -> Result<(), String> {
        let released = self.bytes.checked_sub(retained_bytes).ok_or_else(|| {
            format!(
                "result credit cannot grow from {} to {retained_bytes} bytes",
                self.bytes
            )
        })?;
        if released > 0 {
            // Commit the smaller ownership before invoking host code so an
            // unwinding release callback cannot make Drop return the original
            // reservation a second time.
            self.bytes = retained_bytes;
            if let Some(release) = self.release.as_ref() {
                release(released);
            }
        }
        Ok(())
    }
}

impl std::fmt::Debug for ResultWriteCredit {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ResultWriteCredit")
            .field("bytes", &self.bytes)
            .finish_non_exhaustive()
    }
}

impl Drop for ResultWriteCredit {
    fn drop(&mut self) {
        if let Some(release) = self.release.take() {
            release(self.bytes);
        }
    }
}

#[derive(Debug)]
pub enum ResultWriteAdmission {
    Granted(ResultWriteCredit),
    Blocked,
}

/// An opened host result stream. Encoding and presentation remain with the
/// host; the execution kernel only writes Arrow chunks and terminal state.
pub trait FragmentResultSession: Send + Sync + 'static {
    /// Returns the upper-bound reservation required before the host encodes
    /// this chunk.
    ///
    /// The driver calls this before `try_acquire`, so a host must use the same
    /// configured packet bound, encode once in `write_with_credit`, reject an
    /// oversized encoding, and release the unused suffix before retaining it.
    fn reservation_bytes(&self, chunk: &Chunk) -> Result<usize, FragmentIoError>;

    /// Attempts to reserve result-owned bytes without blocking the calling
    /// driver.
    fn try_acquire(&self, bytes: usize) -> Result<ResultWriteAdmission, FragmentIoError>;

    /// Stable readiness observable for a blocked credit acquisition.
    fn writable_observable(&self) -> Option<Arc<Observable>>;

    /// Transfers both the chunk and its exact byte reservation to the result
    /// owner.
    fn write_with_credit(
        &self,
        chunk: Chunk,
        credit: ResultWriteCredit,
    ) -> Result<(), FragmentIoError>;

    fn finish(&self) -> Result<(), FragmentIoError>;
    fn abort(&self, reason: ResultAbort);
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ResultPresentation {
    MysqlText,
    HttpJson,
    Statistic,
}

#[derive(Clone, Debug)]
pub struct ResultProjection {
    slot_id: SlotId,
    primitive: PrimitiveType,
    field_schema: FieldRenderSchema,
}

impl ResultProjection {
    pub fn new(slot_id: SlotId, primitive: PrimitiveType, field_schema: FieldRenderSchema) -> Self {
        Self {
            slot_id,
            primitive,
            field_schema,
        }
    }

    pub const fn slot_id(&self) -> SlotId {
        self.slot_id
    }

    pub const fn primitive(&self) -> PrimitiveType {
        self.primitive
    }

    pub fn field_schema(&self) -> &FieldRenderSchema {
        &self.field_schema
    }
}

#[derive(Clone, Debug)]
pub struct ResultWriteSpec {
    task_identity: Option<TaskIdentity>,
    fragment_instance_id: UniqueId,
    presentation: ResultPresentation,
    projections: Option<Vec<ResultProjection>>,
    typed: bool,
}

impl ResultWriteSpec {
    pub fn new(
        fragment_instance_id: UniqueId,
        presentation: ResultPresentation,
        projections: Option<Vec<ResultProjection>>,
        typed: bool,
    ) -> Self {
        Self {
            task_identity: None,
            fragment_instance_id,
            presentation,
            projections,
            typed,
        }
    }

    /// Binds a native result stream to its complete Task identity.
    ///
    /// Standalone execution tests may omit this, but a native Worker result
    /// writer must reject an unbound spec rather than fall back to the kernel
    /// fragment id as an admission identity.
    pub const fn with_task_identity(mut self, identity: TaskIdentity) -> Self {
        self.task_identity = Some(identity);
        self
    }

    pub const fn task_identity(&self) -> Option<TaskIdentity> {
        self.task_identity
    }

    pub const fn fragment_instance_id(&self) -> UniqueId {
        self.fragment_instance_id
    }

    pub const fn presentation(&self) -> ResultPresentation {
        self.presentation
    }

    pub fn projections(&self) -> Option<&[ResultProjection]> {
        self.projections.as_deref()
    }

    pub const fn is_typed(&self) -> bool {
        self.typed
    }
}

/// Host-owned result-session factory. Presentation encoding remains outside
/// execution; the kernel only owns the lifecycle and Arrow chunk writes.
pub trait FragmentResultWriter: Send + Sync + 'static {
    fn open(
        &self,
        spec: ResultWriteSpec,
    ) -> Result<Arc<dyn FragmentResultSession>, FragmentIoError>;
}

#[cfg(test)]
pub(crate) fn discard_result_session() -> Arc<dyn FragmentResultSession> {
    Arc::new(DiscardResultSession)
}

#[cfg(test)]
pub(crate) fn discard_result_writer() -> Arc<dyn FragmentResultWriter> {
    Arc::new(DiscardResultWriter)
}

#[cfg(test)]
struct DiscardResultSession;

#[cfg(test)]
struct DiscardResultWriter;

#[cfg(test)]
impl FragmentResultWriter for DiscardResultWriter {
    fn open(
        &self,
        _spec: ResultWriteSpec,
    ) -> Result<Arc<dyn FragmentResultSession>, FragmentIoError> {
        Ok(discard_result_session())
    }
}

#[cfg(test)]
impl FragmentResultSession for DiscardResultSession {
    fn reservation_bytes(&self, chunk: &Chunk) -> Result<usize, FragmentIoError> {
        Ok(chunk.logical_bytes())
    }

    fn try_acquire(&self, bytes: usize) -> Result<ResultWriteAdmission, FragmentIoError> {
        Ok(ResultWriteAdmission::Granted(ResultWriteCredit::new(
            bytes,
            |_| {},
        )))
    }

    fn writable_observable(&self) -> Option<Arc<Observable>> {
        None
    }

    fn write_with_credit(
        &self,
        chunk: Chunk,
        credit: ResultWriteCredit,
    ) -> Result<(), FragmentIoError> {
        debug_assert_eq!(credit.bytes(), chunk.logical_bytes());
        Ok(())
    }

    fn finish(&self) -> Result<(), FragmentIoError> {
        Ok(())
    }

    fn abort(&self, _reason: ResultAbort) {}
}
