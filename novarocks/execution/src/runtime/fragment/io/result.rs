use std::sync::Arc;

use crate::exec::chunk::Chunk;
use novarocks_types::{FieldRenderSchema, PrimitiveType, SlotId, UniqueId};

use super::FragmentIoError;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ResultAbort {
    PrepareRollback,
    NeverStarted,
    Failed(String),
    Cancelled(String),
}

/// An opened host result stream. Encoding and presentation remain with the
/// host; the execution kernel only writes Arrow chunks and terminal state.
pub trait FragmentResultSession: Send + Sync + 'static {
    fn write(&self, chunk: Chunk) -> Result<(), FragmentIoError>;
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
            fragment_instance_id,
            presentation,
            projections,
            typed,
        }
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
    fn write(&self, _chunk: Chunk) -> Result<(), FragmentIoError> {
        Ok(())
    }

    fn finish(&self) -> Result<(), FragmentIoError> {
        Ok(())
    }

    fn abort(&self, _reason: ResultAbort) {}
}
