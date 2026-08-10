use std::sync::Arc;

use novarocks_types::PrimitiveType;

use crate::common::types::UniqueId;
use crate::common::util::FieldRenderSchema;
use crate::exec::chunk::Chunk;
use novarocks_types::SlotId;

use super::FragmentIoError;

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

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ResultAbort {
    PrepareRollback,
    NeverStarted,
    Failed(String),
    Cancelled(String),
}

pub trait FragmentResultWriter: Send + Sync + 'static {
    fn open(
        &self,
        spec: ResultWriteSpec,
    ) -> Result<Arc<dyn FragmentResultSession>, FragmentIoError>;
}

pub trait FragmentResultSession: Send + Sync + 'static {
    fn write(&self, chunk: Chunk) -> Result<(), FragmentIoError>;
    fn finish(&self) -> Result<(), FragmentIoError>;
    fn abort(&self, reason: ResultAbort);
}

#[cfg(test)]
pub(crate) fn discard_result_writer() -> Arc<dyn FragmentResultWriter> {
    Arc::new(DiscardResultWriter)
}

#[cfg(test)]
pub(crate) fn in_process_test_result_writer() -> Arc<dyn FragmentResultWriter> {
    Arc::new(InProcessTestResultWriter)
}

#[cfg(test)]
pub(crate) fn discard_result_session() -> Arc<dyn FragmentResultSession> {
    Arc::new(DiscardResultSession)
}

#[cfg(test)]
struct DiscardResultWriter;

#[cfg(test)]
impl FragmentResultWriter for DiscardResultWriter {
    fn open(
        &self,
        _spec: ResultWriteSpec,
    ) -> Result<Arc<dyn FragmentResultSession>, FragmentIoError> {
        Ok(Arc::new(DiscardResultSession))
    }
}

#[cfg(test)]
struct DiscardResultSession;

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

#[cfg(test)]
struct InProcessTestResultWriter;

#[cfg(test)]
impl FragmentResultWriter for InProcessTestResultWriter {
    fn open(
        &self,
        spec: ResultWriteSpec,
    ) -> Result<Arc<dyn FragmentResultSession>, FragmentIoError> {
        if spec.presentation() == ResultPresentation::Statistic {
            return Err(FragmentIoError::new(
                super::FragmentIoOperation::ResultOpen,
                super::FragmentIoErrorKind::RemoteRejected,
                "in-process native tests do not support STATISTIC result presentation",
            ));
        }
        let handle = crate::runtime::result_buffer::ResultBufferWriteHandle::open(
            spec.fragment_instance_id(),
            spec.is_typed(),
            None,
        )
        .map_err(|error| {
            FragmentIoError::new(
                super::FragmentIoOperation::ResultOpen,
                super::FragmentIoErrorKind::Internal,
                error,
            )
        })?;
        Ok(Arc::new(InProcessTestResultSession { spec, handle }))
    }
}

#[cfg(test)]
struct InProcessTestResultSession {
    spec: ResultWriteSpec,
    handle: crate::runtime::result_buffer::ResultBufferWriteHandle,
}

#[cfg(test)]
impl FragmentResultSession for InProcessTestResultSession {
    fn write(&self, chunk: Chunk) -> Result<(), FragmentIoError> {
        if chunk.is_empty() {
            return Ok(());
        }
        if !self.spec.is_typed() {
            return Err(FragmentIoError::new(
                super::FragmentIoOperation::ResultWrite,
                super::FragmentIoErrorKind::InvalidResponse,
                "in-process native tests require typed result output",
            ));
        }
        let payload = crate::runtime::exchange::encode_chunks(&[chunk], true).map_err(|error| {
            FragmentIoError::new(
                super::FragmentIoOperation::ResultWrite,
                super::FragmentIoErrorKind::Internal,
                error,
            )
        })?;
        self.handle
            .write_typed(payload)
            .map(|_| ())
            .map_err(|error| {
                FragmentIoError::new(
                    super::FragmentIoOperation::ResultWrite,
                    super::FragmentIoErrorKind::Internal,
                    error,
                )
            })
    }

    fn finish(&self) -> Result<(), FragmentIoError> {
        self.handle.finish().map(|_| ()).map_err(|error| {
            FragmentIoError::new(
                super::FragmentIoOperation::ResultFinish,
                super::FragmentIoErrorKind::Internal,
                error,
            )
        })
    }

    fn abort(&self, reason: ResultAbort) {
        self.handle.abort(reason);
    }
}
