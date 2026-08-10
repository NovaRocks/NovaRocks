use std::sync::Arc;

use novarocks_execution::exec::chunk::Chunk;
use novarocks_execution::runtime::fragment::io::{
    FragmentIoError, FragmentIoErrorKind, FragmentIoOperation, FragmentResultSession,
    FragmentResultWriter, ResultAbort, ResultPresentation, ResultWriteSpec,
};

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
                FragmentIoOperation::ResultOpen,
                FragmentIoErrorKind::RemoteRejected,
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
                FragmentIoOperation::ResultOpen,
                FragmentIoErrorKind::Internal,
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
                FragmentIoOperation::ResultWrite,
                FragmentIoErrorKind::InvalidResponse,
                "in-process native tests require typed result output",
            ));
        }
        let payload = novarocks_execution::runtime::exchange::encode_chunks(&[chunk], true)
            .map_err(|error| {
                FragmentIoError::new(
                    FragmentIoOperation::ResultWrite,
                    FragmentIoErrorKind::Internal,
                    error,
                )
            })?;
        self.handle
            .write_typed(payload)
            .map(|_| ())
            .map_err(|error| {
                FragmentIoError::new(
                    FragmentIoOperation::ResultWrite,
                    FragmentIoErrorKind::Internal,
                    error,
                )
            })
    }

    fn finish(&self) -> Result<(), FragmentIoError> {
        self.handle.finish().map(|_| ()).map_err(|error| {
            FragmentIoError::new(
                FragmentIoOperation::ResultFinish,
                FragmentIoErrorKind::Internal,
                error,
            )
        })
    }

    fn abort(&self, reason: ResultAbort) {
        self.handle.abort(reason);
    }
}
