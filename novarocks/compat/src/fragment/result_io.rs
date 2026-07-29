use std::sync::Arc;

use novarocks::common::types::FetchResult;
use novarocks::runtime::exchange;
use novarocks::runtime::fragment::io::result_format::{
    build_result_batch, build_statistic_result_batch,
};
use novarocks::runtime::fragment::io::{
    FragmentIoError, FragmentIoErrorKind, FragmentIoOperation, FragmentResultSession,
    FragmentResultWriter, ResultAbort, ResultPresentation, ResultWriteSpec,
};
use novarocks::runtime::result_buffer::ResultBufferWriteHandle;

pub(crate) fn compat_result_writer() -> Arc<dyn FragmentResultWriter> {
    Arc::new(CompatFragmentResultWriter)
}

struct CompatFragmentResultWriter;

impl FragmentResultWriter for CompatFragmentResultWriter {
    fn open(
        &self,
        spec: ResultWriteSpec,
    ) -> Result<Arc<dyn FragmentResultSession>, FragmentIoError> {
        let handle =
            ResultBufferWriteHandle::open(spec.fragment_instance_id(), spec.is_typed(), None)
                .map_err(|error| {
                    FragmentIoError::new(
                        FragmentIoOperation::ResultOpen,
                        FragmentIoErrorKind::Internal,
                        error,
                    )
                })?;
        Ok(Arc::new(CompatFragmentResultSession { spec, handle }))
    }
}

struct CompatFragmentResultSession {
    spec: ResultWriteSpec,
    handle: ResultBufferWriteHandle,
}

impl FragmentResultSession for CompatFragmentResultSession {
    fn write(&self, chunk: novarocks::exec::chunk::Chunk) -> Result<(), FragmentIoError> {
        if chunk.is_empty() {
            return Ok(());
        }
        if self.spec.is_typed() {
            if self.spec.presentation() != ResultPresentation::MysqlText {
                return Err(FragmentIoError::new(
                    FragmentIoOperation::ResultWrite,
                    FragmentIoErrorKind::InvalidResponse,
                    "typed result session only supports MYSQL text presentation",
                ));
            }
            let payload = exchange::encode_chunks(&[chunk], true).map_err(|error| {
                FragmentIoError::new(
                    FragmentIoOperation::ResultWrite,
                    FragmentIoErrorKind::Internal,
                    error,
                )
            })?;
            return self.handle.write_typed(payload).map_err(|error| {
                FragmentIoError::new(
                    FragmentIoOperation::ResultWrite,
                    FragmentIoErrorKind::Internal,
                    error,
                )
            });
        }
        let batch = match self.spec.presentation() {
            ResultPresentation::Statistic => {
                let projections = self.spec.projections().ok_or_else(|| {
                    FragmentIoError::new(
                        FragmentIoOperation::ResultWrite,
                        FragmentIoErrorKind::InvalidResponse,
                        "STATISTIC result session requires output projections",
                    )
                })?;
                build_statistic_result_batch(
                    &chunk,
                    projections,
                    super::statistic_result::thrift_statistic_row_encoder,
                )
            }
            presentation => build_result_batch(&chunk, self.spec.projections(), presentation),
        }
        .map_err(|error| {
            FragmentIoError::new(
                FragmentIoOperation::ResultWrite,
                FragmentIoErrorKind::Internal,
                error,
            )
        })?;
        self.handle
            .write_legacy(FetchResult {
                packet_seq: 0,
                eos: false,
                result_batch: batch,
            })
            .map_err(|error| {
                FragmentIoError::new(
                    FragmentIoOperation::ResultWrite,
                    FragmentIoErrorKind::Internal,
                    error,
                )
            })
    }

    fn finish(&self) -> Result<(), FragmentIoError> {
        self.handle.finish().map_err(|error| {
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
