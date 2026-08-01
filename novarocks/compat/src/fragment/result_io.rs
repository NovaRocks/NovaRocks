use std::sync::Arc;

use novarocks::common::types::FetchResult;
use novarocks::runtime::exchange;
use novarocks::runtime::fragment::{
    FragmentIoError, FragmentIoErrorKind, FragmentIoOperation, FragmentResultSession,
    FragmentResultWriter, ResultAbort, ResultPresentation, ResultWriteSpec, build_result_batch,
    build_statistic_result_batch,
};
use novarocks::runtime::result_buffer::{ResultBufferWriteHandle, ResultPublication};

use crate::ffi_support;

fn notify_fetch_ready(spec: &ResultWriteSpec, publication: ResultPublication) {
    if !matches!(
        publication,
        ResultPublication::DataReady | ResultPublication::TerminalReady
    ) {
        return;
    }

    ffi_support::notify_fetch_ready(spec.fragment_instance_id());
}

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
            let publication = self.handle.write_typed(payload).map_err(|error| {
                FragmentIoError::new(
                    FragmentIoOperation::ResultWrite,
                    FragmentIoErrorKind::Internal,
                    error,
                )
            })?;
            notify_fetch_ready(&self.spec, publication);
            return Ok(());
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
        let publication = self
            .handle
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
            })?;
        notify_fetch_ready(&self.spec, publication);
        Ok(())
    }

    fn finish(&self) -> Result<(), FragmentIoError> {
        let publication = self.handle.finish().map_err(|error| {
            FragmentIoError::new(
                FragmentIoOperation::ResultFinish,
                FragmentIoErrorKind::Internal,
                error,
            )
        })?;
        notify_fetch_ready(&self.spec, publication);
        Ok(())
    }

    fn abort(&self, reason: ResultAbort) {
        notify_fetch_ready(&self.spec, self.handle.abort(reason));
    }
}
