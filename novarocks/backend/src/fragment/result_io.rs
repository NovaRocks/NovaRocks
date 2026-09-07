use std::sync::Arc;

use crate::runtime::result_batch::FetchResult;
use crate::runtime::result_buffer::{ResultBufferWriteHandle, ResultPublication};
use crate::runtime::result_format::build_result_batch;
use novarocks_execution::runtime::exchange;
use novarocks_execution::runtime::fragment::io::{
    FragmentIoError, FragmentIoErrorKind, FragmentIoOperation, FragmentResultSession,
    FragmentResultWriter, ResultAbort, ResultPresentation, ResultWriteSpec,
};

pub(crate) fn native_result_writer() -> Arc<dyn FragmentResultWriter> {
    Arc::new(NativeFragmentResultWriter)
}

struct NativeFragmentResultWriter;

impl FragmentResultWriter for NativeFragmentResultWriter {
    fn open(
        &self,
        spec: ResultWriteSpec,
    ) -> Result<Arc<dyn FragmentResultSession>, FragmentIoError> {
        if spec.presentation() == ResultPresentation::Statistic {
            return Err(FragmentIoError::new(
                FragmentIoOperation::ResultOpen,
                FragmentIoErrorKind::RemoteRejected,
                "native backend does not support STATISTIC result presentation",
            ));
        }
        let handle =
            ResultBufferWriteHandle::open(spec.fragment_instance_id(), spec.is_typed(), None)
                .map_err(|error| {
                    FragmentIoError::new(
                        FragmentIoOperation::ResultOpen,
                        FragmentIoErrorKind::Internal,
                        error,
                    )
                })?;
        Ok(Arc::new(NativeFragmentResultSession { spec, handle }))
    }
}

struct NativeFragmentResultSession {
    spec: ResultWriteSpec,
    handle: ResultBufferWriteHandle,
}

impl FragmentResultSession for NativeFragmentResultSession {
    fn write(&self, chunk: novarocks_execution::exec::chunk::Chunk) -> Result<(), FragmentIoError> {
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
            return self
                .handle
                .write_typed(payload)
                .map(|_| ())
                .map_err(|error| {
                    FragmentIoError::new(
                        FragmentIoOperation::ResultWrite,
                        FragmentIoErrorKind::Internal,
                        error,
                    )
                });
        }

        let batch = build_result_batch(&chunk, self.spec.projections(), self.spec.presentation())
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
        self.handle
            .finish()
            .map(|publication| {
                if publishes_terminal(publication) {
                    crate::metrics::record_fragment_result_terminal("finished");
                }
            })
            .map_err(|error| {
                FragmentIoError::new(
                    FragmentIoOperation::ResultFinish,
                    FragmentIoErrorKind::Internal,
                    error,
                )
            })
    }

    fn abort(&self, reason: ResultAbort) {
        if publishes_terminal(self.handle.abort(reason)) {
            crate::metrics::record_fragment_result_terminal("aborted");
        }
    }
}

fn publishes_terminal(publication: ResultPublication) -> bool {
    publication == ResultPublication::TerminalReady
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_terminal_ready_is_a_published_result_terminal() {
        assert!(publishes_terminal(ResultPublication::TerminalReady));
        assert!(!publishes_terminal(ResultPublication::DataReady));
        assert!(!publishes_terminal(ResultPublication::Removed));
        assert!(!publishes_terminal(ResultPublication::NoChange));
    }
}
