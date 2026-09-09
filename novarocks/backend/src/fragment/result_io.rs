use std::num::NonZeroUsize;
use std::sync::Arc;

use crate::runtime::result_batch::FetchResult;
use crate::runtime::result_buffer::{
    ResultBufferKey, ResultBufferWriteHandle, ResultPublication, ResultRetainedBudget,
};
use crate::runtime::result_format::build_result_batch;
use novarocks_execution::runtime::exchange;
use novarocks_execution::runtime::fragment::io::{
    FragmentIoError, FragmentIoErrorKind, FragmentIoOperation, FragmentResultSession,
    FragmentResultWriter, ResultAbort, ResultPresentation, ResultWriteAdmission, ResultWriteCredit,
    ResultWriteSpec,
};
use novarocks_execution::runtime::observable::Observable;

pub(crate) fn native_result_writer(
    retained_budget: Arc<ResultRetainedBudget>,
    root_retained_byte_cap: NonZeroUsize,
) -> Arc<dyn FragmentResultWriter> {
    Arc::new(NativeFragmentResultWriter {
        retained_budget,
        root_retained_byte_cap,
    })
}

#[cfg(test)]
pub(crate) fn test_native_result_writer() -> Arc<dyn FragmentResultWriter> {
    native_result_writer(
        ResultRetainedBudget::new(
            NonZeroUsize::new(32 * 1024 * 1024).expect("test process result cap is nonzero"),
        ),
        NonZeroUsize::new(16 * 1024 * 1024).expect("test root result cap is nonzero"),
    )
}

struct NativeFragmentResultWriter {
    retained_budget: Arc<ResultRetainedBudget>,
    root_retained_byte_cap: NonZeroUsize,
}

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
        if !spec.is_typed() {
            return Err(FragmentIoError::new(
                FragmentIoOperation::ResultOpen,
                FragmentIoErrorKind::RemoteRejected,
                "native task result stream requires typed Arrow IPC",
            ));
        }
        let identity = spec.task_identity().ok_or_else(|| {
            FragmentIoError::new(
                FragmentIoOperation::ResultOpen,
                FragmentIoErrorKind::InvalidResponse,
                "native result stream requires a complete task identity",
            )
        })?;
        let handle = ResultBufferWriteHandle::open(
            ResultBufferKey::Task(identity),
            spec.is_typed(),
            self.root_retained_byte_cap,
            Arc::clone(&self.retained_budget),
            None,
        )
        .map_err(|error| {
            FragmentIoError::new(
                FragmentIoOperation::ResultOpen,
                FragmentIoErrorKind::Internal,
                error,
            )
        })?;
        Ok(Arc::new(NativeFragmentResultSession {
            spec,
            handle,
            root_retained_byte_cap: self.root_retained_byte_cap,
        }))
    }
}

struct NativeFragmentResultSession {
    spec: ResultWriteSpec,
    handle: ResultBufferWriteHandle,
    root_retained_byte_cap: NonZeroUsize,
}

impl FragmentResultSession for NativeFragmentResultSession {
    fn reservation_bytes(
        &self,
        chunk: &novarocks_execution::exec::chunk::Chunk,
    ) -> Result<usize, FragmentIoError> {
        if chunk.is_empty() {
            return Ok(0);
        }
        if self.spec.is_typed() {
            if self.spec.presentation() != ResultPresentation::MysqlText {
                return Err(FragmentIoError::new(
                    FragmentIoOperation::ResultWrite,
                    FragmentIoErrorKind::InvalidResponse,
                    "typed result session only supports MYSQL text presentation",
                ));
            }
        }
        if chunk.logical_bytes() >= self.root_retained_byte_cap.get() {
            return Err(FragmentIoError::new(
                FragmentIoOperation::ResultWrite,
                FragmentIoErrorKind::Unavailable,
                format!(
                    "result chunk retains {} bytes and leaves no encoded capacity within joint cap {}",
                    chunk.logical_bytes(),
                    self.root_retained_byte_cap
                ),
            ));
        }
        Ok(self.root_retained_byte_cap.get())
    }

    fn try_acquire(&self, bytes: usize) -> Result<ResultWriteAdmission, FragmentIoError> {
        self.handle.try_acquire(bytes).map_err(|error| {
            FragmentIoError::new(
                FragmentIoOperation::ResultWrite,
                FragmentIoErrorKind::Unavailable,
                error,
            )
        })
    }

    fn writable_observable(&self) -> Option<Arc<Observable>> {
        Some(self.handle.writable_observable())
    }

    fn write_with_credit(
        &self,
        chunk: novarocks_execution::exec::chunk::Chunk,
        mut credit: ResultWriteCredit,
    ) -> Result<(), FragmentIoError> {
        if chunk.is_empty() {
            return Ok(());
        }
        if self.spec.is_typed() {
            let arrow_retained_bytes = chunk.logical_bytes();
            let encoded_byte_cap = credit
                .bytes()
                .checked_sub(arrow_retained_bytes)
                .ok_or_else(|| {
                    FragmentIoError::new(
                        FragmentIoOperation::ResultWrite,
                        FragmentIoErrorKind::InvalidResponse,
                        format!(
                            "result write owns {} joint bytes but its Arrow input retains {arrow_retained_bytes} bytes",
                            credit.bytes()
                        ),
                    )
                })?;
            let encoded =
                exchange::encode_chunks_bounded(&[chunk], true, encoded_byte_cap).map_err(|error| {
                    FragmentIoError::new(
                        FragmentIoOperation::ResultWrite,
                        FragmentIoErrorKind::Unavailable,
                        format!(
                            "typed result exceeds joint retained-byte cap: Arrow input retained {arrow_retained_bytes} bytes and encoded capacity is {encoded_byte_cap} bytes: {error}"
                        ),
                    )
                })?;
            return self
                .handle
                .write_typed_with_credit(encoded, credit)
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
        if batch.heap_size_bytes() > credit.bytes() {
            return Err(FragmentIoError::new(
                FragmentIoOperation::ResultWrite,
                FragmentIoErrorKind::Unavailable,
                format!(
                    "rendered result payload of {} bytes exceeds reserved packet cap {}",
                    batch.heap_size_bytes(),
                    credit.bytes()
                ),
            ));
        }
        credit.shrink_to(batch.heap_size_bytes()).map_err(|error| {
            FragmentIoError::new(
                FragmentIoOperation::ResultWrite,
                FragmentIoErrorKind::InvalidResponse,
                error,
            )
        })?;
        self.handle
            .write_legacy_with_credit(
                FetchResult {
                    packet_seq: 0,
                    eos: false,
                    result_batch: batch,
                },
                credit,
            )
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
    use arrow::array::{ArrayRef, Int32Array};
    use arrow::datatypes::{DataType, Field};
    use novarocks_execution::exec::chunk::{Chunk, ChunkSchema, ChunkSlotSchema};
    use novarocks_execution_contract::task_execution::identity::TaskIdentity;
    use novarocks_types::identity::{
        AttemptId, BackendProcessId, QueryExecutionId, QueryId, StageId, TaskId,
    };
    use novarocks_types::{SlotId, UniqueId};

    fn task_identity(seed: u32) -> TaskIdentity {
        TaskIdentity::new(
            QueryExecutionId::new(
                QueryId::new(i64::from(seed), i64::from(seed) + 1),
                AttemptId::new(1).expect("nonzero attempt"),
            )
            .expect("nonzero query identity"),
            StageId::new(seed).expect("nonzero stage"),
            TaskId::new(seed).expect("nonzero task"),
            BackendProcessId::new_v7(),
        )
    }

    fn typed_chunk() -> Chunk {
        let schema = Arc::new(
            ChunkSchema::try_new(vec![ChunkSlotSchema::new_with_field(
                SlotId::new(1),
                Field::new("value", DataType::Int32, false),
                None,
                None,
            )])
            .expect("test schema"),
        );
        Chunk::try_new_with_columns(
            schema,
            vec![Arc::new(Int32Array::from(vec![1])) as ArrayRef],
        )
        .expect("test chunk")
    }

    #[test]
    fn native_result_writer_rejects_a_stream_without_task_identity() {
        let result = test_native_result_writer().open(ResultWriteSpec::new(
            novarocks_types::UniqueId::new(1, 2),
            ResultPresentation::MysqlText,
            None,
            true,
        ));
        let Err(error) = result else {
            panic!("native result ownership requires the complete task identity");
        };
        assert_eq!(error.kind(), FragmentIoErrorKind::InvalidResponse);
    }

    #[test]
    fn native_result_writer_rejects_untyped_task_compatibility_path() {
        let identity = task_identity(811);
        let result = test_native_result_writer().open(
            ResultWriteSpec::new(
                UniqueId::new(811, 1),
                ResultPresentation::MysqlText,
                None,
                false,
            )
            .with_task_identity(identity),
        );
        let Err(error) = result else {
            panic!("native task results must use the bounded typed path");
        };
        assert_eq!(error.kind(), FragmentIoErrorKind::RemoteRejected);
    }

    #[test]
    fn typed_result_joint_cap_counts_arrow_and_encoded_backings() {
        let arrow_retained = typed_chunk().logical_bytes();
        let required_encoded_bytes =
            exchange::encode_chunks_bounded(&[typed_chunk()], true, 1024 * 1024)
                .expect("measure bounded result payload")
                .len();
        let joint_cap = arrow_retained
            .checked_add(required_encoded_bytes)
            .expect("joint cap fits usize");
        let too_small = NonZeroUsize::new(joint_cap - 1).expect("nonzero joint cap");
        let identity = task_identity(812);
        let session = native_result_writer(ResultRetainedBudget::new(too_small), too_small)
            .open(
                ResultWriteSpec::new(
                    UniqueId::new(812, 1),
                    ResultPresentation::MysqlText,
                    None,
                    true,
                )
                .with_task_identity(identity),
            )
            .expect("open bounded typed task result");
        let reservation = session
            .reservation_bytes(&typed_chunk())
            .expect("Arrow input fits the joint cap");
        let ResultWriteAdmission::Granted(credit) = session
            .try_acquire(reservation)
            .expect("joint reservation is valid")
        else {
            panic!("empty stream owns the joint cap");
        };

        let error = session
            .write_with_credit(typed_chunk(), credit)
            .expect_err("Arrow plus encoded backing must not exceed the joint cap");
        assert_eq!(error.kind(), FragmentIoErrorKind::Unavailable);
        assert!(error.to_string().contains("joint retained-byte cap"));
        session.abort(ResultAbort::NeverStarted);
    }

    #[test]
    fn only_terminal_ready_is_a_published_result_terminal() {
        assert!(publishes_terminal(ResultPublication::TerminalReady));
        assert!(!publishes_terminal(ResultPublication::DataReady));
        assert!(!publishes_terminal(ResultPublication::Removed));
        assert!(!publishes_terminal(ResultPublication::NoChange));
    }
}
