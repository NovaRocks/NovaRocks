// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Serial Internal domain state moved between application consumers and finite CPU jobs.

use std::sync::Arc;

use crate::query_execution::{
    row_mutation::CowMatchRootConsumer, statistics::StatisticsRootResultDecoder,
    write_result::RootWriteResultDecoder,
};
use novarocks_execution_contract::root_result::RootResultEnd;
use novarocks_query_application::api::{RetainedRootReply, RootReplyView};
use novarocks_spi::connector::{ConnectorRowMutationSelection, StatisticsArtifactDraft};

/// The actual Internal domain retains typed sources through its CPU result.
/// Legacy diagnostic-only domains keep their original projection separately.
#[derive(Debug)]
pub(crate) enum InternalDomainFailure {
    Diagnostic(String),
    Source(novarocks_failure_contract::SharedFailureCause),
}
impl From<String> for InternalDomainFailure {
    fn from(message: String) -> Self {
        Self::Diagnostic(message)
    }
}
impl From<&str> for InternalDomainFailure {
    fn from(message: &str) -> Self {
        Self::Diagnostic(message.to_owned())
    }
}
impl From<novarocks_spi::connector::ConnectorError> for InternalDomainFailure {
    fn from(source: novarocks_spi::connector::ConnectorError) -> Self {
        Self::Source(novarocks_failure_contract::SharedFailureCause::new(source))
    }
}
impl From<crate::query_execution::write_result::RootWriteDecodeFailure> for InternalDomainFailure {
    fn from(error: crate::query_execution::write_result::RootWriteDecodeFailure) -> Self {
        let (message, source) = error.into_parts();
        match source {
            Some(source) => Self::Source(source),
            None => Self::Diagnostic(message),
        }
    }
}
impl From<crate::query_execution::statistics::StatisticsDecodeFailure> for InternalDomainFailure {
    fn from(error: crate::query_execution::statistics::StatisticsDecodeFailure) -> Self {
        use crate::query_execution::statistics::StatisticsDecodeFailure;
        use novarocks_failure_contract::SharedFailureCause;
        use novarocks_native_adapter::root_record_assembly::RootRecordFramingError;
        match error {
            StatisticsDecodeFailure::Connector(source) => Self::from(source),
            StatisticsDecodeFailure::Record(source)
            | StatisticsDecodeFailure::Framing(RootRecordFramingError::Statistics(source)) => {
                Self::Source(SharedFailureCause::new(source))
            }
            StatisticsDecodeFailure::Framing(source) => {
                Self::Source(SharedFailureCause::new(source))
            }
            StatisticsDecodeFailure::Contract(detail) => Self::Diagnostic(detail),
        }
    }
}

impl InternalDomainFailure {
    pub(crate) fn execution_input(
        &self,
    ) -> Option<novarocks_query_application::execution_error::ExecutionFailureInput> {
        use novarocks_failure_contract::FailureProjection;
        let Self::Source(source) = self else {
            return None;
        };
        Some(
            novarocks_query_application::execution_error::ExecutionFailureInput::new(
                source.failure_fact(),
                novarocks_execution_contract::SafeDetail::truncating(
                    "statement execution failed in its coordinator",
                ),
                novarocks_query_application::execution_error::ExecutionFailureSite::Coordinator,
            ),
        )
    }
}

pub(crate) enum InternalDomainState {
    Statistics(StatisticsRootResultDecoder),
    WriteComputation {
        decoder: RootWriteResultDecoder,
        decode: crate::query_execution::write_session::WriteComputationDecode,
    },
    Cow(Box<CowMatchRootConsumer>),
}

pub(crate) enum InternalDomainCompletion {
    Statistics(StatisticsRootResultDecoder),
    WriteDecoded(crate::query_execution::write_session::SessionDecodedWriteSegment),
    Cow(ConnectorRowMutationSelection),
}

pub(crate) struct InternalDomainEof {
    completion: InternalDomainCompletion,
    #[cfg(test)]
    end: RootResultEnd,
}

pub(crate) enum InternalDomainSlot {
    // Only a closed worker completion marker. Provider facts/capabilities stay
    // in its original packet, never disguised as a result domain payload.
    ProviderCreatorFinished,
    Collecting {
        state: Box<InternalDomainState>,
        #[cfg(test)]
        receipt: Option<u64>,
    },
    Eof(Box<InternalDomainEof>),
    StatisticsFinished(Vec<StatisticsArtifactDraft>),
    WriteAccepted(crate::query_execution::write_session::AcceptedWriteSegment),
}
impl InternalDomainSlot {
    #[cfg(test)]
    pub(crate) fn receipt(&self) -> Option<u64> {
        match self {
            Self::Collecting { receipt, .. } => *receipt,
            _ => None,
        }
    }
    #[cfg(test)]
    pub(crate) fn end(&self) -> Option<RootResultEnd> {
        match self {
            Self::Eof(eof) => Some(eof.end),
            _ => None,
        }
    }
    #[cfg(test)]
    pub(crate) fn statistics_finished(&self) -> bool {
        matches!(self, Self::StatisticsFinished(_))
    }
    pub(crate) fn apply_body(
        self,
        reply: Arc<RetainedRootReply>,
    ) -> Result<Self, InternalDomainFailure> {
        let Self::Collecting { mut state, .. } = self else {
            return Err("internal domain received data after EOF".into());
        };
        let RootReplyView::Data {
            #[cfg(test)]
            sequence,
            body,
            ..
        } = reply.outcome()
        else {
            return Err("internal domain data owner has no data".into());
        };
        let result = match &mut *state {
            InternalDomainState::Statistics(decoder) => decoder
                .apply_relay_body(body)
                .map_err(InternalDomainFailure::from),
            InternalDomainState::WriteComputation { decoder, .. } => decoder
                .apply_relay_body(body)
                .map_err(InternalDomainFailure::from),
            InternalDomainState::Cow(consumer) => consumer
                .push_body(body)
                .map_err(InternalDomainFailure::from),
        };
        // Receipt contains no body alias. Relinquish this job's original reply
        // before CPU completion, local consumption and the remote ACK.
        drop(reply);
        result?;
        Ok(Self::Collecting {
            state,
            #[cfg(test)]
            receipt: Some(sequence.get() - 1),
        })
    }
    pub(crate) fn finish_eof(self, end: RootResultEnd) -> Result<Self, InternalDomainFailure> {
        let Self::Collecting { state, .. } = self else {
            return Err("internal domain received duplicate EOF".into());
        };
        let completion = match *state {
            InternalDomainState::Statistics(mut decoder) => {
                decoder.check_relay_end(end.output_rows)?;
                decoder.observe_root_eof()?;
                InternalDomainCompletion::Statistics(decoder)
            }
            InternalDomainState::WriteComputation {
                mut decoder,
                decode,
            } => {
                decoder.check_relay_end(end.output_rows)?;
                decoder.observe_root_eof()?;
                let prepared = decode.interpret(decoder.finish()?)?;
                InternalDomainCompletion::WriteDecoded(prepared)
            }
            InternalDomainState::Cow(consumer) => {
                consumer.check_end(end.output_rows)?;
                InternalDomainCompletion::Cow((*consumer).finish()?)
            }
        };
        Ok(Self::Eof(Box::new(InternalDomainEof {
            completion,
            #[cfg(test)]
            end,
        })))
    }
    pub(crate) fn finish_statistics(self) -> Result<Self, InternalDomainFailure> {
        let Self::Eof(eof) = self else {
            return Err("statistics finalization requires its checked EOF".into());
        };
        let InternalDomainEof {
            completion: InternalDomainCompletion::Statistics(mut decoder),
            ..
        } = *eof
        else {
            return Err("statistics finalization requires its checked EOF".into());
        };
        decoder.observe_execution_success()?;
        Ok(Self::StatisticsFinished(decoder.finish()?))
    }
    pub(crate) fn accept_write(
        self,
        receiving: crate::query_execution::write_session::WriteReceivingPort,
    ) -> Result<Self, InternalDomainFailure> {
        let Self::Eof(eof) = self else {
            return Err("write receiving requires its original checked End".into());
        };
        let InternalDomainEof {
            completion: InternalDomainCompletion::WriteDecoded(segment),
            ..
        } = *eof
        else {
            return Err("write receiving requires its original checked End".into());
        };
        Ok(Self::WriteAccepted(receiving.accept(segment)?))
    }
    pub(crate) fn into_accepted_write(
        self,
    ) -> Result<crate::query_execution::write_session::AcceptedWriteSegment, String> {
        match self {
            Self::WriteAccepted(accepted) => Ok(accepted),
            _ => Err("write domain has no original accepted segment".into()),
        }
    }
    pub(crate) fn into_cow(self) -> Result<ConnectorRowMutationSelection, String> {
        match self {
            Self::Eof(eof) => match *eof {
                InternalDomainEof {
                    completion: InternalDomainCompletion::Cow(selection),
                    ..
                } => Ok(selection),
                _ => Err("COW lost its CPU-validated selection".into()),
            },
            _ => Err("COW lost its CPU-validated selection".into()),
        }
    }
    pub(crate) fn into_statistics(self) -> Result<Vec<StatisticsArtifactDraft>, String> {
        match self {
            Self::StatisticsFinished(artifacts) => Ok(artifacts),
            _ => Err("statistics lost its CPU-validated artifact membership".into()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::native::data_runtime::FrontendDataRuntime;
    use crate::query_execution::internal_result_cpu::{
        InternalResultCpuOwner, InternalResultValue, admitted_internal_fixture,
    };
    use crate::task_execution::status_intake::TestIntakeWake;
    use novarocks_query_application::cancellation::QueryCancellationSource;
    use std::{
        num::NonZeroU64,
        sync::Arc,
        time::{Duration, Instant},
    };

    #[test]
    fn cow_source_keeps_its_fact_and_safe_public_data_projection() {
        use novarocks_failure_contract::{FailureClass, FailureProjection};
        use novarocks_spi::connector::{ConnectorError, ConnectorErrorKind};
        let source = ConnectorError::new(
            ConnectorErrorKind::InvalidRequest,
            "signed COW required field contains NULL at row 0",
        );
        let expected = source.failure_fact();
        let failure = InternalDomainFailure::from(source.clone());
        let InternalDomainFailure::Source(original) = &failure else {
            panic!("the actual domain source is required")
        };
        assert_eq!(original.downcast_ref::<ConnectorError>(), Some(&source));
        assert_eq!(expected.class(), FailureClass::Data);
        let input = failure.execution_input().unwrap();
        assert_eq!(input.fact(), expected);
        assert_eq!(
            input.user_error().code().as_str(),
            "query.execution.invalid_data"
        );
        assert_eq!(
            input.user_error().to_string(),
            "[query.execution.invalid_data] statement execution failed in its coordinator"
        );
        assert!(
            InternalDomainFailure::from("signed COW required field contains NULL at row 0")
                .execution_input()
                .is_none()
        );
        for kind in [
            ConnectorErrorKind::ResourceExhausted,
            ConnectorErrorKind::Cancelled,
        ] {
            let source = ConnectorError::new(kind, "actual source diagnostic");
            let expected = source.failure_fact();
            let failure = InternalDomainFailure::from(source.clone());
            assert_eq!(failure.execution_input().unwrap().fact(), expected);
            let InternalDomainFailure::Source(original) = failure else {
                panic!("the actual domain source is required")
            };
            assert_eq!(original.downcast_ref::<ConnectorError>(), Some(&source));
        }
    }

    async fn claim(
        job: &mut crate::query_execution::internal_result_cpu::InternalResultCpuJob<
            Result<InternalDomainSlot, InternalDomainFailure>,
        >,
    ) -> Result<InternalResultValue<InternalDomainSlot>, InternalDomainFailure> {
        tokio::time::timeout(Duration::from_secs(5), async {
            let result = std::future::poll_fn(|cx| job.poll_take(cx))
                .await
                .expect("real CPU completion")
                .try_transform(|slot, _| slot);
            // The result alone is not the stable original job's exit.
            // Retire its original packet only after the real forwarder
            // joins, before asserting complete capacity reclamation.
            job.join_closed()
                .await
                .expect("actual original CPU job exit");
            result
        })
        .await
        .expect("domain CPU watchdog")
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn cow_source_survives_the_actual_cpu_result_without_a_success_receipt() {
        use novarocks_failure_contract::FailureProjection;
        use novarocks_spi::connector::{ConnectorError, ConnectorErrorKind};
        let (_control, root, binding, capacity) = admitted_internal_fixture();
        let mut owner = InternalResultCpuOwner::try_new_for_test().unwrap();
        let runtime = FrontendDataRuntime::new(tokio::runtime::Handle::current());
        let cancellation = QueryCancellationSource::new();
        let source = ConnectorError::new(
            ConnectorErrorKind::InvalidRequest,
            "signed COW required field contains NULL at row 0",
        );
        let expected = source.failure_fact();
        let input =
            InternalResultValue::try_produce(binding.scope(), binding.window_alias(), |_| {
                Ok(source.clone())
            })
            .unwrap()
            .track_actual_exit(&binding)
            .unwrap();
        let mut job = owner.runtime().submit(
            input,
            cancellation.view(),
            &runtime,
            Arc::new(TestIntakeWake::default()),
            |source, _| Err::<InternalDomainSlot, _>(InternalDomainFailure::from(source)),
        );
        let failure = match claim(&mut job).await {
            Err(failure) => failure,
            Ok(_) => panic!("a rejected COW body cannot yield a receipt domain"),
        };
        assert_eq!(failure.execution_input().unwrap().fact(), expected);
        let InternalDomainFailure::Source(original) = failure else {
            panic!("the actual domain source is required")
        };
        assert_eq!(original.downcast_ref::<ConnectorError>(), Some(&source));
        drop(original);
        binding.wait_result_activities_exited();
        drop(binding);
        root.owner.complete();
        root.business.release();
        assert_eq!(capacity.snapshot().held_positions, [0; 4]);
        owner
            .shutdown_until(Instant::now() + Duration::from_secs(2))
            .await
            .unwrap();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn statistics_eof_and_all_success_finalization_are_distinct_cpu_receipts() {
        let (_control, root, binding, capacity) = admitted_internal_fixture();
        let mut owner = InternalResultCpuOwner::try_new_for_test().unwrap();
        let runtime = FrontendDataRuntime::new(tokio::runtime::Handle::current());
        let cancellation = QueryCancellationSource::new();
        let end = RootResultEnd {
            sequence: NonZeroU64::new(1).unwrap(),
            output_rows: 0,
        };
        let input =
            InternalResultValue::try_produce(binding.scope(), binding.window_alias(), |_| {
                Ok(InternalDomainSlot::Collecting {
                    state: Box::new(InternalDomainState::Statistics(
                        StatisticsRootResultDecoder::for_test_with_capacity([], &binding),
                    )),
                    receipt: None,
                })
            })
            .unwrap()
            .track_actual_exit(&binding)
            .unwrap();
        let mut eof = owner.runtime().submit(
            input,
            cancellation.view(),
            &runtime,
            Arc::new(TestIntakeWake::default()),
            move |slot, _| slot.finish_eof(end),
        );
        let checked_eof = claim(&mut eof).await.unwrap();
        binding.wait_result_activities_exited();
        assert_eq!(checked_eof.value().end(), Some(end));
        assert!(!checked_eof.value().statistics_finished());
        assert!(checked_eof.value().receipt().is_none());
        let mut finalization = owner.runtime().submit(
            checked_eof.track_actual_exit(&binding).unwrap(),
            cancellation.view(),
            &runtime,
            Arc::new(TestIntakeWake::default()),
            |slot, _| slot.finish_statistics(),
        );
        let finished = claim(&mut finalization).await.unwrap();
        binding.wait_result_activities_exited();
        assert!(finished.value().statistics_finished());
        drop(binding);
        root.owner.complete();
        root.business.release();
        assert_eq!(capacity.snapshot().held_positions, [0, 0, 1, 0]);
        let artifacts = finished.hand_off(|slot, _| slot.into_statistics()).unwrap();
        assert!(artifacts.is_empty());
        assert_eq!(capacity.snapshot().held_positions, [0; 4]);
        owner
            .shutdown_until(Instant::now() + Duration::from_secs(5))
            .await
            .unwrap();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn bad_domain_body_never_produces_an_ack_receipt() {
        use novarocks_execution_contract::{
            identity::TaskIdentity,
            root_result::{RootReadOutcome, RootResultData, RootResultReply},
        };
        use novarocks_result_contract::{InternalResultDomain, RootOutputKind, RootProfileId};
        use novarocks_types::{
            AttemptId, BackendProcessId, QueryExecutionId, QueryId, StageId, TaskId,
        };
        let (_control, root, binding, capacity) = admitted_internal_fixture();
        let mut owner = InternalResultCpuOwner::try_new_for_test().unwrap();
        let runtime = FrontendDataRuntime::new(tokio::runtime::Handle::current());
        let cancellation = QueryCancellationSource::new();
        let kind = RootOutputKind::InternalFacts(InternalResultDomain::StatisticsArtifactV1);
        let reply = RetainedRootReply::try_new(
            novarocks_execution_contract::root_result::RootReadResponse::Reply(RootResultReply {
                root_task: TaskIdentity::new(
                    QueryExecutionId::new(QueryId::new(1, 2), AttemptId::new(1).unwrap()).unwrap(),
                    StageId::new(1).unwrap(),
                    TaskId::new(1).unwrap(),
                    BackendProcessId::new_v7(),
                ),
                profile: RootProfileId::V1,
                kind,
                accepted_consumed: 0,
                outcome: RootReadOutcome::Data(
                    RootResultData::try_new(
                        kind,
                        NonZeroU64::new(1).unwrap(),
                        bytes::Bytes::from_static(
                            b"invalid record prefix with enough bytes to fail validation",
                        ),
                        None,
                    )
                    .unwrap(),
                ),
            }),
            binding.window_alias(),
            1024,
        )
        .unwrap();
        let input =
            InternalResultValue::try_produce(binding.scope(), binding.window_alias(), |_| {
                Ok(InternalDomainSlot::Collecting {
                    state: Box::new(InternalDomainState::Statistics(
                        StatisticsRootResultDecoder::for_test_with_capacity([], &binding),
                    )),
                    receipt: None,
                })
            })
            .unwrap()
            .transform(|slot, _| (slot, reply))
            .track_actual_exit(&binding)
            .unwrap();
        let mut job = owner.runtime().submit(
            input,
            cancellation.view(),
            &runtime,
            Arc::new(TestIntakeWake::default()),
            |(slot, reply), _| slot.apply_body(Arc::new(reply)),
        );
        assert!(claim(&mut job).await.is_err());
        binding.wait_result_activities_exited();
        drop(binding);
        root.owner.complete();
        root.business.release();
        assert_eq!(capacity.snapshot().held_positions, [0; 4]);
        owner
            .shutdown_until(Instant::now() + Duration::from_secs(5))
            .await
            .unwrap();
    }
}
