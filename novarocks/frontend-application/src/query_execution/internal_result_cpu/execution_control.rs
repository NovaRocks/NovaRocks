// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

//! Control for the same closed CPU call. Existing Provider obligations use
//! their original finite context, never a freshly uncancelled statement view.
use super::*;
use novarocks_spi::connector::{
    ConnectorOperationControl, ConnectorOperationControlView, ConnectorRequestContext,
    ConnectorStopView,
};

#[derive(Clone)]
pub(super) enum CpuWorkControl {
    Statement(QueryCancellationView),
    ExistingMetadataObligation {
        control: ConnectorOperationControlView,
        stop: ConnectorStopView,
    },
}
impl From<QueryCancellationView> for CpuWorkControl {
    fn from(value: QueryCancellationView) -> Self {
        Self::Statement(value)
    }
}
impl CpuWorkControl {
    // Only the positive-payload settlement port calls this constructor after
    // its original MIO has recorded publication_obligation_started.
    pub(super) fn existing_metadata_obligation(context: &ConnectorRequestContext) -> Self {
        Self::ExistingMetadataObligation {
            control: context.original_operation_control(),
            stop: context.stop().clone(),
        }
    }
    pub(super) fn requires_original_source(&self) -> bool {
        matches!(self, Self::ExistingMetadataObligation { .. })
    }
    pub(super) fn failure(&self) -> Option<ExecutionFailureInput> {
        match self {
            Self::Statement(view) => view.reason().map(cpu_cancellation_failure),
            Self::ExistingMetadataObligation { control, .. } => {
                control.check_active().err().map(|error| {
                    ExecutionFailureInput::new(
                        error.failure_fact(),
                        SafeDetail::truncating(error.message()),
                        ExecutionFailureSite::Coordinator,
                    )
                })
            }
        }
    }
    pub(super) async fn stopped(&self) -> ExecutionFailureInput {
        match self {
            Self::Statement(view) => cpu_cancellation_failure(view.cancelled().await),
            Self::ExistingMetadataObligation { control, stop } => {
                if let Some(failure) = self.failure() {
                    return failure;
                }
                tokio::select! {
                    _ = stop.stopped() => {},
                    _ = tokio::time::sleep_until(control.deadline().into()) => {},
                }
                self.failure()
                    .expect("the same obligation stop or deadline fired")
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn existing_metadata_control_keeps_the_original_absolute_deadline() {
        let stop = novarocks_spi::connector::ConnectorStopOwner::new();
        let deadline = Instant::now() - std::time::Duration::from_millis(1);
        let context = ConnectorRequestContext::try_new(deadline, stop.view(), 1024, 4096).unwrap();
        let control = CpuWorkControl::existing_metadata_obligation(&context);
        let expected = context.check_active().unwrap_err().failure_fact();
        assert_eq!(control.failure().unwrap().fact(), expected);
        assert_eq!(
            futures::executor::block_on(control.stopped()).fact(),
            expected
        );
        let CpuWorkControl::ExistingMetadataObligation { control, .. } = control else {
            panic!("same concrete policy")
        };
        assert_eq!(control.deadline(), deadline);
    }
}
