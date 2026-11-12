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

//! Work bodies bound to the exact effect selected by the attempt host.

use super::{
    DiagnosticRecords, DynamicFilterRecords, PumpRecordError,
    split::{BoundSplitRunner, SplitPump},
};
use crate::control_runtime::transport_withdrawal::AttemptEffectEnvelope;
use novarocks_attempt_protocol::{effect::EffectBody, input::ExecutorFact};
use std::sync::Arc;

#[expect(
    clippy::large_enum_variant,
    reason = "The completed executor fact moves through the existing result custody."
)]
pub(crate) enum PumpOutcome {
    Completed(Option<ExecutorFact>),
    Fenced,
}

pub(crate) enum BoundPumpWork {
    Start(Arc<AttemptEffectEnvelope>),
    StartSplit(Arc<AttemptEffectEnvelope>, BoundSplitRunner),
    ControlSplit(Arc<AttemptEffectEnvelope>, Arc<SplitPump>),
    Stop(Arc<AttemptEffectEnvelope>, Arc<DynamicFilterRecords>),
    Read(Arc<AttemptEffectEnvelope>, Arc<DynamicFilterRecords>),
    Admit(Arc<AttemptEffectEnvelope>, Arc<DynamicFilterRecords>),
    Collect(Arc<AttemptEffectEnvelope>, Arc<DiagnosticRecords>),
    Dispose(Arc<AttemptEffectEnvelope>, Arc<DiagnosticRecords>),
}

impl BoundPumpWork {
    pub(crate) fn matches(&self, effect: &Arc<AttemptEffectEnvelope>) -> bool {
        let original = match self {
            Self::Start(effect)
            | Self::StartSplit(effect, _)
            | Self::ControlSplit(effect, _)
            | Self::Stop(effect, _)
            | Self::Read(effect, _)
            | Self::Admit(effect, _)
            | Self::Collect(effect, _)
            | Self::Dispose(effect, _) => effect,
        };
        Arc::ptr_eq(original, effect)
    }

    pub(crate) fn runs_blocking(&self) -> bool {
        matches!(
            self,
            Self::Admit(..) | Self::Dispose(..) | Self::Stop(..) | Self::ControlSplit(..)
        )
    }

    pub(crate) async fn run(self) -> Result<Option<ExecutorFact>, PumpRecordError> {
        match self {
            // The original feedback domain is configured before Core starts.
            Self::Start(_) => Ok(None),
            Self::StartSplit(_, runner) => runner.run().await.map(Some),
            Self::Read(effect, original) => {
                let EffectBody::ReadDynamicFilters {
                    request,
                    task,
                    operation,
                    acknowledged,
                    ..
                } = effect.protocol.body
                else {
                    unreachable!("bound dynamic filter read");
                };
                original
                    .run_admitted_read(request, task, operation, acknowledged)
                    .await
                    .map(Some)
            }
            Self::Collect(effect, original) => {
                let EffectBody::CollectDiagnostics { request, task, .. } = effect.protocol.body
                else {
                    unreachable!("bound diagnostic read");
                };
                original.run_admitted_read(request, task).await.map(Some)
            }
            _ => unreachable!("bound asynchronous pump work"),
        }
    }

    pub(crate) fn run_blocking(self) -> Result<Option<ExecutorFact>, PumpRecordError> {
        match self {
            Self::ControlSplit(effect, original) => {
                match &effect.protocol.body {
                    EffectBody::StopPump { .. } => original.request_stop(),
                    notice => original
                        .apply_notice(notice)
                        .map_err(PumpRecordError::Split)?,
                }
                Ok(None)
            }
            Self::Stop(_, original) => {
                original.close_external();
                Ok(None)
            }
            Self::Admit(effect, original) => {
                let EffectBody::AdmitDynamicFilterFeedback {
                    request,
                    task,
                    version,
                    feedback,
                } = effect.protocol.body
                else {
                    unreachable!("bound dynamic filter admission");
                };
                original
                    .run_admitted_feedback(request, task, version, feedback)
                    .map(Some)
            }
            Self::Dispose(effect, original) => {
                let EffectBody::DisposeDiagnosticInfo { information, .. } = effect.protocol.body
                else {
                    unreachable!("bound diagnostic disposition");
                };
                drop(original.discard_information(information)?);
                Ok(None)
            }
            _ => unreachable!("bound blocking pump work"),
        }
    }
}
