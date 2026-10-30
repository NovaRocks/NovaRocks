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

//! Scoped Statistics CPU is a domain Source handoff, never a closure runner.
use super::*;
use std::task::{Context, Poll};

/// Each implementation is an original stable first-party Provider Source.
/// Its input and partial output must remain in that Source on unwind. These
/// methods perform only their named finite CPU phase, never network/SDK I/O,
/// runtime bridges, arbitrary callbacks, or last-owner Provider destruction.
/// Only the original table-binding metadata phase is allowed in a foreground
/// Local/Client window. It grants no collection/decoder Internal capacity.
pub trait StatisticsBindingCpuSource: Send + Sync + std::any::Any + 'static {
    fn descriptor(&self) -> &ConnectorInstanceDescriptor;
    fn incarnation(&self) -> ProviderBindingEpoch;
    fn run_binding(&self);
}
/// The private Provider's original table-statistics Source. A foreground read
/// has only this named pure phase; it gains no collection/decoder capability.
pub trait StatisticsReadCpuSource: Send + Sync + std::any::Any + 'static {
    fn descriptor(&self) -> &ConnectorInstanceDescriptor;
    fn incarnation(&self) -> ProviderBindingEpoch;
    fn run_read(&self);
}
pub trait StatisticsSchemaCpuSource: Send + Sync + std::any::Any + 'static {
    fn descriptor(&self) -> &ConnectorInstanceDescriptor;
    fn incarnation(&self) -> ProviderBindingEpoch;
    fn run_schema(&self);
}
pub trait StatisticsArtifactsCpuSource: Send + Sync + std::any::Any + 'static {
    fn descriptor(&self) -> &ConnectorInstanceDescriptor;
    fn incarnation(&self) -> ProviderBindingEpoch;
    fn run_artifacts(&self);
}
pub trait StatisticsEvidenceCpuSource: Send + Sync + std::any::Any + 'static {
    fn descriptor(&self) -> &ConnectorInstanceDescriptor;
    fn incarnation(&self) -> ProviderBindingEpoch;
    fn run_evidence(&self);
}

/// Neutral original source values, with no arbitrary F/Any or resources.
/// Constructing an input grants no CPU position or finite qualification. Only
/// the FE's bound closed factory can accept it against its authentic scope.
pub enum StatisticsCpuInput {
    Binding(Arc<dyn StatisticsBindingCpuSource>),
    Read(Arc<dyn StatisticsReadCpuSource>),
    Schema(Arc<dyn StatisticsSchemaCpuSource>),
    Artifacts(Arc<dyn StatisticsArtifactsCpuSource>),
    Evidence(Arc<dyn StatisticsEvidenceCpuSource>),
}
impl StatisticsCpuInput {
    /// The Any supertrait supplies the actual concrete type, not a Provider
    /// callback returning a claimed identity. No business-value downcast occurs.
    pub fn original_source_type_id(&self) -> std::any::TypeId {
        match self {
            Self::Binding(source) => std::any::Any::type_id(source.as_ref()),
            Self::Read(source) => std::any::Any::type_id(source.as_ref()),
            Self::Schema(source) => std::any::Any::type_id(source.as_ref()),
            Self::Artifacts(source) => std::any::Any::type_id(source.as_ref()),
            Self::Evidence(source) => std::any::Any::type_id(source.as_ref()),
        }
    }
    pub fn descriptor(&self) -> &ConnectorInstanceDescriptor {
        match self {
            Self::Binding(s) => s.descriptor(),
            Self::Read(s) => s.descriptor(),
            Self::Schema(s) => s.descriptor(),
            Self::Artifacts(s) => s.descriptor(),
            Self::Evidence(s) => s.descriptor(),
        }
    }
    pub fn incarnation(&self) -> ProviderBindingEpoch {
        match self {
            Self::Binding(s) => s.incarnation(),
            Self::Read(s) => s.incarnation(),
            Self::Schema(s) => s.incarnation(),
            Self::Artifacts(s) => s.incarnation(),
            Self::Evidence(s) => s.incarnation(),
        }
    }
    /// Called once only by the actual closed worker. The Source, not this
    /// invocation or a Future, owns every original input and partial output.
    pub fn run_original_phase(&self) {
        match self {
            Self::Binding(s) => s.run_binding(),
            Self::Read(s) => s.run_read(),
            Self::Schema(s) => s.run_schema(),
            Self::Artifacts(s) => s.run_artifacts(),
            Self::Evidence(s) => s.run_evidence(),
        }
    }
}

/// One serial job slot belonging to the original admitted Statistics action.
/// submit refusal leaves its input intact. Dropping a borrowed waiter does
/// not drop or cancel the actual worker receipt/input. No method mints Work.
pub trait StatisticsCpuPort: Send + Sync {
    fn submit_in(&self, source: &mut Option<StatisticsCpuInput>) -> Result<(), ConnectorError>;
    fn poll_finished(&self, cx: &mut Context<'_>) -> Poll<Result<(), ConnectorError>>;
    fn poll_closed(&self, cx: &mut Context<'_>) -> Poll<Result<(), ConnectorError>>;
    fn local_failure(&self) -> Option<Arc<dyn std::error::Error + Send + Sync>>;
    fn metadata_bytes(&self) -> Result<std::num::NonZeroU64, ConnectorError>;
}

/// A borrowed waiter never owns the original request, partial projection or
/// final evidence. The admitted Action keeps this same Source independently.
pub trait StatisticsReadOriginal: Send + Sync + std::any::Any {
    fn descriptor(&self) -> &ConnectorInstanceDescriptor;
    fn incarnation(&self) -> ProviderBindingEpoch;
    fn read(
        self: Arc<Self>,
        cpu: Arc<dyn StatisticsCpuPort>,
        metadata: Arc<dyn crate::connector::ProviderMetadataCpuPort>,
    ) -> StatisticsSessionFuture<'static, ()>;
    fn close_original(&self) -> Result<(), ConnectorError>;
    fn take_result_in(
        &self,
        target: &mut Option<Result<StatisticsEvidence, ConnectorError>>,
    ) -> Result<(), ConnectorError>;
}
