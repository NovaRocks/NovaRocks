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

//! Original asynchronous rewrite planning and its closed CPU borrowing port.

use super::*;
use std::task::{Context, Poll};

/// The selected logical target precedes metadata I/O. The concrete Provider
/// freezes its real table handle and planning digest after original observation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ConnectorRewriteSelection {
    DataFiles {
        rewrite_all: bool,
    },
    PositionDeletes {
        rewrite_all: bool,
        min_input_files: Option<u32>,
    },
}
#[derive(Clone)]
pub struct ConnectorRewriteSourceRequest {
    pub operation_id: ConnectorWriteOperationId,
    pub table: crate::connector::ConnectorTableIdentity,
    pub selection: ConnectorRewriteSelection,
    pub context: ConnectorRequestContext,
}
impl ConnectorRewriteSourceRequest {
    pub fn validate(&self) -> Result<(), ConnectorError> {
        crate::connector::ConnectorOperationControl::check_active(&self.context)?;
        if matches!(
            self.selection,
            ConnectorRewriteSelection::PositionDeletes {
                min_input_files: Some(0),
                ..
            }
        ) {
            return Err(ConnectorError::new(
                crate::connector::ConnectorErrorKind::InvalidRequest,
                "rewrite position deletes min_input_files must be positive",
            ));
        }
        Ok(())
    }
}

/// Only the concrete first-party source selected by the same lease may enter
/// the closed CPU factory. Input/output remain in this source on unwind.
pub trait RewritePlanCpuSource: Send + Sync + std::any::Any + 'static {
    fn descriptor(&self) -> &ConnectorInstanceDescriptor;
    fn incarnation(&self) -> crate::connector::ProviderBindingEpoch;
    fn run_plan(&self);
}

pub struct RewriteCpuInput(pub Arc<dyn RewritePlanCpuSource>);
impl RewriteCpuInput {
    pub fn original_source_type_id(&self) -> std::any::TypeId {
        std::any::Any::type_id(self.0.as_ref())
    }
    pub fn descriptor(&self) -> &ConnectorInstanceDescriptor {
        self.0.descriptor()
    }
    pub fn incarnation(&self) -> crate::connector::ProviderBindingEpoch {
        self.0.incarnation()
    }
    pub fn run_original_phase(&self) {
        self.0.run_plan();
    }
}

/// Typed facade over the existing serial creator slot, never a closure runner.
pub trait RewriteCpuPort: Send + Sync {
    fn submit_in(&self, input: &mut Option<RewriteCpuInput>) -> Result<(), ConnectorError>;
    fn poll_finished(&self, cx: &mut Context<'_>) -> Poll<Result<(), ConnectorError>>;
    fn poll_closed(&self, cx: &mut Context<'_>) -> Poll<Result<(), ConnectorError>>;
    fn local_failure(&self) -> Option<Arc<dyn std::error::Error + Send + Sync>>;
}

/// Original immutable result, exact table observation and SAME original
/// operation retention. Neither identity nor retention grants payload bytes.
pub struct ConnectorRewritePrepared {
    pub plan: Arc<ConnectorDistributedRewritePlan>,
    pub table: crate::connector::ConnectorTableMetadata,
    pub capture: crate::connector::ConnectorOriginalResultScope,
}

/// The original source is installed before its first I/O/CPU poll. A Future
/// borrows it; it can never own the only request, partial plan or output.
/// Preparation keeps the SAME Root control, absolute deadline and retention.
/// Full plan validation runs in the selected CPU phase before reception.
pub trait ConnectorRewritePlanSource: Send {
    fn request(&self) -> &ConnectorRewriteSourceRequest;
    fn prepare<'a>(
        &'a mut self,
        cpu: &'a dyn RewriteCpuPort,
        metadata_cpu: &'a Arc<dyn crate::connector::ProviderMetadataCpuPort>,
        capture: &'a crate::connector::ConnectorOriginalResultScope,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), ConnectorError>> + Send + 'a>>;
    /// Move the prepared result once into an empty stable receiving slot,
    /// after the actual CPU and forwarder exits. Refusal preserves both slots.
    fn take_preparation_in(
        &mut self,
        receiving: &mut Option<ConnectorRewritePrepared>,
    ) -> Result<(), ConnectorError>;
    fn close<'a>(
        &'a mut self,
        cpu: &'a dyn RewriteCpuPort,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), ConnectorError>> + Send + 'a>>;
}

/// Stable original WriteControl begin source. The concrete Provider retains
/// its exact request, pure continuation, observed input and accepted plan.
/// The borrowed driver never owns the only session or result. Its same-scope
/// control/retention stays with the source through actual exit; no layout or
/// prospective byte invoice is part of this protocol.
pub trait ConnectorRewriteWriteBeginSource: Send + Sync {
    fn prepare<'a>(
        &'a self,
        cpu: &'a dyn RewriteCpuPort,
        metadata_cpu: &'a Arc<dyn crate::connector::ProviderMetadataCpuPort>,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), ConnectorError>> + Send + 'a>>;
    /// Move the original plan or typed failure once into an empty receiving
    /// slot. Poll/timeout/cancellation never substitutes for actual exit.
    fn take_plan_in(
        &self,
        receiving: &mut Option<
            Result<crate::connector::write_stack::ConnectorWriteSessionPlan, ConnectorError>,
        >,
    ) -> Result<(), ConnectorError>;
    fn close<'a>(
        &'a self,
        cpu: &'a dyn RewriteCpuPort,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), ConnectorError>> + Send + 'a>>;
}
