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

//! Closed CPU borrowing for the actual Provider metadata author.
use super::{ConnectorError, ConnectorInstanceDescriptor, ProviderBindingEpoch};
use std::{
    sync::Arc,
    task::{Context, Poll},
};

pub trait ProviderMetadataCpuSource: Send + Sync + std::any::Any + 'static {
    fn descriptor(&self) -> &ConnectorInstanceDescriptor;
    fn incarnation(&self) -> ProviderBindingEpoch;
    fn run_metadata(&self);
}
pub struct ProviderMetadataCpuInput(pub Arc<dyn ProviderMetadataCpuSource>);
impl ProviderMetadataCpuInput {
    pub fn original_source_type_id(&self) -> std::any::TypeId {
        std::any::Any::type_id(self.0.as_ref())
    }
    pub fn descriptor(&self) -> &ConnectorInstanceDescriptor {
        self.0.descriptor()
    }
    pub fn incarnation(&self) -> ProviderBindingEpoch {
        self.0.incarnation()
    }
    pub fn run_original_phase(&self) {
        self.0.run_metadata()
    }
}
/// Qualified metadata binding and snapshot parsing/projection only. The actual
/// Provider owns the closed input variants; this cannot run a Statistics product
/// operation, arbitrary Future or caller-supplied closure.
pub trait ProviderMetadataCpuPort: Send + Sync {
    fn submit_metadata_in(
        &self,
        input: &mut Option<ProviderMetadataCpuInput>,
    ) -> Result<(), ConnectorError>;
    fn poll_finished(&self, cx: &mut Context<'_>) -> Poll<Result<(), ConnectorError>>;
    fn poll_closed(&self, cx: &mut Context<'_>) -> Poll<Result<(), ConnectorError>>;
    fn local_failure(&self) -> Option<Arc<dyn std::error::Error + Send + Sync>>;
}
