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

//! Same statement Source, first seeded by an actual selected request or factory.
use crate::query_execution::maintenance::io_admission::MetadataIoAdmission;
use novarocks_spi::connector::*;
use novarocks_workload_control::{WorkError, WorkOwner, WorkScope};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

pub(crate) struct ReadMetadataIoSource {
    root: WorkScope,
    local_preparation: ConnectorMetadataMaintenanceIoProfile,
    original: Mutex<Option<Arc<MetadataIoAdmission>>>,
}
impl ReadMetadataIoSource {
    pub(crate) fn new(
        root: WorkScope,
        local_preparation: ConnectorMetadataMaintenanceIoProfile,
    ) -> Arc<Self> {
        Arc::new(Self {
            root,
            local_preparation,
            original: Mutex::new(None),
        })
    }
    pub(crate) fn original(&self) -> Option<Arc<MetadataIoAdmission>> {
        self.original
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .clone()
    }
    fn install(&self) -> Result<Arc<MetadataIoAdmission>, ConnectorError> {
        let mut original = self.original.lock().unwrap_or_else(|p| p.into_inner());
        if original.is_none() {
            // The actual local preparation source is the initial author. A
            // Provider adds its exact selected route before its own request.
            *original = Some(
                MetadataIoAdmission::new(
                    self.root.clone(),
                    self.local_preparation.clone(),
                    self.local_preparation.metadata_bytes().get(),
                )
                .map_err(work_error)?,
            );
        }
        Ok(Arc::clone(
            original.as_ref().expect("same installed source"),
        ))
    }
    pub(crate) fn poll_progress(
        &self,
        owner: &WorkOwner,
        cx: &mut Context<'_>,
    ) -> Result<bool, WorkError> {
        match self.original() {
            Some(io) => io.poll_progress(owner, cx),
            None => Ok(false),
        }
    }
    pub(crate) fn health_failure(
        &self,
    ) -> Option<novarocks_query_application::api::StatementNativeHealthFailure> {
        self.original().and_then(|io| io.health_failure())
    }
}
fn work_error(error: WorkError) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::ResourceExhausted, error.to_string())
}
impl ConnectorIoAdmissionPort for ReadMetadataIoSource {
    fn configure_read_source(
        &self,
        profile: &ConnectorMetadataMaintenanceIoProfile,
    ) -> Result<(), ConnectorError> {
        self.install()?.configure_read_source(profile)
    }
    fn poll_read_source_ready(&self, cx: &mut Context<'_>) -> Poll<Result<(), ConnectorError>> {
        match self.original() {
            Some(io) => io.poll_read_source_ready(cx),
            None => Poll::Ready(Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "read source has no selected route",
            ))),
        }
    }
    fn request(&self, dependency: SharedDependency) -> Result<ConnectorIoRequest, ConnectorError> {
        self.install()?.request(dependency)
    }

    fn request_owned(
        &self,
        additional: std::num::NonZeroU64,
    ) -> Result<ConnectorIoRequest, ConnectorError> {
        self.original()
            .ok_or_else(|| {
                ConnectorError::new(
                    ConnectorErrorKind::InvalidRequest,
                    "owned read request precedes its original selected route",
                )
            })?
            .request_owned(additional)
    }
    fn publication_obligation_started(&self) {
        if let Some(io) = self.original() {
            io.publication_obligation_started();
        }
    }
    fn retain_payload(&self) -> Result<ConnectorPayloadRetentionGuard, ConnectorError> {
        self.original()
            .ok_or_else(|| {
                ConnectorError::new(
                    ConnectorErrorKind::InvalidRequest,
                    "read payload is not installed",
                )
            })?
            .retain_payload()
    }
}
