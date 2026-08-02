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

//! Iceberg-backed materialized-view backend.

use std::sync::{Arc, Weak};

use crate::connector::backend::MvBackend;
use crate::engine::StandaloneState;
use crate::engine::mv::lifecycle::{CreateMvRequest, DropMvRequest, ListMvsRequest, MvListRow};
use crate::mv::model::MvStorageEngine;

pub(crate) struct IcebergMvBackend {
    state: Weak<StandaloneState>,
}

impl IcebergMvBackend {
    pub(crate) fn new(state: &Arc<StandaloneState>) -> Self {
        Self {
            state: Arc::downgrade(state),
        }
    }

    fn state(&self) -> Result<Arc<StandaloneState>, String> {
        self.state
            .upgrade()
            .ok_or_else(|| "standalone state dropped".to_string())
    }
}

impl MvBackend for IcebergMvBackend {
    fn name(&self) -> &'static str {
        "iceberg"
    }

    fn create_mv(&self, req: CreateMvRequest) -> Result<(), String> {
        let state = self.state()?;
        crate::engine::mv::iceberg_refresh::create_iceberg_mv_with_connector_context(
            &state,
            req.current_catalog.as_deref(),
            &req.current_database,
            &req.stmt,
            &req.connector_context,
        )
        .map(|_| ())
    }

    fn drop_mv(&self, req: DropMvRequest) -> Result<(), String> {
        let state = self.state()?;
        crate::engine::mv::iceberg_refresh::drop_iceberg_mv_with_connector_context(
            &state,
            req.current_catalog.as_deref(),
            &req.current_database,
            &req.stmt,
            &req.connector_context,
        )
        .map(|_| ())
    }

    fn list_mvs(&self, req: ListMvsRequest) -> Result<Vec<MvListRow>, String> {
        let state = self.state()?;
        crate::engine::mv::analysis_adapter::list_mv_rows(
            &state,
            req.current_catalog.as_deref(),
            &req.stmt,
            Some(MvStorageEngine::Iceberg),
        )
    }
}
