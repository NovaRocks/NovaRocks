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

use crate::connector::backend::MvBackend;
use crate::engine::mv::lifecycle::{CreateMvRequest, DropMvRequest, ListMvsRequest, MvListRow};
use crate::mv::model::MvStorageEngine;

pub(crate) struct IcebergMvBackend {
    ports: crate::engine::mv::iceberg_refresh::IcebergMvCorePorts,
}

impl IcebergMvBackend {
    pub(crate) fn new_with_ports(
        ports: crate::engine::mv::iceberg_refresh::IcebergMvCorePorts,
    ) -> Self {
        Self { ports }
    }
}

impl MvBackend for IcebergMvBackend {
    fn name(&self) -> &'static str {
        "iceberg"
    }

    fn create_mv(&self, req: CreateMvRequest) -> Result<(), String> {
        crate::engine::mv::iceberg_refresh::create_iceberg_mv_with_ports(
            self.ports.clone(),
            req.current_catalog.as_deref(),
            &req.current_database,
            &req.stmt,
            &req.connector_context,
        )
        .map(|_| ())
    }

    fn drop_mv(&self, req: DropMvRequest) -> Result<(), String> {
        crate::engine::mv::iceberg_refresh::drop_iceberg_mv_with_ports(
            &self.ports,
            req.current_catalog.as_deref(),
            &req.current_database,
            &req.stmt,
            &req.connector_context,
        )
        .map(|_| ())
    }

    fn list_mvs(&self, req: ListMvsRequest) -> Result<Vec<MvListRow>, String> {
        crate::engine::mv::analysis_adapter::list_mv_rows_with_ports(
            self.ports.repository().as_ref(),
            req.current_catalog.as_deref(),
            &req.stmt,
            Some(MvStorageEngine::Iceberg),
        )
    }
}
