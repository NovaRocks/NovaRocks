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

use crate::mv::domain::lifecycle::{CreateMvRequest, DropMvRequest, ListMvsRequest, MvListRow};
use crate::mv::domain::model::MvStorageEngine;

pub struct IcebergMvBackend {
    ports: crate::mv::domain::iceberg_refresh::IcebergMvCorePorts,
}

impl IcebergMvBackend {
    pub fn new_with_ports(ports: crate::mv::domain::iceberg_refresh::IcebergMvCorePorts) -> Self {
        Self { ports }
    }
}

impl IcebergMvBackend {
    pub fn create_mv(&self, req: CreateMvRequest) -> Result<(), String> {
        crate::mv::domain::iceberg_refresh::create_iceberg_mv_with_ports(
            self.ports.clone(),
            req.current_catalog.as_deref(),
            &req.current_database,
            &req.stmt,
            &req.connector_context,
        )
        .map(|_| ())
    }

    pub fn drop_mv(&self, req: DropMvRequest) -> Result<(), String> {
        crate::mv::domain::iceberg_refresh::drop_iceberg_mv_with_ports(
            &self.ports,
            req.current_catalog.as_deref(),
            &req.current_database,
            &req.stmt,
            &req.connector_context,
        )
        .map(|_| ())
    }

    pub fn list_mvs(&self, req: ListMvsRequest) -> Result<Vec<MvListRow>, String> {
        crate::mv::domain::analysis_adapter::list_mv_rows_with_ports(
            self.ports.repository().as_ref(),
            req.current_catalog.as_deref(),
            &req.stmt,
            Some(MvStorageEngine::Iceberg),
        )
    }
}
