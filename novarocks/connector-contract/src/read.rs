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

use crate::{CatalogHandle, ConnectorInstanceDescriptor};

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct ConnectorReadBinding {
    descriptor: ConnectorInstanceDescriptor,
    catalog_handle: CatalogHandle,
}

impl ConnectorReadBinding {
    pub const fn new(
        descriptor: ConnectorInstanceDescriptor,
        catalog_handle: CatalogHandle,
    ) -> Self {
        Self {
            descriptor,
            catalog_handle,
        }
    }

    pub const fn descriptor(&self) -> &ConnectorInstanceDescriptor {
        &self.descriptor
    }

    pub const fn catalog_handle(&self) -> &CatalogHandle {
        &self.catalog_handle
    }
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum ConnectorReadRelationKind {
    Table,
    TableFunction,
    ChangeWindow,
    SystemTable,
    MergeTable,
}
