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

//! Long-lived frontend Catalog application state.
//!
//! This crate owns process-local Catalog generations and their retirement.
//! Query-local compiler mappings and Connector execution instances belong to
//! their respective application domains and are intentionally absent.

mod generation;

pub use generation::{
    CatalogGenerationError, CatalogGenerationLease, CatalogGenerationOwner,
    PreparedCatalogGeneration,
};
