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

//! Static system-table composition and statement-scoped snapshot contracts.
//!
//! The registry stores descriptions only. Actual providers are constructed by
//! the statement owner with its original catalog, CPU and I/O capabilities.

mod error;
mod provider;
mod registry;
mod request;
mod snapshot;

pub use error::{SystemTableError, SystemTableRegistryError};
pub use provider::SystemTableProvider;
pub use registry::{
    SystemTableLookup, SystemTableRegistration, SystemTableRegistry, SystemTableScope,
};
pub use request::SystemTableRequest;
pub use snapshot::SystemTableSnapshot;
