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

//! Transport-neutral task execution contracts.
//!
//! This module is the single definition point for the native task protocol's
//! domain model: identities, the immutable task descriptor, closed typed
//! operations and domains, task and query-context states, immutable versioned
//! status, the query execution lease, and the pure transitions that classify
//! every one of them.
//!
//! It deliberately depends on neither generated protobuf, gRPC, the frontend,
//! the backend, nor any connector provider implementation. The wire grammar is
//! a separate representation of these types, and the frontend and backend own
//! their own runtime state machines built on top of them; only immutable
//! values, this module's pure validation, and the central codec are shared
//! across the process boundary.
// Design: ADR-0135 (docs/adr/ADR-0135-native-distributed-work-as-tasks.md)

pub mod descriptor {
    pub use novarocks_execution_contract::descriptor::*;
}
pub mod domain {
    pub use novarocks_execution_contract::domain::*;
}
pub mod identity {
    pub use novarocks_execution_contract::identity::*;
}
pub mod lease {
    pub use novarocks_execution_contract::lease::*;
}
pub mod operation {
    pub use novarocks_execution_contract::operation::*;
}
pub mod status {
    pub use novarocks_execution_contract::status::*;
}
pub mod transition {
    pub use novarocks_execution_contract::transition::*;
}

pub use novarocks_execution_contract::*;
