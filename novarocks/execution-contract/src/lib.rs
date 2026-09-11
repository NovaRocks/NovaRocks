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

//! Stable, transport-neutral contracts shared by query coordination and workers.
//!
//! This crate owns exact execution identities, immutable task descriptions,
//! closed operations, receipts, lifecycle observations, and pure transition
//! classifiers. It contains no generated transport model, I/O, compiler,
//! scheduler, or execution-kernel implementation.

pub mod context_convergence;
pub mod descriptor;
pub mod domain;
pub mod identity;
pub mod lease;
pub mod operation;
pub mod status;
pub mod transition;

/// Stable namespace used by protocol codecs and role applications.
pub mod task_execution {
    pub mod context_convergence {
        pub use crate::context_convergence::*;
    }
    pub mod descriptor {
        pub use crate::descriptor::*;
    }
    pub mod domain {
        pub use crate::domain::*;
    }
    pub mod identity {
        pub use crate::identity::*;
    }
    pub mod lease {
        pub use crate::lease::*;
    }
    pub mod operation {
        pub use crate::operation::*;
    }
    pub mod status {
        pub use crate::status::*;
    }
    pub mod transition {
        pub use crate::transition::*;
    }
}

pub use context_convergence::*;
pub use descriptor::*;
pub use domain::*;
pub use identity::*;
pub use lease::*;
pub use operation::*;
pub use status::*;
pub use transition::*;
