// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

//! Carrier-neutral coordination facts shared by execution owners.
//! Runtime capabilities and owner decisions remain with their application owners.
//!
//! Design: ADR-0198 (docs/adr/ADR-0198-one-logical-execution-owner-for-every-distributed-intent.md).

mod clock;
mod conclusion;
mod identity;
mod recovery;
mod result;
mod status;

pub use clock::*;
pub use conclusion::*;
pub use identity::*;
pub use recovery::*;
pub use result::*;
pub use status::*;
