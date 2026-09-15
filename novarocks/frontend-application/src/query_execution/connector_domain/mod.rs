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

//! Engine-side outer values for the typed connector read stack.
//!
//! The engine owns the catalog-scoped wrappers, the scheduled-split identity,
//! and the scan node; the connector owns everything inside a handle or split
//! variant. Nothing here interprets a provider variant, so the frontend never
//! links a provider crate.

mod handle;
mod scheduling;
mod table_scan;

pub(crate) use handle::{Split, TableHandle};
pub(crate) use scheduling::{
    PlanNodeAssignmentState, ScheduledSplit, SplitAssignment, SplitAssignmentError,
    SplitSequenceAllocator, TaskUpdateRequest,
};
pub(crate) use table_scan::{DynamicFilterBinding, TableScanNode, TableScanNodeError};
