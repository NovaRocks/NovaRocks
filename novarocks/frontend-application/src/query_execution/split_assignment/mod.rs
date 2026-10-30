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

//! Coordinator-side runtime split assignment.
//!
//! One driver exists per execution round. It owns its split sources, its
//! sequence spaces, and its senders; aborting a round closes all of them, and
//! a replacement round rebuilds them from scratch under a new attempt id. No
//! state survives a round.

mod driver;
#[cfg(test)]
pub(crate) use driver::SplitAssignmentDriver;
mod round;
mod supervised_source;
mod transport;

pub(crate) use supervised_source::SupervisedSplitSource;

pub(crate) use driver::{
    AssignmentTarget, ScanNodeKey, SplitAssignmentDriverError, SplitAssignmentStop,
};
pub(crate) use round::{
    RoundSplitAssignment, RoundSplitAssignmentStop, RoundSplitEnumeration,
    RoundSplitEnumerationRequest, RoundSplitEnumerationResult, RoundSplitSource,
};
#[cfg(test)]
pub(crate) use transport::TaskUpdateTransportErrorKind;
pub(crate) use transport::{
    AcceptedPlanNode, TaskUpdateOutcome, TaskUpdateTicket, TaskUpdateTransport,
    TaskUpdateTransportError,
};
