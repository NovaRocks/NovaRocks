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

//! Resume the original participant and Task candidate scan after QA settles.

use crate::{effect::OperationIntent, execution::ReleaseCheck};
use novarocks_execution_contract::ParticipationRef;
use novarocks_types::TaskId;

#[derive(Debug)]
pub(super) enum Authorization {
    Waiting(OperationIntent),
    Ready(OperationIntent),
}

#[derive(Debug)]
pub(super) enum Maintenance {
    Quiesce,
    Renew,
    Release(ReleaseCheck),
}

#[derive(Debug, Default)]
pub(super) struct Candidates {
    pub(super) enabled: bool,
    pub(super) wake: bool,
    pub(super) participant_after: Option<ParticipationRef>,
    pub(super) maintaining: Option<Maintenance>,
    pub(super) task_after: Option<TaskId>,
    pub(super) creates: bool,
    pub(super) authorization: Option<Authorization>,
}

impl Candidates {
    pub(super) fn begin_round(&mut self) {
        self.wake = false;
        self.participant_after = None;
        self.maintaining = None;
        self.task_after = None;
        self.creates = false;
    }
}
