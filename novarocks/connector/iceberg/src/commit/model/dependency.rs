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

//! Only the dependencies used by current operation admission are represented here.

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Dependency {
    RefUnchanged,
    NoReadDependency,
    RegisteredFilesNotLive(Vec<String>),
    /// Preparation-time check only; metadata publication has no atomic snapshot-existence requirement.
    MeasuredSnapshotExists(i64),
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum ValidationInput {
    Metadata,
    LiveSet,
    HistoryWindow,
}

impl Dependency {
    pub fn validation_input(&self) -> Option<ValidationInput> {
        match self {
            Self::RefUnchanged | Self::MeasuredSnapshotExists(_) => Some(ValidationInput::Metadata),
            Self::RegisteredFilesNotLive(_) => Some(ValidationInput::LiveSet),
            Self::NoReadDependency => None,
        }
    }
}
