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

//! Dependency validation reads only the input class that the dependency requires.

mod builtin;
mod history;
mod inputs;

pub use builtin::validate;
pub use history::{HistoryGap, HistoryWindow, history_window};
pub use inputs::{LiveEntry, LiveSet, ValidationInputs};

use super::model::{Dependency, EntryIdentity};

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Culprit {
    Ref {
        expected: Option<i64>,
        actual: Option<i64>,
    },
    Snapshot(i64),
    Entry {
        snapshot: i64,
        identity: EntryIdentity,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Verdict<D = Dependency> {
    Holds,
    Broken { dependency: D, culprit: Culprit },
    Unprovable { dependency: D, reason: HistoryGap },
}
