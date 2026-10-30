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

use novarocks_execution_contract::{TaskIdentity, TerminationDetail};

/// The original Task identity and its selected cause move together. This is
/// the serialized query owner's projection, not a new Task acceptance point.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SelectedTaskTermination {
    identity: TaskIdentity,
    detail: TerminationDetail,
}
impl SelectedTaskTermination {
    pub const fn new(identity: TaskIdentity, detail: TerminationDetail) -> Self {
        Self { identity, detail }
    }
    pub const fn identity(&self) -> TaskIdentity {
        self.identity
    }
    pub const fn detail(&self) -> &TerminationDetail {
        &self.detail
    }
}
