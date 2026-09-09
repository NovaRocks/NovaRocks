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

use novarocks_execution_contract::TerminationDetail;

/// Query coordination's first failure-conclusion latch.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct TerminationLatch {
    first: Option<TerminationDetail>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum LatchOutcome {
    Won,
    Lost(TerminationDetail),
    Refined(TerminationDetail),
}

impl TerminationLatch {
    pub const fn open() -> Self {
        Self { first: None }
    }

    pub const fn cause(&self) -> Option<&TerminationDetail> {
        self.first.as_ref()
    }

    pub const fn is_latched(&self) -> bool {
        self.first.is_some()
    }

    pub fn latch(&mut self, cause: TerminationDetail) -> LatchOutcome {
        match &self.first {
            Some(existing) if existing.is_derived() && !cause.is_derived() => {
                let replaced = existing.clone();
                self.first = Some(cause);
                LatchOutcome::Refined(replaced)
            }
            Some(existing) => LatchOutcome::Lost(existing.clone()),
            None => {
                self.first = Some(cause);
                LatchOutcome::Won
            }
        }
    }
}
