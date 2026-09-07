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

//! The failure vocabulary of the backend runtime-filter contract.
//!
//! Every path that produces one of these is a runtime-filter contract path:
//! decoding a participant contribution, installing a participant, binding a
//! fragment's session, sealing a terminal projection, or closing a
//! participant. The one consumer that maps it onto a task failure category is
//! a function named `runtime_filter_rejection`, so the vocabulary is named for
//! what actually produces it rather than for the lifecycle owner it used to
//! travel through.
//!
//! Both codes are constructed by this crate's own runtime-filter paths. There
//! is deliberately no code for registry state, admission, capacity, or
//! transport: those were categories of the owner this vocabulary used to
//! belong to, and nothing in a runtime-filter contract path can produce one.

/// Why a runtime-filter contract path refused.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RuntimeFilterContractErrorCode {
    /// The contribution, install, session binding, or terminal projection is
    /// structurally illegal or disagrees with the attempt it names.
    InvalidContract,
    /// The participant that would have to answer is not the one this attempt
    /// installed, so there is nothing left to bind.
    ParticipantClosed,
}

/// One runtime-filter contract refusal, with the detail its producer wrote.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct RuntimeFilterContractError {
    code: RuntimeFilterContractErrorCode,
    detail: String,
}

impl RuntimeFilterContractError {
    pub(crate) fn new(code: RuntimeFilterContractErrorCode, detail: impl Into<String>) -> Self {
        Self {
            code,
            detail: detail.into(),
        }
    }

    pub(crate) fn invalid_contract(detail: impl Into<String>) -> Self {
        Self::new(RuntimeFilterContractErrorCode::InvalidContract, detail)
    }

    pub(crate) const fn code(&self) -> RuntimeFilterContractErrorCode {
        self.code
    }

    pub(crate) fn detail(&self) -> &str {
        &self.detail
    }
}

impl std::fmt::Display for RuntimeFilterContractError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{:?}: {}", self.code, self.detail)
    }
}

impl std::error::Error for RuntimeFilterContractError {}
