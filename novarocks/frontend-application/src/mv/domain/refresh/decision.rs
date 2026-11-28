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

//! Publication decision at the original MV computation boundary.

use novarocks_result_contract::DeleteVerificationCounts;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum VerificationDecision {
    Publish,
    Retire(DeleteVerificationCounts),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum VerificationIncomplete {
    SplitCoverage,
    MissingDeleteVerification,
    UnexpectedDeleteVerification,
    InvalidDeleteVerification,
}

impl std::fmt::Display for VerificationIncomplete {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::SplitCoverage => {
                "MV refresh did not prove complete scan enumeration and delivery"
            }
            Self::MissingDeleteVerification => {
                "MV content location did not produce its delete verification summary"
            }
            Self::UnexpectedDeleteVerification => {
                "MV refresh produced an unauthorized delete verification summary"
            }
            Self::InvalidDeleteVerification => {
                "MV delete verification counts contradict their original contract"
            }
        })
    }
}

impl std::error::Error for VerificationIncomplete {}

pub(crate) fn verify_native_refresh(
    requires_delete_verification: bool,
    complete_coverage: bool,
    summary: Option<DeleteVerificationCounts>,
) -> Result<VerificationDecision, VerificationIncomplete> {
    if !complete_coverage {
        return Err(VerificationIncomplete::SplitCoverage);
    }
    let counts = match (requires_delete_verification, summary) {
        (false, None) => return Ok(VerificationDecision::Publish),
        (false, Some(_)) => return Err(VerificationIncomplete::UnexpectedDeleteVerification),
        (true, None) => return Err(VerificationIncomplete::MissingDeleteVerification),
        (true, Some(counts)) => counts,
    };
    match counts.requested().checked_sub(counts.matched()) {
        Some(0) if counts.insufficient_keys() == 0 => Ok(VerificationDecision::Publish),
        Some(missing)
            if counts.insufficient_keys() > 0 && counts.insufficient_keys() <= missing =>
        {
            Ok(VerificationDecision::Retire(counts))
        }
        _ => Err(VerificationIncomplete::InvalidDeleteVerification),
    }
}

#[cfg(test)]
mod tests;
