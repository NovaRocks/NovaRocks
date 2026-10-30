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

use std::fmt;

use novarocks_execution_contract::{
    FinalTaskInfo, IdentityMismatch, TaskStatus, TaskStatusVersion,
};

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum GoneObservation {
    RetentionEnded,
    TerminalNeverObserved,
}

pub fn classify_gone(held: Option<&TaskStatus>) -> GoneObservation {
    if held.is_some_and(TaskStatus::is_terminal) {
        GoneObservation::RetentionEnded
    } else {
        GoneObservation::TerminalNeverObserved
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum FinalInfoDisagreement {
    Identity(IdentityMismatch),
    TerminalVersion {
        observed: TaskStatusVersion,
        carried: TaskStatusVersion,
    },
    SnapshotConflict(TaskStatusVersion),
}

impl fmt::Display for FinalInfoDisagreement {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Identity(mismatch) => write!(formatter, "final task info {mismatch}"),
            Self::TerminalVersion { observed, carried } => write!(
                formatter,
                "final task info carries terminal version {carried}, but version {observed} was observed"
            ),
            Self::SnapshotConflict(version) => write!(
                formatter,
                "final task info carries a different snapshot under terminal version {version}"
            ),
        }
    }
}

impl std::error::Error for FinalInfoDisagreement {}

pub fn verify_final_info(
    observed_terminal: &TaskStatus,
    info: &FinalTaskInfo,
) -> Result<(), FinalInfoDisagreement> {
    let carried = info.final_status();
    if let Err(mismatch) = observed_terminal
        .identity()
        .verify_matches(carried.identity())
    {
        return Err(FinalInfoDisagreement::Identity(mismatch));
    }
    if carried.version() != observed_terminal.version() {
        return Err(FinalInfoDisagreement::TerminalVersion {
            observed: observed_terminal.version(),
            carried: carried.version(),
        });
    }
    if carried != observed_terminal {
        return Err(FinalInfoDisagreement::SnapshotConflict(carried.version()));
    }
    Ok(())
}
