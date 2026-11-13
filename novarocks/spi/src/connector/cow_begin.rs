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

//! Move-only checked COW begin custody. Ordinary writes keep their entrypoint.

use super::{ConnectorError, OriginalResultCheckError};
use std::{error::Error, fmt};

pub enum ConnectorCowBeginCause {
    Provider(ConnectorError),
    OriginalResult(OriginalResultCheckError),
}
impl From<ConnectorError> for ConnectorCowBeginCause {
    fn from(e: ConnectorError) -> Self {
        Self::Provider(e)
    }
}
impl From<OriginalResultCheckError> for ConnectorCowBeginCause {
    fn from(e: OriginalResultCheckError) -> Self {
        Self::OriginalResult(e)
    }
}
impl fmt::Debug for ConnectorCowBeginCause {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Provider(_) => "COW provider failure",
            Self::OriginalResult(_) => "COW original result failure",
        })
    }
}
impl fmt::Display for ConnectorCowBeginCause {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("copy-on-write begin failed")
    }
}
impl Error for ConnectorCowBeginCause {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(match self {
            Self::Provider(e) => e,
            Self::OriginalResult(e) => e,
        })
    }
}

impl novarocks_failure_contract::FailureProjection for ConnectorCowBeginCause {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        match self {
            Self::Provider(error) => error.failure_fact(),
            Self::OriginalResult(error) => error.failure_fact(),
        }
    }
}
