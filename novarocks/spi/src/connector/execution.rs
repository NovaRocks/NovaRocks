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

use super::{ConnectorError, ConnectorErrorKind, ConnectorRequestContext};

/// A hard bound on the independently schedulable physical leaves carried by
/// one frontend-frozen connector split. This is deliberately independent of
/// the native carrier: providers must fail preparation rather than truncate a
/// sealed membership.
pub const MAX_CONNECTOR_PREPARED_SCAN_UNITS_PER_SPLIT: usize = 4096;

#[derive(Clone)]
pub struct ConnectorPrepareSplitRequest {
    pub context: ConnectorRequestContext,
}

impl ConnectorPrepareSplitRequest {
    pub fn check_active(&self) -> Result<(), ConnectorError> {
        if self.context.is_cancelled() {
            return Err(ConnectorError::new(
                ConnectorErrorKind::Cancelled,
                "connector split preparation was cancelled",
            ));
        }
        if std::time::Instant::now() >= self.context.deadline() {
            return Err(ConnectorError::new(
                ConnectorErrorKind::DeadlineExceeded,
                "connector split preparation deadline elapsed",
            ));
        }
        Ok(())
    }
}
