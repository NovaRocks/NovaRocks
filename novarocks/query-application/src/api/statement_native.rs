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

//! Original process-health failures retained by the logical execution owner.

use std::sync::Arc;

/// Process-health failure: this original owner cannot prove a local exit.
/// It is not a domain query failure and cannot change Task/SQL conclusions.
/// The original typed source is retained by the process supervision owner.
#[derive(Clone, Debug)]
pub struct StatementNativeHealthFailure {
    detail: &'static str,
    cause: Arc<dyn std::error::Error + Send + Sync>,
}
impl StatementNativeHealthFailure {
    pub fn actual_exit_unproven(
        detail: &'static str,
        cause: Arc<dyn std::error::Error + Send + Sync>,
    ) -> Self {
        Self { detail, cause }
    }
    pub const fn detail(&self) -> &'static str {
        self.detail
    }
    pub fn cause(&self) -> &(dyn std::error::Error + Send + Sync + 'static) {
        self.cause.as_ref()
    }
    pub(crate) fn into_parts(self) -> (&'static str, Arc<dyn std::error::Error + Send + Sync>) {
        (self.detail, self.cause)
    }
}
