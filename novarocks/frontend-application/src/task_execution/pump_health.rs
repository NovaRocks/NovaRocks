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

//! Unresolved exit evidence from an original attempt pump.

use std::sync::Arc;

#[derive(Clone, Debug)]
pub(crate) struct PumpCloseUnresolved {
    pub(crate) owner: &'static str,
    pub(crate) cause: Arc<dyn std::error::Error + Send + Sync>,
}
impl std::fmt::Display for PumpCloseUnresolved {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "the original {} close job did not exit normally",
            self.owner
        )
    }
}
impl std::error::Error for PumpCloseUnresolved {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.cause.as_ref())
    }
}
