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

use crate::common::types::UniqueId;

/// Temporary consumer-owned bridge for a synchronous fragment execution request.
///
/// The consumer intentionally sees only an encoded payload and a neutral fragment
/// identity. Compat owns the protocol decode and concrete execution. RCI-5D removes
/// this bridge together with the remaining core stream-load route owner.
pub trait SyncFragmentExecutor: Send + Sync + 'static {
    fn execute_encoded(&self, payload: &[u8]) -> Result<UniqueId, String>;
}
