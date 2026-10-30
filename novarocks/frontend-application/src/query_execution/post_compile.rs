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

//! Identity shared by the artifacts produced by one Native plan encoding.

use std::sync::atomic::{AtomicU64, Ordering};

/// One encoding identity; fragments from another encode cannot join this template.
pub(crate) fn mint_native_encoding_provenance() -> u64 {
    static NEXT_PROVENANCE: AtomicU64 = AtomicU64::new(1);
    loop {
        let provenance = NEXT_PROVENANCE.fetch_add(1, Ordering::Relaxed);
        if provenance != 0 {
            return provenance;
        }
    }
}
