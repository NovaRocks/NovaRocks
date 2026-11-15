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

//! Concrete application executors. The FE carrier invokes their methods and
//! owns the original work exit; executors return passive results only.

use std::sync::Arc;

/// All application owners for one installed logical execution. The carrier
/// retains this exact set through cleanup; no process-global executor registry.
#[derive(Clone)]
pub struct LogicalExecutors {
    pub native: Arc<NativeExecutor>,
    pub relay: Arc<RelayExecutor>,
    pub consumer: Arc<ConsumerExecutor>,
    pub cancel: Arc<CancelObserver>,
    pub governance: Arc<GovernanceExecutor>,
}

// P07 declares concrete owners before P08 supplies their original resource
// fields and methods in separate module trees. None has a public constructor.
pub struct NativeExecutor {
    _private: (),
}
pub struct RelayExecutor {
    _private: (),
}
pub struct ConsumerExecutor {
    _private: (),
}
pub struct CancelObserver {
    _private: (),
}
pub struct GovernanceExecutor {
    _private: (),
}
