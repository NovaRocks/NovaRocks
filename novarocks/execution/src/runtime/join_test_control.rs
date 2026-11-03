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

//! Explicit, development-only action loan from the Native Task host.
//! Observation cannot install this capability or release a publication.
use crate::runtime::fragment::ExecutionResult;

pub trait JoinBuildPublicationControl: Send + Sync {
    /// Called only at the existing sole data-publication point, outside its lock.
    fn before_publish(&self, node: usize) -> ExecutionResult<bool>;
    /// A held development action uses the native action-control observation cadence.
    fn is_held(&self) -> bool;
}
