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

/// A frozen lookup-node endpoint consumed by the local fetch operator.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LookupNodeInfo {
    pub id: i64,
    pub option: i64,
    pub host: String,
    pub async_internal_port: u16,
}

/// The immutable lookup-node snapshot attached to a fragment plan.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LookupNodesInfo {
    pub version: i64,
    pub nodes: Vec<LookupNodeInfo>,
}
