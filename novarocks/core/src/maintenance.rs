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

/// Stable maintenance target identity consumed by Core connector facts.
///
/// The table-maintenance execution port and all action values are Frontend
/// query-assembly ownership; this value remains Core-neutral until the MV and
/// connector domains reach their own owner cuts.
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct MaintenanceTarget {
    pub catalog: String,
    pub namespace: String,
    pub table: String,
}
