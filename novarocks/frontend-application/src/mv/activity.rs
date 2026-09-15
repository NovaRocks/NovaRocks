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

//! Frontend adapters for MV application-owned activity coordination.

use novarocks_mv_application::activity::CanonicalMvTarget;

use novarocks_sql::planning::mv::SqlMvTarget as MvTarget;

/// Adapts a frontend repository target to the product's provider-neutral key.
pub(crate) fn canonical_mv_target(target: &MvTarget) -> CanonicalMvTarget {
    CanonicalMvTarget::from_parts(target.catalog.as_deref(), &target.database, &target.name)
}
