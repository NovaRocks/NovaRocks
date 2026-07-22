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

use crate::mv::refresh::apply_key::ApplyKeyContract;
use novarocks_catalog::identifier::TableIdentity;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ImvRefreshContract {
    pub(crate) base_refs: Vec<TableIdentity>,
    pub(crate) apply_key: ApplyKeyContract,
    pub(crate) aggregate: Option<AggregateRefreshContract>,
    pub(crate) join: Option<JoinRefreshContract>,
    pub(crate) branch: Option<BranchRefreshContract>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct AggregateRefreshContract {
    pub(crate) group_key_count: usize,
    pub(crate) aggregate_count: usize,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct JoinRefreshContract {
    pub(crate) join_key_count: usize,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct BranchRefreshContract {
    pub(crate) branch_count: usize,
}
