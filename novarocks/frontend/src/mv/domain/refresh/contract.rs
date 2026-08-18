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

use crate::mv::domain::refresh::apply_key::ApplyKeyContract;
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

/// The logical effect an MV refresh has on its target ref.
///
/// This is the MV application's own vocabulary. It says *what the refresh does
/// to the materialized rows*, never how a table format encodes that — choosing
/// between fast-append, row-delta, deletion vectors or position deletes is the
/// Provider's decision, made from this effect plus the staged reports it owns.
///
/// Keeping the two apart is the point of SPI-5I: before it, every MV refresh
/// site picked an Iceberg `CommitOpKind` directly, so the same logical effect
/// could be spelled three ways in three files with nothing to keep them
/// consistent.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum MvTargetWriteEffect {
    /// Add rows only. No previously materialized row is retracted.
    Append,
    /// Replace everything currently visible on the target ref.
    Overwrite,
    /// Add rows and retract others, where the retracted rows are identified by
    /// the data files the writer already staged.
    DeltaRetractingStagedFiles,
    /// Add rows and retract others, where the caller supplied the retracted row
    /// positions explicitly rather than deriving them from staged files.
    DeltaRetractingExplicitPositions,
}
