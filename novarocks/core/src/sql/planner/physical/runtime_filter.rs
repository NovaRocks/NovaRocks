#![allow(dead_code)]
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

//! Physical-stage runtime-filter intent and execution vocabulary.
//!
//! Intent annotations are complete after placement, before fragment topology
//! exists. They therefore carry no optional fragment-routing fields.

use std::num::NonZeroU32;

use crate::runtime_filter::model::contract::{NullOrder, SortDirection};
use crate::sql::analysis::TypedExpr;

#[derive(Clone, Debug)]
pub(crate) struct RuntimeFilterBuildIntent {
    pub filter_id: i32,
    pub build_expr: TypedExpr,
    pub probe_expr: TypedExpr,
    pub expr_order: usize,
    pub execution_mode: JoinExecutionMode,
}

#[derive(Clone, Debug)]
pub(crate) struct RuntimeFilterProbeIntent {
    pub filter_id: i32,
    pub probe_expr: TypedExpr,
}

#[derive(Clone, Debug)]
pub(crate) struct AggregateTopNRuntimeFilterBuildIntent {
    pub filter_id: i32,
    pub group_key_expr: TypedExpr,
    pub group_key_ordinal: usize,
    pub limit: NonZeroU32,
    pub direction: SortDirection,
    pub null_order: NullOrder,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum JoinExecutionMode {
    Broadcast,
    Partitioned,
    Colocate,
}
