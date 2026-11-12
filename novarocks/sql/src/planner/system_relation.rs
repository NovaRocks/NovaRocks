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

//! System-table leaves retain declarations through optimization; data arrives
//! only in the provider completion round and keeps its original storage.
use crate::common::OutputColumn;
use crate::system_table::{SystemTableDeclaration, SystemTableKeyConstraint};
use arrow::record_batch::RecordBatch;
use novarocks_type_contract::ValueTypeStorageLifetime;
use std::sync::Arc;

#[derive(Clone, Debug)]
pub(crate) struct PlanSystemTableNode {
    pub declaration: Arc<SystemTableDeclaration>,
    pub columns: Vec<OutputColumn>,
    /// Original declaration ordinals, aligned exactly with columns. Empty is
    /// a real zero-column projection and preserves the snapshot cardinality.
    pub required_columns: Vec<usize>,
}
#[derive(Clone, Debug)]
pub(crate) enum PhysicalSystemTableState {
    Planning,
    Finalized {
        batches: Arc<[RecordBatch]>,
        storage: ValueTypeStorageLifetime,
    },
}
#[derive(Clone, Debug)]
pub(crate) struct PhysicalSystemTableNode {
    pub relation: PlanSystemTableNode,
    pub key_constraint: SystemTableKeyConstraint,
    pub state: PhysicalSystemTableState,
    pub occurrence: Option<crate::system_table::SystemTableOccurrenceId>,
}
impl PhysicalSystemTableNode {
    pub fn planning(relation: PlanSystemTableNode) -> Self {
        Self {
            relation,
            occurrence: None,
            key_constraint: SystemTableKeyConstraint::default(),
            state: PhysicalSystemTableState::Planning,
        }
    }
}
