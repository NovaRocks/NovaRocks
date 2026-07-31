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

use std::collections::BTreeMap;
use std::sync::Arc;

use novarocks::engine::insert_engine::{
    IcebergInsertCommit, IcebergWriteReport, InsertEngine, PreparedIcebergInsert,
};

use crate::dml::model::{
    CommitOpKind, CommitOutcome, CommitServiceError, OperationKind, OperationTarget,
    WriteTransactionSpec,
};
use crate::dml::runner::{CoordinatedWriteReport, WriteExecutor};

pub(super) struct IcebergInsertWriteExecutor<'a> {
    engine: &'a dyn InsertEngine,
    prepared: &'a PreparedIcebergInsert,
}

impl<'a> IcebergInsertWriteExecutor<'a> {
    pub(super) fn new(engine: &'a dyn InsertEngine, prepared: &'a PreparedIcebergInsert) -> Self {
        Self { engine, prepared }
    }
}

impl WriteExecutor for IcebergInsertWriteExecutor<'_> {
    type CommitHandle = Arc<dyn IcebergInsertCommit>;

    fn run_coordinated_write(
        &self,
        _spec: &WriteTransactionSpec,
    ) -> Result<CoordinatedWriteReport<Self::CommitHandle>, String> {
        Ok(
            match self
                .engine
                .run_iceberg_write(self.prepared.handle.as_ref())?
            {
                IcebergWriteReport::Aborted {
                    reason,
                    has_staged_files,
                } => CoordinatedWriteReport::Aborted {
                    reason,
                    has_staged: has_staged_files,
                },
                IcebergWriteReport::NoOp(handle) => CoordinatedWriteReport::NoOp(handle),
                IcebergWriteReport::Committable(handle) => {
                    CoordinatedWriteReport::Committable(handle)
                }
            },
        )
    }

    fn commit(
        &self,
        _spec: &WriteTransactionSpec,
        handle: &Self::CommitHandle,
    ) -> Result<CommitOutcome, CommitServiceError> {
        self.engine
            .commit_iceberg_write(self.prepared.handle.as_ref(), handle.as_ref())
    }

    fn finalize(&self, _spec: &WriteTransactionSpec) -> Result<(), String> {
        self.engine
            .finalize_iceberg_write(self.prepared.handle.as_ref())
    }
}

pub(super) fn write_transaction_spec(prepared: &PreparedIcebergInsert) -> WriteTransactionSpec {
    let operation = &prepared.operation;
    WriteTransactionSpec {
        target: OperationTarget {
            catalog: operation.catalog.clone(),
            namespace: operation.namespace.clone(),
            table: operation.table.clone(),
            ref_name: (operation.target_ref != "main").then(|| operation.target_ref.clone()),
        },
        operation_kind: match operation.commit_op_kind {
            CommitOpKind::FastAppend => OperationKind::InsertAppend,
            _ => OperationKind::InsertOverwrite,
        },
        commit_op_kind: operation.commit_op_kind,
        attempt_id: operation.attempt_id.clone(),
        base_snapshot_id: operation.base_snapshot_id,
        base_snapshot_map: BTreeMap::new(),
    }
}
