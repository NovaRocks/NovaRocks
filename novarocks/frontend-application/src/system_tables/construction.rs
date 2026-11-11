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

//! Snapshot data inside the original planning CPU body. This is retained input,
//! not a task owner: the existing CPU source owns execution, close and health.
use super::source::{MaterializedSystemTableBatch, SystemTableRow};
use novarocks_failure_contract::{FailureClass, FailureProjection};
use novarocks_query_application::system_tables::{SystemTableError, SystemTableSnapshot};
use novarocks_spi::connector::{ConnectorError, ConnectorErrorKind};
use novarocks_sql::{compiler::SqlCompileControl, system_table::SystemTableDeclaration};
use novarocks_type_contract::owned_resources::layout::{LayoutResourceError, arc_layout};
use novarocks_type_contract::{CompileCheckpoints, CompileControlError, CompilePhase};
use novarocks_workload_control::{ResultPayloadBytes, ResultWindowAlias, WorkError, WorkScope};
use std::{alloc::Layout, sync::Arc};

pub(crate) fn control_error(cause: CompileControlError) -> SystemTableError {
    let kind = match cause.failure_fact().class() {
        FailureClass::Resource => ConnectorErrorKind::ResourceExhausted,
        FailureClass::Control => ConnectorErrorKind::Interrupted,
        FailureClass::Data | FailureClass::Representation => ConnectorErrorKind::CorruptData,
        FailureClass::Dependency => ConnectorErrorKind::Unavailable,
        FailureClass::Unsupported => ConnectorErrorKind::Unsupported,
        FailureClass::Invariant | FailureClass::Unexpected => ConnectorErrorKind::Internal,
    };
    ConnectorError::from_domain_static(kind, cause, "System table construction refused").into()
}
fn work_error(error: WorkError) -> SystemTableError {
    control_error(CompileControlError::Source(
        novarocks_failure_contract::SharedFailureCause::new(error),
    ))
}
fn overflow() -> SystemTableError {
    work_error(WorkError::ArithmeticOverflow)
}
pub(crate) fn array_layout<T>(count: usize) -> Result<Layout, SystemTableError> {
    Layout::array::<T>(count).map_err(|_| overflow())
}
pub(crate) fn layout_error(error: LayoutResourceError) -> SystemTableError {
    control_error(CompileControlError::from_domain(error))
}
fn reserve(bytes: &mut ResultPayloadBytes, additional: usize) -> Result<(), SystemTableError> {
    bytes
        .try_grow(u64::try_from(additional).map_err(|_| overflow())?)
        .map_err(work_error)
}
fn release(bytes: &mut ResultPayloadBytes, destroyed: usize) {
    bytes
        .release_destroyed(u64::try_from(destroyed).expect("previously admitted input extent"))
        .expect("original system table input receipt");
}
fn extent_add(a: usize, b: usize) -> Result<usize, SystemTableError> {
    a.checked_add(b).ok_or_else(overflow)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RowStage {
    Staged,
    FlushRequired,
}

pub(crate) struct SystemTableCpuStaging {
    // Destruction order matters: real containers die before the input receipt.
    rows: Vec<SystemTableRow>,
    batches: Vec<MaterializedSystemTableBatch>,
    required_columns: Vec<usize>,
    column_bytes: Vec<usize>,
    input_bytes: ResultPayloadBytes,
    live_row_bytes: usize,
    declaration: Arc<SystemTableDeclaration>,
    root: WorkScope,
    window: ResultWindowAlias,
    control: SqlCompileControl,
}
impl SystemTableCpuStaging {
    pub(crate) fn new(
        declaration: Arc<SystemTableDeclaration>,
        required: &[usize],
        root: WorkScope,
        window: ResultWindowAlias,
        control: SqlCompileControl,
    ) -> Result<Self, SystemTableError> {
        if !window.is_for_scope(&root) {
            return Err(work_error(WorkError::ForeignAuthority));
        }
        root.check().map_err(work_error)?;
        novarocks_type_contract::PureCompileControl::checkpoint(
            &control,
            CompilePhase::LowerProgram,
            0,
        )
        .map_err(control_error)?;
        if required
            .iter()
            .any(|ordinal| *ordinal >= declaration.columns.len())
        {
            return Err(SystemTableError::InvalidRequest(
                "system table projection ordinal is outside declaration".into(),
            ));
        }
        let mut input_bytes = window.reserve_payload_bytes(0).map_err(work_error)?;
        reserve(
            &mut input_bytes,
            array_layout::<usize>(required.len())?
                .size()
                .checked_mul(2)
                .ok_or_else(overflow)?,
        )?;
        let mut required_columns = Vec::new();
        required_columns
            .try_reserve_exact(required.len())
            .map_err(|_| control_error(CompileControlError::ResourceExhausted))?;
        required_columns.extend_from_slice(required);
        let mut column_bytes = Vec::new();
        column_bytes
            .try_reserve_exact(required.len())
            .map_err(|_| control_error(CompileControlError::ResourceExhausted))?;
        column_bytes.resize(required.len(), 0);
        Ok(Self {
            rows: Vec::new(),
            batches: Vec::new(),
            required_columns,
            column_bytes,
            input_bytes,
            live_row_bytes: 0,
            declaration,
            root,
            window,
            control,
        })
    }
    pub(crate) fn stage_row(
        &mut self,
        values: &[Option<&str>],
    ) -> Result<RowStage, SystemTableError> {
        self.root.check().map_err(work_error)?;
        novarocks_type_contract::PureCompileControl::checkpoint(
            &self.control,
            CompilePhase::LowerProgram,
            0,
        )
        .map_err(control_error)?;
        let mut row_bytes = array_layout::<Option<String>>(self.required_columns.len())?.size();
        for (column, &ordinal) in self.required_columns.iter().enumerate() {
            let value = values.get(ordinal).ok_or_else(|| {
                SystemTableError::InvalidRequest(
                    "system table row is narrower than its declaration".into(),
                )
            })?;
            if let Some(value) = value {
                if value.len() > i32::MAX as usize {
                    return Err(SystemTableError::InvalidRequest(
                        "system table UTF8 value exceeds Arrow offset width".into(),
                    ));
                }
                if extent_add(self.column_bytes[column], value.len())? > i32::MAX as usize {
                    if !self.rows.is_empty() {
                        return Ok(RowStage::FlushRequired);
                    }
                    return Err(overflow());
                }
                row_bytes = extent_add(row_bytes, value.len())?;
            }
        }
        let old_capacity = self.rows.capacity();
        let grow = self.rows.len() == old_capacity;
        let next_capacity = if grow {
            old_capacity.max(1).checked_mul(2).ok_or_else(overflow)?
        } else {
            old_capacity
        };
        let top_bytes = if grow {
            array_layout::<SystemTableRow>(next_capacity)?.size()
        } else {
            0
        };
        reserve(&mut self.input_bytes, extent_add(row_bytes, top_bytes)?)?;
        if grow {
            self.rows
                .try_reserve_exact(next_capacity - self.rows.len())
                .map_err(|_| control_error(CompileControlError::ResourceExhausted))?;
            release(
                &mut self.input_bytes,
                array_layout::<SystemTableRow>(old_capacity)?.size(),
            );
        }
        let mut row = Vec::new();
        row.try_reserve_exact(self.required_columns.len())
            .map_err(|_| control_error(CompileControlError::ResourceExhausted))?;
        for (column, &ordinal) in self.required_columns.iter().enumerate() {
            let value = if let Some(value) = values[ordinal] {
                let mut owned = String::new();
                owned
                    .try_reserve_exact(value.len())
                    .map_err(|_| control_error(CompileControlError::ResourceExhausted))?;
                owned.push_str(value);
                self.column_bytes[column] += value.len();
                Some(owned)
            } else {
                None
            };
            row.push(value);
        }
        self.live_row_bytes = extent_add(self.live_row_bytes, row_bytes)?;
        self.rows.push(row);
        Ok(RowStage::Staged)
    }
    pub(crate) fn flush(&mut self) -> Result<(), SystemTableError> {
        if self.rows.is_empty() {
            return Ok(());
        }
        let old_capacity = self.batches.capacity();
        if self.batches.len() == old_capacity {
            let count = old_capacity.max(1).checked_mul(2).ok_or_else(overflow)?;
            reserve(
                &mut self.input_bytes,
                array_layout::<MaterializedSystemTableBatch>(count)?.size(),
            )?;
            self.batches
                .try_reserve_exact(count - self.batches.len())
                .map_err(|_| control_error(CompileControlError::ResourceExhausted))?;
            release(
                &mut self.input_bytes,
                array_layout::<MaterializedSystemTableBatch>(old_capacity)?.size(),
            );
        }
        let batch = crate::query_execution::required_constant_eval::with_root_value_creation(
            &self.root,
            &self.window,
            &self.control,
            |creation| {
                super::batch::build_system_table_batch(
                    &self.declaration,
                    &self.required_columns,
                    &self.rows,
                    creation,
                    &self.control,
                )
                .map(|batch| MaterializedSystemTableBatch {
                    batch,
                    storage: creation.storage_lifetime(),
                })
            },
        )
        .map_err(control_error)??;
        self.batches.push(batch);
        self.rows.clear();
        release(&mut self.input_bytes, self.live_row_bytes);
        self.live_row_bytes = 0;
        self.column_bytes.fill(0);
        Ok(())
    }
    #[cfg(test)]
    pub(crate) fn first_field_for_test(&self) -> Option<std::sync::Weak<arrow::datatypes::Field>> {
        self.batches
            .first()
            .and_then(|batch| batch.batch.schema().fields().first().map(Arc::downgrade))
    }
    pub(crate) fn finish(mut self) -> Result<SystemTableSnapshot, SystemTableError> {
        self.flush()?;
        crate::query_execution::required_constant_eval::with_root_value_creation(
            &self.root,
            &self.window,
            &self.control,
            |creation| {
                let count = self.batches.len();
                creation
                    .request_layout(array_layout::<arrow::record_batch::RecordBatch>(count)?)
                    .map_err(control_error)?;
                creation
                    .request_layout(
                        arc_layout(array_layout::<arrow::record_batch::RecordBatch>(count)?)
                            .map_err(layout_error)?,
                    )
                    .map_err(control_error)?;
                let mut work =
                    CompileCheckpoints::try_new(&self.control, CompilePhase::LowerProgram)
                        .map_err(control_error)?;
                let storage = creation
                    .storage_lifetime()
                    .retaining_flat_observed(
                        self.batches
                            .iter()
                            .map(|batch| Ok::<_, StorageRefusal>(&batch.storage)),
                        |layout| creation.request_layout(layout).map_err(StorageRefusal),
                        &mut work,
                    )
                    .map_err(|error| control_error(error.0))?;
                let mut batches = Vec::new();
                batches
                    .try_reserve_exact(count)
                    .map_err(|_| control_error(CompileControlError::ResourceExhausted))?;
                for batch in self.batches.drain(..) {
                    batches.push(batch.batch);
                }
                let batches: Arc<[arrow::record_batch::RecordBatch]> = batches.into();
                work.finish().map_err(control_error)?;
                SystemTableSnapshot::try_new(batches, storage)
            },
        )
        .map_err(control_error)?
    }
}
struct StorageRefusal(CompileControlError);
impl From<CompileControlError> for StorageRefusal {
    fn from(error: CompileControlError) -> Self {
        Self(error)
    }
}
impl From<LayoutResourceError> for StorageRefusal {
    fn from(error: LayoutResourceError) -> Self {
        Self(CompileControlError::from_domain(error))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use novarocks_query_application::system_tables::{SystemTableLookup, SystemTableRegistry};
    use novarocks_sql::system_table::SystemTableBinding;
    fn declaration() -> Arc<SystemTableDeclaration> {
        let registry = SystemTableRegistry::try_new([]).unwrap();
        let SystemTableLookup::Declared(declaration) = registry.declare(
            &SystemTableBinding::Catalog("ice".into()),
            "information_schema",
            "tables",
        ) else {
            panic!("table declaration");
        };
        declaration
    }
    #[test]
    fn original_window_funds_projected_batches_and_is_retained_by_snapshot_aliases() {
        let (_control, root, capacity, _positions) =
            crate::query_execution::internal_result_cpu::admitted_internal_fixture();
        let window = capacity.try_window_alias().unwrap();
        assert_eq!(window.retained_payload_bytes(), 0);
        let mut staging = SystemTableCpuStaging::new(
            declaration(),
            &[3, 1],
            root.owner.scope(),
            window.clone(),
            SqlCompileControl::unbounded(),
        )
        .unwrap();
        for _ in 0..3 {
            staging
                .stage_row(&[Some("ice"), Some("db"), Some("orders"), Some("BASE TABLE")])
                .unwrap();
        }
        staging.flush().unwrap();
        staging
            .stage_row(&[Some("ice"), Some("other"), Some("orders"), Some("VIEW")])
            .unwrap();
        let snapshot = staging.finish().unwrap();
        assert_eq!(
            snapshot
                .batches
                .iter()
                .map(|b| b.num_rows())
                .collect::<Vec<_>>(),
            [3, 1]
        );
        assert_eq!(snapshot.batches[0].schema().field(0).name(), "table_type");
        assert_eq!(snapshot.batches[0].schema().field(1).name(), "table_schema");
        let first = snapshot.batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .unwrap();
        assert_eq!(first.value(0), "BASE TABLE");
        let retained = window.retained_payload_bytes();
        assert!(retained > 0);
        let alias = snapshot.clone();
        drop(snapshot);
        assert_eq!(window.retained_payload_bytes(), retained);
        assert_eq!(alias.batches[1].num_rows(), 1);
        drop(alias);
        assert_eq!(window.retained_payload_bytes(), 0);
    }
    #[test]
    fn empty_projection_keeps_cardinality_and_original_backing_lifetime() {
        let (_control, root, capacity, _positions) =
            crate::query_execution::internal_result_cpu::admitted_internal_fixture();
        let window = capacity.try_window_alias().unwrap();
        let mut staging = SystemTableCpuStaging::new(
            declaration(),
            &[],
            root.owner.scope(),
            window.clone(),
            SqlCompileControl::unbounded(),
        )
        .unwrap();
        for _ in 0..7 {
            staging.stage_row(&[]).unwrap();
        }
        let snapshot = staging.finish().unwrap();
        assert_eq!(snapshot.batches.len(), 1);
        assert_eq!(snapshot.batches[0].num_columns(), 0);
        assert_eq!(snapshot.batches[0].num_rows(), 7);
        assert!(window.retained_payload_bytes() > 0);
        drop(snapshot);
        assert_eq!(window.retained_payload_bytes(), 0);
    }
    #[test]
    fn original_budget_refusal_preserves_staged_rows_and_refunds_only_destroyed_input() {
        let (_control, root, capacity, _positions) =
            crate::query_execution::internal_result_cpu::admitted_internal_fixture();
        let window = capacity.try_window_alias().unwrap();
        let mut staging = SystemTableCpuStaging::new(
            declaration(),
            &[2],
            root.owner.scope(),
            window.clone(),
            SqlCompileControl::unbounded(),
        )
        .unwrap();
        staging
            .stage_row(&[
                Some("ice"),
                Some("db"),
                Some("original"),
                Some("BASE TABLE"),
            ])
            .unwrap();
        let input_bytes = window.retained_payload_bytes();
        let occupied = window
            .reserve_payload_bytes(window.payload_capacity_bytes() - input_bytes)
            .unwrap();
        assert!(matches!(
            staging.flush(),
            Err(SystemTableError::Provider(_))
        ));
        assert_eq!(staging.rows.len(), 1);
        assert_eq!(staging.rows[0][0].as_deref(), Some("original"));
        assert!(staging.batches.is_empty());
        assert_eq!(
            window.retained_payload_bytes(),
            window.payload_capacity_bytes()
        );
        drop(occupied);
        assert_eq!(window.retained_payload_bytes(), input_bytes);
        let snapshot = staging.finish().unwrap();
        assert_eq!(snapshot.batches[0].num_rows(), 1);
        drop(snapshot);
        assert_eq!(window.retained_payload_bytes(), 0);
    }
}
