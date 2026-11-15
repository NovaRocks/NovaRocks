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
use super::source::{MaterializedSystemTableBatch, SystemTableRow, SystemValue, SystemValueRef};
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

pub(super) fn validate_value(
    value: SystemValueRef<'_>,
    ty: &arrow::datatypes::DataType,
    nullable: bool,
) -> Result<(), SystemTableError> {
    use arrow::datatypes::{DataType, TimeUnit};
    let valid = match value {
        SystemValueRef::Null => nullable,
        SystemValueRef::Utf8(_) => *ty == DataType::Utf8,
        SystemValueRef::Int64(_) => *ty == DataType::Int64,
        SystemValueRef::Boolean(_) => *ty == DataType::Boolean,
        SystemValueRef::TimestampMillis(_) => {
            matches!(ty, DataType::Timestamp(TimeUnit::Millisecond, _))
        }
        SystemValueRef::Utf8List(_) => {
            matches!(ty, DataType::List(child) if *child.data_type() == DataType::Utf8 && !child.is_nullable())
        }
    };
    if valid {
        Ok(())
    } else {
        Err(SystemTableError::Internal(
            "system-table value violates its declared type or nullability".into(),
        ))
    }
}
fn value_extent(value: SystemValueRef<'_>) -> Result<(usize, usize, usize), SystemTableError> {
    match value {
        SystemValueRef::Utf8(value) => Ok((value.len(), 0, value.len())),
        SystemValueRef::Utf8List(values) => {
            let payload = values
                .iter()
                .try_fold(0usize, |n, value| extent_add(n, value.len()))?;
            Ok((
                payload,
                values.len(),
                extent_add(array_layout::<String>(values.len())?.size(), payload)?,
            ))
        }
        _ => Ok((0, 0, 0)),
    }
}
fn copy_text(value: &str) -> Result<String, SystemTableError> {
    let mut owned = String::new();
    owned
        .try_reserve_exact(value.len())
        .map_err(|_| control_error(CompileControlError::ResourceExhausted))?;
    owned.push_str(value);
    Ok(owned)
}
pub(super) fn copy_value(value: SystemValueRef<'_>) -> Result<SystemValue, SystemTableError> {
    Ok(match value {
        SystemValueRef::Null => SystemValue::Null,
        SystemValueRef::Utf8(value) => SystemValue::Utf8(copy_text(value)?),
        SystemValueRef::Int64(value) => SystemValue::Int64(value),
        SystemValueRef::Boolean(value) => SystemValue::Boolean(value),
        SystemValueRef::TimestampMillis(value) => SystemValue::TimestampMillis(value),
        SystemValueRef::Utf8List(values) => {
            let mut copied = Vec::new();
            copied
                .try_reserve_exact(values.len())
                .map_err(|_| control_error(CompileControlError::ResourceExhausted))?;
            for value in values {
                copied.push(copy_text(value)?);
            }
            SystemValue::Utf8List(copied)
        }
    })
}
pub(crate) struct SystemTableCpuStaging {
    // Destruction order matters: real containers die before the input receipt.
    rows: Vec<SystemTableRow>,
    batches: Vec<MaterializedSystemTableBatch>,
    required_columns: Vec<usize>,
    column_bytes: Vec<(usize, usize)>,
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
            extent_add(
                array_layout::<usize>(required.len())?.size(),
                array_layout::<(usize, usize)>(required.len())?.size(),
            )?,
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
        column_bytes.resize(required.len(), (0, 0));
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
        values: &[SystemValueRef<'_>],
    ) -> Result<RowStage, SystemTableError> {
        self.root.check().map_err(work_error)?;
        novarocks_type_contract::PureCompileControl::checkpoint(
            &self.control,
            CompilePhase::LowerProgram,
            0,
        )
        .map_err(control_error)?;
        let mut row_bytes = array_layout::<SystemValue>(self.required_columns.len())?.size();
        for (column, &ordinal) in self.required_columns.iter().enumerate() {
            let value = values.get(ordinal).ok_or_else(|| {
                SystemTableError::InvalidRequest(
                    "system table row is narrower than its declaration".into(),
                )
            })?;
            let declared = &self.declaration.columns[ordinal].value_type;
            validate_value(*value, declared.data_type(), declared.nullable())?;
            let (payload, items, owned_bytes) = value_extent(*value)?;
            let (old_payload, old_items) = self.column_bytes[column];
            if extent_add(old_payload, payload)? > i32::MAX as usize
                || extent_add(old_items, items)? > i32::MAX as usize
            {
                if !self.rows.is_empty() {
                    return Ok(RowStage::FlushRequired);
                }
                return Err(SystemTableError::InvalidRequest(
                    "system table value exceeds Arrow offset width".into(),
                ));
            }
            row_bytes = extent_add(row_bytes, owned_bytes)?;
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
        for &ordinal in &self.required_columns {
            row.push(copy_value(values[ordinal])?);
        }
        for (column, &ordinal) in self.required_columns.iter().enumerate() {
            let (payload, items, _) = value_extent(values[ordinal])?;
            self.column_bytes[column].0 += payload;
            self.column_bytes[column].1 += items;
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
        self.column_bytes.fill((0, 0));
        Ok(())
    }
    #[cfg(test)]
    fn stage_text_row<const N: usize>(
        &mut self,
        values: &[Option<&str>; N],
    ) -> Result<RowStage, SystemTableError> {
        self.stage_row(&values.map(SystemValueRef::from))
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
                .stage_text_row(&[Some("ice"), Some("db"), Some("orders"), Some("BASE TABLE")])
                .unwrap();
        }
        staging.flush().unwrap();
        staging
            .stage_text_row(&[Some("ice"), Some("other"), Some("orders"), Some("VIEW")])
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
            staging.stage_text_row(&[]).unwrap();
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
            .stage_text_row(&[
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
        assert_eq!(staging.rows[0][0], SystemValue::Utf8("original".into()));
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
    fn state_declaration() -> Arc<SystemTableDeclaration> {
        let registry = novarocks_query_application::system_tables::SystemTableRegistry::try_new([
            crate::system_tables::materialized_view_states_registration(),
        ])
        .unwrap();
        let novarocks_query_application::system_tables::SystemTableLookup::Declared(declaration) =
            registry.declare(
                &novarocks_sql::system_table::SystemTableBinding::System,
                "runtime",
                "materialized_view_states",
            )
        else {
            panic!("state table declaration")
        };
        declaration
    }
    #[test]
    fn typed_batches_keep_values_child_nullability_and_original_window_until_alias_exit() {
        use arrow::array::{
            Array, BooleanArray, Int64Array, ListArray, StringArray, TimestampMillisecondArray,
        };
        let (_control, root, capacity, _positions) =
            crate::query_execution::internal_result_cpu::admitted_internal_fixture();
        let window = capacity.try_window_alias().unwrap();
        let mut staging = SystemTableCpuStaging::new(
            state_declaration(),
            &[4, 5, 6, 7, 10, 11],
            root.owner.scope(),
            window.clone(),
            SqlCompileControl::unbounded(),
        )
        .unwrap();
        let names = vec!["ice.db.base".into(), "ice.db.other".into()];
        let values = [
            SystemValueRef::Null,
            SystemValueRef::Null,
            SystemValueRef::Null,
            SystemValueRef::Null,
            SystemValueRef::Int64(60000),
            SystemValueRef::Boolean(true),
            SystemValueRef::TimestampMillis(1234),
            SystemValueRef::Null,
            SystemValueRef::Null,
            SystemValueRef::Null,
            SystemValueRef::Utf8List(&names),
            SystemValueRef::Null,
        ];
        staging.stage_row(&values).unwrap();
        assert!(
            window.retained_payload_bytes() > names.iter().map(String::len).sum::<usize>() as u64
        );
        let snapshot = staging.finish().unwrap();
        let batch = &snapshot.batches[0];
        assert_eq!(
            batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0),
            60000
        );
        assert!(
            batch
                .column(1)
                .as_any()
                .downcast_ref::<BooleanArray>()
                .unwrap()
                .value(0)
        );
        assert_eq!(
            batch
                .column(2)
                .as_any()
                .downcast_ref::<TimestampMillisecondArray>()
                .unwrap()
                .value(0),
            1234
        );
        assert!(batch.column(3).is_null(0));
        assert!(batch.column(5).is_null(0));
        let array = batch
            .column(4)
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap();
        let child = array.value(0);
        let child = child.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(
            child.iter().collect::<Vec<_>>(),
            [Some("ice.db.base"), Some("ice.db.other")]
        );
        let alias = snapshot.clone();
        let retained = window.retained_payload_bytes();
        drop(snapshot);
        assert_eq!(window.retained_payload_bytes(), retained);
        drop(alias);
        assert_eq!(window.retained_payload_bytes(), 0);
    }
    #[test]
    fn typed_input_rejects_wrong_types_and_nullability_before_copy_and_flushes_list_geometry() {
        let (_control, root, capacity, _positions) =
            crate::query_execution::internal_result_cpu::admitted_internal_fixture();
        let window = capacity.try_window_alias().unwrap();
        let mut staging = SystemTableCpuStaging::new(
            state_declaration(),
            &[10],
            root.owner.scope(),
            window.clone(),
            SqlCompileControl::unbounded(),
        )
        .unwrap();
        let mut values = [SystemValueRef::Null; 12];
        assert!(staging.stage_row(&values).is_err());
        values[10] = SystemValueRef::Utf8("not an array");
        assert!(staging.stage_row(&values).is_err());
        let names = vec!["x".into()];
        values[10] = SystemValueRef::Utf8List(&names);
        staging.stage_row(&values).unwrap();
        staging.column_bytes[0].1 = i32::MAX as usize;
        assert_eq!(staging.stage_row(&values).unwrap(), RowStage::FlushRequired);
        assert_eq!(staging.rows.len(), 1);
        staging.column_bytes[0].1 = 1;
        staging.flush().unwrap();
        assert_eq!(staging.stage_row(&values).unwrap(), RowStage::Staged);
        drop(staging);
        assert_eq!(window.retained_payload_bytes(), 0);
    }
}
