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
use super::*;
use crate::exec::pipeline::operator::input_construction::try_runtime_input_construction_factory;
use crate::runtime::mem_tracker::MemTracker;
use crate::runtime::task::{ExecutionFailure, ExecutionFailureCause, PipelineOperation};
use arrow::array::StringArray;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::{RecordBatch, RecordBatchOptions};
use novarocks_local_program::StaticLayout;
use novarocks_type_contract::{
    TypedValueBinding, ValueKind, ValueLayout, ValueLogicalType, ValueTypeRef,
    ValueTypeStorageLifetime,
};
struct Control;
impl novarocks_type_contract::PureCompileControl for Control {
    fn checkpoint(
        &self,
        _: novarocks_type_contract::CompilePhase,
        _: u32,
    ) -> Result<(), novarocks_type_contract::CompileControlError> {
        Ok(())
    }
}
fn values(zero_columns: bool) -> StaticValues {
    let storage = ValueTypeStorageLifetime::pure();
    let binding = TypedValueBinding::try_new(
        ValueTypeRef::try_new(
            ValueKind::Nominal(ValueLogicalType::Json),
            false,
            storage.clone(),
        )
        .unwrap(),
        ValueLayout::try_new(&DataType::Utf8, storage.clone()).unwrap(),
    )
    .unwrap();
    let schema = Arc::new(Schema::new(if zero_columns {
        Vec::<Field>::new()
    } else {
        vec![Field::new("value", DataType::Utf8, false)]
    }));
    let layout = StaticLayout::try_new_typed_for_compile(
        schema.clone(),
        Arc::from(if zero_columns {
            Vec::new()
        } else {
            vec![novarocks_types::SlotId::new(3)]
        }),
        if zero_columns {
            Vec::new()
        } else {
            vec![(binding, None)]
        },
        storage,
        |_| Ok(()),
        &Control,
    )
    .unwrap();
    let batch = if zero_columns {
        RecordBatch::try_new_with_options(
            schema,
            Vec::new(),
            &RecordBatchOptions::new().with_row_count(Some(7)),
        )
        .unwrap()
    } else {
        RecordBatch::try_new(
            schema,
            vec![Arc::new(StringArray::from(vec!["{\"v\":1}", "null"])) as ArrayRef],
        )
        .unwrap()
    };
    StaticValues::try_new(batch, layout).unwrap()
}
fn fixture() -> (
    RuntimeState,
    Arc<MemTracker>,
    Arc<dyn InputConstructionFactory>,
) {
    let state = RuntimeState::for_local_execution(None, None, None, None, None, None, None);
    let tracker = MemTracker::new_root("native-values-real-creation");
    let factory = try_runtime_input_construction_factory(
        crate::exec::pipeline::pipeline::representation_construction_profile().unwrap(),
        tracker.clone(),
        state.error_state(),
    )
    .unwrap();
    (state, tracker, factory)
}
#[test]
fn native_values_share_original_payload_and_last_projection_keeps_creation() {
    let values = values(false);
    let original = values.batch_projection().unwrap();
    let factory = TypedValuesSourceFactory::try_new(values, 2).unwrap();
    let (state, tracker, host) = fixture();
    let mut operator = factory.create(1, 0);
    operator.set_construction_factory(host.clone());
    operator.prepare().unwrap();
    let output = operator
        .drive_for_test(&state)
        .map(|step| step.output)
        .unwrap()
        .unwrap();
    assert!(
        Arc::ptr_eq(&output.columns()[0], original.record_batch().column(0)),
        "original Arrow payload/reference is shared"
    );
    assert_eq!(
        output.chunk_schema().slots()[0]
            .typed_binding()
            .value_type()
            .domain(),
        Some(ValueLogicalType::Json)
    );
    let projection = output.into_batch_projection();
    drop(operator);
    drop(factory);
    drop(original);
    drop(host);
    assert!(tracker.current() > 0);
    drop(projection);
    assert_eq!(tracker.current(), 0);
}
#[test]
fn native_values_keep_original_zero_column_row_count_and_refuse_missing_owner() {
    let factory = TypedValuesSourceFactory::try_new(values(true), 2).unwrap();
    let mut operator = factory.create(1, 0);
    assert!(matches!(
        operator.prepare().unwrap_err().cause(),
        ExecutionFailureCause::Construction(ConstructionFailure::MissingOwner)
    ));
    drop(operator);
    let mut operator = factory.create(1, 0);
    let (state, tracker, host) = fixture();
    operator.set_construction_factory(host.clone());
    operator.prepare().unwrap();
    let output = operator
        .drive_for_test(&state)
        .map(|step| step.output)
        .unwrap()
        .unwrap();
    assert_eq!(output.len(), 7);
    assert!(output.columns().is_empty());
    drop(output);
    drop(operator);
    drop(factory);
    drop(host);
    assert_eq!(tracker.current(), 0);
}
#[test]
fn native_values_first_memory_and_original_caller_refusal_have_no_allocation_tail() {
    let factory = TypedValuesSourceFactory::try_new(values(false), 2).unwrap();
    let (_state, tracker, host) = fixture();
    let mut operator = factory.create(1, 0);
    operator.set_construction_factory(host.clone());
    let baseline = tracker.current();
    tracker.install_limit_once(baseline).unwrap();
    let failure = operator.prepare().unwrap_err();
    assert!(matches!(
        failure.cause(),
        ExecutionFailureCause::Construction(ConstructionFailure::Memory(_))
    ));
    assert_eq!(tracker.current(), baseline);
    drop(operator);
    drop(host);
    assert_eq!(tracker.current(), 0);
    let (state, tracker, host) = fixture();
    let mut operator = factory.create(1, 0);
    operator.set_construction_factory(host.clone());
    let original = ExecutionFailure::from(novarocks_functions::KernelFailure::Internal(
        novarocks_functions::KernelDiagnostic::new("original Values caller"),
    ))
    .at_operator(2, PipelineOperation::Admission);
    state.error_state().set_error(original.clone());
    let before = tracker.allocated();
    let failure = operator.prepare().unwrap_err();
    assert_eq!(failure.cause(), original.cause());
    assert_eq!(failure.site(), original.site());
    assert_eq!(
        tracker.allocated(),
        before,
        "no owner/header allocation after Caller refusal"
    );
    drop(operator);
    drop(host);
    assert_eq!(tracker.current(), 0);
}
