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

//! Original formatter comparisons validate geometry, not a granted operation.
//! Actual grant/refusal/last-Drop probes live in Execution.
use super::*;
use crate::KernelEvaluationControl;
use arrow_schema::{TimeUnit, UnionFields, UnionMode};
use std::{sync::Arc, time::Duration};
struct Control;
impl KernelEvaluationControl for Control {
    fn checkpoint(&self, _work: u32) -> Result<(), KernelFailure> {
        Ok(())
    }
    fn wait(&self, _duration: Duration) -> Result<(), KernelFailure> {
        panic!("synchronous concat must not wait")
    }
}
#[test]
fn by_copy_original_concat_diagnostic_geometry_preserves_all_original_type_format_extents() {
    let mut field = Field::new(
        "a field with ' and Unicode 窗口\n".repeat(20),
        DataType::Utf8,
        false,
    );
    field.set_metadata(std::collections::HashMap::<String, String>::from([
        ("z-last".into(), "large-original-value".repeat(100)),
        ("a-first".into(), "escaped\n\tvalue".into()),
    ]));
    let field = Arc::new(field);
    let fields = vec![
        field.clone(),
        Arc::new(Field::new("item", DataType::Decimal128(9, -3), false)),
    ];
    let union =
        UnionFields::try_new(vec![0, 127], fields.clone()).expect("union field IDs are distinct");
    let cases = vec![
        DataType::Null,
        DataType::Int64,
        DataType::Decimal256(76, -128),
        DataType::Timestamp(
            TimeUnit::Microsecond,
            Some("long-original-zone".repeat(100).into()),
        ),
        DataType::List(field.clone()),
        DataType::LargeList(field.clone()),
        DataType::ListView(field.clone()),
        DataType::LargeListView(field.clone()),
        DataType::FixedSizeList(field.clone(), -2147483648),
        DataType::Struct(fields.into()),
        DataType::Union(union, UnionMode::Sparse),
        DataType::Map(field.clone(), true),
        DataType::Dictionary(
            Box::new(DataType::Int8),
            Box::new(DataType::List(field.clone())),
        ),
        DataType::RunEndEncoded(
            Arc::new(Field::new("run_ends", DataType::Int16, false)),
            field,
        ),
    ];
    for ty in cases {
        let mut work = EvaluationCheckpoints::new(&Control);
        let facts = TypeFormatGeometry::data_type(&ty, &mut work).unwrap();
        let text = ty.to_string();
        assert!(
            facts.bytes >= text.len(),
            "original formatter geometry for {ty:?}"
        );
        work.finish().unwrap();
    }
}
#[test]
fn by_copy_original_concat_dictionary_resource_domain_does_not_apply_native_key_gate() {
    use arrow_array::{ArrayRef, StringArray};
    let left: ArrayRef = Arc::new(StringArray::from(vec!["first"; 70]));
    let right: ArrayRef = Arc::new(StringArray::from(vec!["second"; 70]));
    let mut invoice = super::super::take_host::CopyInvoiceTotals::default();
    invoice
        .original_dictionary_merge_scratch(&[left.as_ref(), right.as_ref()], &DataType::Int8, 2)
        .unwrap();
    let facts = invoice.finish(0).unwrap();
    assert!(facts.operation_peak_bytes() >= 140 * size_of::<(usize, Option<&[u8]>)>());
}
#[test]
fn by_copy_original_concat_run_geometry_tracks_physical_ends_not_logical_rows() {
    let mut invoice = super::super::take_host::CopyInvoiceTotals::default();
    invoice
        .original_run_concat_scratch(4, 2, size_of::<i16>())
        .unwrap();
    let facts = invoice.finish(0).unwrap();
    assert!(facts.retained_new_backing_upper() >= 4 * size_of::<i16>());
    assert!(facts.retained_new_backing_upper() < 2000 * size_of::<i16>());
}

#[test]
fn take_validation_formatter_source_observation_returns_each_original_control_without_tail() {
    let fields = arrow_schema::UnionFields::try_new(
        [5],
        [Field::new(
            "source field with Unicode 窗口",
            DataType::Struct(vec![Arc::new(Field::new("required", DataType::Utf8, false))].into()),
            false,
        )],
    )
    .unwrap();
    let ty = DataType::Union(fields, UnionMode::Sparse);
    let mut baseline = Vec::new();
    assert!(
        original_take_validation_diagnostic_observed(&ty, &mut |boundary| {
            baseline.push(boundary);
            Ok::<_, CopyError>(())
        })
        .unwrap()
        .is_some()
    );
    for at in 0..baseline.len() {
        let cause = KernelFailure::Cancelled;
        let mut observed = Vec::new();
        let result = original_take_validation_diagnostic_observed(&ty, &mut |boundary| {
            observed.push(boundary);
            if observed.len() == at + 1 {
                Err(CopyError::Control(cause.clone()))
            } else {
                Ok(())
            }
        });
        assert!(matches!(result, Err(CopyError::Control(actual)) if actual == cause));
        assert_eq!(observed, baseline[..=at]);
    }
}
