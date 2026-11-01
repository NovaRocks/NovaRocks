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

//! Role-neutral native submission assembly helpers.

use std::collections::BTreeSet;
#[cfg(test)]
use std::sync::Arc;

#[cfg(test)]
use arrow::array::{ArrayRef, RecordBatchOptions};
#[cfg(test)]
use arrow::datatypes::{DataType, Schema};
#[cfg(test)]
use arrow::record_batch::RecordBatch;

use crate::query_execution::artifact::FragmentId;
#[cfg(test)]
use crate::query_execution::attempt_plan_facts::PlanOutputColumn;
use crate::query_execution::native_fragment::NativeFragmentAttachment;
use crate::query_execution::schedule::SchedulingPlan;
#[cfg(test)]
use novarocks_execution::exec::chunk::Chunk;

#[cfg(test)]
pub(crate) fn align_fetch_chunks_to_output_columns(
    chunks: Vec<Chunk>,
    output_columns: &[PlanOutputColumn],
) -> Result<Vec<Chunk>, String> {
    chunks
        .into_iter()
        .map(|chunk| align_fetch_chunk_to_output_columns(chunk, output_columns))
        .collect()
}

#[cfg(test)]
fn align_fetch_chunk_to_output_columns(
    chunk: Chunk,
    output_columns: &[PlanOutputColumn],
) -> Result<Chunk, String> {
    let row_count = chunk.record_batch().num_rows();
    if chunk.record_batch().num_columns() != output_columns.len() {
        return Err(format!(
            "typed root result column count mismatch: chunk has {}, output metadata has {}",
            chunk.record_batch().num_columns(),
            output_columns.len()
        ));
    }
    if chunk.chunk_schema().slots().len() != output_columns.len() {
        return Err(format!(
            "typed root result slot count mismatch: chunk schema has {}, output metadata has {}",
            chunk.chunk_schema().slots().len(),
            output_columns.len()
        ));
    }

    let mut fields = Vec::with_capacity(output_columns.len());
    let mut arrays = Vec::with_capacity(output_columns.len());
    for (idx, output) in output_columns.iter().enumerate() {
        let slot = &chunk.chunk_schema().slots()[idx];
        let source = novarocks_type_contract::FunctionValueType::try_from_typed_binding(
            slot.typed_binding().clone(),
        )
        .map_err(|error| error.to_string())?;
        if !source.same_semantics(&output.ty) {
            return Err(
                "typed root result source complete value type differs from output declaration"
                    .into(),
            );
        }
        let array = align_typed_root_array(
            idx,
            chunk.record_batch().column(idx).clone(),
            output.data_type(),
        )?;
        let dictionary_carrier =
            result_string_dictionary_carrier(output.data_type(), array.data_type());
        if matches!(output.data_type(), DataType::Utf8 | DataType::LargeUtf8)
            && matches!(array.data_type(), DataType::Dictionary(_, _))
            && !dictionary_carrier
        {
            return Err("typed root result string dictionary carrier is unsupported".into());
        }
        if !dictionary_carrier
            && let Err(mismatch) = novarocks_execution::exec::chunk::type_compatibility::check_exact(
                output.data_type(),
                array.data_type(),
            )
        {
            return Err(format!(
                "typed root result column {idx} type mismatch: output={:?} chunk={:?} ({:?})",
                output.data_type(),
                array.data_type(),
                mismatch.kind
            ));
        }
        let canonical = arrow::datatypes::Field::new(
            &output.name,
            output.data_type().clone(),
            output.nullable(),
        );
        let canonical = if dictionary_carrier {
            canonical.with_data_type(array.data_type().clone())
        } else {
            canonical
        };
        fields.push(if slot.field() == &canonical {
            Arc::clone(slot.field_ref())
        } else {
            Arc::new(canonical)
        });
        arrays.push(array);
    }

    let batch = RecordBatch::try_new_with_options(
        Arc::new(Schema::new(fields)),
        arrays,
        &RecordBatchOptions::new().with_row_count(Some(row_count)),
    )
    .map_err(|e| format!("align typed root result batch failed: {e}"))?;
    // Retain the signed value binding while projecting the visible alias.
    let slots = chunk
        .chunk_schema()
        .slots()
        .iter()
        .zip(batch.schema().fields())
        .zip(output_columns)
        .map(|((slot, field), output)| {
            novarocks_execution::exec::chunk::ChunkSlotSchema::try_new_typed(
                slot.slot_id(),
                Arc::clone(field),
                output.ty.typed_binding().clone(),
                slot.unique_id(),
                novarocks_type_contract::ValueTypeStorageLifetime::pure(),
            )
        })
        .collect::<Result<Vec<_>, _>>()
        .map_err(|source| source.to_string())?;
    let chunk_schema = Arc::new(
        novarocks_execution::exec::chunk::ChunkSchema::try_new(slots)
            .map_err(|source| source.to_string())?,
    );
    Chunk::try_new_with_chunk_schema(batch, chunk_schema).map_err(|source| source.to_string())
}

#[cfg(test)]
fn align_typed_root_array(
    idx: usize,
    array: ArrayRef,
    output_type: &DataType,
) -> Result<ArrayRef, String> {
    if result_string_dictionary_carrier(output_type, array.data_type()) {
        return Ok(array);
    }
    if novarocks_execution::exec::chunk::type_compatibility::check_exact(
        output_type,
        array.data_type(),
    )
    .is_ok()
    {
        if array.data_type() != output_type
            && matches!(
                output_type,
                DataType::List(_)
                    | DataType::LargeList(_)
                    | DataType::Map(_, _)
                    | DataType::Struct(_)
            )
        {
            // All original Field/cache domains were validated before this
            // metadata-only completion. Existing buffers remain unchanged.
            return novarocks_execution::exec::chunk::type_compatibility::retag_column(
                &array,
                output_type,
            )
            .map_err(|_| "typed root result nested metadata completion failed".into());
        }
        return Ok(array);
    }
    if !same_unit_timestamp_metadata_mismatch(output_type, array.data_type()) {
        return Ok(array);
    }
    novarocks_execution::exec::chunk::type_compatibility::retag_column(&array, output_type).map_err(|mismatch| {
        format!(
            "typed root result column {idx} timestamp metadata retag failed: output={:?} chunk={:?} ({:?})",
            output_type,
            array.data_type(),
            mismatch.kind
        )
    })
}

/// ResultBufferSink::accepts_encoded_column explicitly retains Int32 string
/// dictionaries (Utf8 or LargeUtf8). This is a result-boundary carrier rule;
/// it neither broadens generic type compatibility nor hydrates the column.
#[cfg(test)]
fn result_string_dictionary_carrier(expected: &DataType, actual: &DataType) -> bool {
    match (expected, actual) {
        (DataType::Utf8, DataType::Dictionary(key, value)) => {
            key.as_ref() == &DataType::Int32
                && matches!(value.as_ref(), DataType::Utf8 | DataType::LargeUtf8)
        }
        (DataType::LargeUtf8, DataType::Dictionary(key, value)) => {
            key.as_ref() == &DataType::Int32 && value.as_ref() == &DataType::LargeUtf8
        }
        _ => false,
    }
}

#[cfg(test)]
fn same_unit_timestamp_metadata_mismatch(expected: &DataType, actual: &DataType) -> bool {
    matches!(
        (expected, actual),
        (DataType::Timestamp(expected_unit, _), DataType::Timestamp(actual_unit, _))
            if expected_unit == actual_unit
    )
}

/// Validate the submitted fragment owns a supported static sink.
pub fn ensure_native_fragment_sink_supported(
    fragment_id: FragmentId,
    is_root: bool,
    has_stream_edge: bool,
) -> Result<(), String> {
    if is_root || has_stream_edge {
        return Ok(());
    }

    let dynamic_sink = "unowned fragment sink";
    Err(format!(
        "native submission cannot encode {dynamic_sink} for fragment {fragment_id}; \
         the completed plan must declare a supported static sink"
    ))
}

pub(crate) fn validate_artifact_fragment_sets(
    expected: &BTreeSet<FragmentId>,
    native_bundle: &NativeFragmentAttachment,
    scheduling: &SchedulingPlan,
) -> Result<(), String> {
    let native = native_bundle.fragment_ids().collect::<BTreeSet<_>>();
    if &native != expected {
        return Err(fragment_set_mismatch("native", expected, &native));
    }
    let scheduled = scheduling.fragment_ids().collect::<BTreeSet<_>>();
    if &scheduled != expected {
        return Err(fragment_set_mismatch("scheduled", expected, &scheduled));
    }
    Ok(())
}

fn fragment_set_mismatch(
    label: &str,
    expected: &BTreeSet<FragmentId>,
    actual: &BTreeSet<FragmentId>,
) -> String {
    let missing = expected.difference(actual).copied().collect::<Vec<_>>();
    let unknown = actual.difference(expected).copied().collect::<Vec<_>>();
    format!(
        "{label} fragment ids mismatch: expected={expected:?} actual={actual:?} missing={missing:?} unknown={unknown:?}"
    )
}

pub(crate) fn validate_scheduling_placements(plan: &SchedulingPlan) -> Result<(), String> {
    for (&fragment_id, placements) in &plan.by_fragment {
        if placements.is_empty() {
            return Err(format!(
                "native scheduling plan fragment {fragment_id} has no placements"
            ));
        }
        for (placement_index, placement) in placements.iter().enumerate() {
            if placement.fragment_id != fragment_id {
                return Err(format!(
                    "native scheduling plan map key {fragment_id} does not match placement \
                     {placement_index} fragment_id {}",
                    placement.fragment_id
                ));
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::Arc;

    use arrow::array::{Decimal128Array, Int32Array};
    use arrow::datatypes::{DataType, Field, Schema};
    #[cfg(test)]
    use arrow::record_batch::RecordBatch;

    use super::*;
    use crate::query_execution::schedule::FragmentInstancePlacement;
    use novarocks_execution::exec::chunk::ChunkSchema;
    use novarocks_execution::runtime::endpoint::RuntimeEndpoint;
    use novarocks_types::SlotId;
    use novarocks_types::UniqueId;

    fn placement(fragment_id: FragmentId, instance_lo: i64) -> FragmentInstancePlacement {
        FragmentInstancePlacement {
            fragment_id,
            instance_index: 0,
            finst_id: UniqueId::new(92_000, instance_lo),
            backend_idx: 0,
            endpoint: RuntimeEndpoint::new("10.0.0.2", 9030).unwrap(),
            scan_ranges: BTreeMap::new(),
        }
    }

    #[test]
    fn validates_native_sink_and_output_roles() {
        ensure_native_fragment_sink_supported(7, false, true).expect("stream sink is supported");
        assert!(
            ensure_native_fragment_sink_supported(10, false, false)
                .expect_err("unowned dynamic sink must be rejected")
                .contains("unowned fragment sink")
        );
    }

    #[test]
    fn rejects_invalid_scheduling_placements_before_assembly() {
        let empty = SchedulingPlan {
            root_fragment_id: 7,
            by_fragment: BTreeMap::from([(3, Vec::new()), (7, vec![placement(7, 7)])]),
            root_finst_id: UniqueId::new(92_000, 7),
            root_backend_idx: 0,
        };
        assert!(
            validate_scheduling_placements(&empty)
                .expect_err("empty placements must fail")
                .contains("fragment 3 has no placements")
        );

        let drift = SchedulingPlan {
            root_fragment_id: 7,
            by_fragment: BTreeMap::from([(7, vec![placement(8, 7)])]),
            root_finst_id: UniqueId::new(92_000, 7),
            root_backend_idx: 0,
        };
        let error =
            validate_scheduling_placements(&drift).expect_err("placement id drift must fail");
        assert!(error.contains("map key 7"), "{error}");
        assert!(error.contains("fragment_id 8"), "{error}");
    }

    #[test]
    fn aligns_typed_root_fields_and_rejects_decimal_drift() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "wire_i",
            DataType::Int32,
            true,
        )]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int32Array::from(vec![Some(1), None]))],
        )
        .unwrap();
        let chunk_schema =
            ChunkSchema::try_ref_from_schema_and_slot_ids(schema.as_ref(), &[SlotId::new(7)])
                .unwrap();
        let chunk = Chunk::try_new_with_chunk_schema(batch, chunk_schema).unwrap();
        let aligned = align_fetch_chunks_to_output_columns(
            vec![chunk],
            &[PlanOutputColumn {
                name: "col1".to_string(),
                ty: crate::query_execution::attempt_plan_facts::declared_test_value(
                    DataType::Int32,
                    true,
                    novarocks_physical_plan::ResultValueDomain::Plain,
                )
                .unwrap(),
            }],
        )
        .unwrap();
        assert_eq!(aligned[0].record_batch().schema().field(0).name(), "col1");
        assert!(aligned[0].record_batch().schema().field(0).is_nullable());

        let decimal = Decimal128Array::from(vec![Some(100_i128)])
            .with_precision_and_scale(38, 2)
            .unwrap();
        let schema = Arc::new(Schema::new(vec![Field::new(
            "wire_price",
            DataType::Decimal128(38, 2),
            false,
        )]));
        let batch = RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(decimal)]).unwrap();
        let chunk_schema =
            ChunkSchema::try_ref_from_schema_and_slot_ids(schema.as_ref(), &[SlotId::new(8)])
                .unwrap();
        let chunk = Chunk::try_new_with_chunk_schema(batch, chunk_schema).unwrap();
        assert!(
            align_fetch_chunks_to_output_columns(
                vec![chunk],
                &[PlanOutputColumn {
                    name: "price".to_string(),
                    ty: crate::query_execution::attempt_plan_facts::declared_test_value(
                        DataType::Decimal128(20, 2),
                        false,
                        novarocks_physical_plan::ResultValueDomain::Plain
                    )
                    .unwrap(),
                }],
            )
            .expect_err("decimal precision drift must fail")
            .contains("complete value type differs")
        );
    }

    #[test]
    fn root_alignment_checks_explicit_domain_and_ignores_source_attributes() {
        use arrow::array::BinaryArray;
        use novarocks_execution::exec::chunk::ChunkSlotSchema;
        use novarocks_physical_plan::ResultValueDomain as D;
        use novarocks_type_contract::ValueTypeStorageLifetime as S;
        let ty = super::super::attempt_plan_facts::declared_test_value(
            DataType::Binary,
            false,
            D::Bitmap,
        )
        .unwrap();
        let field = Arc::new(Field::new("wire", DataType::Binary, false).with_metadata(
            std::collections::HashMap::<String, String>::from([(
                "nr_logical_type".into(),
                "unknown-source-attribute".into(),
            )]),
        ));
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![field.clone()])),
            vec![Arc::new(BinaryArray::from(vec![b"payload".as_slice()]))],
        )
        .unwrap();
        let slot = ChunkSlotSchema::try_new_typed(
            SlotId::new(9),
            field,
            ty.typed_binding().clone(),
            None,
            S::pure(),
        )
        .unwrap();
        let chunk = Chunk::try_new_with_chunk_schema(
            batch,
            Arc::new(ChunkSchema::try_new(vec![slot]).unwrap()),
        )
        .unwrap();
        for domain in [D::Hll, D::Object, D::Percentile, D::Plain] {
            let output = PlanOutputColumn {
                name: "declared".into(),
                ty: super::super::attempt_plan_facts::declared_test_value(
                    DataType::Binary,
                    false,
                    domain,
                )
                .unwrap(),
            };
            assert!(
                align_fetch_chunks_to_output_columns(vec![chunk.clone()], &[output])
                    .unwrap_err()
                    .contains("complete value type differs")
            );
        }
        let output = PlanOutputColumn {
            name: "declared".into(),
            ty: ty.clone(),
        };
        let aligned = align_fetch_chunks_to_output_columns(vec![chunk], &[output]).unwrap();
        assert_eq!(
            aligned[0].chunk_schema().slots()[0].typed_binding(),
            ty.typed_binding()
        );
        assert!(
            aligned[0]
                .record_batch()
                .schema()
                .field(0)
                .metadata()
                .is_empty()
        );
    }
}
