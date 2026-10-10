// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

//! Source requests of ONE normal positive-layout ChunkSchema constructor.
//! These numerical bounds grant nothing. Native callers separately establish
//! immutable bounded type provenance; arbitrary public layouts keep Direct.
use super::{ChunkFieldSchema, ChunkSchema, ChunkSlotSchema};
use arrow::datatypes::{DataType, Field, FieldRef, Schema};
use novarocks_local_program::StaticLayout;
use novarocks_type_contract::{
    CompileCheckpoints, CompilePhase, CompleteMetadataRequestFacts, MetadataRequestError,
    PureCompileControl, ValueTypeVisit,
    owned_resources::{
        formatting, hashmap, layout,
        metadata_materialization::{MetadataAllocationLoan, MetadataFieldLoan},
        metadata_request::MetadataRequestSum,
        type_validation, vec,
    },
    validate_value_type_structure_with_scratch_observed,
};
use novarocks_types::{SlotId, arrow_metadata_owner::MetadataOwnedField};
use std::{alloc::Layout, mem::MaybeUninit};

pub fn original_compiled_schema_metadata_request(
    source_layout: &StaticLayout,
    control: &dyn PureCompileControl,
) -> Result<CompleteMetadataRequestFacts, MetadataRequestError> {
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::LowerProgram)?;
    let result = original_request(source_layout, &mut work)?;
    work.finish()?;
    Ok(result)
}

fn original_request(
    source_layout: &StaticLayout,
    work: &mut CompileCheckpoints<'_>,
) -> Result<CompleteMetadataRequestFacts, MetadataRequestError> {
    if source_layout.field_metadata_origins().is_some() {
        return Err(MetadataRequestError::SourceModel(
            "compiled schema request requires its normal original metadata route",
        ));
    }
    let source =
        source_layout
            .metadata_materializations()
            .ok_or(MetadataRequestError::SourceModel(
                "compiled schema request has no original metadata source",
            ))?;
    if !source.schema_owner().lends(source_layout.schema()) {
        return Err(MetadataRequestError::SourceModel(
            "compiled schema request does not borrow its original Schema",
        ));
    }
    let mut sum = MetadataRequestSum::default();
    let mut storage = MaybeUninit::<type_validation::TypeValidationScratch<'_>>::uninit();
    let scratch = type_validation::initialize_scratch_observed(&mut storage, work)?;
    let count = source_layout.schema().fields().len();
    for field in source_layout.schema().fields() {
        // The first owned Field clone keeps the same metadata bucket origin;
        // the second owned clone therefore has the same request geometry.
        let mut twice =
            |layout, occurrences: usize| sum.allocation(layout, checked_mul(occurrences, 2)?);
        let mut allocations: Option<MetadataAllocationLoan<'_, MetadataRequestError>> =
            Some(&mut twice);
        source
            .original_borrowed_field_clone_allocation_requests_observed(
                field,
                scratch,
                &mut || work.step().map_err(Into::into),
                &mut allocations,
            )?
            .ok_or(MetadataRequestError::SourceModel(
                "compiled schema request has an unpaired original root Field",
            ))?;
        sum.allocation(layout::arc_layout(Layout::new::<Field>())?, 2)?;
        let mut debug = DebugRequestBound::default();
        validate_value_type_structure_with_scratch_observed::<MetadataRequestError>(
            field.data_type(),
            scratch,
            |visit| {
                work.step()?;
                debug.observe(visit, work)?;
                // The original semantic constructor follows a subset of the
                // same full grammar. Map entries Struct and ignored-carrier
                // descendants are declared conservative request overcounts.
                match visit {
                    ValueTypeVisit::TypeNode(DataType::Struct(fields)) => {
                        fresh_push::<ChunkFieldSchema>(fields.len(), work, &mut sum)?
                    }
                    ValueTypeVisit::TypeNode(DataType::List(_) | DataType::LargeList(_)) => {
                        array::<ChunkFieldSchema>(1, &mut sum)?
                    }
                    ValueTypeVisit::TypeNode(DataType::Map(..)) => {
                        array::<ChunkFieldSchema>(2, &mut sum)?
                    }
                    _ => {}
                }
                Ok(())
            },
        )?;
        // Count both original error destinations without running a formatter.
        // The full root bound dominates every offending entries subtree.
        for (initial, upper) in [
            (54, checked_add(27, debug.bytes()?)?),
            (84, checked_add(42, usize::MAX.ilog10() as usize + 1)?),
        ] {
            let bound = formatting::original_format_string_request_bound(initial, upper)?;
            sum.add(
                bound.request_bytes_upper_bound,
                bound.allocation_requests_upper_bound,
            )?;
        }
    }
    fresh_push::<ChunkSlotSchema>(count, work, &mut sum)?;
    let metadata = source
        .schema_owner()
        .original_metadata_clone_request(&mut || work.step().map_err(MetadataRequestError::from))?;
    if let Some(table) = metadata.table_backing {
        sum.allocation(table, 1)?;
    }
    sum.add(metadata.text_bytes, metadata.text_requests)?;
    let index = hashmap::fresh_table_layout::<SlotId, usize>(count)?;
    sum.add(
        index.request_bytes_upper_bound,
        index.allocation_requests_upper_bound,
    )?;
    array::<SlotId>(count, &mut sum)?;
    array::<FieldRef>(count, &mut sum)?;
    let loans = checked_add(source.fields().len(), count)?;
    array::<MetadataFieldLoan>(loans, &mut sum)?;
    for payload in [
        Layout::array::<FieldRef>(count).map_err(|_| MetadataRequestError::Arithmetic)?,
        Layout::array::<MetadataFieldLoan>(loans).map_err(|_| MetadataRequestError::Arithmetic)?,
        Layout::new::<Schema>(),
        Layout::new::<ChunkSchema>(),
    ] {
        sum.allocation(layout::arc_layout(payload)?, 1)?;
    }
    if count == 0 {
        // The original all(origins.is_some()) is vacuously true. Its empty
        // MetadataOwnedField Arc slice still requests the original header.
        sum.allocation(
            layout::arc_layout(
                Layout::array::<MetadataOwnedField>(0)
                    .map_err(|_| MetadataRequestError::Arithmetic)?,
            )?,
            1,
        )?;
    }
    Ok(sum.facts())
}

fn checked_add(left: usize, right: usize) -> Result<usize, MetadataRequestError> {
    left.checked_add(right)
        .ok_or(MetadataRequestError::Arithmetic)
}
fn checked_mul(left: usize, right: usize) -> Result<usize, MetadataRequestError> {
    left.checked_mul(right)
        .ok_or(MetadataRequestError::Arithmetic)
}
fn array<T>(count: usize, sum: &mut MetadataRequestSum) -> Result<(), MetadataRequestError> {
    sum.allocation(
        Layout::array::<T>(count).map_err(|_| MetadataRequestError::Arithmetic)?,
        1,
    )
}
fn fresh_push<T>(
    count: usize,
    work: &mut CompileCheckpoints<'_>,
    sum: &mut MetadataRequestSum,
) -> Result<(), MetadataRequestError> {
    let mut allocation = |layout, occurrences| sum.allocation(layout, occurrences);
    vec::original_fresh_push_allocation_requests_observed::<T, MetadataRequestError>(
        count,
        &mut || work.step().map_err(Into::into),
        &mut Some(&mut allocation),
    )?;
    Ok(())
}

#[derive(Default)]
struct DebugRequestBound {
    types: usize,
    fields: usize,
    metadata: usize,
    strings: usize,
    text_bytes: usize,
    containers: usize,
    entries: usize,
}
impl DebugRequestBound {
    fn text(&mut self, value: &str) -> Result<(), MetadataRequestError> {
        self.strings = checked_add(self.strings, 1)?;
        self.text_bytes = checked_add(self.text_bytes, value.len())?;
        Ok(())
    }
    fn observe(
        &mut self,
        visit: ValueTypeVisit<'_>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<(), MetadataRequestError> {
        match visit {
            ValueTypeVisit::TypeNode(data_type) => {
                self.types = checked_add(self.types, 1)?;
                let container = match data_type {
                    DataType::Struct(fields) => Some(fields.len()),
                    DataType::Union(fields, _) => Some(fields.len()),
                    _ => None,
                };
                if let Some(entries) = container {
                    self.containers = checked_add(self.containers, 1)?;
                    self.entries = checked_add(self.entries, entries)?;
                }
                if let DataType::Timestamp(_, Some(zone)) = data_type {
                    self.text(zone)?;
                }
            }
            ValueTypeVisit::Field(field) => {
                self.fields = checked_add(self.fields, 1)?;
                self.metadata = checked_add(self.metadata, field.metadata().len())?;
                self.text(field.name())?;
                work.flush()?;
                for (key, value) in field.metadata() {
                    self.text(key)?;
                    self.text(value)?;
                    work.step()?;
                }
                work.flush()?;
            }
            ValueTypeVisit::ChildEdge(_) => {}
        }
        Ok(())
    }
    fn bytes(&self) -> Result<usize, MetadataRequestError> {
        let mut result = 0;
        for (count, factor) in [
            (self.types, 43),
            (self.fields, 121),
            (self.metadata, 4),
            (self.strings, 2),
            (self.text_bytes, 10),
            (self.containers, 2),
            (self.entries, 10),
        ] {
            result = checked_add(result, checked_mul(count, factor)?)?;
        }
        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use novarocks_type_contract::{
        CompileControlError, FunctionValueType, ValueLogicalType,
        owned_resources::metadata_materialization::{
            MaterializedFieldNamespace, OriginalFieldMaterialization, TypedSchemaMaterializations,
            materialize_value_field,
        },
    };
    use std::sync::{Arc, Mutex};
    struct Control {
        trace: Mutex<Vec<(CompilePhase, u32)>>,
        refusal: Option<(usize, CompileControlError)>,
    }
    impl Control {
        fn good() -> Self {
            Self {
                trace: Mutex::default(),
                refusal: None,
            }
        }
    }
    impl PureCompileControl for Control {
        fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
            let mut trace = self.trace.lock().unwrap();
            trace.push((phase, units));
            if let Some((at, error)) = self.refusal
                && trace.len() == at
            {
                return Err(error);
            }
            Ok(())
        }
    }
    fn positive(types: &[FunctionValueType]) -> StaticLayout {
        let fields = types
            .iter()
            .enumerate()
            .map(|(index, ty)| materialize_value_field(ty, format!("field_{index}")).unwrap())
            .collect();
        let source = TypedSchemaMaterializations::new(
            fields,
            MaterializedFieldNamespace::from_original_loans(Arc::from([])),
        )
        .into_original_schema();
        StaticLayout::try_new_materialized_for_compile(
            source,
            (0..types.len())
                .map(|index| SlotId::new(u32::try_from(index).unwrap() + 1))
                .collect::<Vec<_>>()
                .into(),
            &Control::good(),
        )
        .unwrap()
    }
    #[test]
    fn compiled_layout_metadata_request_empty_includes_vacuous_origins_and_all_arc_headers() {
        let source = positive(&[]);
        let facts = original_compiled_schema_metadata_request(&source, &Control::good()).unwrap();
        let payloads = [
            Layout::array::<FieldRef>(0).unwrap(),
            Layout::array::<MetadataFieldLoan>(0).unwrap(),
            Layout::new::<Schema>(),
            Layout::new::<ChunkSchema>(),
            Layout::array::<MetadataOwnedField>(0).unwrap(),
        ];
        assert_eq!(facts.allocation_requests_upper_bound, 5);
        assert_eq!(
            facts.allocation_request_bytes_upper_bound,
            payloads
                .into_iter()
                .map(|payload| layout::arc_layout(payload).unwrap().size())
                .sum::<usize>()
        );
        let original = ChunkSchema::from_compiled_layout(&source).unwrap();
        assert!(original.field_metadata_origins().is_some());
        assert!(original.slots().is_empty());
    }
    #[test]
    fn compiled_layout_metadata_request_both_root_clones_preserve_nominal_map_multiplicity() {
        let plain = FunctionValueType::new(DataType::Utf8, true);
        let json =
            FunctionValueType::try_with_logical_type(DataType::Utf8, true, ValueLogicalType::Json)
                .unwrap();
        for count in [1, 3] {
            let physical = positive(&vec![plain.clone(); count]);
            let nominal = positive(&vec![json.clone(); count]);
            let left =
                original_compiled_schema_metadata_request(&physical, &Control::good()).unwrap();
            let right =
                original_compiled_schema_metadata_request(&nominal, &Control::good()).unwrap();
            let root_table = hashmap::fresh_table_layout::<String, String>(1).unwrap();
            assert_eq!(
                right.allocation_request_bytes_upper_bound
                    - left.allocation_request_bytes_upper_bound,
                2 * count
                    * (root_table.request_bytes_upper_bound
                        + novarocks_type_contract::NR_LOGICAL_TYPE_KEY.len()
                        + "json".len())
            );
            assert_eq!(
                right.allocation_requests_upper_bound - left.allocation_requests_upper_bound,
                2 * count * (root_table.allocation_requests_upper_bound + 2)
            );
            let original = ChunkSchema::from_compiled_layout(&nominal).unwrap();
            assert_eq!(original.slots().len(), count);
            assert!(
                original
                    .slots()
                    .iter()
                    .all(|slot| slot.field_schema().json_semantic())
            );
        }
    }
    #[test]
    fn compiled_layout_metadata_request_map_failure_keeps_original_string_and_source_graph() {
        let malformed = FunctionValueType::new(
            DataType::Map(
                Arc::new(Field::new("entries", DataType::Int64, false)),
                false,
            ),
            true,
        );
        let source = positive(&[malformed]);
        let before = Arc::clone(source.schema());
        let facts = original_compiled_schema_metadata_request(&source, &Control::good()).unwrap();
        assert!(facts.allocation_request_bytes_upper_bound > 0);
        assert_eq!(
            ChunkSchema::from_compiled_layout(&source).unwrap_err(),
            "map entries is not struct: Int64"
        );
        assert!(Arc::ptr_eq(&before, source.schema()));
        let malformed = FunctionValueType::new(
            DataType::Map(
                Arc::new(Field::new(
                    "entries",
                    DataType::Struct(
                        vec![Arc::new(Field::new("key", DataType::Int64, false))].into(),
                    ),
                    false,
                )),
                false,
            ),
            true,
        );
        let source = positive(&[malformed]);
        original_compiled_schema_metadata_request(&source, &Control::good()).unwrap();
        assert_eq!(
            ChunkSchema::from_compiled_layout(&source).unwrap_err(),
            "map entries expected 2 struct fields, got 1"
        );
    }
    #[test]
    fn compiled_layout_metadata_request_foreign_and_mixed_sources_do_not_mint_facts() {
        let field = Field::new("foreign", DataType::Int64, true);
        let foreign = StaticLayout::try_new_for_compile(
            Arc::new(Schema::new(vec![field.clone()])),
            Arc::from([SlotId::new(1)]),
            &Control::good(),
        )
        .unwrap();
        assert!(matches!(
            original_compiled_schema_metadata_request(&foreign, &Control::good()),
            Err(MetadataRequestError::SourceModel(_))
        ));
        ChunkSchema::from_compiled_layout(&foreign).unwrap();
        let source = TypedSchemaMaterializations::from_original_fields(
            vec![OriginalFieldMaterialization::Plain(field)],
            MaterializedFieldNamespace::from_original_loans(Arc::from([])),
        )
        .into_original_schema();
        let mixed = StaticLayout::try_new_materialized_for_compile(
            source,
            Arc::from([SlotId::new(1)]),
            &Control::good(),
        )
        .unwrap();
        assert!(matches!(
            original_compiled_schema_metadata_request(&mixed, &Control::good()),
            Err(MetadataRequestError::SourceModel(_))
        ));
        ChunkSchema::from_compiled_layout(&mixed).unwrap();
    }
    #[test]
    fn compiled_layout_metadata_request_nested_wide_source_keeps_every_control_prefix() {
        let mut metadata = std::collections::HashMap::new();
        for index in 0..320 {
            metadata.insert(format!("key_{index}"), "quote\"\\\n".repeat(3));
        }
        let nested = FunctionValueType::new(
            DataType::List(Arc::new(Field::new(
                "item",
                DataType::Struct(
                    vec![Arc::new(
                        Field::new(
                            "child",
                            DataType::Timestamp(
                                arrow::datatypes::TimeUnit::Nanosecond,
                                Some("UTC".into()),
                            ),
                            true,
                        )
                        .with_metadata(metadata),
                    )]
                    .into(),
                ),
                true,
            ))),
            true,
        );
        let source = positive(&[nested]);
        let baseline = Control::good();
        original_compiled_schema_metadata_request(&source, &baseline).unwrap();
        ChunkSchema::from_compiled_layout(&source).unwrap();
        let trace = baseline.trace.into_inner().unwrap();
        assert!(trace.iter().any(|(_, units)| *units == 256));
        for error in [
            CompileControlError::Cancelled,
            CompileControlError::DeadlineExceeded,
            CompileControlError::ResourceExhausted,
        ] {
            for at in 1..=trace.len() {
                let control = Control {
                    trace: Mutex::default(),
                    refusal: Some((at, error)),
                };
                assert_eq!(
                    original_compiled_schema_metadata_request(&source, &control),
                    Err(MetadataRequestError::Control(error))
                );
                assert_eq!(*control.trace.lock().unwrap(), trace[..at]);
            }
        }
    }
}
