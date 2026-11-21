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

//! Canonical metadata partition rows keyed by Iceberg partition field identity.

use std::collections::{BTreeMap, BTreeSet};
use std::convert::Infallible;
use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Fields};
use novarocks_spi::connector::{ConnectorError, ConnectorErrorKind};

use crate::iceberg::spec::{PartitionSpec, Schema, Transform};

#[derive(Clone, Debug)]
pub(crate) struct UnifiedPartitionField {
    pub field_id: i32,
    pub source_id: i32,
    pub name: String,
    pub effective_transform: Transform,
    pub data_type: DataType,
}

#[derive(Clone, Debug)]
pub(crate) struct UnifiedPartitionRow {
    pub fields: Vec<UnifiedPartitionField>,
    pub slots_by_spec: BTreeMap<i32, Vec<Option<usize>>>,
}

pub(crate) enum PartitionRowError<E> {
    Semantic(ConnectorError),
    Original(E),
}

fn corrupt(message: impl Into<String>) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::CorruptData, message)
}

impl UnifiedPartitionRow {
    pub fn of(schema: &Schema, specs: &[PartitionSpec]) -> Result<Self, ConnectorError> {
        Self::of_checked(schema, &specs.iter().collect::<Vec<_>>(), || {
            Ok::<(), Infallible>(())
        })
        .map_err(|cause| match cause {
            PartitionRowError::Semantic(cause) => cause,
            PartitionRowError::Original(never) => match never {},
        })
    }

    /// Observe the same canonical construction under the caller's control owner.
    pub fn of_checked<E>(
        schema: &Schema,
        specs: &[&PartitionSpec],
        mut check: impl FnMut() -> Result<(), E>,
    ) -> Result<Self, PartitionRowError<E>> {
        let mut ordered = specs.to_vec();
        check().map_err(PartitionRowError::Original)?;
        ordered.sort_unstable_by_key(|spec| std::cmp::Reverse(spec.spec_id()));
        let mut by_id: BTreeMap<i32, UnifiedPartitionField> = BTreeMap::new();
        for spec in &ordered {
            for part in spec.fields() {
                check().map_err(PartitionRowError::Original)?;
                let Some(source) = schema.field_by_id(part.source_id) else {
                    continue;
                };
                let result = part
                    .transform
                    .result_type(source.field_type.as_ref())
                    .map_err(|error| {
                        PartitionRowError::Semantic(corrupt(format!(
                            "iceberg partition field {} has no result type: {error}",
                            part.field_id
                        )))
                    })?;
                let data_type = match &result {
                    crate::iceberg::spec::Type::Primitive(
                        crate::iceberg::spec::PrimitiveType::Timestamptz,
                    ) => DataType::Timestamp(
                        arrow::datatypes::TimeUnit::Microsecond,
                        Some("UTC".into()),
                    ),
                    crate::iceberg::spec::Type::Primitive(
                        crate::iceberg::spec::PrimitiveType::TimestamptzNs,
                    ) => DataType::Timestamp(
                        arrow::datatypes::TimeUnit::Nanosecond,
                        Some("UTC".into()),
                    ),
                    _ => crate::metadata_batch_reader::iceberg_type_to_arrow_type(&result)
                        .map_err(|message| PartitionRowError::Semantic(corrupt(message)))?,
                };
                match by_id.entry(part.field_id) {
                    std::collections::btree_map::Entry::Vacant(entry) => {
                        entry.insert(UnifiedPartitionField {
                            field_id: part.field_id,
                            source_id: part.source_id,
                            name: part.name.clone(),
                            effective_transform: part.transform,
                            data_type,
                        });
                    }
                    std::collections::btree_map::Entry::Occupied(mut entry) => {
                        let field = entry.get_mut();
                        if field.source_id != part.source_id {
                            return Err(PartitionRowError::Semantic(corrupt(format!(
                                "iceberg partition field {} has conflicting source IDs",
                                part.field_id
                            ))));
                        }
                        let previous_void = field.effective_transform == Transform::Void;
                        let current_void = part.transform == Transform::Void;
                        if !previous_void
                            && !current_void
                            && field.effective_transform != part.transform
                        {
                            return Err(PartitionRowError::Semantic(corrupt(format!(
                                "iceberg partition field {} has conflicting non-void transforms",
                                part.field_id
                            ))));
                        }
                        if previous_void && !current_void {
                            field.effective_transform = part.transform;
                            field.data_type = data_type;
                        }
                    }
                }
            }
        }
        let mut fields: Vec<_> = by_id.into_values().collect();
        // SQL scope keys use Unicode lowercase. Reserve all natural
        // spellings before allocating generated candidates.
        let reserved: BTreeSet<_> = fields
            .iter()
            .map(|field| field.name.to_lowercase())
            .collect();
        let mut winners = BTreeMap::new();
        for field in &fields {
            check().map_err(PartitionRowError::Original)?;
            winners.insert(field.name.to_lowercase(), field.field_id);
        }
        let mut assigned = reserved.clone();
        for field in &mut fields {
            check().map_err(PartitionRowError::Original)?;
            if winners[&field.name.to_lowercase()] == field.field_id {
                continue;
            }
            let base = format!("{}_{}", field.name, field.field_id);
            let mut candidate = base.clone();
            let mut suffix = 0usize;
            while assigned.contains(&candidate.to_lowercase()) {
                check().map_err(PartitionRowError::Original)?;
                suffix += 1;
                candidate = format!("{base}_{suffix}");
            }
            assigned.insert(candidate.to_lowercase());
            field.name = candidate;
        }
        let slot_by_id: BTreeMap<_, _> = fields
            .iter()
            .enumerate()
            .map(|(slot, field)| (field.field_id, slot))
            .collect();
        let mut slots_by_spec = BTreeMap::new();
        for spec in specs {
            let mut slots = Vec::with_capacity(spec.fields().len());
            for part in spec.fields() {
                check().map_err(PartitionRowError::Original)?;
                slots.push(
                    schema
                        .field_by_id(part.source_id)
                        .and_then(|_| slot_by_id.get(&part.field_id).copied()),
                );
            }
            slots_by_spec.insert(spec.spec_id(), slots);
        }
        Ok(Self {
            fields,
            slots_by_spec,
        })
    }

    pub fn field_ids(&self) -> Vec<i32> {
        self.fields.iter().map(|field| field.field_id).collect()
    }

    pub fn arrow_type(&self) -> Option<DataType> {
        (!self.fields.is_empty()).then(|| {
            DataType::Struct(Fields::from(
                self.fields
                    .iter()
                    .map(|field| Arc::new(Field::new(&field.name, field.data_type.clone(), true)))
                    .collect::<Vec<_>>(),
            ))
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::iceberg::spec::{NestedField, PrimitiveType, Type};

    fn schema() -> Schema {
        Schema::builder()
            .with_fields([
                Arc::new(NestedField::optional(
                    1,
                    "a",
                    Type::Primitive(PrimitiveType::Long),
                )),
                Arc::new(NestedField::optional(
                    2,
                    "b",
                    Type::Primitive(PrimitiveType::String),
                )),
            ])
            .build()
            .unwrap()
    }
    fn spec(id: i32, fields: &[(i32, i32, &str, Transform)]) -> PartitionSpec {
        serde_json::from_value(serde_json::json!({
            "spec-id": id,
            "fields": fields.iter().map(|(field_id, source_id, name, transform)| serde_json::json!({
                "field-id": field_id, "source-id": source_id, "name": name, "transform": transform
            })).collect::<Vec<_>>()
        }))
        .unwrap()
    }

    #[test]
    fn renamed_ids_share_a_slot_and_names_resolve_complete_collision_chains() {
        let old = spec(0, &[(1000, 1, "old", Transform::Identity)]);
        let renamed = spec(1, &[(1000, 1, "p", Transform::Identity)]);
        let added = spec(2, &[(1001, 1, "P", Transform::Identity)]);
        let natural = spec(3, &[(1002, 1, "p_1000", Transform::Identity)]);
        let natural_suffix = spec(4, &[(1003, 1, "P_1000_1", Transform::Identity)]);
        let specs = vec![old, renamed, added, natural, natural_suffix];
        let row = UnifiedPartitionRow::of(&schema(), &specs).unwrap();
        assert_eq!(row.field_ids(), vec![1000, 1001, 1002, 1003]);
        assert_eq!(
            row.fields
                .iter()
                .map(|field| field.name.as_str())
                .collect::<Vec<_>>(),
            vec!["p_1000_2", "P", "p_1000", "P_1000_1"]
        );
        assert_eq!(row.slots_by_spec[&0], vec![Some(0)]);
        assert_eq!(row.slots_by_spec[&1], vec![Some(0)]);
        assert_eq!(row.slots_by_spec[&2], vec![Some(1)]);
        assert_eq!(row.slots_by_spec[&3], vec![Some(2)]);
        let reversed =
            UnifiedPartitionRow::of(&schema(), &specs.into_iter().rev().collect::<Vec<_>>())
                .unwrap();
        assert_eq!(row.arrow_type(), reversed.arrow_type());
        assert_eq!(row.slots_by_spec, reversed.slots_by_spec);
    }

    #[test]
    fn display_names_use_the_same_unicode_lowercase_key_as_sql_scope() {
        let row = UnifiedPartitionRow::of(
            &schema(),
            &[
                spec(0, &[(1000, 1, "Ä", Transform::Identity)]),
                spec(1, &[(1001, 1, "ä", Transform::Identity)]),
            ],
        )
        .unwrap();
        assert_eq!(
            row.fields
                .iter()
                .map(|field| field.name.as_str())
                .collect::<Vec<_>>(),
            vec!["Ä_1000", "ä"]
        );
    }

    #[test]
    fn caller_control_error_keeps_its_original_identity() {
        let spec = spec(0, &[(1000, 1, "p", Transform::Identity)]);
        let error =
            UnifiedPartitionRow::of_checked(&schema(), &[&spec], || Err("original cancellation"));
        assert!(matches!(
            error,
            Err(PartitionRowError::Original("original cancellation"))
        ));
    }

    #[test]
    fn void_preserves_latest_name_but_adopts_effective_transform_and_type() {
        let bucket = spec(0, &[(1000, 1, "old_bucket", Transform::Bucket(8))]);
        let void = spec(1, &[(1000, 1, "removed", Transform::Void)]);
        for specs in [
            [bucket.clone(), void.clone()],
            [void.clone(), bucket.clone()],
        ] {
            let row = UnifiedPartitionRow::of(&schema(), &specs).unwrap();
            assert_eq!(row.fields[0].name, "removed");
            assert_eq!(row.fields[0].effective_transform, Transform::Bucket(8));
            assert_eq!(row.fields[0].data_type, DataType::Int32);
        }
        let resurrected = spec(2, &[(1000, 1, "restored", Transform::Bucket(8))]);
        let row = UnifiedPartitionRow::of(&schema(), &[bucket, void, resurrected]).unwrap();
        assert_eq!(row.fields[0].name, "restored");
        assert_eq!(row.fields[0].data_type, DataType::Int32);
    }

    #[test]
    fn conflicts_refuse_but_deleted_sources_are_omitted() {
        let first = spec(0, &[(1000, 1, "p", Transform::Identity)]);
        let conflicting_source = spec(1, &[(1000, 2, "p", Transform::Identity)]);
        assert_eq!(
            UnifiedPartitionRow::of(&schema(), &[first.clone(), conflicting_source])
                .unwrap_err()
                .kind(),
            ConnectorErrorKind::CorruptData
        );
        let conflicting_transform = spec(1, &[(1000, 1, "p", Transform::Bucket(8))]);
        assert_eq!(
            UnifiedPartitionRow::of(&schema(), &[first.clone(), conflicting_transform])
                .unwrap_err()
                .kind(),
            ConnectorErrorKind::CorruptData
        );
        let dropped = Schema::builder()
            .with_fields([Arc::new(NestedField::optional(
                2,
                "b",
                Type::Primitive(PrimitiveType::String),
            ))])
            .build()
            .unwrap();
        let row = UnifiedPartitionRow::of(&dropped, &[first]).unwrap();
        assert!(row.fields.is_empty());
        assert_eq!(row.arrow_type(), None);
        assert_eq!(row.slots_by_spec[&0], vec![None]);
    }
}
