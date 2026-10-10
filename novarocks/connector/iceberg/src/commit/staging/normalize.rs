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

use super::invalid;
use crate::iceberg::spec::TableMetadata;
use crate::iceberg::{Result, TableUpdate};

#[derive(Clone, Default)]
pub(super) struct Selections {
    schema: Option<i32>,
    spec: Option<i32>,
    order: Option<i64>,
}

/// Replay once from M: builder-local last-added state spans the entire prefix.
/// The vendor builder ignores REST add-schema's explicit high-water mark;
/// restore that public format fact after replay rather than silently losing it.
pub(super) fn replay(
    base: &TableMetadata,
    location: Option<&str>,
    updates: &[TableUpdate],
) -> Result<TableMetadata> {
    let mut builder = base.clone().into_builder(location.map(str::to_owned));
    let mut last_column = base.last_column_id();
    for update in updates {
        if let TableUpdate::AddSchema {
            schema,
            last_column_id,
        } = update
        {
            last_column = last_column
                .max(schema.highest_field_id())
                .max(last_column_id.unwrap_or(0));
        }
        builder = update.clone().apply(builder)?;
    }
    let metadata = builder.build()?.metadata;
    if metadata.last_column_id() == last_column {
        return Ok(metadata);
    }
    let mut json = serde_json::to_value(&metadata)
        .map_err(|e| invalid(format!("Serialize staged metadata: {e}")))?;
    json["last-column-id"] = last_column.into();
    serde_json::from_value(json)
        .map_err(|e| invalid(format!("Restore staged column high-water mark: {e}")))
}

pub(super) fn normalize(
    metadata: &TableMetadata,
    update: TableUpdate,
    selections: &mut Selections,
) -> Result<Option<TableUpdate>> {
    match update {
        TableUpdate::AddSchema {
            schema,
            last_column_id,
        } => {
            // Identifier fields are a set; SDK iterator-order equality is not
            // semantic equality. Resolve reuse before invoking its allocator.
            let identifiers: std::collections::BTreeSet<_> =
                schema.identifier_field_ids().collect();
            if let Some(id) = metadata
                .schemas_iter()
                .filter(|existing| {
                    existing.as_struct() == schema.as_struct()
                        && existing
                            .identifier_field_ids()
                            .collect::<std::collections::BTreeSet<_>>()
                            == identifiers
                })
                .map(|existing| existing.schema_id())
                .min()
            {
                if last_column_id.is_some_and(|id| id > metadata.last_column_id()) {
                    return Err(invalid(
                        "Equivalent schema cannot advance the field high-water mark without a new schema",
                    ));
                }
                selections.schema = Some(id);
                return Ok(None);
            }
            // Ask the format builder for the canonical ID of a new schema.
            let result = TableUpdate::AddSchema {
                schema,
                last_column_id,
            }
            .apply(metadata.clone().into_builder(None))?
            .build()?;
            let Some(TableUpdate::AddSchema { schema, .. }) = result
                .changes
                .into_iter()
                .find(|u| matches!(u, TableUpdate::AddSchema { .. }))
            else {
                return Err(invalid(
                    "Schema addition did not resolve a canonical schema ID",
                ));
            };
            selections.schema = Some(schema.schema_id());
            if metadata.schema_by_id(schema.schema_id()).is_some() {
                Ok(None)
            } else {
                Ok(Some(TableUpdate::AddSchema {
                    schema,
                    last_column_id,
                }))
            }
        }
        TableUpdate::AddSpec { spec } => {
            let result = TableUpdate::AddSpec { spec }
                .apply(metadata.clone().into_builder(None))?
                .build()?;
            let Some(TableUpdate::AddSpec { spec }) = result
                .changes
                .into_iter()
                .find(|u| matches!(u, TableUpdate::AddSpec { .. }))
            else {
                return Err(invalid(
                    "Partition addition did not resolve a canonical spec ID",
                ));
            };
            let id = spec
                .spec_id()
                .ok_or_else(|| invalid("Canonical partition spec has no ID"))?;
            selections.spec = Some(id);
            if metadata.partition_spec_by_id(id).is_some() {
                Ok(None)
            } else {
                let bound = result.metadata.partition_spec_by_id(id).ok_or_else(|| {
                    invalid("Canonical partition spec is absent from staged metadata")
                })?;
                Ok(Some(TableUpdate::AddSpec {
                    spec: bound.as_ref().clone().into_unbound(),
                }))
            }
        }
        TableUpdate::AddSortOrder { sort_order } => {
            let result = TableUpdate::AddSortOrder { sort_order }
                .apply(metadata.clone().into_builder(None))?
                .build()?;
            let Some(TableUpdate::AddSortOrder { sort_order }) = result
                .changes
                .into_iter()
                .find(|u| matches!(u, TableUpdate::AddSortOrder { .. }))
            else {
                return Err(invalid(
                    "Sort addition did not resolve a canonical order ID",
                ));
            };
            let id = sort_order.order_id;
            selections.order = Some(id);
            if metadata.sort_order_by_id(id).is_some() {
                Ok(None)
            } else {
                Ok(Some(TableUpdate::AddSortOrder { sort_order }))
            }
        }
        TableUpdate::SetCurrentSchema { schema_id: -1 } => {
            Ok(Some(TableUpdate::SetCurrentSchema {
                schema_id: selections.schema.ok_or_else(|| {
                    invalid("Schema -1 does not name an object selected in this request")
                })?,
            }))
        }
        TableUpdate::SetDefaultSpec { spec_id: -1 } => Ok(Some(TableUpdate::SetDefaultSpec {
            spec_id: selections.spec.ok_or_else(|| {
                invalid("Spec -1 does not name an object selected in this request")
            })?,
        })),
        TableUpdate::SetDefaultSortOrder { sort_order_id: -1 } => {
            Ok(Some(TableUpdate::SetDefaultSortOrder {
                sort_order_id: selections.order.ok_or_else(|| {
                    invalid("Sort order -1 does not name an object selected in this request")
                })?,
            }))
        }
        update => Ok(Some(update)),
    }
}

impl Selections {
    pub(super) fn initialize(&mut self, updates: &[TableUpdate]) -> Result<Vec<TableUpdate>> {
        let mut result = Vec::with_capacity(updates.len());
        for update in updates {
            match update {
                TableUpdate::AddSchema { schema, .. } => self.schema = Some(schema.schema_id()),
                TableUpdate::AddSpec { spec } => self.spec = spec.spec_id(),
                TableUpdate::AddSortOrder { sort_order } => self.order = Some(sort_order.order_id),
                _ => {}
            }
            result.push(match update {
                TableUpdate::SetCurrentSchema { schema_id: -1 } => TableUpdate::SetCurrentSchema {
                    schema_id: self
                        .schema
                        .ok_or_else(|| invalid("Create schema selector lacks its addition"))?,
                },
                TableUpdate::SetDefaultSpec { spec_id: -1 } => TableUpdate::SetDefaultSpec {
                    spec_id: self
                        .spec
                        .ok_or_else(|| invalid("Create spec selector lacks its addition"))?,
                },
                TableUpdate::SetDefaultSortOrder { sort_order_id: -1 } => {
                    TableUpdate::SetDefaultSortOrder {
                        sort_order_id: self
                            .order
                            .ok_or_else(|| invalid("Create sort selector lacks its addition"))?,
                    }
                }
                _ => update.clone(),
            });
        }
        Ok(result)
    }
}
