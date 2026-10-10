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

use crate::commit::model::RequestShape;
use crate::iceberg::spec::TableMetadata;
use crate::iceberg::{TableRequirement, TableUpdate};

pub(super) fn push_unique(requirements: &mut Vec<TableRequirement>, requirement: TableRequirement) {
    if !requirements.contains(&requirement) {
        requirements.push(requirement);
    }
}

/// A stage-local assertion guards preparation; its external assertion guards M.
pub(super) fn against_base(
    requirement: &TableRequirement,
    base: &TableMetadata,
) -> TableRequirement {
    match requirement {
        TableRequirement::NotExist => TableRequirement::NotExist,
        TableRequirement::UuidMatch { .. } => TableRequirement::UuidMatch { uuid: base.uuid() },
        TableRequirement::RefSnapshotIdMatch { r#ref, .. } => {
            TableRequirement::RefSnapshotIdMatch {
                r#ref: r#ref.clone(),
                snapshot_id: base.snapshot_for_ref(r#ref).map(|s| s.snapshot_id()),
            }
        }
        TableRequirement::LastAssignedFieldIdMatch { .. } => {
            TableRequirement::LastAssignedFieldIdMatch {
                last_assigned_field_id: base.last_column_id(),
            }
        }
        TableRequirement::CurrentSchemaIdMatch { .. } => TableRequirement::CurrentSchemaIdMatch {
            current_schema_id: base.current_schema_id(),
        },
        TableRequirement::LastAssignedPartitionIdMatch { .. } => {
            TableRequirement::LastAssignedPartitionIdMatch {
                last_assigned_partition_id: base.last_partition_id(),
            }
        }
        TableRequirement::DefaultSpecIdMatch { .. } => TableRequirement::DefaultSpecIdMatch {
            default_spec_id: base.default_partition_spec_id(),
        },
        TableRequirement::DefaultSortOrderIdMatch { .. } => {
            TableRequirement::DefaultSortOrderIdMatch {
                default_sort_order_id: base.default_sort_order_id(),
            }
        }
    }
}

/// Java UpdateRequirements rules, evaluated against the immutable attempt base.
pub(super) fn implicit(
    base: &TableMetadata,
    shape: RequestShape,
    updates: &[TableUpdate],
    out: &mut Vec<TableRequirement>,
) {
    if shape == RequestShape::Create {
        return;
    }
    for update in updates {
        let requirement = match update {
            TableUpdate::AddSchema { .. } => Some(TableRequirement::LastAssignedFieldIdMatch {
                last_assigned_field_id: base.last_column_id(),
            }),
            TableUpdate::SetCurrentSchema { .. } => Some(TableRequirement::CurrentSchemaIdMatch {
                current_schema_id: base.current_schema_id(),
            }),
            TableUpdate::AddSpec { .. } => Some(TableRequirement::LastAssignedPartitionIdMatch {
                last_assigned_partition_id: base.last_partition_id(),
            }),
            TableUpdate::SetDefaultSpec { .. } | TableUpdate::RemovePartitionSpecs { .. } => {
                Some(TableRequirement::DefaultSpecIdMatch {
                    default_spec_id: base.default_partition_spec_id(),
                })
            }
            TableUpdate::SetDefaultSortOrder { .. } => {
                Some(TableRequirement::DefaultSortOrderIdMatch {
                    default_sort_order_id: base.default_sort_order_id(),
                })
            }
            TableUpdate::RemoveSchemas { .. } => Some(TableRequirement::CurrentSchemaIdMatch {
                current_schema_id: base.current_schema_id(),
            }),
            TableUpdate::SetSnapshotRef { ref_name, .. } => {
                Some(TableRequirement::RefSnapshotIdMatch {
                    r#ref: ref_name.clone(),
                    snapshot_id: base.snapshot_for_ref(ref_name).map(|s| s.snapshot_id()),
                })
            }
            _ => None,
        };
        if let Some(requirement) = requirement {
            push_unique(out, requirement);
        }
        if matches!(
            update,
            TableUpdate::RemoveSchemas { .. } | TableUpdate::RemovePartitionSpecs { .. }
        ) {
            let mut names: Vec<_> = base
                .refs()
                .iter()
                .filter(|(name, reference)| name.as_str() != "main" && reference.is_branch())
                .map(|(name, _)| name)
                .collect();
            names.sort();
            for name in names {
                push_unique(
                    out,
                    TableRequirement::RefSnapshotIdMatch {
                        r#ref: name.clone(),
                        snapshot_id: base.snapshot_for_ref(name).map(|s| s.snapshot_id()),
                    },
                );
            }
        }
    }
}
