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

use super::{
    StagedView, invalid,
    normalize::{self, Selections},
    requirements,
};
use crate::commit::model::{
    ArtifactWriter, BaseIdentity, FrozenRequest, FrozenRequestParts, ObjectIdentity,
    OperationIntent, RequestShape, StagedCreateIdentity,
};
use crate::iceberg::spec::TableMetadata;
use crate::iceberg::{Result, TableRequirement, TableUpdate};
use async_trait::async_trait;

pub enum StagingBase {
    Existing {
        metadata: TableMetadata,
        metadata_location: String,
    },
    Create {
        staged: StagedCreateIdentity,
        initialization_updates: Vec<TableUpdate>,
    },
}

#[derive(Clone, Default)]
pub struct PreparedChange {
    pub updates: Vec<TableUpdate>,
    pub requirements: Vec<TableRequirement>,
}

#[async_trait]
pub trait Preparer: Send + Sync {
    async fn prepare(
        &self,
        view: &StagedView<'_>,
        intent: &OperationIntent,
    ) -> Result<PreparedChange>;
}

pub struct StagingEngine<'a> {
    base: TableMetadata,
    identity: BaseIdentity,
    location: Option<String>,
    intent: &'a OperationIntent,
    artifacts: &'a dyn ArtifactWriter,
    metadata: TableMetadata,
    selections: Selections,
    updates: Vec<TableUpdate>,
    initialization_len: usize,
    requirements: Vec<TableRequirement>,
}

impl<'a> StagingEngine<'a> {
    pub fn begin(
        base: StagingBase,
        intent: &'a OperationIntent,
        artifacts: &'a dyn ArtifactWriter,
    ) -> Result<Self> {
        artifacts.check_active()?;
        if artifacts.attempt_token().operation() != intent.token() {
            return Err(invalid(
                "Staging attempt does not own this operation intent",
            ));
        }
        let mut selections = Selections::default();
        let (base, identity, location, updates, mut requirements) = match base {
            StagingBase::Existing {
                metadata,
                metadata_location,
            } => {
                if metadata_location.is_empty() {
                    return Err(invalid(
                        "Existing staging base has no authoritative metadata location",
                    ));
                }
                if intent.shape() == RequestShape::Create
                    || intent.target().uuid != Some(metadata.uuid())
                {
                    return Err(invalid(
                        "Staging base does not match the existing intent table UUID",
                    ));
                }
                let identity = BaseIdentity::Existing {
                    uuid: metadata.uuid(),
                    parent: metadata
                        .snapshot_for_ref(intent.target_ref())
                        .map(|s| s.snapshot_id()),
                    metadata_location: metadata_location.clone(),
                };
                let requirements = vec![TableRequirement::UuidMatch {
                    uuid: metadata.uuid(),
                }];
                (
                    metadata,
                    identity,
                    Some(metadata_location),
                    Vec::new(),
                    requirements,
                )
            }
            StagingBase::Create {
                staged,
                initialization_updates,
            } => {
                if intent.shape() != RequestShape::Create || staged.operation() != intent.token() {
                    return Err(invalid(
                        "Staged create identity does not match its operation",
                    ));
                }
                let base = staged.initial_metadata().clone();
                let updates = selections.initialize(&initialization_updates)?;
                validate_initialization(&base, &updates)?;
                (
                    base,
                    BaseIdentity::Create { staged },
                    None,
                    updates,
                    vec![TableRequirement::NotExist],
                )
            }
        };
        if intent.shape() == RequestShape::SnapshotProducing {
            requirements.push(TableRequirement::RefSnapshotIdMatch {
                r#ref: intent.target_ref().to_owned(),
                snapshot_id: base
                    .snapshot_for_ref(intent.target_ref())
                    .map(|s| s.snapshot_id()),
            });
        }
        let initialization_len = updates.len();
        Ok(Self {
            metadata: base.clone(),
            base,
            identity,
            location,
            intent,
            artifacts,
            selections,
            updates,
            initialization_len,
            requirements,
        })
    }
    pub fn metadata(&self) -> &TableMetadata {
        &self.metadata
    }
    pub fn updates(&self) -> &[TableUpdate] {
        &self.updates
    }
    pub fn view(&self) -> StagedView<'_> {
        StagedView::new(&self.metadata, self.intent, self.artifacts)
    }
    pub async fn stage(&mut self, preparer: &dyn Preparer) -> Result<()> {
        self.artifacts.check_active()?;
        let prepared = preparer.prepare(&self.view(), self.intent).await?;
        self.stage_change(prepared)
    }
    pub fn stage_change(&mut self, change: PreparedChange) -> Result<()> {
        self.artifacts.check_active()?;
        let mut requirements = self.requirements.clone();
        for requirement in &change.requirements {
            requirement.check(
                if *requirement == TableRequirement::NotExist
                    && self.intent.shape() == RequestShape::Create
                {
                    None
                } else {
                    Some(&self.metadata)
                },
            )?;
            if self.intent.shape() != RequestShape::Create {
                requirements::push_unique(
                    &mut requirements,
                    requirements::against_base(requirement, &self.base),
                );
            }
        }
        let mut selections = self.selections.clone();
        let mut updates = self.updates.clone();
        let mut metadata = self.metadata.clone();
        for update in change.updates {
            if self.intent.shape() == RequestShape::MetadataOnly
                && matches!(
                    update,
                    TableUpdate::AddSnapshot { .. } | TableUpdate::SetSnapshotRef { .. }
                )
            {
                return Err(invalid("Metadata-only staging cannot produce a snapshot"));
            }
            if let Some(update) = normalize::normalize(&metadata, update, &mut selections)? {
                updates.push(update);
                metadata = normalize::replay(
                    &self.base,
                    self.location.as_deref(),
                    &updates[self.initialization_len..],
                )?;
            }
        }
        requirements::implicit(&self.base, self.intent.shape(), &updates, &mut requirements);
        self.updates = updates;
        self.metadata = metadata;
        self.selections = selections;
        self.requirements = requirements;
        Ok(())
    }
    // Design: ADR-0171 (docs/adr/ADR-0171-commit-operation-model.md)
    pub fn freeze(self, owned_references: &[ObjectIdentity]) -> Result<FrozenRequest> {
        self.artifacts.check_active()?;
        FrozenRequest::new(FrozenRequestParts {
            shape: self.intent.shape(),
            target: self.intent.target().ident.clone(),
            target_ref: self.intent.target_ref().to_owned(),
            base: self.identity,
            requirements: self.requirements,
            updates: self.updates,
            artifacts: self.artifacts.snapshot_artifacts(owned_references)?,
        })
    }
}

fn validate_initialization(base: &TableMetadata, updates: &[TableUpdate]) -> Result<()> {
    if base.snapshots().len() != 0 || !base.refs().is_empty() {
        return Err(invalid(
            "Create initialization must precede its first snapshot",
        ));
    }
    let mut properties = std::collections::HashMap::new();
    for update in updates {
        let valid = match update {
            TableUpdate::UpgradeFormatVersion { format_version } => {
                *format_version == base.format_version()
            }
            TableUpdate::AssignUuid { uuid } => *uuid == base.uuid(),
            TableUpdate::SetLocation { location } => location == base.location(),
            TableUpdate::AddSchema {
                schema,
                last_column_id,
            } => {
                base.schema_by_id(schema.schema_id())
                    .is_some_and(|s| s.as_ref() == schema)
                    && last_column_id.is_none_or(|id| id == base.last_column_id())
            }
            TableUpdate::SetCurrentSchema { schema_id } => *schema_id == base.current_schema_id(),
            TableUpdate::AddSpec { spec } => spec
                .spec_id()
                .and_then(|id| base.partition_spec_by_id(id))
                .is_some_and(|existing| existing.as_ref().clone().into_unbound() == *spec),
            TableUpdate::SetDefaultSpec { spec_id } => *spec_id == base.default_partition_spec_id(),
            TableUpdate::AddSortOrder { sort_order } => base
                .sort_order_by_id(sort_order.order_id)
                .is_some_and(|s| s.as_ref() == sort_order),
            TableUpdate::SetDefaultSortOrder { sort_order_id } => {
                *sort_order_id == base.default_sort_order_id()
            }
            TableUpdate::SetProperties { updates } => {
                properties.extend(updates.clone());
                true
            }
            _ => false,
        };
        if !valid {
            return Err(invalid(
                "Create initialization contradicts authoritative initial metadata",
            ));
        }
    }
    if properties != *base.properties() {
        return Err(invalid(
            "Create initialization omits authoritative table properties",
        ));
    }
    for schema in base.schemas_iter() {
        if !updates.iter().any(|u| matches!(u, TableUpdate::AddSchema { schema: added, .. } if added == schema.as_ref())) {
            return Err(invalid("Create initialization omits an authoritative schema"));
        }
    }
    for spec in base.partition_specs_iter() {
        if !updates.iter().any(|u| matches!(u, TableUpdate::AddSpec { spec: added } if *added == spec.as_ref().clone().into_unbound())) {
            return Err(invalid("Create initialization omits an authoritative partition spec"));
        }
    }
    for order in base.sort_orders_iter() {
        if !updates.iter().any(|u| matches!(u, TableUpdate::AddSortOrder { sort_order: added } if added == order.as_ref())) {
            return Err(invalid("Create initialization omits an authoritative sort order"));
        }
    }
    let required = [
        updates.iter().any(|u| matches!(u, TableUpdate::AssignUuid { uuid } if *uuid == base.uuid())),
        updates.iter().any(|u| matches!(u, TableUpdate::UpgradeFormatVersion { format_version } if *format_version == base.format_version())),
        updates.iter().any(|u| matches!(u, TableUpdate::SetLocation { location } if location == base.location())),
        updates.iter().any(|u| matches!(u, TableUpdate::AddSchema { schema, .. } if base.schema_by_id(schema.schema_id()).is_some_and(|s| s.as_ref() == schema))),
        updates.iter().any(|u| matches!(u, TableUpdate::SetCurrentSchema { schema_id } if *schema_id == base.current_schema_id())),
        updates.iter().any(|u| matches!(u, TableUpdate::AddSpec { spec } if spec.spec_id() == Some(base.default_partition_spec_id()))),
        updates.iter().any(|u| matches!(u, TableUpdate::SetDefaultSpec { spec_id } if *spec_id == base.default_partition_spec_id())),
        updates.iter().any(|u| matches!(u, TableUpdate::AddSortOrder { sort_order } if sort_order == base.default_sort_order().as_ref())),
        updates.iter().any(|u| matches!(u, TableUpdate::SetDefaultSortOrder { sort_order_id } if *sort_order_id == base.default_sort_order_id())),
    ];
    if required.into_iter().any(|present| !present) {
        return Err(invalid(
            "Create request lacks authoritative initialization updates",
        ));
    }
    Ok(())
}
