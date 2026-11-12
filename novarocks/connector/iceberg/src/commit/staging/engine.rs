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
    ArtifactWriter, AttemptArtifacts, AttemptToken, BaseIdentity, FileChanges, FrozenRequest,
    FrozenRequestParts, IsolationLevel, ObjectIdentity, OperationIntent, OperationIntentParts,
    RequestShape, StagedCreateIdentity, TableTarget,
};
use crate::commit::preparation::{CapturedMergePolicy, ManifestMergePolicy, PreparationContext};
use crate::iceberg::spec::{ManifestList, TableMetadata};
use crate::iceberg::{Error, ErrorKind, Result, TableIdent, TableRequirement, TableUpdate};
use async_trait::async_trait;
use novarocks_spi::connector::{ConnectorOperationControl, ConnectorRequestContext};

#[expect(
    clippy::large_enum_variant,
    reason = "The metadata snapshot is moved directly into staging; boxing it would add an allocation without avoiding a copy"
)]
#[derive(Clone)]
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

// Canonical replay and requirement derivation are pure. Artifact-producing
// preparers and metadata-only callers share this state without sharing I/O.
struct StagingState {
    base: TableMetadata,
    identity: BaseIdentity,
    location: Option<String>,
    metadata: TableMetadata,
    selections: Selections,
    updates: Vec<TableUpdate>,
    initialization_len: usize,
    requirements: Vec<TableRequirement>,
}

impl StagingState {
    fn begin(base: StagingBase, intent: &OperationIntent) -> Result<Self> {
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
        if intent.shape() == RequestShape::DocumentExpiry {
            requirements::push_unique(
                &mut requirements,
                TableRequirement::RefSnapshotIdMatch {
                    r#ref: intent.target_ref().to_owned(),
                    snapshot_id: base
                        .snapshot_for_ref(intent.target_ref())
                        .map(|s| s.snapshot_id()),
                },
            );
            let mut refs: Vec<_> = base.refs().iter().collect();
            refs.sort_by_key(|(name, _)| *name);
            for (name, reference) in refs {
                requirements::push_unique(
                    &mut requirements,
                    TableRequirement::RefSnapshotIdMatch {
                        r#ref: name.clone(),
                        snapshot_id: Some(reference.snapshot_id),
                    },
                );
            }
        }
        let initialization_len = updates.len();
        Ok(Self {
            metadata: base.clone(),
            base,
            identity,
            location,
            selections,
            updates,
            initialization_len,
            requirements,
        })
    }
    fn stage_change(&mut self, intent: &OperationIntent, change: PreparedChange) -> Result<()> {
        let mut requirements = self.requirements.clone();
        for requirement in &change.requirements {
            requirement.check(
                if *requirement == TableRequirement::NotExist
                    && intent.shape() == RequestShape::Create
                {
                    None
                } else {
                    Some(&self.metadata)
                },
            )?;
            if intent.shape() != RequestShape::Create {
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
            if intent.shape() == RequestShape::DocumentExpiry {
                crate::commit::model::validate_document_expiry_update(&update)?;
            }
            if intent.shape() == RequestShape::RefRetention {
                crate::commit::model::validate_ref_retention_update(&update)?;
            }
            if intent.shape() == RequestShape::MetadataOnly
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
        requirements::implicit(&self.base, intent.shape(), &updates, &mut requirements);
        self.updates = updates;
        self.metadata = metadata;
        self.selections = selections;
        self.requirements = requirements;
        Ok(())
    }
    fn freeze(
        self,
        intent: &OperationIntent,
        artifacts: AttemptArtifacts,
    ) -> Result<FrozenRequest> {
        FrozenRequest::new(FrozenRequestParts {
            shape: intent.shape(),
            target: intent.target().ident.clone(),
            target_ref: intent.target_ref().to_owned(),
            base: self.identity,
            requirements: self.requirements,
            updates: self.updates,
            artifacts,
        })?
        .with_base_facts(&self.base)
    }
}

pub struct StagingEngine<'a> {
    state: StagingState,
    intent: &'a OperationIntent,
    artifacts: &'a dyn ArtifactWriter,
    context: PreparationContext,
    completed_list: Option<(String, ManifestList)>,
}

impl<'a> StagingEngine<'a> {
    #[cfg(test)]
    pub fn begin(
        base: StagingBase,
        intent: &'a OperationIntent,
        artifacts: &'a dyn ArtifactWriter,
    ) -> Result<Self> {
        // Direct staging fixtures drive their FileIO serially.
        Self::begin_attempt(base, intent, artifacts, 1, &mut None)
    }

    pub(crate) fn begin_attempt(
        base: StagingBase,
        intent: &'a OperationIntent,
        artifacts: &'a dyn ArtifactWriter,
        io_window_width: usize,
        captured: &mut Option<CapturedMergePolicy>,
    ) -> Result<Self> {
        artifacts.check_active()?;
        if artifacts.attempt_token().operation() != intent.token() {
            return Err(invalid(
                "Staging attempt does not own this operation intent",
            ));
        }
        let state = StagingState::begin(base, intent)?;
        let merge_policy = match *captured {
            Some(policy) => policy,
            None => {
                let policy = ManifestMergePolicy::capture(&state.metadata);
                *captured = Some(policy);
                policy
            }
        };
        Ok(Self {
            state,
            intent,
            artifacts,
            context: PreparationContext {
                merge_policy,
                io_window_width,
            },
            completed_list: None,
        })
    }
    pub fn metadata(&self) -> &TableMetadata {
        &self.state.metadata
    }
    pub(crate) async fn snapshot_sources<'s>(
        &'s self,
        snapshot: &'s crate::iceberg::spec::Snapshot,
    ) -> Result<crate::commit::manifest_sources::SnapshotSources<'s>> {
        super::view::load_snapshot_sources(
            self.metadata(),
            self.artifacts,
            self.completed_list.as_ref(),
            snapshot,
        )
        .await
    }

    pub fn updates(&self) -> &[TableUpdate] {
        &self.state.updates
    }
    pub fn view(&self) -> StagedView<'_> {
        StagedView::new(
            &self.state.metadata,
            self.intent,
            self.artifacts,
            self.context,
            self.completed_list.as_ref(),
        )
    }
    pub async fn stage(&mut self, preparer: &dyn Preparer) -> Result<()> {
        self.artifacts.check_active()?;
        let prepared = preparer.prepare(&self.view(), self.intent).await?;
        self.artifacts.check_active()?;
        let mut completed_list = None;
        for update in &prepared.updates {
            if let TableUpdate::AddSnapshot { snapshot } = update {
                let list = crate::manifest::load_manifest_list(
                    snapshot,
                    self.artifacts.file_io(),
                    &self.state.metadata,
                )
                .await?;
                completed_list = Some((snapshot.manifest_list().to_owned(), list));
            }
        }
        self.stage_change(prepared)?;
        if completed_list.is_some() {
            self.completed_list = completed_list;
        }
        Ok(())
    }
    pub fn stage_change(&mut self, change: PreparedChange) -> Result<()> {
        self.artifacts.check_active()?;
        self.state.stage_change(self.intent, change)
    }
    // Design: ADR-0171 (docs/adr/ADR-0171-commit-operation-model.md)
    pub fn freeze(self, owned_references: &[ObjectIdentity]) -> Result<FrozenRequest> {
        self.artifacts.check_active()?;
        self.state.freeze(
            self.intent,
            self.artifacts.snapshot_artifacts(owned_references)?,
        )
    }
}

/// Freeze an existing-table metadata/ref change through canonical staging.
/// This capability owns no FileIO and cannot allocate or adopt any artifact.
pub(crate) fn freeze_existing_change(
    target: TableIdent,
    target_ref: &str,
    base: &TableMetadata,
    metadata_location: &str,
    shape: RequestShape,
    attempt: AttemptToken,
    change: PreparedChange,
    context: &ConnectorRequestContext,
) -> Result<FrozenRequest> {
    let check = || {
        context.check_active().map_err(|error| {
            Error::new(ErrorKind::Unexpected, "Iceberg staging preparation stopped")
                .with_source(error)
        })
    };
    check()?;
    if !matches!(
        shape,
        RequestShape::MetadataOnly | RequestShape::RefRetention
    ) {
        return Err(invalid(
            "Artifactless staging requires a metadata or ref-retention shape",
        ));
    }
    let intent = OperationIntent::new(OperationIntentParts {
        target: TableTarget {
            ident: target,
            uuid: Some(base.uuid()),
        },
        target_ref: target_ref.to_owned(),
        start: None,
        changes: FileChanges::default(),
        dependencies: Vec::new(),
        isolation: IsolationLevel::Snapshot,
        shape,
        summary: Default::default(),
        token: attempt.operation(),
    })?;
    let mut state = StagingState::begin(
        StagingBase::Existing {
            metadata: base.clone(),
            metadata_location: metadata_location.to_owned(),
        },
        &intent,
    )?;
    state.stage_change(&intent, change)?;
    check()?;
    state.freeze(&intent, AttemptArtifacts::empty(attempt))
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
            TableUpdate::AddSchema { schema } => base
                .schema_by_id(schema.schema_id())
                .is_some_and(|s| s.as_ref() == schema),
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
