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

//! Phase-1 metadata-only commit for `CREATE/DROP BRANCH|TAG`.
//!
//! Unlike the six data-commit modules, ref actions never produce a new
//! snapshot — they emit only `SetSnapshotRef` / `RemoveSnapshotRef`
//! `TableUpdate`s plus an `AssertRefSnapshotId` requirement.

use crate::iceberg::{TableIdent, TableRequirement, TableUpdate};
use uuid::Uuid;

#[derive(Clone, Debug, PartialEq)]
pub struct RefActionPlan {
    pub catalog: String,
    pub namespace: String,
    pub table: String,
    pub action: RefAction,
}

#[derive(Clone, Debug, PartialEq)]
pub enum RefAction {
    CreateBranch {
        name: String,
        snapshot_id: i64,
        replace: bool,
        if_not_exists: bool,
        expected_table_uuid: Option<Uuid>,
    },
    CreateTag {
        name: String,
        snapshot_id: i64,
        replace: bool,
        if_not_exists: bool,
        expected_table_uuid: Option<Uuid>,
    },
    DropBranch {
        name: String,
        if_exists: bool,
    },
    DropTag {
        name: String,
        if_exists: bool,
    },
    /// Internal MV publication primitive. SQL never constructs this action.
    FastForwardBranch {
        source_branch: String,
        target_branch: String,
        source_snapshot_id: i64,
        expected_target_snapshot_id: Option<i64>,
    },
}

/// Lower a provider-independent reference action into the Iceberg commit
/// primitive after validating the pinned table metadata.
pub fn lower_ref_action(
    action: novarocks_spi::connector::ConnectorRefAction,
    metadata: &crate::iceberg::spec::TableMetadata,
    namespace: &str,
    table: &str,
    catalog: &str,
) -> Result<RefActionPlan, novarocks_spi::connector::ConnectorError> {
    use novarocks_spi::connector::{
        ConnectorError, ConnectorErrorKind, ConnectorRefAction, ConnectorRefKind,
        CreateOrReplacePolicy, DropPolicy,
    };

    fn assert_kind(
        metadata: &crate::iceberg::spec::TableMetadata,
        name: &str,
        expected: ConnectorRefKind,
    ) -> Result<(), ConnectorError> {
        let Some(existing) = metadata.refs().get(name) else {
            return Ok(());
        };
        let actual = match existing.retention {
            crate::iceberg::spec::SnapshotRetention::Branch { .. } => ConnectorRefKind::Branch,
            crate::iceberg::spec::SnapshotRetention::Tag { .. } => ConnectorRefKind::Tag,
        };
        if actual == expected {
            return Ok(());
        }
        Err(ConnectorError::new(
            ConnectorErrorKind::InvalidRequest,
            format!("Iceberg ref `{name}` has a different kind"),
        ))
    }

    let action = match action {
        ConnectorRefAction::Create {
            kind,
            name,
            snapshot_id,
            policy,
            expected_table_uuid,
        } => {
            if name.eq_ignore_ascii_case("main") {
                return Err(ConnectorError::new(
                    ConnectorErrorKind::InvalidRequest,
                    "Iceberg ref `main` is reserved",
                ));
            }
            assert_kind(metadata, &name, kind)?;
            let expected_table_uuid = expected_table_uuid
                .as_deref()
                .map(Uuid::parse_str)
                .transpose()
                .map_err(|error| {
                    ConnectorError::new(
                        ConnectorErrorKind::InvalidRequest,
                        format!("Iceberg ref create has an invalid expected table UUID: {error}"),
                    )
                })?;
            if expected_table_uuid.is_some_and(|uuid| uuid != metadata.uuid()) {
                return Err(ConnectorError::new(
                    ConnectorErrorKind::InvalidRequest,
                    "Iceberg ref create target table incarnation changed",
                ));
            }
            let snapshot_id = match snapshot_id.or_else(|| metadata.current_snapshot_id()) {
                Some(snapshot_id) if metadata.snapshot_by_id(snapshot_id).is_some() => snapshot_id,
                Some(snapshot_id) => {
                    return Err(ConnectorError::new(
                        ConnectorErrorKind::NotFound,
                        format!(
                            "Iceberg ref create requires an existing snapshot; snapshot {snapshot_id} was not found"
                        ),
                    ));
                }
                None => {
                    return Err(ConnectorError::new(
                        ConnectorErrorKind::NotFound,
                        "Iceberg ref create requires an existing snapshot",
                    ));
                }
            };
            let (replace, if_not_exists) = match policy {
                CreateOrReplacePolicy::FailIfExists => (false, false),
                CreateOrReplacePolicy::NoOpIfExists => (false, true),
                CreateOrReplacePolicy::ReplaceIfExists => (true, false),
            };
            match kind {
                ConnectorRefKind::Branch => RefAction::CreateBranch {
                    name: name.to_string(),
                    snapshot_id,
                    replace,
                    if_not_exists,
                    expected_table_uuid,
                },
                ConnectorRefKind::Tag => RefAction::CreateTag {
                    name: name.to_string(),
                    snapshot_id,
                    replace,
                    if_not_exists,
                    expected_table_uuid,
                },
            }
        }
        ConnectorRefAction::Drop { kind, name, policy } => {
            if name.eq_ignore_ascii_case("main") {
                return Err(ConnectorError::new(
                    ConnectorErrorKind::InvalidRequest,
                    "Iceberg ref `main` is reserved",
                ));
            }
            assert_kind(metadata, &name, kind)?;
            let if_exists = policy == DropPolicy::NoOpIfMissing;
            match kind {
                ConnectorRefKind::Branch => RefAction::DropBranch {
                    name: name.to_string(),
                    if_exists,
                },
                ConnectorRefKind::Tag => RefAction::DropTag {
                    name: name.to_string(),
                    if_exists,
                },
            }
        }
        ConnectorRefAction::FastForwardBranch { .. } => {
            return Err(ConnectorError::new(
                ConnectorErrorKind::Internal,
                "guarded MV publication bypassed its provider commit path",
            ));
        }
    };
    Ok(RefActionPlan {
        catalog: catalog.to_string(),
        namespace: namespace.to_string(),
        table: table.to_string(),
        action,
    })
}

impl RefActionPlan {
    /// Lower against the exact locked observation. Publication belongs to the journal.
    pub(crate) fn prepare(
        &self,
        loaded: &crate::iceberg::table::Table,
    ) -> Result<
        crate::commit::staging::PreparedChange,
        novarocks_spi::connector::ConnectorMutationFailure,
    > {
        use crate::commit::staging::PreparedChange;
        use novarocks_spi::connector::{
            ConnectorMutationFailure, ConnectorMutationFailureKind as Kind,
        };
        let invalid =
            |message: String| ConnectorMutationFailure::new(Kind::InvalidRequest, message);
        let missing = |message: String| ConnectorMutationFailure::new(Kind::NotFound, message);
        let ident = TableIdent::from_strs([self.namespace.as_str(), self.table.as_str()])
            .map_err(|error| invalid(format!("Invalid Iceberg ref table identifier: {error}")))?;
        if loaded.identifier() != &ident {
            return Err(invalid(
                "Request-scoped table identity does not match the ref plan".into(),
            ));
        }
        let metadata = loaded.metadata();
        let (updates, mut requirements) = match &self.action {
            RefAction::CreateBranch {
                name,
                snapshot_id,
                replace,
                if_not_exists,
                expected_table_uuid,
            }
            | RefAction::CreateTag {
                name,
                snapshot_id,
                replace,
                if_not_exists,
                expected_table_uuid,
            } => {
                if expected_table_uuid.is_some_and(|uuid| uuid != metadata.uuid()) {
                    return Err(invalid("Iceberg ref target table UUID changed".into()));
                }
                let branch = matches!(&self.action, RefAction::CreateBranch { .. });
                let existing = metadata.refs().get(name);
                if existing.is_some_and(|reference| reference.is_branch() != branch) {
                    return Err(invalid(format!(
                        "Iceberg ref '{name}' has a different kind"
                    )));
                }
                if existing.is_some() {
                    if *if_not_exists {
                        return Ok(PreparedChange::default());
                    }
                    if !*replace {
                        return Err(ConnectorMutationFailure::new(
                            Kind::AlreadyExists,
                            format!("Iceberg ref '{name}' already exists"),
                        ));
                    }
                }
                if metadata.snapshot_by_id(*snapshot_id).is_none() {
                    return Err(missing(format!(
                        "Iceberg ref snapshot {snapshot_id} does not exist"
                    )));
                }
                (
                    vec![TableUpdate::SetSnapshotRef {
                        ref_name: name.clone(),
                        reference: crate::ref_snapshot::moved_reference(
                            existing,
                            *snapshot_id,
                            branch,
                        ),
                    }],
                    vec![TableRequirement::RefSnapshotIdMatch {
                        r#ref: name.clone(),
                        snapshot_id: existing.map(|reference| reference.snapshot_id),
                    }],
                )
            }
            RefAction::DropBranch { name, if_exists } | RefAction::DropTag { name, if_exists } => {
                let Some(existing) = metadata.refs().get(name) else {
                    if *if_exists {
                        return Ok(PreparedChange::default());
                    }
                    return Err(missing(format!("Iceberg ref '{name}' does not exist")));
                };
                let branch = matches!(&self.action, RefAction::DropBranch { .. });
                if existing.is_branch() != branch {
                    return Err(invalid(format!(
                        "Iceberg ref '{name}' has a different kind"
                    )));
                }
                (
                    vec![TableUpdate::RemoveSnapshotRef {
                        ref_name: name.clone(),
                    }],
                    vec![TableRequirement::RefSnapshotIdMatch {
                        r#ref: name.clone(),
                        snapshot_id: Some(existing.snapshot_id),
                    }],
                )
            }
            RefAction::FastForwardBranch {
                source_branch,
                target_branch,
                source_snapshot_id,
                expected_target_snapshot_id,
            } => {
                let source = metadata.refs().get(source_branch).ok_or_else(|| {
                    missing(format!(
                        "Iceberg source branch '{source_branch}' does not exist"
                    ))
                })?;
                if !source.is_branch() || source.snapshot_id != *source_snapshot_id {
                    return Err(invalid(format!(
                        "Iceberg source branch '{source_branch}' changed"
                    )));
                }
                if metadata.snapshot_by_id(*source_snapshot_id).is_none() {
                    return Err(missing(format!(
                        "Iceberg source snapshot {source_snapshot_id} does not exist"
                    )));
                }
                let target = metadata.refs().get(target_branch);
                if target.is_some_and(|reference| !reference.is_branch()) {
                    return Err(invalid(format!(
                        "Iceberg target '{target_branch}' is not a branch"
                    )));
                }
                let head = if target_branch == "main" {
                    metadata.current_snapshot_id()
                } else {
                    Some(
                        target
                            .ok_or_else(|| {
                                missing(format!(
                                    "Iceberg target branch '{target_branch}' does not exist"
                                ))
                            })?
                            .snapshot_id,
                    )
                };
                if head != *expected_target_snapshot_id {
                    return Err(ConnectorMutationFailure::new(
                        Kind::Conflict,
                        "Iceberg target branch changed before publication",
                    ));
                }
                (
                    vec![TableUpdate::SetSnapshotRef {
                        ref_name: target_branch.clone(),
                        reference: crate::ref_snapshot::moved_reference(
                            target,
                            *source_snapshot_id,
                            true,
                        ),
                    }],
                    vec![
                        TableRequirement::RefSnapshotIdMatch {
                            r#ref: source_branch.clone(),
                            snapshot_id: Some(*source_snapshot_id),
                        },
                        TableRequirement::RefSnapshotIdMatch {
                            r#ref: target_branch.clone(),
                            snapshot_id: *expected_target_snapshot_id,
                        },
                    ],
                )
            }
        };
        requirements.insert(
            0,
            TableRequirement::UuidMatch {
                uuid: metadata.uuid(),
            },
        );
        Ok(PreparedChange {
            updates,
            requirements,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::iceberg::spec::{
        FormatVersion, NestedField, Operation, PartitionSpec, PrimitiveType, Schema, Snapshot,
        SnapshotReference, SnapshotRetention, SortOrder, Summary, TableMetadata,
        TableMetadataBuilder, Type,
    };
    use crate::iceberg::table::Table;
    use std::collections::HashMap;
    use std::sync::Arc;

    fn metadata() -> TableMetadata {
        let schema = Schema::builder()
            .with_fields(vec![Arc::new(NestedField::required(
                1,
                "id",
                Type::Primitive(PrimitiveType::Long),
            ))])
            .build()
            .unwrap();
        let base = TableMetadataBuilder::new(
            schema,
            PartitionSpec::unpartition_spec(),
            SortOrder::unsorted_order(),
            "memory://warehouse/db/t".into(),
            FormatVersion::V2,
            HashMap::new(),
        )
        .unwrap()
        .assign_uuid(Uuid::from_u128(7))
        .build()
        .unwrap()
        .metadata;
        let time = base.last_updated_ms();
        let snapshot = |id| {
            Snapshot::builder()
                .with_snapshot_id(id)
                .with_sequence_number(id)
                .with_timestamp_ms(time)
                .with_manifest_list(format!("memory://snap-{id}.avro"))
                .with_summary(Summary {
                    operation: Operation::Append,
                    additional_properties: HashMap::new(),
                })
                .build()
        };
        base.into_builder(None)
            .add_snapshot(snapshot(7))
            .unwrap()
            .add_snapshot(snapshot(11))
            .unwrap()
            .set_ref(
                "main",
                SnapshotReference::new(7, SnapshotRetention::branch(None, None, None)),
            )
            .unwrap()
            .build()
            .unwrap()
            .metadata
    }
    fn table(metadata: TableMetadata) -> Table {
        Table::builder()
            .runtime(crate::iceberg::Runtime::try_current().unwrap())
            .identifier(TableIdent::from_strs(["db", "t"]).unwrap())
            .file_io(crate::iceberg::io::FileIO::new_with_memory())
            .metadata(metadata)
            .build()
            .unwrap()
    }
    fn plan(action: RefAction) -> RefActionPlan {
        RefActionPlan {
            catalog: "ice".into(),
            namespace: "db".into(),
            table: "t".into(),
            action,
        }
    }
    fn apply(table: &Table, change: crate::commit::staging::PreparedChange) -> TableMetadata {
        for requirement in &change.requirements {
            requirement.check(Some(table.metadata())).unwrap();
        }
        let mut builder = table.metadata().clone().into_builder(None);
        for update in change.updates {
            builder = update.apply(builder).unwrap();
        }
        builder.build().unwrap().metadata
    }
    #[tokio::test]
    async fn every_ref_action_freezes_the_uuid_of_the_metadata_it_used() {
        let mut loaded = table(metadata());
        let create_branch = RefAction::CreateBranch {
            name: "dev".into(),
            snapshot_id: 7,
            replace: false,
            if_not_exists: false,
            expected_table_uuid: None,
        };
        let create_tag = RefAction::CreateTag {
            name: "v1".into(),
            snapshot_id: 7,
            replace: false,
            if_not_exists: false,
            expected_table_uuid: None,
        };
        let fast_forward = RefAction::FastForwardBranch {
            source_branch: "main".into(),
            target_branch: "dev".into(),
            source_snapshot_id: 11,
            expected_target_snapshot_id: Some(7),
        };
        let drop_tag = RefAction::DropTag {
            name: "v1".into(),
            if_exists: false,
        };
        let drop_branch = RefAction::DropBranch {
            name: "dev".into(),
            if_exists: false,
        };
        for (index, action) in [
            create_branch,
            create_tag,
            fast_forward,
            drop_tag,
            drop_branch,
        ]
        .into_iter()
        .enumerate()
        {
            if index == 2 {
                loaded = table(
                    loaded
                        .metadata()
                        .clone()
                        .into_builder(None)
                        .set_ref(
                            "main",
                            SnapshotReference::new(11, SnapshotRetention::branch(None, None, None)),
                        )
                        .unwrap()
                        .build()
                        .unwrap()
                        .metadata,
                );
            }
            let change = plan(action).prepare(&loaded).unwrap();
            let uuids = change
                .requirements
                .iter()
                .filter_map(|r| match r {
                    TableRequirement::UuidMatch { uuid } => Some(*uuid),
                    _ => None,
                })
                .collect::<Vec<_>>();
            assert_eq!(uuids, vec![Uuid::from_u128(7)]);
            loaded = table(apply(&loaded, change));
        }
        assert_eq!(
            loaded
                .metadata()
                .refs()
                .keys()
                .map(String::as_str)
                .collect::<Vec<_>>(),
            vec!["main"]
        );
        assert_eq!(loaded.metadata().current_snapshot_id(), Some(11));
    }
    #[tokio::test]
    async fn ref_create_with_another_incarnation_uuid_is_not_dispatched() {
        let loaded = table(metadata());
        let error = plan(RefAction::CreateBranch {
            name: "dev".into(),
            snapshot_id: 7,
            replace: false,
            if_not_exists: false,
            expected_table_uuid: Some(Uuid::from_u128(8)),
        })
        .prepare(&loaded)
        .err()
        .expect("preparation must refuse");
        assert_eq!(
            error.kind(),
            novarocks_spi::connector::ConnectorMutationFailureKind::InvalidRequest
        );
        assert!(error.message().contains("table UUID"));
        assert!(!loaded.metadata().refs().contains_key("dev"));
    }
    #[tokio::test]
    async fn pure_ref_replacement_preserves_policy_and_exact_cas() {
        for branch in [true, false] {
            let retention = if branch {
                SnapshotRetention::branch(Some(11), Some(12), Some(13))
            } else {
                SnapshotRetention::Tag {
                    max_ref_age_ms: Some(13),
                }
            };
            let loaded = table(
                metadata()
                    .into_builder(None)
                    .set_ref("release", SnapshotReference::new(7, retention.clone()))
                    .unwrap()
                    .build()
                    .unwrap()
                    .metadata,
            );
            let action = |replace, if_not_exists| {
                if branch {
                    RefAction::CreateBranch {
                        name: "release".into(),
                        snapshot_id: 11,
                        replace,
                        if_not_exists,
                        expected_table_uuid: Some(Uuid::from_u128(7)),
                    }
                } else {
                    RefAction::CreateTag {
                        name: "release".into(),
                        snapshot_id: 11,
                        replace,
                        if_not_exists,
                        expected_table_uuid: Some(Uuid::from_u128(7)),
                    }
                }
            };
            assert_eq!(
                plan(action(false, false))
                    .prepare(&loaded)
                    .err()
                    .expect("preparation must refuse")
                    .kind(),
                novarocks_spi::connector::ConnectorMutationFailureKind::AlreadyExists
            );
            assert!(
                plan(action(false, true))
                    .prepare(&loaded)
                    .unwrap()
                    .updates
                    .is_empty()
            );
            let change = plan(action(true, false)).prepare(&loaded).unwrap();
            assert_eq!(
                change.requirements,
                vec![
                    TableRequirement::UuidMatch {
                        uuid: Uuid::from_u128(7)
                    },
                    TableRequirement::RefSnapshotIdMatch {
                        r#ref: "release".into(),
                        snapshot_id: Some(7)
                    }
                ]
            );
            let [
                TableUpdate::SetSnapshotRef {
                    ref_name,
                    reference,
                },
            ] = change.updates.as_slice()
            else {
                panic!("one ref update")
            };
            assert_eq!(ref_name, "release");
            assert_eq!(reference.snapshot_id, 11);
            assert_eq!(reference.retention, retention);
        }
    }
}
