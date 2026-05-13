use std::collections::BTreeMap;

use iceberg::spec::{Snapshot, SnapshotReference};
use iceberg::{Catalog, TableCommit, TableIdent, TableRequirement, TableUpdate};

pub const MV_REFRESH_ID_PROP: &str = "novarocks.mv.refresh_id";
pub const MV_ID_PROP: &str = "novarocks.mv.id";
pub const MV_REFRESH_TOKEN_PROP: &str = "novarocks.mv.refresh_token";

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MvRefreshSnapshotMarker {
    pub refresh_id: i64,
    pub mv_id: i64,
    pub token: String,
}

impl MvRefreshSnapshotMarker {
    pub fn to_summary_properties(&self) -> BTreeMap<String, String> {
        BTreeMap::from([
            (MV_REFRESH_ID_PROP.to_string(), self.refresh_id.to_string()),
            (MV_ID_PROP.to_string(), self.mv_id.to_string()),
            (MV_REFRESH_TOKEN_PROP.to_string(), self.token.clone()),
        ])
    }
}

pub fn snapshot_matches_refresh_marker(
    snapshot: &Snapshot,
    marker: &MvRefreshSnapshotMarker,
) -> bool {
    let props = &snapshot.summary().additional_properties;
    props
        .get(MV_REFRESH_ID_PROP)
        .and_then(|value| value.parse::<i64>().ok())
        == Some(marker.refresh_id)
        && props
            .get(MV_ID_PROP)
            .and_then(|value| value.parse::<i64>().ok())
            == Some(marker.mv_id)
        && props.get(MV_REFRESH_TOKEN_PROP).map(String::as_str) == Some(marker.token.as_str())
}

fn ensure_staging_ref_is_branch(
    staging_branch: &str,
    staging_ref: &SnapshotReference,
) -> Result<(), String> {
    if !staging_ref.is_branch() {
        return Err(format!(
            "iceberg mv publish: staging ref {staging_branch} is a tag, expected branch"
        ));
    }
    Ok(())
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MvRefreshPublishPlan {
    pub namespace: String,
    pub table: String,
    pub staging_branch: String,
    pub expected_main_snapshot_id: Option<i64>,
    pub staging_snapshot_id: i64,
    pub marker: MvRefreshSnapshotMarker,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MvRefreshPublishOutcome {
    pub published_snapshot_id: i64,
}

pub async fn publish_staging_branch_to_main(
    catalog: &dyn Catalog,
    plan: &MvRefreshPublishPlan,
) -> Result<MvRefreshPublishOutcome, String> {
    let ident = TableIdent::from_strs([plan.namespace.as_str(), plan.table.as_str()])
        .map_err(|e| format!("iceberg mv publish: invalid table identifier: {e}"))?;
    let table = catalog
        .load_table(&ident)
        .await
        .map_err(|e| format!("iceberg mv publish: load table failed: {e}"))?;
    let metadata = table.metadata();
    let main_snapshot = metadata.current_snapshot().map(|s| s.snapshot_id());
    if main_snapshot != plan.expected_main_snapshot_id {
        return Err(format!(
            "iceberg mv publish: main snapshot mismatch for {}.{}: expected {:?}, current {:?}",
            plan.namespace, plan.table, plan.expected_main_snapshot_id, main_snapshot
        ));
    }
    let staging_ref = metadata.refs().get(&plan.staging_branch).ok_or_else(|| {
        format!(
            "iceberg mv publish: staging branch {} does not exist",
            plan.staging_branch
        )
    })?;
    ensure_staging_ref_is_branch(&plan.staging_branch, staging_ref)?;
    if staging_ref.snapshot_id != plan.staging_snapshot_id {
        return Err(format!(
            "iceberg mv publish: staging branch {} points to {}, expected {}",
            plan.staging_branch, staging_ref.snapshot_id, plan.staging_snapshot_id
        ));
    }
    let staging_snapshot = metadata
        .snapshot_by_id(plan.staging_snapshot_id)
        .ok_or_else(|| {
            format!(
                "iceberg mv publish: staging snapshot {} not found",
                plan.staging_snapshot_id
            )
        })?;
    if !snapshot_matches_refresh_marker(staging_snapshot, &plan.marker) {
        return Err(format!(
            "iceberg mv publish: staging snapshot {} marker mismatch",
            plan.staging_snapshot_id
        ));
    }

    let commit = build_publish_commit(ident, plan);
    catalog
        .update_table(commit)
        .await
        .map_err(|e| format!("iceberg mv publish: commit failed: {e}"))?;
    Ok(MvRefreshPublishOutcome {
        published_snapshot_id: plan.staging_snapshot_id,
    })
}

fn build_publish_commit(ident: TableIdent, plan: &MvRefreshPublishPlan) -> TableCommit {
    TableCommit::builder()
        .ident(ident)
        .updates(vec![TableUpdate::SetSnapshotRef {
            ref_name: "main".to_string(),
            reference: iceberg::spec::SnapshotReference {
                snapshot_id: plan.staging_snapshot_id,
                retention: iceberg::spec::SnapshotRetention::Branch {
                    min_snapshots_to_keep: None,
                    max_snapshot_age_ms: None,
                    max_ref_age_ms: None,
                },
            },
        }])
        .requirements(vec![
            TableRequirement::RefSnapshotIdMatch {
                r#ref: "main".to_string(),
                snapshot_id: plan.expected_main_snapshot_id,
            },
            TableRequirement::RefSnapshotIdMatch {
                r#ref: plan.staging_branch.clone(),
                snapshot_id: Some(plan.staging_snapshot_id),
            },
        ])
        .build()
}

#[cfg(test)]
mod tests {
    use super::*;
    use iceberg::spec::{Operation, SnapshotRetention, Summary};

    #[test]
    fn marker_round_trips_through_snapshot_summary() {
        let marker = MvRefreshSnapshotMarker {
            refresh_id: 77,
            mv_id: 12,
            token: "token-77".to_string(),
        };
        let summary = Summary {
            operation: Operation::Append,
            additional_properties: marker.to_summary_properties().into_iter().collect(),
        };
        let snapshot = Snapshot::builder()
            .with_snapshot_id(300)
            .with_sequence_number(1)
            .with_timestamp_ms(1)
            .with_manifest_list("file:/tmp/manifest-list.avro".to_string())
            .with_summary(summary)
            .with_schema_id(0)
            .build();
        assert!(snapshot_matches_refresh_marker(&snapshot, &marker));
    }

    #[test]
    fn marker_rejects_missing_non_numeric_and_wrong_token_properties() {
        let marker = MvRefreshSnapshotMarker {
            refresh_id: 77,
            mv_id: 12,
            token: "token-77".to_string(),
        };
        let mut props = marker.to_summary_properties();
        props.remove(MV_REFRESH_ID_PROP);
        assert!(!snapshot_matches_refresh_marker(
            &snapshot_with_properties(props),
            &marker
        ));

        let mut props = marker.to_summary_properties();
        props.insert(MV_REFRESH_ID_PROP.to_string(), "not-a-number".to_string());
        assert!(!snapshot_matches_refresh_marker(
            &snapshot_with_properties(props),
            &marker
        ));

        let mut props = marker.to_summary_properties();
        props.insert(MV_REFRESH_TOKEN_PROP.to_string(), "other-token".to_string());
        assert!(!snapshot_matches_refresh_marker(
            &snapshot_with_properties(props),
            &marker
        ));
    }

    #[test]
    fn staging_ref_branch_check_rejects_tags() {
        let staging_ref = SnapshotReference {
            snapshot_id: 300,
            retention: SnapshotRetention::Tag {
                max_ref_age_ms: None,
            },
        };

        let err = ensure_staging_ref_is_branch("mv_refresh_77", &staging_ref).unwrap_err();
        assert_eq!(
            err,
            "iceberg mv publish: staging ref mv_refresh_77 is a tag, expected branch"
        );
    }

    #[test]
    fn publish_commit_requirements_guard_main_and_staging_refs() {
        let plan = MvRefreshPublishPlan {
            namespace: "db".to_string(),
            table: "tbl".to_string(),
            staging_branch: "mv_refresh_77".to_string(),
            expected_main_snapshot_id: Some(100),
            staging_snapshot_id: 300,
            marker: MvRefreshSnapshotMarker {
                refresh_id: 77,
                mv_id: 12,
                token: "token-77".to_string(),
            },
        };
        let ident = TableIdent::from_strs(["db", "tbl"]).unwrap();
        let mut commit = build_publish_commit(ident, &plan);
        let requirements = commit.take_requirements();

        assert!(
            requirements.contains(&TableRequirement::RefSnapshotIdMatch {
                r#ref: "main".to_string(),
                snapshot_id: Some(100),
            })
        );
        assert!(
            requirements.contains(&TableRequirement::RefSnapshotIdMatch {
                r#ref: "mv_refresh_77".to_string(),
                snapshot_id: Some(300),
            })
        );
    }

    fn snapshot_with_properties(properties: BTreeMap<String, String>) -> Snapshot {
        let summary = Summary {
            operation: Operation::Append,
            additional_properties: properties.into_iter().collect(),
        };
        Snapshot::builder()
            .with_snapshot_id(300)
            .with_sequence_number(1)
            .with_timestamp_ms(1)
            .with_manifest_list("file:/tmp/manifest-list.avro".to_string())
            .with_summary(summary)
            .with_schema_id(0)
            .build()
    }
}
