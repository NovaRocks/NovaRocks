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

use crate::mv::domain::persistence::schema::MvSchemaContract;
use crate::mv::domain::storage_observation::MvPublishedBaseFact;

/// Assert the lake descriptor's schema contract matches the store's contract.
/// Fail-loud: the descriptor is the authoritative home (W2); a missing or
/// drifted descriptor contract means a create/alter path failed to sync it.
pub(crate) fn ensure_descriptor_schema_contract_matches(
    descriptor_contract: Option<&MvSchemaContract>,
    stored_contract: &MvSchemaContract,
) -> Result<(), String> {
    match descriptor_contract {
        None => Err(
            "MV descriptor is missing its schema_contract; expected a W2+ MV package \
             (create/alter must sync the descriptor)"
                .to_string(),
        ),
        Some(found) if found != stored_contract => Err(
            "MV descriptor schema_contract drifted from the metadata store; \
             a create/alter path failed to keep the descriptor in sync"
                .to_string(),
        ),
        Some(_) => Ok(()),
    }
}

/// Assert the provenance watermark (from the MV table's current snapshot
/// summary) matches the metadata store's last_refresh_snapshots. Fail-loud on
/// drift — the summary is the authoritative watermark home (W3a).
pub(crate) fn ensure_summary_watermark_matches_store(
    provenance_bases: &[MvPublishedBaseFact],
    store_last_refresh_snapshots: &std::collections::BTreeMap<String, i64>,
) -> Result<(), String> {
    for base in provenance_bases {
        match store_last_refresh_snapshots.get(&base.table_fqn) {
            None => {
                return Err(format!(
                    "MV refresh watermark drift: base table {} is present in the MV \
                     table's snapshot-summary provenance (to_snapshot={}) but missing from \
                     the metadata store's last_refresh_snapshots",
                    base.table_fqn, base.to_snapshot
                ));
            }
            Some(&stored_snapshot) if stored_snapshot != base.to_snapshot => {
                return Err(format!(
                    "MV refresh watermark drift for base table {}: snapshot-summary \
                     provenance says to_snapshot={}, metadata store says \
                     last_refresh_snapshots={}",
                    base.table_fqn, base.to_snapshot, stored_snapshot
                ));
            }
            Some(_) => {}
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mv::domain::persistence::schema::{
        BaseContract, BaseFieldRecord, BaseSchemaSnapshot, ExpressionKind, ExpressionLineage,
        HiddenApplyKeyContract, OutputColumnLineage, OutputContract, TargetContract,
        TargetVisibleColumn,
    };
    use novarocks_sql::planning::mv::{
        MV_HIDDEN_APPLY_KEY_COLUMN_NAME as HIDDEN_APPLY_KEY_COLUMN_NAME, SqlMvApplyKeySourceFacts,
    };

    fn minimal_base_row_id_contract() -> MvSchemaContract {
        MvSchemaContract {
            contract_version: 1,
            base: BaseContract {
                table_fqn: "ice.db.orders".to_string(),
                table_uuid: "base-uuid".to_string(),
                alias_at_create: None,
                schema_id_at_create: 1,
                schema_at_create: BaseSchemaSnapshot {
                    fields: vec![BaseFieldRecord {
                        field_id: 1,
                        name_at_create: "id".to_string(),
                        type_signature: "int".to_string(),
                        required: true,
                    }],
                },
            },
            bases: Vec::new(),
            output: OutputContract {
                columns: vec![OutputColumnLineage {
                    expression: ExpressionLineage {
                        kind: ExpressionKind::Column,
                        referenced_base_field_ids: vec![1],
                        referenced_base_fields: Vec::new(),
                    },
                }],
                filter: None,
            },
            join: None,
            aggregate: None,
            branch: None,
            target: TargetContract {
                table_fqn: "ice.db.mv_orders".to_string(),
                table_uuid: "target-uuid".to_string(),
                schema_id_at_create: 11,
                visible_columns: vec![TargetVisibleColumn {
                    output_name: "id".to_string(),
                    target_field_id: 1,
                    type_signature: "int".to_string(),
                    nullable: false,
                }],
                hidden_apply_key: HiddenApplyKeyContract {
                    column_name: HIDDEN_APPLY_KEY_COLUMN_NAME.to_string(),
                    target_field_id: 2,
                    source: SqlMvApplyKeySourceFacts::BaseRowId.into(),
                },
                partition: None,
            },
        }
    }

    #[test]
    fn descriptor_contract_matching_store_is_ok() {
        let stored = minimal_base_row_id_contract();
        let descriptor = minimal_base_row_id_contract();
        assert!(ensure_descriptor_schema_contract_matches(Some(&descriptor), &stored).is_ok());
    }

    #[test]
    fn descriptor_contract_missing_is_fail_loud() {
        let stored = minimal_base_row_id_contract();
        let err = ensure_descriptor_schema_contract_matches(None, &stored)
            .expect_err("missing descriptor contract must fail");
        assert!(err.contains("missing"), "err={err}");
    }

    #[test]
    fn descriptor_contract_drift_is_fail_loud() {
        let stored = minimal_base_row_id_contract();
        let mut descriptor = minimal_base_row_id_contract();
        descriptor.contract_version += 1;
        let err = ensure_descriptor_schema_contract_matches(Some(&descriptor), &stored)
            .expect_err("drifted descriptor contract must fail");
        assert!(err.contains("drifted"), "err={err}");
    }

    fn provenance_base(table_fqn: &str, to_snapshot: i64) -> MvPublishedBaseFact {
        MvPublishedBaseFact {
            table_fqn: table_fqn.to_string(),
            table_uuid: format!("uuid-{table_fqn}"),
            from_snapshot: None,
            to_snapshot,
        }
    }

    #[test]
    fn summary_watermark_matching_store_is_ok() {
        let bases = vec![
            provenance_base("ice.sales.orders", 200),
            provenance_base("ice.sales.customers", 50),
        ];
        let store: std::collections::BTreeMap<String, i64> = [
            ("ice.sales.orders".to_string(), 200),
            ("ice.sales.customers".to_string(), 50),
        ]
        .into_iter()
        .collect();

        assert!(ensure_summary_watermark_matches_store(&bases, &store).is_ok());
    }

    #[test]
    fn summary_watermark_ignores_store_entries_not_in_provenance() {
        let bases = vec![provenance_base("ice.sales.orders", 200)];
        let store: std::collections::BTreeMap<String, i64> = [
            ("ice.sales.orders".to_string(), 200),
            ("ice.sales.stale_base".to_string(), 999),
        ]
        .into_iter()
        .collect();

        assert!(ensure_summary_watermark_matches_store(&bases, &store).is_ok());
    }

    #[test]
    fn summary_watermark_mismatch_is_fail_loud() {
        let bases = vec![provenance_base("ice.sales.orders", 200)];
        let store: std::collections::BTreeMap<String, i64> =
            [("ice.sales.orders".to_string(), 199)]
                .into_iter()
                .collect();

        let err = ensure_summary_watermark_matches_store(&bases, &store)
            .expect_err("watermark mismatch must fail");
        assert!(err.contains("ice.sales.orders"), "err={err}");
        assert!(err.contains("200"), "err={err}");
        assert!(err.contains("199"), "err={err}");
    }

    #[test]
    fn summary_watermark_base_missing_from_store_is_fail_loud() {
        let bases = vec![provenance_base("ice.sales.orders", 200)];
        let store: std::collections::BTreeMap<String, i64> = std::collections::BTreeMap::new();

        let err = ensure_summary_watermark_matches_store(&bases, &store)
            .expect_err("missing base in store must fail");
        assert!(err.contains("ice.sales.orders"), "err={err}");
        assert!(err.contains("missing"), "err={err}");
    }
}
