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

//! Provider-neutral materialized-view target schema helpers.

use novarocks_sql::planning::mv::{SqlMvInternalTargetColumn, mv_internal_target_column};
use novarocks_sql::semantic::TableColumnDef;

pub(crate) use novarocks_sql::planning::mv::{
    iceberg_mv_physical_select_sql, validate_reserved_projection_output_names,
};

pub(crate) fn apply_key_table_column() -> TableColumnDef {
    internal_target_column(SqlMvInternalTargetColumn::ApplyKey)
}

pub(crate) fn join_apply_key_table_column() -> TableColumnDef {
    internal_target_column(SqlMvInternalTargetColumn::JoinApplyKey)
}

pub(crate) fn branch_id_table_column() -> TableColumnDef {
    internal_target_column(SqlMvInternalTargetColumn::BranchId)
}

fn internal_target_column(kind: SqlMvInternalTargetColumn) -> TableColumnDef {
    let facts = mv_internal_target_column(kind);
    TableColumnDef {
        name: facts.name,
        data_type: facts.data_type,
        nullable: facts.nullable,
        aggregation: None,
        default: None,
    }
}

pub(crate) fn ensure_base_row_lineage_contract(
    observation: &crate::mv::domain::storage_observation::MvSourceSchemaObservation,
    base_fqn: &str,
) -> Result<(), novarocks_sql::compiler::SqlCompileError> {
    // The MV owner authors a data refusal before compilation/publication; keep
    // this original fact through the existing compile-source bridge.
    let refusal = |message| {
        novarocks_sql::compiler::SqlCompileError::CompilationSource(
            novarocks_failure_contract::SharedFailureCause::new(
                novarocks_mv_application::product::MvProductError::new(
                    novarocks_mv_application::product::MvProductErrorKind::InvalidRequest,
                    message,
                ),
            ),
        )
    };
    if !observation.is_format_v3() {
        return Err(refusal(format!(
            "iceberg-backed materialized views require base table {base_fqn} to be Iceberg format-version=3"
        )));
    }
    if !observation.selected_snapshot_has_row_range() {
        let snapshot = observation
            .selected_snapshot_id()
            .map(|id| id.to_string())
            .unwrap_or_else(|| "<none>".to_owned());
        return Err(refusal(format!(
            "base table {base_fqn} selected snapshot {snapshot} has no row lineage (no row range); \
             perform a v3 commit, for example OPTIMIZE, then retry"
        )));
    }
    Ok(())
}
