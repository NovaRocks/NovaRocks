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
use novarocks_sql::syntax::TableColumnDef;

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
    observation: &crate::mv::domain::storage_observation::MvSchemaValidationObservation,
    base_fqn: &str,
) -> Result<(), String> {
    if !observation.is_format_v3() || !observation.stored_row_lineage_enabled() {
        return Err(format!(
            "iceberg-backed materialized views require base table {base_fqn} to be Iceberg format-version=3 with write.row-lineage=true; \
             upgrade the table or recreate it with TBLPROPERTIES (\"format-version\"=\"3\", \"write.row-lineage\"=\"true\")"
        ));
    }
    Ok(())
}
