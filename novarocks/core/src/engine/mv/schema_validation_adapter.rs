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

use crate::mv::persistence::schema::MvSchemaContract;
use crate::mv::schema_validation::{
    ContractDecision, CurrentIcebergTableView, JoinContractDecision, JoinSchemaValidationError,
    validate_join_schema_contract, validate_schema_contract,
};
use novarocks_catalog::identifier::TableIdentity;

const ICEBERG_ROW_LINEAGE_PROP: &str = "write.row-lineage";

pub(crate) fn current_iceberg_table_view(
    table: &iceberg::table::Table,
) -> CurrentIcebergTableView<'_> {
    current_iceberg_table_view_with_schema(table, table.metadata().current_schema())
}

pub(crate) fn current_iceberg_table_view_with_schema<'a>(
    table: &'a iceberg::table::Table,
    schema: &'a iceberg::spec::Schema,
) -> CurrentIcebergTableView<'a> {
    let metadata = table.metadata();
    CurrentIcebergTableView {
        table_uuid: metadata.uuid().to_string(),
        format_version: metadata.format_version(),
        row_lineage_enabled: row_lineage_enabled(metadata.properties()),
        schema,
        default_partition_spec: metadata.default_partition_spec(),
    }
}

pub(crate) fn validate_current_schema_contract(
    contract: &MvSchemaContract,
    current_base_table: &iceberg::table::Table,
    current_target_table: &iceberg::table::Table,
) -> ContractDecision {
    validate_current_schema_contract_with_base_schema(
        contract,
        current_base_table,
        current_base_table.metadata().current_schema(),
        current_target_table,
    )
}

pub(crate) fn validate_current_schema_contract_with_base_schema(
    contract: &MvSchemaContract,
    current_base_table: &iceberg::table::Table,
    base_schema: &iceberg::spec::Schema,
    current_target_table: &iceberg::table::Table,
) -> ContractDecision {
    let base_view = current_iceberg_table_view_with_schema(current_base_table, base_schema);
    let target_view = current_iceberg_table_view(current_target_table);
    validate_schema_contract(contract, &base_view, &target_view)
}

pub(crate) fn validate_current_join_schema_contract(
    contract: &MvSchemaContract,
    bases: &[(&TableIdentity, &iceberg::table::Table); 2],
    current_target_table: &iceberg::table::Table,
) -> Result<JoinContractDecision, JoinSchemaValidationError> {
    let base_fqns = [bases[0].0.fqn(), bases[1].0.fqn()];
    let base_views = [
        (
            base_fqns[0].as_str(),
            current_iceberg_table_view(bases[0].1),
        ),
        (
            base_fqns[1].as_str(),
            current_iceberg_table_view(bases[1].1),
        ),
    ];
    let target_view = current_iceberg_table_view(current_target_table);
    validate_join_schema_contract(contract, &base_views, &target_view)
}

fn row_lineage_enabled(props: &std::collections::HashMap<String, String>) -> bool {
    props
        .get(ICEBERG_ROW_LINEAGE_PROP)
        .map(|value| value.eq_ignore_ascii_case("true"))
        .unwrap_or(false)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn row_lineage_enabled_recognizes_case_insensitive_true() {
        let mut properties = std::collections::HashMap::new();
        properties.insert(ICEBERG_ROW_LINEAGE_PROP.to_string(), "TRUE".to_string());
        assert!(row_lineage_enabled(&properties));
        properties.insert(ICEBERG_ROW_LINEAGE_PROP.to_string(), "false".to_string());
        assert!(!row_lineage_enabled(&properties));
        properties.clear();
        assert!(!row_lineage_enabled(&properties));
    }
}
