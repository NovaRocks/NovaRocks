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

use std::collections::HashSet;

use crate::sql::parser::ast::TableColumnDef;
use novarocks_catalog::identifier::normalize_identifier;
use novarocks_catalog::schema::SqlType;

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct StarRocksPhysicalColumn {
    pub(crate) column: TableColumnDef,
    pub(crate) visible: bool,
    pub(crate) is_key: bool,
}

pub(crate) fn starrocks_physical_column(
    name: String,
    data_type: SqlType,
    nullable: bool,
    visible: bool,
    is_key: bool,
) -> StarRocksPhysicalColumn {
    StarRocksPhysicalColumn {
        column: TableColumnDef {
            name,
            data_type,
            nullable,
            aggregation: None,
            default: None,
        },
        visible,
        is_key,
    }
}

pub(crate) fn validate_unique_aggregate_physical_column_names(
    physical_columns: &[StarRocksPhysicalColumn],
) -> Result<(), String> {
    let mut names = HashSet::with_capacity(physical_columns.len());
    for column in physical_columns {
        let normalized = normalize_identifier(&column.column.name)?;
        if !names.insert(normalized.clone()) {
            return Err(format!(
                "aggregate MV physical column name collision: hidden column name collision or duplicate physical column `{normalized}`"
            ));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use novarocks_catalog::schema::SqlType;

    fn column(name: &str) -> StarRocksPhysicalColumn {
        starrocks_physical_column(name.to_string(), SqlType::BigInt, false, true, false)
    }

    #[test]
    fn aggregate_physical_column_names_accept_unique_normalized_names() {
        validate_unique_aggregate_physical_column_names(&[column("Group_Key"), column("sum_v")])
            .expect("unique names");
    }

    #[test]
    fn aggregate_physical_column_names_reject_normalized_duplicates_with_exact_error() {
        let error = validate_unique_aggregate_physical_column_names(&[
            column("Visible_Output"),
            column("`visible_output`"),
        ])
        .expect_err("normalized duplicate");

        assert_eq!(
            error,
            "aggregate MV physical column name collision: hidden column name collision or duplicate physical column `visible_output`"
        );
    }
}
