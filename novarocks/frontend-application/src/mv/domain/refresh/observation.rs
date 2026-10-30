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

//! Refresh schema observations from retained provider metadata and canonical D/L.

use crate::mv::domain::refresh::schema_contract::MvOccurrenceFieldRebind;
use novarocks_types::naming::TableIdentity;

pub(crate) fn validate_observed_schema_locator(
    observed: &novarocks_spi::connector::ConnectorTableIdentity,
    identity: &novarocks_spi::connector::ConnectorTableIdentity,
) -> Result<(), String> {
    if observed != identity {
        return Err(
            "MV schema observation does not match the retained metadata locator".to_string(),
        );
    }
    Ok(())
}

pub(crate) fn validate_occurrence_locator(
    occurrence: &novarocks_mv_application::persistence::codec::RelationOccurrence,
    table: &TableIdentity,
) -> Result<(), String> {
    if occurrence.catalog_at_binding != table.catalog
        || occurrence.namespace_at_binding != table.namespace
        || occurrence.relation_at_binding != table.table
    {
        return Err(format!(
            "MV refresh source locator does not match occurrence {}",
            occurrence.occurrence_id
        ));
    }
    Ok(())
}

pub(crate) fn require_occurrence_rewriter(
    renames: &[MvOccurrenceFieldRebind],
) -> Result<(), String> {
    if renames.is_empty() {
        Ok(())
    } else {
        Err("MV refresh requires occurrence-aware SQL field rebinding".to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use novarocks_mv_application::persistence::identity::FieldIdentity;

    #[test]
    fn renamed_fields_cannot_fall_back_to_the_stored_sql() {
        let rename = MvOccurrenceFieldRebind {
            occurrence_id: 7,
            field_id: FieldIdentity::try_new(vec![0xff, 1]).unwrap(),
            qualifier_at_binding: "left_input".to_string(),
            name_at_binding: "old_name".to_string(),
            current_name: "new_name".to_string(),
        };
        assert!(require_occurrence_rewriter(&[]).is_ok());
        assert_eq!(
            require_occurrence_rewriter(&[rename]).unwrap_err(),
            "MV refresh requires occurrence-aware SQL field rebinding"
        );
    }
}
