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

//! Resolve provider-owned snapshot summary declarations.

use crate::commit::{
    MV_PUBLICATION_PROVENANCE_PROP, MV_REFRESH_ROW_COUNT_PROP, MvPublicationProvenanceV2,
};
use std::collections::{BTreeMap, HashMap};

pub(super) fn merge_snapshot_summary_properties(
    mut built_in: HashMap<String, String>,
    snapshot_properties: &BTreeMap<String, String>,
    table_uuid: uuid::Uuid,
    snapshot_id: i64,
) -> Result<HashMap<String, String>, String> {
    let mut provider_properties =
        crate::document_storage::publication::resolve_snapshot_properties(
            snapshot_properties,
            table_uuid,
            snapshot_id,
        )
        .map_err(|error| error.to_string())?;
    if let Some(raw_provenance) = provider_properties
        .get(MV_PUBLICATION_PROVENANCE_PROP)
        .cloned()
    {
        let total_records = built_in
            .get("total-records")
            .ok_or_else(|| "MV commit summary is missing total-records".to_string())?
            .parse::<i64>()
            .map_err(|error| format!("MV commit summary has invalid total-records: {error}"))?;
        let provenance = MvPublicationProvenanceV2::from_json(&raw_provenance)?;
        let canonical = provenance.with_rows(total_records)?.to_canonical_json()?;
        provider_properties.insert(MV_PUBLICATION_PROVENANCE_PROP.to_string(), canonical);
        provider_properties.insert(
            MV_REFRESH_ROW_COUNT_PROP.to_string(),
            total_records.to_string(),
        );
    }
    for key in provider_properties.keys() {
        if built_in.contains_key(key) {
            return Err(format!(
                "snapshot property {key} conflicts with built-in summary field"
            ));
        }
    }
    built_in.extend(provider_properties);
    Ok(built_in)
}
