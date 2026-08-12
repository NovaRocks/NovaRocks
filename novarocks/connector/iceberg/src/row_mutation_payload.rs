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

//! Provider-private COW row-mutation recipes.

use std::collections::BTreeSet;

use bytes::Bytes;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct IcebergCowRecipePayloadV3 {
    pub version: u8,
    pub role: String,
    pub old_file: String,
    pub matched_row_ids: Vec<i64>,
    pub base_snapshot_id: Option<i64>,
    pub frozen_source_digest_hex: Option<String>,
}

pub fn encode_cow_recipe(
    role: &[u8],
    old_file: &str,
    row_ids: &[i64],
    base_snapshot_id: Option<i64>,
    frozen_source_digest: Option<[u8; 32]>,
) -> Result<Bytes, String> {
    let role = std::str::from_utf8(role)
        .map_err(|_| "Iceberg COW recipe role is not UTF-8".to_string())?;
    let recipe = IcebergCowRecipePayloadV3 {
        version: 3,
        role: role.to_string(),
        old_file: old_file.to_string(),
        matched_row_ids: row_ids.to_vec(),
        base_snapshot_id,
        frozen_source_digest_hex: frozen_source_digest.map(lowercase_hex),
    };
    validate_cow_recipe(&recipe)?;
    serde_json::to_vec(&recipe)
        .map(Bytes::from)
        .map_err(|error| format!("encode Iceberg COW recipe: {error}"))
}

pub fn decode_cow_recipe(payload: &Bytes) -> Result<IcebergCowRecipePayloadV3, String> {
    let recipe = serde_json::from_slice(payload)
        .map_err(|error| format!("decode Iceberg COW recipe: {error}"))?;
    validate_cow_recipe(&recipe)?;
    Ok(recipe)
}

fn validate_cow_recipe(recipe: &IcebergCowRecipePayloadV3) -> Result<(), String> {
    if recipe.version != 3
        || !matches!(recipe.role.as_str(), "rewrite" | "append")
        || (recipe.role == "rewrite"
            && (recipe.old_file.is_empty()
                || recipe.matched_row_ids.is_empty()
                || recipe.base_snapshot_id.is_none()
                || recipe
                    .frozen_source_digest_hex
                    .as_deref()
                    .is_none_or(|digest| !is_lowercase_digest(digest))))
        || (recipe.role == "append"
            && (!recipe.old_file.is_empty()
                || !recipe.matched_row_ids.is_empty()
                || recipe.base_snapshot_id.is_some()
                || recipe.frozen_source_digest_hex.is_some()))
    {
        return Err("Iceberg COW recipe has an invalid role or payload shape".to_string());
    }
    if recipe
        .matched_row_ids
        .iter()
        .copied()
        .collect::<BTreeSet<_>>()
        .len()
        != recipe.matched_row_ids.len()
    {
        return Err("Iceberg COW recipe repeats a row identity".to_string());
    }
    Ok(())
}

fn lowercase_hex(value: [u8; 32]) -> String {
    value.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn is_lowercase_digest(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cow_recipe_round_trips_in_selection_order_and_rejects_duplicate_identity() {
        let payload = encode_cow_recipe(
            b"rewrite",
            "s3://bucket/file.parquet",
            &[9, 3, 17],
            Some(41),
            Some([7; 32]),
        )
        .expect("encode COW recipe");
        assert_eq!(
            decode_cow_recipe(&payload)
                .expect("decode COW recipe")
                .matched_row_ids,
            vec![9, 3, 17]
        );
        assert!(
            encode_cow_recipe(
                b"rewrite",
                "s3://bucket/file.parquet",
                &[9, 9],
                Some(41),
                Some([7; 32]),
            )
            .is_err()
        );
    }
}
