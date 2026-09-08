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

use std::collections::{BTreeMap, BTreeSet};

use novarocks_spi::connector::{ConnectorError, ConnectorErrorKind};

use crate::domain::{PaimonBucketMode, PaimonColumn, PaimonDataCompression, PaimonMergeEngine};

/// Frozen PAI-1 read recipe derived from semantic table properties.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PaimonReadOptions {
    pub merge_engine: PaimonMergeEngine,
    pub bucket_mode: PaimonBucketMode,
    pub data_compression: PaimonDataCompression,
    pub sequence_field_id: Option<i32>,
}

impl PaimonReadOptions {
    pub fn analyze(
        properties: &BTreeMap<String, String>,
        columns: &[PaimonColumn],
        primary_key_field_ids: &[i32],
        partition_field_ids: &[i32],
    ) -> Result<Self, ConnectorError> {
        if truthy(properties.get("deletion-vectors.enabled"))? {
            return Err(unsupported("Paimon deletion vectors are unsupported"));
        }
        if properties
            .get("data-evolution.enabled")
            .is_some_and(|v| v != "false")
        {
            return Err(unsupported("Paimon data-evolution tables are unsupported"));
        }

        let merge_engine = if primary_key_field_ids.is_empty() {
            if properties.contains_key("merge-engine") {
                return Err(unsupported(
                    "append-only Paimon table cannot declare merge-engine",
                ));
            }
            PaimonMergeEngine::AppendOnly
        } else {
            match properties
                .get("merge-engine")
                .map(String::as_str)
                .unwrap_or("deduplicate")
            {
                "deduplicate" => PaimonMergeEngine::Deduplicate,
                _ => {
                    return Err(unsupported(
                        "only Paimon merge-engine=deduplicate is supported",
                    ));
                }
            }
        };

        let bucket = properties.get("bucket").map(String::as_str).unwrap_or("-1");
        let bucket_mode = match bucket.parse::<i32>() {
            Ok(-2) => return Err(unsupported("Paimon postpone bucket mode is unsupported")),
            Ok(-1) if primary_key_field_ids.is_empty() => PaimonBucketMode::Unbucketed,
            Ok(-1) => PaimonBucketMode::Dynamic,
            Ok(value) if value > 0 => PaimonBucketMode::Fixed,
            _ => return Err(unsupported("Paimon bucket option is unsupported")),
        };

        match properties
            .get("file.format")
            .map(String::as_str)
            .unwrap_or("parquet")
        {
            "parquet" => {}
            _ => return Err(unsupported("only Paimon Parquet data files are supported")),
        }
        let data_compression = match properties
            .get("file.compression")
            .map(String::as_str)
            .unwrap_or("zstd")
        {
            "none" | "uncompressed" => PaimonDataCompression::Uncompressed,
            "snappy" => PaimonDataCompression::Snappy,
            "zstd" => PaimonDataCompression::Zstd,
            "lz4" | "lz4_raw" => PaimonDataCompression::Lz4Raw,
            _ => return Err(unsupported("Paimon Parquet compression is unsupported")),
        };

        let by_name = columns
            .iter()
            .map(|column| (column.name(), column))
            .collect::<BTreeMap<_, _>>();
        let sequence_field_id = match properties.get("sequence.field") {
            None => None,
            Some(value) if value.contains(',') => {
                return Err(unsupported(
                    "multiple Paimon sequence fields are unsupported",
                ));
            }
            Some(value) => {
                let column = by_name
                    .get(value.as_str())
                    .ok_or_else(|| invalid("Paimon sequence field does not exist"))?;
                if !column.data_type().is_signed_integer() {
                    return Err(unsupported(
                        "Paimon sequence field must be a signed integer",
                    ));
                }
                Some(column.field_id())
            }
        };

        let by_id = columns
            .iter()
            .map(|column| (column.field_id(), column))
            .collect::<BTreeMap<_, _>>();
        for id in primary_key_field_ids {
            let column = by_id
                .get(id)
                .ok_or_else(|| invalid("Paimon key field does not exist"))?;
            if column.nullable() || !column.data_type().supported_as_key() {
                return Err(unsupported(
                    "Paimon primary-key field type or nullability is unsupported",
                ));
            }
        }
        for id in partition_field_ids {
            let column = by_id
                .get(id)
                .ok_or_else(|| invalid("Paimon partition field does not exist"))?;
            if !column.data_type().supported_as_key() {
                return Err(unsupported("Paimon partition field type is unsupported"));
            }
        }

        reject_unknown_read_properties(properties)?;
        Ok(Self {
            merge_engine,
            bucket_mode,
            data_compression,
            sequence_field_id,
        })
    }
}

fn reject_unknown_read_properties(
    properties: &BTreeMap<String, String>,
) -> Result<(), ConnectorError> {
    const SEMANTIC: &[&str] = &[
        "bucket",
        "bucket-key",
        "data-evolution.enabled",
        "deletion-vectors.enabled",
        "deletion-vectors.merge-on-read",
        "file.compression",
        "file.format",
        "merge-engine",
        "sequence.field",
    ];
    const WRITE_ONLY: &[&str] = &[
        "changelog-producer",
        "commit.force-compact",
        "compaction.max.file-num",
        "compaction.min.file-num",
        "dynamic-bucket.target-row-num",
        "manifest.compression",
        "manifest.delete-file-drop-stats",
        "manifest.format",
        "manifest.target-file-size",
        "metadata.stats-mode",
        "owner",
        "snapshot.num-retained.max",
        "snapshot.time-retained",
        "target-file-size",
        "write-buffer-size",
        "write-only",
    ];
    let recognized = SEMANTIC
        .iter()
        .chain(WRITE_ONLY)
        .copied()
        .collect::<BTreeSet<_>>();
    for key in properties.keys() {
        if recognized.contains(key.as_str()) {
            continue;
        }
        return Err(unsupported(format!(
            "unknown Paimon table option may affect read semantics: {key}"
        )));
    }
    if truthy(properties.get("deletion-vectors.merge-on-read"))? {
        return Err(unsupported(
            "Paimon deletion-vector merge-on-read is unsupported",
        ));
    }
    Ok(())
}

fn truthy(value: Option<&String>) -> Result<bool, ConnectorError> {
    match value.map(String::as_str).unwrap_or("false") {
        "true" => Ok(true),
        "false" => Ok(false),
        _ => Err(invalid("Paimon boolean option must be true or false")),
    }
}

fn invalid(message: &'static str) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::InvalidRequest, message)
}

fn unsupported(message: impl Into<String>) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::Unsupported, message)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::PaimonDataType;

    fn columns() -> Vec<PaimonColumn> {
        vec![
            PaimonColumn::try_new(1, "id", PaimonDataType::Int64, false, 0).unwrap(),
            PaimonColumn::try_new(2, "version", PaimonDataType::Int32, true, 1).unwrap(),
            PaimonColumn::try_new(3, "payload", PaimonDataType::Utf8, true, 2).unwrap(),
        ]
    }

    #[test]
    fn append_and_default_deduplicate_recipes_are_distinct() {
        let append = PaimonReadOptions::analyze(&BTreeMap::new(), &columns(), &[], &[]).unwrap();
        assert_eq!(append.merge_engine, PaimonMergeEngine::AppendOnly);
        assert_eq!(append.bucket_mode, PaimonBucketMode::Unbucketed);

        let mut properties = BTreeMap::from([
            ("bucket".to_string(), "4".to_string()),
            ("sequence.field".to_string(), "version".to_string()),
        ]);
        let primary = PaimonReadOptions::analyze(&properties, &columns(), &[1], &[]).unwrap();
        assert_eq!(primary.merge_engine, PaimonMergeEngine::Deduplicate);
        assert_eq!(primary.bucket_mode, PaimonBucketMode::Fixed);
        assert_eq!(primary.sequence_field_id, Some(2));

        properties.insert("merge-engine".to_string(), "partial-update".to_string());
        assert!(PaimonReadOptions::analyze(&properties, &columns(), &[1], &[]).is_err());
    }

    #[test]
    fn unsupported_storage_semantics_fail_closed_while_writer_tuning_is_ignored() {
        let writer_only = BTreeMap::from([
            ("target-file-size".to_string(), "134217728".to_string()),
            ("compaction.min.file-num".to_string(), "5".to_string()),
        ]);
        assert!(PaimonReadOptions::analyze(&writer_only, &columns(), &[], &[]).is_ok());

        for properties in [
            BTreeMap::from([("deletion-vectors.enabled".to_string(), "true".to_string())]),
            BTreeMap::from([("file.format".to_string(), "orc".to_string())]),
            BTreeMap::from([("unknown.read.option".to_string(), "x".to_string())]),
            BTreeMap::from([(
                "fields.payload.unknown-read-switch".to_string(),
                "x".to_string(),
            )]),
        ] {
            assert!(PaimonReadOptions::analyze(&properties, &columns(), &[], &[]).is_err());
        }
    }
}
