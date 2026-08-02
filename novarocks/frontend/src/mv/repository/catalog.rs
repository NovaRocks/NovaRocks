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
use std::sync::LazyLock;

use apache_avro::Schema;
use apache_avro::rabin::Rabin;
use apache_avro::schema_compatibility::SchemaCompatibility;

static CATALOG: LazyLock<Result<MvSchemaCatalog, String>> =
    LazyLock::new(|| MvSchemaCatalog::from_sources(schema_sources()));

#[derive(Debug)]
pub struct MvSchemaEntry {
    subject: String,
    id: i32,
    raw_schema: &'static str,
    schema: Schema,
    fingerprint: String,
}

impl MvSchemaEntry {
    pub fn subject(&self) -> &str {
        &self.subject
    }
    pub fn id(&self) -> i32 {
        self.id
    }
    pub fn raw_schema(&self) -> &'static str {
        self.raw_schema
    }
    pub fn schema(&self) -> &Schema {
        &self.schema
    }
    pub fn fingerprint(&self) -> &str {
        &self.fingerprint
    }
}

#[derive(Debug)]
pub struct MvSchemaCatalog {
    by_subject: BTreeMap<String, BTreeMap<i32, MvSchemaEntry>>,
}

impl MvSchemaCatalog {
    fn from_sources(sources: &[SchemaSource]) -> Result<Self, String> {
        let mut by_subject = BTreeMap::new();
        for source in sources {
            let schema = Schema::parse_str(source.raw_schema).map_err(|error| {
                format!(
                    "failed to parse MV Avro schema {} v{}: {error}",
                    source.subject, source.id
                )
            })?;
            let entry = MvSchemaEntry {
                subject: source.subject.to_string(),
                id: source.id,
                raw_schema: source.raw_schema,
                fingerprint: schema.fingerprint::<Rabin>().to_string(),
                schema,
            };
            if by_subject
                .entry(entry.subject.clone())
                .or_insert_with(BTreeMap::new)
                .insert(entry.id, entry)
                .is_some()
            {
                return Err(format!(
                    "duplicate MV Avro schema {} v{}",
                    source.subject, source.id
                ));
            }
        }
        let catalog = Self { by_subject };
        catalog.validate_unique_entries()?;
        catalog.validate_full_transitive()?;
        Ok(catalog)
    }

    pub fn entry(&self, subject: &str, id: i32) -> Result<&MvSchemaEntry, String> {
        self.by_subject
            .get(subject)
            .and_then(|entries| entries.get(&id))
            .ok_or_else(|| format!("unknown MV Avro schema entry for subject `{subject}` id {id}"))
    }

    pub fn latest(&self, subject: &str) -> Result<&MvSchemaEntry, String> {
        self.by_subject
            .get(subject)
            .and_then(|entries| entries.last_key_value().map(|(_, entry)| entry))
            .ok_or_else(|| format!("unknown MV Avro schema subject `{subject}`"))
    }

    pub fn validate_unique_entries(&self) -> Result<(), String> {
        let mut ids = BTreeSet::new();
        let mut fingerprints = BTreeSet::new();
        for (subject, entries) in &self.by_subject {
            for (id, entry) in entries {
                if !ids.insert((subject.as_str(), *id))
                    || !fingerprints.insert(entry.fingerprint.as_str())
                {
                    return Err(format!("duplicate MV Avro schema entry `{subject}`/{id}"));
                }
                if entry.schema.fingerprint::<Rabin>().to_string() != entry.fingerprint {
                    return Err(format!("MV Avro fingerprint mismatch for `{subject}`/{id}"));
                }
            }
        }
        Ok(())
    }

    pub fn validate_full_transitive(&self) -> Result<(), String> {
        for (subject, entries) in &self.by_subject {
            for writer in entries.values() {
                for reader in entries.values() {
                    SchemaCompatibility::can_read(writer.schema(), reader.schema()).map_err(|error| {
                        format!("MV Avro FULL_TRANSITIVE compatibility failed for `{subject}` writer {} reader {}: {error}", writer.id(), reader.id())
                    })?;
                }
            }
        }
        Ok(())
    }
}

pub fn schema_catalog() -> Result<&'static MvSchemaCatalog, String> {
    CATALOG.as_ref().map_err(Clone::clone)
}

#[derive(Clone, Copy)]
struct SchemaSource {
    subject: &'static str,
    id: i32,
    raw_schema: &'static str,
}

fn schema_sources() -> &'static [SchemaSource] {
    &[
        SchemaSource {
            subject: "mv.definition",
            id: 1,
            raw_schema: include_str!("schemas/mv.definition/0001.avsc"),
        },
        SchemaSource {
            subject: "mv.definition",
            id: 2,
            raw_schema: include_str!("schemas/mv.definition/0002.avsc"),
        },
        SchemaSource {
            subject: "mv.target_lookup",
            id: 1,
            raw_schema: include_str!("schemas/mv.target_lookup/0001.avsc"),
        },
        SchemaSource {
            subject: "mv.refresh",
            id: 1,
            raw_schema: include_str!("schemas/mv.refresh/0001.avsc"),
        },
        SchemaSource {
            subject: "mv.refresh",
            id: 2,
            raw_schema: include_str!("schemas/mv.refresh/0002.avsc"),
        },
        SchemaSource {
            subject: "mv.refresh",
            id: 3,
            raw_schema: include_str!("schemas/mv.refresh/0003.avsc"),
        },
        SchemaSource {
            subject: "mv.partition_state",
            id: 1,
            raw_schema: include_str!("schemas/mv.partition_state/0001.avsc"),
        },
        SchemaSource {
            subject: "mv.dependency",
            id: 1,
            raw_schema: include_str!("schemas/mv.dependency/0001.avsc"),
        },
        SchemaSource {
            subject: "mv.sequence",
            id: 1,
            raw_schema: include_str!("schemas/mv.sequence/0001.avsc"),
        },
        SchemaSource {
            subject: "mv.sequence",
            id: 2,
            raw_schema: include_str!("schemas/mv.sequence/0002.avsc"),
        },
    ]
}
