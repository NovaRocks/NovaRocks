use std::collections::{BTreeMap, BTreeSet};
use std::sync::LazyLock;

use apache_avro::Schema;
use apache_avro::rabin::Rabin;
use apache_avro::schema_compatibility::SchemaCompatibility;

use crate::meta::repository::{RepositoryError, RepositoryResult};

static SCHEMA_CATALOG: LazyLock<Result<AvroSchemaCatalog, String>> = LazyLock::new(|| {
    AvroSchemaCatalog::from_sources(schema_sources()).map_err(|err| err.to_string())
});

#[derive(Debug)]
pub struct AvroSchemaEntry {
    subject: String,
    id: i32,
    raw_schema: &'static str,
    schema: Schema,
    fingerprint: String,
}

impl AvroSchemaEntry {
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
pub struct AvroSchemaCatalog {
    by_subject: BTreeMap<String, BTreeMap<i32, AvroSchemaEntry>>,
}

impl AvroSchemaCatalog {
    fn from_sources(sources: &[SchemaSource]) -> RepositoryResult<Self> {
        let mut by_subject = BTreeMap::new();
        for source in sources {
            let schema = Schema::parse_str(source.raw_schema).map_err(|err| {
                RepositoryError::provider(format!(
                    "failed to parse Avro schema {} v{}: {err}",
                    source.subject, source.id
                ))
            })?;
            let fingerprint = schema.fingerprint::<Rabin>().to_string();
            let entry = AvroSchemaEntry {
                subject: source.subject.to_string(),
                id: source.id,
                raw_schema: source.raw_schema,
                schema,
                fingerprint,
            };
            let previous = by_subject
                .entry(entry.subject.clone())
                .or_insert_with(BTreeMap::new)
                .insert(entry.id, entry);
            if previous.is_some() {
                return Err(RepositoryError::provider(format!(
                    "duplicate Avro schema entry for subject `{}` id {}",
                    source.subject, source.id
                )));
            }
        }

        let catalog = Self { by_subject };
        catalog.validate_unique_entries()?;
        catalog.validate_full_transitive()?;
        Ok(catalog)
    }

    pub fn entry(&self, subject: &str, id: i32) -> RepositoryResult<&AvroSchemaEntry> {
        self.by_subject
            .get(subject)
            .and_then(|entries| entries.get(&id))
            .ok_or_else(|| {
                RepositoryError::provider(format!(
                    "unknown Avro schema entry for subject `{subject}` id {id}"
                ))
            })
    }

    pub fn latest(&self, subject: &str) -> RepositoryResult<&AvroSchemaEntry> {
        self.by_subject
            .get(subject)
            .and_then(|entries| entries.last_key_value().map(|(_, entry)| entry))
            .ok_or_else(|| {
                RepositoryError::provider(format!("unknown Avro schema subject `{subject}`"))
            })
    }

    pub fn validate_unique_entries(&self) -> RepositoryResult<()> {
        let mut subject_ids = BTreeSet::new();
        let mut fingerprints = BTreeSet::new();
        for (subject, entries) in &self.by_subject {
            for (id, entry) in entries {
                if !subject_ids.insert((subject.as_str(), *id)) {
                    return Err(RepositoryError::provider(format!(
                        "duplicate Avro schema subject/id `{subject}`/{id}"
                    )));
                }
                if !fingerprints.insert(entry.fingerprint.as_str()) {
                    return Err(RepositoryError::provider(format!(
                        "duplicate Avro schema fingerprint `{}`",
                        entry.fingerprint
                    )));
                }
                let expected = entry.schema.fingerprint::<Rabin>().to_string();
                if expected != entry.fingerprint {
                    return Err(RepositoryError::provider(format!(
                        "Avro schema fingerprint mismatch for `{subject}`/{id}: catalog={}, computed={expected}",
                        entry.fingerprint
                    )));
                }
            }
        }
        Ok(())
    }

    pub fn validate_full_transitive(&self) -> RepositoryResult<()> {
        for (subject, entries) in &self.by_subject {
            for writer in entries.values() {
                for reader in entries.values() {
                    SchemaCompatibility::can_read(writer.schema(), reader.schema()).map_err(
                        |err| {
                            RepositoryError::provider(format!(
                                "Avro schema FULL_TRANSITIVE compatibility failed for subject `{subject}` writer id {} reader id {}: {err}",
                                writer.id(),
                                reader.id()
                            ))
                        },
                    )?;
                }
            }
        }
        Ok(())
    }
}

pub fn schema_catalog() -> RepositoryResult<&'static AvroSchemaCatalog> {
    match &*SCHEMA_CATALOG {
        Ok(catalog) => Ok(catalog),
        Err(err) => Err(RepositoryError::provider(err.clone())),
    }
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
            subject: "dictionary.snapshot",
            id: 1,
            raw_schema: include_str!("schemas/dictionary.snapshot/0001.avsc"),
        },
        SchemaSource {
            subject: "dictionary.lookup",
            id: 1,
            raw_schema: include_str!("schemas/dictionary.lookup/0001.avsc"),
        },
        SchemaSource {
            subject: "iceberg.catalog",
            id: 1,
            raw_schema: include_str!("schemas/iceberg.catalog/0001.avsc"),
        },
        SchemaSource {
            subject: "iceberg.namespace",
            id: 1,
            raw_schema: include_str!("schemas/iceberg.namespace/0001.avsc"),
        },
        SchemaSource {
            subject: "iceberg.table_registration",
            id: 1,
            raw_schema: include_str!("schemas/iceberg.table_registration/0001.avsc"),
        },
        SchemaSource {
            subject: "iceberg.operation",
            id: 1,
            raw_schema: include_str!("schemas/iceberg.operation/0001.avsc"),
        },
        SchemaSource {
            subject: "job.erase",
            id: 1,
            raw_schema: include_str!("schemas/job.erase/0001.avsc"),
        },
        SchemaSource {
            subject: "job.iceberg_optimize",
            id: 1,
            raw_schema: include_str!("schemas/job.iceberg_optimize/0001.avsc"),
        },
        SchemaSource {
            subject: "starrocks.database",
            id: 1,
            raw_schema: include_str!("schemas/starrocks.database/0001.avsc"),
        },
        SchemaSource {
            subject: "starrocks.database_name",
            id: 1,
            raw_schema: include_str!("schemas/starrocks.database_name/0001.avsc"),
        },
        SchemaSource {
            subject: "starrocks.table",
            id: 1,
            raw_schema: include_str!("schemas/starrocks.table/0001.avsc"),
        },
        SchemaSource {
            subject: "starrocks.table_name",
            id: 1,
            raw_schema: include_str!("schemas/starrocks.table_name/0001.avsc"),
        },
        SchemaSource {
            subject: "starrocks.schema",
            id: 1,
            raw_schema: include_str!("schemas/starrocks.schema/0001.avsc"),
        },
        SchemaSource {
            subject: "starrocks.column",
            id: 1,
            raw_schema: include_str!("schemas/starrocks.column/0001.avsc"),
        },
        SchemaSource {
            subject: "starrocks.partition",
            id: 1,
            raw_schema: include_str!("schemas/starrocks.partition/0001.avsc"),
        },
        SchemaSource {
            subject: "starrocks.index",
            id: 1,
            raw_schema: include_str!("schemas/starrocks.index/0001.avsc"),
        },
        SchemaSource {
            subject: "starrocks.tablet",
            id: 1,
            raw_schema: include_str!("schemas/starrocks.tablet/0001.avsc"),
        },
        SchemaSource {
            subject: "starrocks.txn",
            id: 1,
            raw_schema: include_str!("schemas/starrocks.txn/0001.avsc"),
        },
        SchemaSource {
            subject: "mv.definition",
            id: 1,
            raw_schema: include_str!("schemas/mv.definition/0001.avsc"),
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
            subject: "mv.dependency",
            id: 1,
            raw_schema: include_str!("schemas/mv.dependency/0001.avsc"),
        },
        SchemaSource {
            subject: "test.evolution",
            id: 1,
            raw_schema: include_str!("schemas/test.evolution/0001.avsc"),
        },
        SchemaSource {
            subject: "test.evolution",
            id: 2,
            raw_schema: include_str!("schemas/test.evolution/0002.avsc"),
        },
    ]
}
