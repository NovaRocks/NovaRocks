use serde::{Deserialize, Serialize};

use crate::meta::keys::{NS_ICEBERG_CATALOG, normalize_lookup_name};
use crate::meta::repository::mv::MvMetaRepository;
use crate::meta::repository::{
    RepositoryError, RepositoryResult, decode_json_payload, encode_json_payload,
};
use crate::meta::{
    ExpectedRevision, MetaKey, MetaKeyPrefix, MetaReadTxn, MetaRecord, MetaRecordKind,
    MetaRecordPut, MetaWriteTxn,
};

const ICEBERG_CATALOG_KIND: &str = "iceberg.catalog";
const ICEBERG_NAMESPACE_KIND: &str = "iceberg.namespace";
const ICEBERG_TABLE_KIND: &str = "iceberg.table_registration";
const ICEBERG_CATALOG_SCHEMA_VERSION: i32 = 1;
const ICEBERG_NAMESPACE_SCHEMA_VERSION: i32 = 1;
const ICEBERG_TABLE_SCHEMA_VERSION: i32 = 1;

#[derive(Default)]
pub struct IcebergCatalogMetaRepository;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct IcebergCatalogProperties {
    pub properties: Vec<(String, String)>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct IcebergCatalogRecord {
    pub catalog: String,
    pub properties: IcebergCatalogProperties,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct IcebergNamespaceRecord {
    pub catalog: String,
    pub namespace: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct IcebergTableRecord {
    pub catalog: String,
    pub namespace: String,
    pub table: String,
}

impl IcebergCatalogMetaRepository {
    pub fn upsert_catalog(
        &self,
        txn: &mut dyn MetaWriteTxn,
        catalog: &str,
        properties: IcebergCatalogProperties,
    ) -> RepositoryResult<()> {
        txn.put(MetaRecordPut::new(
            key_catalog(catalog)?,
            record_kind(ICEBERG_CATALOG_KIND)?,
            ExpectedRevision::Any,
            encode_json_payload(ICEBERG_CATALOG_SCHEMA_VERSION, &properties)?,
        ))?;
        Ok(())
    }

    pub fn catalog_exists(&self, txn: &dyn MetaReadTxn, catalog: &str) -> RepositoryResult<bool> {
        let Some(record) = txn.get(&key_catalog(catalog)?)? else {
            return Ok(false);
        };
        decode_catalog_record(&record)?;
        Ok(true)
    }

    pub fn list_catalogs(
        &self,
        txn: &dyn MetaReadTxn,
    ) -> RepositoryResult<Vec<IcebergCatalogRecord>> {
        txn.scan(&key_prefix_catalogs()?, None)?
            .into_iter()
            .map(|record| {
                let catalog = record_path_component(&record, 1, "iceberg catalog")?;
                let properties = decode_catalog_record(&record)?;
                Ok(IcebergCatalogRecord {
                    catalog,
                    properties,
                })
            })
            .collect()
    }

    pub fn upsert_namespace(
        &self,
        txn: &mut dyn MetaWriteTxn,
        catalog: &str,
        namespace: &str,
    ) -> RepositoryResult<()> {
        let record = IcebergNamespaceRecord {
            catalog: normalize_lookup_name(catalog),
            namespace: normalize_lookup_name(namespace),
        };
        txn.put(MetaRecordPut::new(
            key_namespace(catalog, namespace)?,
            record_kind(ICEBERG_NAMESPACE_KIND)?,
            ExpectedRevision::Any,
            encode_json_payload(ICEBERG_NAMESPACE_SCHEMA_VERSION, &record)?,
        ))?;
        Ok(())
    }

    pub fn namespace_exists(
        &self,
        txn: &dyn MetaReadTxn,
        catalog: &str,
        namespace: &str,
    ) -> RepositoryResult<bool> {
        let Some(record) = txn.get(&key_namespace(catalog, namespace)?)? else {
            return Ok(false);
        };
        decode_namespace_record(&record)?;
        Ok(true)
    }

    pub fn list_namespaces(
        &self,
        txn: &dyn MetaReadTxn,
    ) -> RepositoryResult<Vec<IcebergNamespaceRecord>> {
        txn.scan(&key_prefix_namespaces()?, None)?
            .into_iter()
            .map(|record| decode_namespace_record(&record))
            .collect()
    }

    pub fn upsert_table(
        &self,
        txn: &mut dyn MetaWriteTxn,
        catalog: &str,
        namespace: &str,
        table: &str,
    ) -> RepositoryResult<()> {
        let record = IcebergTableRecord {
            catalog: normalize_lookup_name(catalog),
            namespace: normalize_lookup_name(namespace),
            table: normalize_lookup_name(table),
        };
        txn.put(MetaRecordPut::new(
            key_table(catalog, namespace, table)?,
            record_kind(ICEBERG_TABLE_KIND)?,
            ExpectedRevision::Any,
            encode_json_payload(ICEBERG_TABLE_SCHEMA_VERSION, &record)?,
        ))?;
        Ok(())
    }

    pub fn table_exists(
        &self,
        txn: &dyn MetaReadTxn,
        catalog: &str,
        namespace: &str,
        table: &str,
    ) -> RepositoryResult<bool> {
        let Some(record) = txn.get(&key_table(catalog, namespace, table)?)? else {
            return Ok(false);
        };
        decode_table_record(&record)?;
        Ok(true)
    }

    pub fn list_tables(&self, txn: &dyn MetaReadTxn) -> RepositoryResult<Vec<IcebergTableRecord>> {
        txn.scan(&key_prefix_tables()?, None)?
            .into_iter()
            .map(|record| decode_table_record(&record))
            .collect()
    }

    pub fn delete_table_and_mv_relationships(
        &self,
        txn: &mut dyn MetaWriteTxn,
        mv_repo: &MvMetaRepository,
        catalog: &str,
        namespace: &str,
        table: &str,
    ) -> RepositoryResult<()> {
        mv_repo.drop_by_target(txn, catalog, namespace, table)?;
        txn.delete(
            &key_table(catalog, namespace, table)?,
            ExpectedRevision::Any,
        )?;
        Ok(())
    }

    pub fn delete_namespace_and_mv_relationships(
        &self,
        txn: &mut dyn MetaWriteTxn,
        mv_repo: &MvMetaRepository,
        catalog: &str,
        namespace: &str,
    ) -> RepositoryResult<()> {
        for mv in mv_repo.list_definitions(txn)? {
            if mv.target_catalog.as_deref().map(normalize_lookup_name)
                == Some(normalize_lookup_name(catalog))
                && mv.target_namespace.as_deref().map(normalize_lookup_name)
                    == Some(normalize_lookup_name(namespace))
                && let (Some(target_catalog), Some(target_namespace), Some(target_table)) = (
                    mv.target_catalog.as_deref(),
                    mv.target_namespace.as_deref(),
                    mv.target_table.as_deref(),
                )
            {
                mv_repo.drop_by_target(txn, target_catalog, target_namespace, target_table)?;
            }
        }
        for table in self.list_tables(txn)? {
            if table.catalog == normalize_lookup_name(catalog)
                && table.namespace == normalize_lookup_name(namespace)
            {
                txn.delete(
                    &key_table(&table.catalog, &table.namespace, &table.table)?,
                    ExpectedRevision::Any,
                )?;
            }
        }
        txn.delete(&key_namespace(catalog, namespace)?, ExpectedRevision::Any)?;
        Ok(())
    }

    pub fn delete_catalog_and_mv_relationships(
        &self,
        txn: &mut dyn MetaWriteTxn,
        mv_repo: &MvMetaRepository,
        catalog: &str,
    ) -> RepositoryResult<()> {
        for mv in mv_repo.list_definitions(txn)? {
            if mv.target_catalog.as_deref().map(normalize_lookup_name)
                == Some(normalize_lookup_name(catalog))
                && let (Some(target_catalog), Some(target_namespace), Some(target_table)) = (
                    mv.target_catalog.as_deref(),
                    mv.target_namespace.as_deref(),
                    mv.target_table.as_deref(),
                )
            {
                mv_repo.drop_by_target(txn, target_catalog, target_namespace, target_table)?;
            }
        }
        for table in self.list_tables(txn)? {
            if table.catalog == normalize_lookup_name(catalog) {
                txn.delete(
                    &key_table(&table.catalog, &table.namespace, &table.table)?,
                    ExpectedRevision::Any,
                )?;
            }
        }
        for namespace in self.list_namespaces(txn)? {
            if namespace.catalog == normalize_lookup_name(catalog) {
                txn.delete(
                    &key_namespace(&namespace.catalog, &namespace.namespace)?,
                    ExpectedRevision::Any,
                )?;
            }
        }
        txn.delete(&key_catalog(catalog)?, ExpectedRevision::Any)?;
        Ok(())
    }
}

fn decode_catalog_record(record: &MetaRecord) -> RepositoryResult<IcebergCatalogProperties> {
    decode_record_payload(record, ICEBERG_CATALOG_KIND, ICEBERG_CATALOG_SCHEMA_VERSION)
}

fn decode_namespace_record(record: &MetaRecord) -> RepositoryResult<IcebergNamespaceRecord> {
    decode_record_payload(
        record,
        ICEBERG_NAMESPACE_KIND,
        ICEBERG_NAMESPACE_SCHEMA_VERSION,
    )
}

fn decode_table_record(record: &MetaRecord) -> RepositoryResult<IcebergTableRecord> {
    decode_record_payload(record, ICEBERG_TABLE_KIND, ICEBERG_TABLE_SCHEMA_VERSION)
}

fn decode_record_payload<T>(
    record: &MetaRecord,
    expected_kind: &str,
    expected_schema_version: i32,
) -> RepositoryResult<T>
where
    T: for<'de> Deserialize<'de>,
{
    if record.kind.as_str() != expected_kind {
        return Err(RepositoryError::provider(format!(
            "metadata record {} has kind {}, expected {expected_kind}",
            record.key.canonical_path(),
            record.kind.as_str()
        )));
    }
    if record.payload.schema_version != expected_schema_version {
        return Err(RepositoryError::provider(format!(
            "metadata record {} has schema version {}, expected {expected_schema_version}",
            record.key.canonical_path(),
            record.payload.schema_version
        )));
    }
    decode_json_payload(&record.payload)
}

fn record_kind(value: &str) -> RepositoryResult<MetaRecordKind> {
    Ok(MetaRecordKind::new(value)?)
}

fn record_path_component(
    record: &MetaRecord,
    index: usize,
    description: &str,
) -> RepositoryResult<String> {
    record
        .key
        .canonical_path()
        .split('/')
        .nth(index)
        .map(str::to_string)
        .ok_or_else(|| {
            RepositoryError::provider(format!(
                "metadata record {} is not a valid {description} key",
                record.key.canonical_path()
            ))
        })
}

fn key_catalog(catalog: &str) -> RepositoryResult<MetaKey> {
    Ok(MetaKey::new(
        NS_ICEBERG_CATALOG,
        ["catalog".to_string(), normalize_lookup_name(catalog)],
    )?)
}

fn key_prefix_catalogs() -> RepositoryResult<MetaKeyPrefix> {
    Ok(MetaKeyPrefix::new(NS_ICEBERG_CATALOG, ["catalog"])?)
}

fn key_prefix_namespaces() -> RepositoryResult<MetaKeyPrefix> {
    Ok(MetaKeyPrefix::new(NS_ICEBERG_CATALOG, ["namespace"])?)
}

fn key_prefix_tables() -> RepositoryResult<MetaKeyPrefix> {
    Ok(MetaKeyPrefix::new(NS_ICEBERG_CATALOG, ["table"])?)
}

fn key_namespace(catalog: &str, namespace: &str) -> RepositoryResult<MetaKey> {
    Ok(MetaKey::new(
        NS_ICEBERG_CATALOG,
        [
            "namespace".to_string(),
            normalize_lookup_name(catalog),
            normalize_lookup_name(namespace),
        ],
    )?)
}

fn key_table(catalog: &str, namespace: &str, table: &str) -> RepositoryResult<MetaKey> {
    Ok(MetaKey::new(
        NS_ICEBERG_CATALOG,
        [
            "table".to_string(),
            normalize_lookup_name(catalog),
            normalize_lookup_name(namespace),
            normalize_lookup_name(table),
        ],
    )?)
}
