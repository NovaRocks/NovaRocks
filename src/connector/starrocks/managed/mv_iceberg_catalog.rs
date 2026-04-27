//! Builds the NovaRocks-internal `__nova_mv__` Iceberg catalog used as the
//! physical store for materialized views with `storage_engine = 'iceberg'`.
//!
//! This catalog is private — it is never registered with the user-visible
//! `IcebergCatalogRegistry` and that registry rejects `__nova_mv__` as a user
//! catalog name.

use std::sync::Arc;

use iceberg::Catalog;
use iceberg::io::{FileIOBuilder, LocalFsStorageFactory};

use crate::connector::iceberg::catalog::hadoop_catalog::HadoopFileSystemCatalog;
use crate::connector::iceberg::catalog::s3_storage::S3StorageFactory;
use crate::connector::starrocks::managed::config::ManagedLakeConfig;

/// Reserved catalog name. The `IcebergCatalogRegistry::create_catalog` path
/// rejects any user attempt to register a catalog with this identifier.
pub(crate) const NOVA_MV_CATALOG_NAME: &str = "__nova_mv__";

/// Build a fresh `HadoopFileSystemCatalog` rooted at the NovaRocks-private MV
/// warehouse derived from `cfg`. Each call returns a new `Arc`; callers in
/// `mv_refresh_iceberg.rs` build a fresh catalog per CREATE/REFRESH/DROP.
pub(crate) fn build_nova_mv_catalog(cfg: &ManagedLakeConfig) -> Result<Arc<dyn Catalog>, String> {
    let warehouse = cfg.mv_iceberg_warehouse();
    let file_io = build_file_io(cfg, &warehouse)?;
    let catalog = HadoopFileSystemCatalog::new(file_io, warehouse);
    Ok(Arc::new(catalog))
}

fn build_file_io(cfg: &ManagedLakeConfig, warehouse: &str) -> Result<iceberg::io::FileIO, String> {
    let scheme_end = warehouse
        .find("://")
        .ok_or_else(|| format!("nova_mv warehouse `{warehouse}` is not a URI"))?;
    let scheme = &warehouse[..scheme_end];
    match scheme {
        "file" => {
            let file_io = FileIOBuilder::new(
                Arc::new(LocalFsStorageFactory) as Arc<dyn iceberg::io::StorageFactory>
            )
            .build();
            Ok(file_io)
        }
        "s3" | "s3a" | "oss" => {
            let factory = S3StorageFactory {
                endpoint: cfg.s3.endpoint.clone(),
                access_key_id: cfg.s3.access_key_id.clone(),
                access_key_secret: cfg.s3.access_key_secret.clone(),
                region: crate::fs::object_store::effective_s3_region(cfg.s3.region.as_deref())
                    .to_string(),
                enable_path_style: cfg.s3.enable_path_style_access.unwrap_or(false),
            };
            let file_io =
                FileIOBuilder::new(Arc::new(factory) as Arc<dyn iceberg::io::StorageFactory>)
                    .build();
            Ok(file_io)
        }
        other => Err(format!("unsupported nova_mv warehouse scheme `{other}`")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connector::starrocks::managed::config::ManagedLakeConfig;
    use crate::runtime::starlet_shard_registry::S3StoreConfig;

    /// Fixture for round-trip tests that need a real writable local filesystem path.
    ///
    /// Sets `mv_iceberg_warehouse_location` directly to a `file://` URI so that
    /// `mv_iceberg_warehouse()` returns that path without going through the
    /// `warehouse_uri` fallback. `warehouse_uri` is intentionally set to a
    /// placeholder that would pass `from_app_config`'s `parse_s3_path` if called,
    /// but here we bypass `from_app_config` to keep round-trip tests self-contained.
    fn local_config_for_round_trip(tmp: &std::path::Path) -> ManagedLakeConfig {
        let warehouse = format!("file://{}/wh", tmp.display());
        ManagedLakeConfig {
            warehouse_uri: "s3://placeholder/wh".to_string(),
            s3: S3StoreConfig {
                endpoint: String::new(),
                bucket: String::new(),
                root: String::new(),
                access_key_id: String::new(),
                access_key_secret: String::new(),
                region: None,
                enable_path_style_access: None,
            },
            mv_default_storage_engine: "iceberg".to_string(),
            mv_iceberg_warehouse_location: Some(warehouse),
        }
    }

    #[tokio::test]
    async fn nova_mv_catalog_creates_namespace_and_table_round_trip() {
        let dir = tempfile::tempdir().expect("tempdir");
        let cfg = local_config_for_round_trip(dir.path());
        let catalog = build_nova_mv_catalog(&cfg).expect("catalog");
        let ns = iceberg::NamespaceIdent::from_strs(["mydb"]).unwrap();
        catalog
            .create_namespace(&ns, std::collections::HashMap::new())
            .await
            .expect("ns");
        let schema = iceberg::spec::Schema::builder()
            .with_fields(vec![std::sync::Arc::new(
                iceberg::spec::NestedField::required(
                    1,
                    "k",
                    iceberg::spec::Type::Primitive(iceberg::spec::PrimitiveType::Int),
                ),
            )])
            .build()
            .expect("schema");
        let creation = iceberg::TableCreation::builder()
            .name("mv1".to_string())
            .schema(schema)
            .build();
        let table = catalog.create_table(&ns, creation).await.expect("create");
        assert_eq!(table.identifier().name(), "mv1");
        assert!(
            catalog
                .table_exists(&iceberg::TableIdent::from_strs(["mydb", "mv1"]).unwrap())
                .await
                .unwrap()
        );
    }

    #[test]
    fn build_file_io_rejects_unsupported_scheme() {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut cfg = local_config_for_round_trip(dir.path());
        // Override warehouse to a scheme that is not file/s3/s3a/oss.
        cfg.mv_iceberg_warehouse_location = Some("ftp://nope/wh".to_string());
        let err = build_nova_mv_catalog(&cfg).unwrap_err();
        assert!(
            err.contains("ftp"),
            "error should mention the unsupported scheme: {err}"
        );
    }

    #[test]
    fn build_file_io_rejects_uri_without_scheme() {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut cfg = local_config_for_round_trip(dir.path());
        // Override warehouse to a path without a URI scheme.
        cfg.mv_iceberg_warehouse_location = Some("/just/a/path".to_string());
        let err = build_nova_mv_catalog(&cfg).unwrap_err();
        assert!(
            err.contains("not a URI"),
            "error should mention the URI parsing failure: {err}"
        );
    }
}
