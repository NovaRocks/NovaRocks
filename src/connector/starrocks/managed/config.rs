use crate::common::app_config::StandaloneManagedLakeConfig as AppManagedLakeConfig;
use crate::runtime::starlet_shard_registry::S3StoreConfig;

use crate::connector::iceberg::catalog::add_files::parse_s3_path;

#[derive(Clone, Debug)]
pub(crate) struct ManagedLakeConfig {
    pub(crate) warehouse_uri: String,
    pub(crate) s3: S3StoreConfig,
    pub(crate) mv_default_storage_engine: String,
}

impl ManagedLakeConfig {
    pub(crate) fn from_app_config(config: AppManagedLakeConfig) -> Result<Self, String> {
        let warehouse_uri = config
            .warehouse_uri
            .trim()
            .trim_end_matches('/')
            .to_string();
        if warehouse_uri.is_empty() {
            return Err("standalone managed lake warehouse_uri is empty".to_string());
        }
        let (bucket, root) = parse_s3_path(&warehouse_uri)?;
        let mv_default_storage_engine = config
            .mv_default_storage_engine
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .unwrap_or("managed_lake")
            .to_string();
        if mv_default_storage_engine != "managed_lake" && mv_default_storage_engine != "iceberg" {
            return Err(format!(
                "invalid mv_default_storage_engine `{mv_default_storage_engine}`; allowed: managed_lake, iceberg"
            ));
        }
        Ok(Self {
            warehouse_uri,
            s3: S3StoreConfig {
                endpoint: config.endpoint.trim().to_string(),
                bucket,
                root: root.trim_matches('/').to_string(),
                access_key_id: config.access_key_id.trim().to_string(),
                access_key_secret: config.access_key_secret.trim().to_string(),
                region: config.region.as_ref().map(|value| value.trim().to_string()),
                enable_path_style_access: config.enable_path_style_access,
            },
            mv_default_storage_engine,
        })
    }

    pub(crate) fn tablet_root_path(&self, db_id: i64, table_id: i64, partition_id: i64) -> String {
        // All tablets in a partition share the same root so partition replacement
        // can switch visibility without rewriting tablet-internal object layout.
        format!(
            "{}/db_{db_id}/table_{table_id}/partition_{partition_id}",
            self.warehouse_uri
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::app_config::StandaloneManagedLakeConfig;

    #[test]
    fn managed_lake_config_propagates_default_storage_engine() {
        let app = StandaloneManagedLakeConfig {
            warehouse_uri: "s3://bucket/wh/".to_string(),
            endpoint: "http://localhost:9000".to_string(),
            access_key_id: "ak".to_string(),
            access_key_secret: "sk".to_string(),
            region: None,
            enable_path_style_access: Some(true),
            mv_default_storage_engine: Some("iceberg".to_string()),
        };
        let cfg = ManagedLakeConfig::from_app_config(app).expect("config");
        assert_eq!(cfg.mv_default_storage_engine, "iceberg");
    }

    #[test]
    fn managed_lake_config_defaults_storage_engine_to_managed_lake() {
        let app = StandaloneManagedLakeConfig {
            warehouse_uri: "s3://bucket/wh/".to_string(),
            endpoint: "http://localhost:9000".to_string(),
            access_key_id: "ak".to_string(),
            access_key_secret: "sk".to_string(),
            region: None,
            enable_path_style_access: Some(true),
            mv_default_storage_engine: None,
        };
        let cfg = ManagedLakeConfig::from_app_config(app).expect("config");
        assert_eq!(cfg.mv_default_storage_engine, "managed_lake");
    }

    #[test]
    fn managed_lake_config_rejects_unknown_storage_engine() {
        let app = StandaloneManagedLakeConfig {
            warehouse_uri: "s3://bucket/wh/".to_string(),
            endpoint: "http://localhost:9000".to_string(),
            access_key_id: "ak".to_string(),
            access_key_secret: "sk".to_string(),
            region: None,
            enable_path_style_access: Some(true),
            mv_default_storage_engine: Some("duckdb".to_string()),
        };
        let err = ManagedLakeConfig::from_app_config(app).unwrap_err();
        assert!(err.contains("duckdb"), "err={err}");
        assert!(err.contains("managed_lake"), "err={err}");
        assert!(err.contains("iceberg"), "err={err}");
    }
}
