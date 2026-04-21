use crate::common::app_config::StandaloneManagedLakeConfig as AppManagedLakeConfig;
use crate::runtime::starlet_shard_registry::S3StoreConfig;

use super::super::iceberg::add_files::parse_s3_path;

#[derive(Clone, Debug)]
pub(crate) struct ManagedLakeConfig {
    pub(crate) warehouse_uri: String,
    pub(crate) s3: S3StoreConfig,
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
