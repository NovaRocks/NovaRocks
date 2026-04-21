use futures::TryStreamExt;
use opendal::EntryMode;
use tempfile::TempDir;

use novarocks::fs::object_store::{ObjectStoreConfig, build_oss_operator};
use novarocks::runtime::global_async_runtime::data_block_on;

const DEFAULT_ACCESS_KEY_ID: &str = "admin";
const DEFAULT_ACCESS_KEY_SECRET: &str = "admin123";
const DEFAULT_BUCKET: &str = "novarocks";
const OBJECT_STORE_ENV: &str = "AWS_S3_ENDPOINT";

pub struct ManagedLakeTestHarness {
    _temp_dir: TempDir,
    pub config_path: std::path::PathBuf,
    pub metadata_db_path: std::path::PathBuf,
    pub warehouse_uri: String,
    object_store_config: ObjectStoreConfig,
    warehouse_prefix: String,
}

impl ManagedLakeTestHarness {
    pub fn maybe_new(test_name: &str) -> Result<Option<Self>, String> {
        let Ok(endpoint) = std::env::var(OBJECT_STORE_ENV) else {
            return Ok(None);
        };
        let temp_dir = tempfile::tempdir().map_err(|e| format!("create tempdir failed: {e}"))?;
        let config_path = temp_dir.path().join("novarocks.toml");
        let metadata_db_path = temp_dir.path().join("standalone.sqlite");
        let run_id = format!(
            "{}_{}_{}",
            sanitize_test_name(test_name),
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map_err(|e| format!("read system clock failed: {e}"))?
                .as_nanos()
        );
        let bucket =
            std::env::var("NOVAROCKS_TEST_BUCKET").unwrap_or_else(|_| DEFAULT_BUCKET.to_string());
        let warehouse_prefix = format!("standalone-managed-lake-tests/{run_id}");
        let warehouse_uri = format!("s3://{bucket}/{warehouse_prefix}");
        let access_key_id =
            std::env::var("MINIO_ROOT_USER").unwrap_or_else(|_| DEFAULT_ACCESS_KEY_ID.to_string());
        let access_key_secret = std::env::var("MINIO_ROOT_PASSWORD")
            .unwrap_or_else(|_| DEFAULT_ACCESS_KEY_SECRET.to_string());
        let object_store_config = ObjectStoreConfig {
            endpoint: endpoint.clone(),
            bucket: bucket.clone(),
            root: String::new(),
            access_key_id: access_key_id.clone(),
            access_key_secret: access_key_secret.clone(),
            session_token: None,
            enable_path_style_access: Some(true),
            region: Some("us-east-1".to_string()),
            retry_max_times: Some(2),
            retry_min_delay_ms: Some(50),
            retry_max_delay_ms: Some(200),
            timeout_ms: Some(10_000),
            io_timeout_ms: Some(10_000),
        };

        std::fs::write(
            &config_path,
            format!(
                r#"[standalone_server]
metadata_db_path = "{}"
warehouse_uri = "{}"

[standalone_server.object_store]
endpoint = "{}"
access_key_id = "{}"
access_key_secret = "{}"
enable_path_style_access = true
"#,
                metadata_db_path.display(),
                warehouse_uri,
                endpoint,
                access_key_id,
                access_key_secret,
            ),
        )
        .map_err(|e| format!("write managed lake test config failed: {e}"))?;

        Ok(Some(Self {
            _temp_dir: temp_dir,
            config_path,
            metadata_db_path,
            warehouse_uri,
            object_store_config,
            warehouse_prefix,
        }))
    }

    pub fn new(test_name: &str) -> Result<Self, String> {
        Self::maybe_new(test_name)?.ok_or_else(|| {
            format!("managed lake object-store tests require {OBJECT_STORE_ENV} to be set")
        })
    }

    pub fn list_warehouse_objects(&self) -> Result<Vec<String>, String> {
        let op = build_oss_operator(&self.object_store_config)
            .map_err(|e| format!("build object store operator failed: {e}"))?;
        let prefix = self.prefixed_root("");
        data_block_on(async move {
            let mut entries = op
                .lister_with(&prefix)
                .recursive(true)
                .await
                .map_err(|e| format!("list object store prefix `{prefix}` failed: {e}"))?;
            let mut paths = Vec::new();
            while let Some(entry) = entries
                .try_next()
                .await
                .map_err(|e| format!("read object store listing `{prefix}` failed: {e}"))?
            {
                if entry.metadata().mode() == EntryMode::FILE {
                    paths.push(entry.path().to_string());
                }
            }
            Ok::<Vec<String>, String>(paths)
        })?
    }

    pub fn list_tablet_objects(&self, tablet_root_path: &str) -> Result<Vec<String>, String> {
        let prefix = tablet_root_path
            .strip_prefix(&format!("s3://{}/", self.object_store_config.bucket))
            .ok_or_else(|| format!("tablet path is outside test bucket: {tablet_root_path}"))?;
        let op = build_oss_operator(&self.object_store_config)
            .map_err(|e| format!("build object store operator failed: {e}"))?;
        let prefix = prefix.trim_matches('/').to_string();
        data_block_on(async move {
            let mut entries = op
                .lister_with(&prefix)
                .recursive(true)
                .await
                .map_err(|e| format!("list tablet prefix `{prefix}` failed: {e}"))?;
            let mut paths = Vec::new();
            while let Some(entry) = entries
                .try_next()
                .await
                .map_err(|e| format!("read tablet listing `{prefix}` failed: {e}"))?
            {
                if entry.metadata().mode() == EntryMode::FILE {
                    paths.push(entry.path().to_string());
                }
            }
            Ok::<Vec<String>, String>(paths)
        })?
    }

    fn cleanup_prefix(&self) -> Result<(), String> {
        let op = build_oss_operator(&self.object_store_config)
            .map_err(|e| format!("build object store operator failed: {e}"))?;
        let prefix = self.prefixed_root("");
        data_block_on(async move {
            let mut entries = op
                .lister_with(&prefix)
                .recursive(true)
                .await
                .map_err(|e| format!("list cleanup prefix `{prefix}` failed: {e}"))?;
            let mut paths = Vec::new();
            while let Some(entry) = entries
                .try_next()
                .await
                .map_err(|e| format!("read cleanup listing `{prefix}` failed: {e}"))?
            {
                if entry.metadata().mode() == EntryMode::FILE {
                    paths.push(entry.path().to_string());
                }
            }
            for path in paths.into_iter().rev() {
                op.delete(&path)
                    .await
                    .map_err(|e| format!("delete object `{path}` failed: {e}"))?;
            }
            Ok::<(), String>(())
        })?
    }

    fn prefixed_root(&self, suffix: &str) -> String {
        if suffix.is_empty() {
            self.warehouse_prefix.clone()
        } else {
            format!("{}/{}", self.warehouse_prefix, suffix.trim_matches('/'))
        }
    }
}

impl Drop for ManagedLakeTestHarness {
    fn drop(&mut self) {
        let _ = self.cleanup_prefix();
    }
}

fn sanitize_test_name(raw: &str) -> String {
    raw.chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() {
                ch.to_ascii_lowercase()
            } else {
                '-'
            }
        })
        .collect()
}
