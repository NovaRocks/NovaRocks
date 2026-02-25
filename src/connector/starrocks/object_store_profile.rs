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

use std::collections::BTreeMap;

use crate::fs::object_store::{ObjectStoreConfig, ObjectStoreRetrySettings};
use crate::runtime::starlet_shard_registry::S3StoreConfig;

const UNSUPPORTED_OBJECT_STORE_PREFIXES: [&str; 7] = [
    "fs.s3a.",
    "fs.s3n.",
    "fs.s3.",
    "fs.oss.",
    "fs.cos.",
    "fs.obs.",
    "aliyun.oss.",
];

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ObjectStoreProfile {
    pub(crate) endpoint: String,
    pub(crate) access_key_id: String,
    pub(crate) access_key_secret: String,
    pub(crate) session_token: Option<String>,
    pub(crate) region: Option<String>,
    pub(crate) enable_path_style_access: Option<bool>,
    pub(crate) retry_max_times: Option<usize>,
    pub(crate) retry_min_delay_ms: Option<u64>,
    pub(crate) retry_max_delay_ms: Option<u64>,
    pub(crate) timeout_ms: Option<u64>,
    pub(crate) io_timeout_ms: Option<u64>,
}

impl ObjectStoreProfile {
    pub(crate) fn from_properties_optional(
        props: &BTreeMap<String, String>,
    ) -> Result<Option<Self>, String> {
        let mut aws_props: BTreeMap<String, String> = BTreeMap::new();
        let mut unsupported: Vec<String> = Vec::new();

        for (key, value) in props {
            if key.starts_with("aws.s3.") {
                aws_props.insert(key.clone(), value.clone());
                continue;
            }
            if is_unsupported_object_storage_key(key) {
                unsupported.push(key.clone());
            }
        }

        if !unsupported.is_empty() {
            unsupported.sort();
            return Err(format!(
                "unsupported object storage properties detected: [{}] (only aws.s3.* is supported)",
                unsupported.join(", ")
            ));
        }

        if aws_props.is_empty() {
            return Ok(None);
        }

        Ok(Some(Self::from_aws_s3_properties(&aws_props)?))
    }

    pub(crate) fn from_properties_required(
        props: &BTreeMap<String, String>,
    ) -> Result<Self, String> {
        Self::from_properties_optional(props)?
            .ok_or_else(|| "missing aws.s3.* properties for object_store mode".to_string())
    }

    pub(crate) fn from_s3_store_config(config: &S3StoreConfig) -> Result<Self, String> {
        let endpoint = normalize_endpoint(&config.endpoint, None)?;
        let access_key_id = non_empty("S3 access_key_id", &config.access_key_id)?;
        let access_key_secret = non_empty("S3 access_key_secret", &config.access_key_secret)?;
        Ok(Self {
            endpoint,
            access_key_id,
            access_key_secret,
            session_token: None,
            region: config
                .region
                .as_ref()
                .map(|v| v.trim())
                .filter(|v| !v.is_empty())
                .map(|v| v.to_string()),
            enable_path_style_access: config.enable_path_style_access,
            retry_max_times: None,
            retry_min_delay_ms: None,
            retry_max_delay_ms: None,
            timeout_ms: None,
            io_timeout_ms: None,
        })
    }

    pub(crate) fn to_object_store_config(&self, bucket: &str, root: &str) -> ObjectStoreConfig {
        let mut cfg = ObjectStoreConfig {
            endpoint: self.endpoint.clone(),
            bucket: bucket.to_string(),
            root: root.trim_matches('/').to_string(),
            access_key_id: self.access_key_id.clone(),
            access_key_secret: self.access_key_secret.clone(),
            session_token: self.session_token.clone(),
            enable_path_style_access: self.enable_path_style_access,
            region: self.region.clone(),
            retry_max_times: self.retry_max_times,
            retry_min_delay_ms: self.retry_min_delay_ms,
            retry_max_delay_ms: self.retry_max_delay_ms,
            timeout_ms: self.timeout_ms,
            io_timeout_ms: self.io_timeout_ms,
        };
        crate::fs::object_store::apply_object_store_runtime_defaults(&mut cfg);
        cfg
    }

    fn from_aws_s3_properties(props: &BTreeMap<String, String>) -> Result<Self, String> {
        let enable_ssl = props.get("aws.s3.enable_ssl").map(|v| is_true_value(v));
        let endpoint_raw = props
            .get("aws.s3.endpoint")
            .ok_or_else(|| "missing aws.s3.endpoint for object_store mode".to_string())?;
        let endpoint = normalize_endpoint(endpoint_raw, enable_ssl)?;
        let access_key_id = non_empty(
            "aws.s3.accessKeyId/aws.s3.access_key",
            props
                .get("aws.s3.accessKeyId")
                .or_else(|| props.get("aws.s3.access_key"))
                .map(String::as_str)
                .unwrap_or(""),
        )?;
        let access_key_secret = non_empty(
            "aws.s3.accessKeySecret/aws.s3.secret_key",
            props
                .get("aws.s3.accessKeySecret")
                .or_else(|| props.get("aws.s3.secret_key"))
                .map(String::as_str)
                .unwrap_or(""),
        )?;
        let session_token = props
            .get("aws.s3.session_token")
            .map(|v| v.trim())
            .filter(|v| !v.is_empty())
            .map(|v| v.to_string());
        let region = props
            .get("aws.s3.region")
            .map(|v| v.trim())
            .filter(|v| !v.is_empty())
            .map(|v| v.to_string());
        let enable_path_style_access = props
            .get("aws.s3.enable_path_style_access")
            .map(|v| is_true_value(v));
        let retry_settings = ObjectStoreRetrySettings::from_aws_s3_props(Some(props));

        Ok(Self {
            endpoint,
            access_key_id,
            access_key_secret,
            session_token,
            region,
            enable_path_style_access,
            retry_max_times: retry_settings.retry_max_times,
            retry_min_delay_ms: retry_settings.retry_min_delay_ms,
            retry_max_delay_ms: retry_settings.retry_max_delay_ms,
            timeout_ms: retry_settings.timeout_ms,
            io_timeout_ms: retry_settings.io_timeout_ms,
        })
    }
}

fn non_empty(field: &str, value: &str) -> Result<String, String> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Err(format!("missing {field} for object_store mode"));
    }
    Ok(trimmed.to_string())
}

fn is_unsupported_object_storage_key(key: &str) -> bool {
    UNSUPPORTED_OBJECT_STORE_PREFIXES
        .iter()
        .any(|prefix| key.starts_with(prefix))
}

fn normalize_endpoint(
    raw_endpoint: &str,
    explicit_enable_ssl: Option<bool>,
) -> Result<String, String> {
    let mut view = raw_endpoint.trim();
    if view.is_empty() {
        return Err("empty aws.s3.endpoint for object_store mode".to_string());
    }
    let mut inferred_enable_ssl = None;
    if let Some(rest) = view.strip_prefix("http://") {
        view = rest;
        inferred_enable_ssl = Some(false);
    } else if let Some(rest) = view.strip_prefix("https://") {
        view = rest;
        inferred_enable_ssl = Some(true);
    }
    if let Some((authority, _)) = view.split_once('/') {
        view = authority;
    }
    let host = view.trim_end_matches('/');
    if host.is_empty() {
        return Err(format!(
            "invalid aws.s3.endpoint for object_store mode: {raw_endpoint}"
        ));
    }

    let enable_ssl = explicit_enable_ssl.or(inferred_enable_ssl).unwrap_or(true);
    let scheme = if enable_ssl { "https" } else { "http" };
    Ok(format!("{scheme}://{host}"))
}

fn is_true_value(value: &str) -> bool {
    let v = value.trim();
    v.eq_ignore_ascii_case("true") || v == "1"
}

#[cfg(test)]
mod tests {
    use super::ObjectStoreProfile;
    use std::collections::BTreeMap;

    #[test]
    fn endpoint_keeps_http_when_enable_ssl_is_false() {
        let mut props = BTreeMap::new();
        props.insert(
            "aws.s3.endpoint".to_string(),
            "minio.local:9000".to_string(),
        );
        props.insert("aws.s3.accessKeyId".to_string(), "ak".to_string());
        props.insert("aws.s3.accessKeySecret".to_string(), "sk".to_string());
        props.insert("aws.s3.enable_ssl".to_string(), "false".to_string());
        let profile = ObjectStoreProfile::from_properties_required(&props).expect("build profile");
        assert_eq!(profile.endpoint, "http://minio.local:9000");
    }

    #[test]
    fn endpoint_strips_path_component() {
        let mut props = BTreeMap::new();
        props.insert(
            "aws.s3.endpoint".to_string(),
            "https://minio.local:9000/path".to_string(),
        );
        props.insert("aws.s3.accessKeyId".to_string(), "ak".to_string());
        props.insert("aws.s3.accessKeySecret".to_string(), "sk".to_string());
        let profile = ObjectStoreProfile::from_properties_required(&props).expect("build profile");
        assert_eq!(profile.endpoint, "https://minio.local:9000");
    }

    #[test]
    fn missing_path_style_property_keeps_auto_mode() {
        let mut props = BTreeMap::new();
        props.insert(
            "aws.s3.endpoint".to_string(),
            "https://oss-cn-zhangjiakou.aliyuncs.com".to_string(),
        );
        props.insert("aws.s3.accessKeyId".to_string(), "ak".to_string());
        props.insert("aws.s3.accessKeySecret".to_string(), "sk".to_string());
        let profile = ObjectStoreProfile::from_properties_required(&props).expect("build profile");
        assert_eq!(profile.enable_path_style_access, None);
    }

    #[test]
    fn retry_and_timeout_properties_are_parsed() {
        let mut props = BTreeMap::new();
        props.insert(
            "aws.s3.endpoint".to_string(),
            "https://oss-cn-zhangjiakou.aliyuncs.com".to_string(),
        );
        props.insert("aws.s3.accessKeyId".to_string(), "ak".to_string());
        props.insert("aws.s3.accessKeySecret".to_string(), "sk".to_string());
        props.insert("aws.s3.max_retries".to_string(), "8".to_string());
        props.insert("aws.s3.retry_min_delay_ms".to_string(), "120".to_string());
        props.insert("aws.s3.retry_max_delay_ms".to_string(), "2600".to_string());
        props.insert("aws.s3.request_timeout_ms".to_string(), "3400".to_string());
        props.insert("aws.s3.io_timeout_ms".to_string(), "4400".to_string());

        let profile = ObjectStoreProfile::from_properties_required(&props).expect("build profile");
        assert_eq!(profile.retry_max_times, Some(8));
        assert_eq!(profile.retry_min_delay_ms, Some(120));
        assert_eq!(profile.retry_max_delay_ms, Some(2600));
        assert_eq!(profile.timeout_ms, Some(3400));
        assert_eq!(profile.io_timeout_ms, Some(4400));
    }
}
