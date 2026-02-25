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
use std::collections::HashMap;
use std::sync::{Mutex, OnceLock};

use anyhow::{Context, Result, anyhow};
use opendal::Operator;
use url::Url;

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct HdfsOperatorCacheKey {
    name_node: String,
    user: Option<String>,
}

static HDFS_OPERATOR_CACHE: OnceLock<Mutex<HashMap<HdfsOperatorCacheKey, Operator>>> =
    OnceLock::new();

fn hdfs_operator_cache() -> &'static Mutex<HashMap<HdfsOperatorCacheKey, Operator>> {
    HDFS_OPERATOR_CACHE.get_or_init(|| Mutex::new(HashMap::new()))
}

#[derive(Clone, Debug)]
pub struct HdfsPath {
    pub name_node: String,
    pub user: Option<String>,
    pub rel_path: String,
}

pub fn parse_hdfs_path(raw: &str) -> Result<HdfsPath, String> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Err("hdfs path is empty".to_string());
    }
    let url =
        Url::parse(trimmed).map_err(|e| format!("invalid hdfs path: {trimmed}, error={e}"))?;
    if url.scheme() != "hdfs" {
        return Err(format!(
            "invalid hdfs path scheme: expected hdfs://, got {}",
            url.scheme()
        ));
    }
    let host = url
        .host_str()
        .ok_or_else(|| format!("hdfs path missing host: {trimmed}"))?;
    let authority = match url.port() {
        Some(port) => format!("{host}:{port}"),
        None => host.to_string(),
    };
    if url.password().is_some() {
        return Err(format!(
            "hdfs path must not include password in authority: {trimmed}"
        ));
    }
    if url.query().is_some() || url.fragment().is_some() {
        return Err(format!(
            "hdfs path must not include query or fragment: {trimmed}"
        ));
    }
    let rel_path = url.path().trim_start_matches('/').to_string();
    if rel_path.is_empty() {
        return Err(format!(
            "hdfs path points to namenode root and cannot be used as file path: {trimmed}"
        ));
    }
    let user = (!url.username().is_empty()).then_some(url.username().to_string());

    Ok(HdfsPath {
        name_node: format!("hdfs://{authority}"),
        user,
        rel_path,
    })
}

#[derive(Clone, Debug)]
pub struct ResolvedHdfsPaths {
    pub name_node: String,
    pub user: Option<String>,
    pub paths: Vec<String>,
}

pub fn resolve_hdfs_scan_paths(paths: &[String]) -> Result<ResolvedHdfsPaths, String> {
    if paths.is_empty() {
        return Err("scan paths are empty".to_string());
    }

    let mut parsed_paths = Vec::with_capacity(paths.len());
    for path in paths {
        parsed_paths.push(parse_hdfs_path(path)?);
    }

    let first = parsed_paths
        .first()
        .cloned()
        .ok_or_else(|| "scan paths are empty".to_string())?;
    let mut normalized = Vec::with_capacity(parsed_paths.len());
    for item in parsed_paths {
        if item.name_node != first.name_node {
            return Err(format!(
                "mixed hdfs namenodes are not allowed in one scan context: {} vs {}",
                first.name_node, item.name_node
            ));
        }
        if item.user != first.user {
            return Err(
                "mixed hdfs users are not allowed in one scan context; use one user per scan"
                    .to_string(),
            );
        }
        normalized.push(item.rel_path);
    }

    Ok(ResolvedHdfsPaths {
        name_node: first.name_node.clone(),
        user: first.user.clone(),
        paths: normalized,
    })
}

fn build_hdfs_native_url(name_node: &str, user: Option<&str>) -> Result<String> {
    let Some(user) = user else {
        return Ok(name_node.to_string());
    };

    let mut url = Url::parse(name_node)
        .with_context(|| format!("invalid hdfs namenode for native backend: {name_node}"))?;
    url.set_username(user)
        .map_err(|_| anyhow!("invalid hdfs user for native backend: {user}"))?;
    Ok(url.to_string().trim_end_matches('/').to_string())
}

fn build_raw_hdfs_native_operator(name_node: &str, user: Option<&str>) -> Result<Operator> {
    let url = build_hdfs_native_url(name_node, user)?;
    let builder = opendal::services::HdfsNative::default()
        .name_node(&url)
        .root("/");
    let op = Operator::new(builder)
        .with_context(|| format!("init opendal hdfs-native operator, url={url}"))?
        .finish();
    Ok(op)
}

fn build_raw_hdfs_operator(name_node: &str, user: Option<&str>) -> Result<Operator> {
    build_raw_hdfs_native_operator(name_node, user)
}

pub fn build_hdfs_operator(name_node: &str, user: Option<&str>) -> Result<Operator> {
    let key = HdfsOperatorCacheKey {
        name_node: name_node.to_string(),
        user: user.map(|v| v.to_string()),
    };
    if let Some(op) = {
        let guard = hdfs_operator_cache()
            .lock()
            .map_err(|_| anyhow!("lock hdfs operator cache failed"))?;
        guard.get(&key).cloned()
    } {
        return Ok(op);
    }

    let op = build_raw_hdfs_operator(name_node, user)?;
    let mut guard = hdfs_operator_cache()
        .lock()
        .map_err(|_| anyhow!("lock hdfs operator cache failed"))?;
    let cached = guard.entry(key).or_insert_with(|| op.clone());
    Ok(cached.clone())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_hdfs_path_works() {
        let parsed = parse_hdfs_path("hdfs://nn-1:9000/user/hive/t.parquet").expect("parse path");
        assert_eq!(parsed.name_node, "hdfs://nn-1:9000");
        assert_eq!(parsed.user, None);
        assert_eq!(parsed.rel_path, "user/hive/t.parquet");
    }

    #[test]
    fn parse_hdfs_path_rejects_root_only() {
        let err = parse_hdfs_path("hdfs://nn-1:9000/").expect_err("reject root path");
        assert!(err.contains("cannot be used as file path"));
    }

    #[test]
    fn resolve_hdfs_scan_paths_rejects_mixed_namenode() {
        let paths = vec![
            "hdfs://nn-1:9000/user/hive/a.parquet".to_string(),
            "hdfs://nn-2:9000/user/hive/b.parquet".to_string(),
        ];
        let err = resolve_hdfs_scan_paths(&paths).expect_err("mixed namenode must fail");
        assert!(err.contains("mixed hdfs namenodes"));
    }
}
