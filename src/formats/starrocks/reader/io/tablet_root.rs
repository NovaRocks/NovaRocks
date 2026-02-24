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
//! Tablet root URI parser.
//!
//! Current limitations:
//! - Supported roots: `oss://`, `s3://`, absolute local path, `file://`.

/// Parsed tablet root location used by the native reader.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum TabletRoot {
    /// `oss://bucket/prefix` or `s3://bucket/prefix`
    S3 { bucket: String, root: String },
    /// Local filesystem root path.
    Local { root: String },
}

impl TabletRoot {
    /// Parse tablet root URI from plan metadata.
    pub(crate) fn parse(value: &str) -> Result<Self, String> {
        let trimmed = value.trim();
        if let Some(rest) = trimmed.strip_prefix("oss://") {
            let (bucket, root) = split_bucket_and_root(rest)?;
            return Ok(Self::S3 { bucket, root });
        }
        if let Some(rest) = trimmed.strip_prefix("s3://") {
            let (bucket, root) = split_bucket_and_root(rest)?;
            return Ok(Self::S3 { bucket, root });
        }
        if let Some(rest) = trimmed.strip_prefix("file://") {
            let root = normalize_local_root(rest)?;
            return Ok(Self::Local { root });
        }
        if trimmed.starts_with('/') {
            return Ok(Self::Local {
                root: trimmed.to_string(),
            });
        }
        Err(format!(
            "unsupported tablet root path for native data loader: {}",
            value
        ))
    }
}

fn split_bucket_and_root(value: &str) -> Result<(String, String), String> {
    let mut parts = value.splitn(2, '/');
    let bucket = parts
        .next()
        .map(str::trim)
        .filter(|v| !v.is_empty())
        .ok_or_else(|| format!("invalid object storage path (missing bucket): {}", value))?;
    let root = parts.next().map(str::trim).unwrap_or_default();
    Ok((bucket.to_string(), root.to_string()))
}

fn normalize_local_root(value: &str) -> Result<String, String> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Err("invalid local file:// path for native data loader: empty root".to_string());
    }
    if !trimmed.starts_with('/') {
        return Err(format!(
            "invalid local file:// path for native data loader: {}",
            value
        ));
    }
    Ok(trimmed.to_string())
}

#[cfg(test)]
mod tests {
    use super::TabletRoot;

    #[test]
    fn parse_local_absolute_path() {
        let root = TabletRoot::parse("/tmp/starrocks/tablet").expect("parse local path");
        assert_eq!(
            root,
            TabletRoot::Local {
                root: "/tmp/starrocks/tablet".to_string()
            }
        );
    }

    #[test]
    fn parse_file_scheme_path() {
        let root = TabletRoot::parse("file:///tmp/starrocks/tablet").expect("parse file path");
        assert_eq!(
            root,
            TabletRoot::Local {
                root: "/tmp/starrocks/tablet".to_string()
            }
        );
    }

    #[test]
    fn parse_s3_path() {
        let root = TabletRoot::parse("s3://bucket/prefix").expect("parse s3 path");
        assert_eq!(
            root,
            TabletRoot::S3 {
                bucket: "bucket".to_string(),
                root: "prefix".to_string()
            }
        );
    }
}
