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
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum ScanPathScheme {
    Local,
    Oss,
}

fn normalize_local_scan_path(raw: &str) -> Result<String, String> {
    let path = raw.trim();
    if let Some(rest) = path.strip_prefix("file://") {
        if rest.is_empty() {
            return Err("invalid file URI: empty path".to_string());
        }
        if let Some(abs) = rest.strip_prefix('/') {
            return Ok(format!("/{}", abs));
        }
        if let Some(host_path) = rest.strip_prefix("localhost/") {
            return Ok(format!("/{}", host_path));
        }
        return Err(format!("unsupported file URI host in local path: {path}"));
    }
    if let Some(rest) = path.strip_prefix("file:/") {
        return Ok(format!("/{}", rest.trim_start_matches('/')));
    }
    Ok(path.to_string())
}

pub fn classify_scan_paths<'a, I>(paths: I) -> Result<ScanPathScheme, String>
where
    I: IntoIterator<Item = &'a str>,
{
    let mut scheme: Option<ScanPathScheme> = None;
    for raw in paths {
        let path = raw.trim();
        if path.is_empty() {
            return Err("scan path is empty".to_string());
        }
        let current = if path.starts_with("oss://") || path.starts_with("s3://") {
            ScanPathScheme::Oss
        } else if path.starts_with("file:/")
            || path.starts_with("file://")
            || path.starts_with('/')
            || !path.contains("://")
        {
            ScanPathScheme::Local
        } else {
            return Err(format!("unsupported scan path scheme: {path}"));
        };
        if let Some(prev) = scheme {
            if prev != current {
                return Err("mixed scan path schemes are not allowed".to_string());
            }
        } else {
            scheme = Some(current);
        }
    }
    scheme.ok_or_else(|| "scan paths are empty".to_string())
}

pub struct ResolvedScanPaths {
    pub scheme: ScanPathScheme,
    pub root: Option<String>,
    pub paths: Vec<String>,
}

pub fn resolve_opendal_paths(
    paths: &[String],
    object_store_cfg: Option<&crate::fs::object_store::ObjectStoreConfig>,
) -> Result<(opendal::Operator, ResolvedScanPaths), String> {
    let scheme = classify_scan_paths(paths.iter().map(|s| s.as_str()))?;
    match scheme {
        ScanPathScheme::Local => {
            let normalized = paths
                .iter()
                .map(|s| normalize_local_scan_path(s))
                .collect::<Result<Vec<_>, _>>()?;
            let raw = normalized.iter().map(|s| s.as_str()).collect::<Vec<_>>();
            let (root, rel_paths) = crate::fs::local::normalize_local_paths(&raw)?;
            let op = crate::fs::local::build_fs_operator(&root).map_err(|e| e.to_string())?;
            Ok((
                op,
                ResolvedScanPaths {
                    scheme,
                    root: Some(root),
                    paths: rel_paths,
                },
            ))
        }
        ScanPathScheme::Oss => {
            let cfg = object_store_cfg.ok_or_else(|| "missing object store config".to_string())?;
            let op = crate::fs::oss::build_oss_operator(cfg).map_err(|e| e.to_string())?;
            let mut normalized = Vec::with_capacity(paths.len());
            for path in paths {
                let p = crate::fs::oss::normalize_oss_path(path, &cfg.bucket, &cfg.root)?;
                normalized.push(p);
            }
            Ok((
                op,
                ResolvedScanPaths {
                    scheme,
                    root: None,
                    paths: normalized,
                },
            ))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalize_local_scan_path_keeps_plain_absolute_path() {
        let path = "/tmp/a.parquet";
        let got = normalize_local_scan_path(path).expect("normalize plain absolute path");
        assert_eq!(got, path);
    }

    #[test]
    fn normalize_local_scan_path_supports_file_uri_variants() {
        let p1 = normalize_local_scan_path("file:/tmp/a.parquet").expect("file:/ path");
        let p2 = normalize_local_scan_path("file:///tmp/a.parquet").expect("file:/// path");
        let p3 =
            normalize_local_scan_path("file://localhost/tmp/a.parquet").expect("localhost path");
        assert_eq!(p1, "/tmp/a.parquet");
        assert_eq!(p2, "/tmp/a.parquet");
        assert_eq!(p3, "/tmp/a.parquet");
    }

    #[test]
    fn normalize_local_scan_path_rejects_non_local_file_uri_host() {
        let err = normalize_local_scan_path("file://remote-host/tmp/a.parquet")
            .expect_err("non-local host should be rejected");
        assert!(err.contains("unsupported file URI host"));
    }

    #[test]
    fn classify_scan_paths_accepts_file_uri_as_local() {
        let scheme = classify_scan_paths(["file:/tmp/a.parquet"].into_iter())
            .expect("classify file URI path");
        assert_eq!(scheme, ScanPathScheme::Local);
    }
}
