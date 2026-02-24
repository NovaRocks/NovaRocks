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
use anyhow::{Context, Result};
use opendal::Operator;
use std::path::{Path, PathBuf};

pub fn build_fs_operator(root: &str) -> Result<Operator> {
    let builder = opendal::services::Fs::default().root(root);
    let op = Operator::new(builder)
        .context("init opendal fs operator")?
        .finish();
    Ok(op)
}

pub fn normalize_local_paths(paths: &[&str]) -> Result<(String, Vec<String>), String> {
    if paths.is_empty() {
        return Err("scan paths are empty".to_string());
    }

    let mut absolute: Option<bool> = None;
    let mut dirs = Vec::with_capacity(paths.len());
    for raw in paths {
        let path = Path::new(raw.trim());
        let is_abs = path.is_absolute();
        if let Some(prev) = absolute {
            if prev != is_abs {
                return Err("mixed absolute and relative local paths are not allowed".to_string());
            }
        } else {
            absolute = Some(is_abs);
        }

        let dir = path.parent().unwrap_or(Path::new(""));
        let dir = if dir.as_os_str().is_empty() {
            PathBuf::from(".")
        } else {
            dir.to_path_buf()
        };
        dirs.push(dir);
    }

    let root = common_path_prefix(&dirs)
        .ok_or_else(|| "failed to compute common local root".to_string())?;
    let root_str = if root.as_os_str().is_empty() {
        ".".to_string()
    } else {
        root.to_string_lossy().to_string()
    };

    let mut out = Vec::with_capacity(paths.len());
    for raw in paths {
        let path = Path::new(raw.trim());
        let rel_path = if root == Path::new(".") {
            path.to_path_buf()
        } else {
            path.strip_prefix(&root)
                .map_err(|_| format!("local path {raw} does not start with root {root_str}"))?
                .to_path_buf()
        };
        let rel = rel_path.to_string_lossy().to_string();
        if rel.is_empty() {
            return Err(format!("invalid local path after stripping root: {raw}"));
        }
        out.push(rel);
    }

    Ok((root_str, out))
}

fn common_path_prefix(paths: &[PathBuf]) -> Option<PathBuf> {
    let mut iter = paths.iter();
    let first = iter.next()?.components().collect::<Vec<_>>();
    let mut prefix_len = first.len();
    for path in iter {
        let comps = path.components().collect::<Vec<_>>();
        prefix_len = prefix_len.min(comps.len());
        for i in 0..prefix_len {
            if comps[i] != first[i] {
                prefix_len = i;
                break;
            }
        }
    }
    if prefix_len == 0 {
        return None;
    }
    let mut out = PathBuf::new();
    for comp in &first[..prefix_len] {
        out.push(comp.as_os_str());
    }
    Some(out)
}
