// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

use serde_json::{Value, json};
use std::collections::BTreeSet;

// The finite driver cases share one original run and use distinct suffixes
// short enough for the scratch sweeper's complete resource-name grammar.
const INTEROP_CASES: &[(&str, &str, &str)] = &[
    ("NR_IRU5", "shapes", "j501"),
    ("NR_IRU5", "d12", "j502"),
    ("NR_IRU5", "cas", "j503"),
    ("NR_IRU5", "reprepare", "j504"),
    ("NR_IRU13A", "lifecycle", "a001"),
    ("NR_IRU13A", "commits", "a002"),
    ("NR_IRU13A", "ctas", "a003"),
    ("NR_IRU13A", "views", "a004"),
    ("NR_IRU13A", "recreated", "a005"),
    ("NR_IRU13B", "ctas", "b001"),
    ("NR_IRU13B", "schema", "b002"),
    ("NR_IRU13B", "replace", "b003"),
    ("NR_IRU13B", "reuse", "b004"),
    ("NR_IRU13B", "dialects", "b005"),
];

fn canonical_interop_s3(uri: &str) -> Option<String> {
    let body = uri.strip_prefix("s3://")?;
    let (bucket, path) = body.split_once('/').unwrap_or((body, ""));
    let edge = |value: u8| value.is_ascii_lowercase() || value.is_ascii_digit();
    if !(3..=63).contains(&bucket.len())
        || !edge(*bucket.as_bytes().first()?)
        || !edge(*bucket.as_bytes().last()?)
        || !bucket
            .bytes()
            .all(|value| edge(value) || matches!(value, b'.' | b'-'))
        || uri.contains(['%', '*', '?', '#'])
        || uri.chars().any(|value| value.is_control() || value <= ' ')
        || path.starts_with('/')
        || path.contains("//")
    {
        return None;
    }
    let path = path.trim_matches('/');
    if !path.is_empty() && path.split('/').any(|part| matches!(part, "" | "." | "..")) {
        return None;
    }
    Some(if path.is_empty() {
        format!("s3://{bucket}")
    } else {
        format!("s3://{bucket}/{path}")
    })
}

fn interop_descendant(root: &str, location: &str) -> bool {
    match (canonical_interop_s3(root), canonical_interop_s3(location)) {
        (Some(root), Some(location)) => location.starts_with(&format!("{root}/")),
        _ => false,
    }
}

fn interop_storage_belongs(storage: &Value, context: &Value, location: &str) -> bool {
    if storage["schema"] != 1
        || storage["identity"] != context["identity"]
        || storage["machine"]["daemon_id"] != storage["storage_owner"]["daemon_id"]
        || storage["storage_owner"]["minio_volume"]
            .as_str()
            .is_none_or(str::is_empty)
    {
        return false;
    }
    let normalized = location
        .strip_prefix("s3a://")
        .map(|body| format!("s3://{body}"));
    let Some(location) = canonical_interop_s3(normalized.as_deref().unwrap_or(location)) else {
        return false;
    };
    let Some(root) = context["identity"]["scope_root"].as_str() else {
        return false;
    };
    if storage["ranges"]["run_root"].as_str() != Some(format!("{root}/").as_str()) {
        return false;
    }
    if location == root || interop_descendant(root, &location) {
        return true;
    }
    let Some(run) = context["identity"]["run_id"].as_str() else {
        return false;
    };
    for range in ["rest", "rest_mv"] {
        let Some(warehouse) = storage["ranges"][range]["warehouse"].as_str() else {
            return false;
        };
        if storage["ranges"][range]["namespace_prefix"].as_str() != Some(format!("{run}_").as_str())
            || canonical_interop_s3(warehouse).is_none_or(|v| format!("{v}/") != warehouse)
        {
            return false;
        }
        if let Some(relative) = location.strip_prefix(warehouse) {
            let namespace = relative.split('/').next().unwrap_or_default();
            if let Some(purpose) = namespace.strip_prefix(&format!("{run}_"))
                && namespace.len() <= 63
                && !purpose.is_empty()
                && purpose.as_bytes()[0].is_ascii_lowercase()
                && purpose
                    .bytes()
                    .all(|v| v.is_ascii_lowercase() || v.is_ascii_digit())
            {
                return true;
            }
        }
    }
    false
}

fn scratch_namespace(context: &Value, driver: &str, case: &str) -> String {
    let identity = &context["identity"];
    let run = identity["run_id"]
        .as_str()
        .expect("original scratch run ID");
    let pieces: Vec<_> = run.split('_').collect();
    assert!(
        pieces.len() == 4
            && pieces[0] == "nr"
            && pieces[1] == "scratch"
            && pieces[2].len() == 13
            && pieces[2].bytes().all(|v| v.is_ascii_digit())
            && pieces[3].len() == 32
            && pieces[3]
                .bytes()
                .all(|v| v.is_ascii_digit() || (b'a'..=b'f').contains(&v)),
        "original scratch run ID must retain the complete frozen grammar"
    );
    assert_eq!(
        identity["created_unix_ms"].as_u64(),
        pieces[2].parse::<u64>().ok()
    );
    let root = identity["scope_root"]
        .as_str()
        .expect("original scratch root");
    assert!(
        canonical_interop_s3(root).as_deref() == Some(root)
            && root.ends_with(&format!("/scratch/{run}"))
    );
    assert_eq!(context["schema"].as_u64(), Some(1));
    let suffix = INTEROP_CASES
        .iter()
        .find(|(d, c, _)| *d == driver && *c == case)
        .expect("shared interop cases require a unique frozen ordinal")
        .2;
    let namespace = format!("{run}_{suffix}");
    assert!(namespace.len() <= 63);
    namespace
}

#[test]
fn shared_interop_case_ordinals_preserve_complete_scratch_identity() {
    let run = "nr_scratch_1792530000000_0123456789abcdef0123456789abcdef";
    let context = json!({"schema":1,"identity":{"run_id":run,"created_unix_ms":1792530000000u64,
        "scope_root":format!("s3://novarocks/worktree/scratch/{run}")}});
    let mut names = BTreeSet::new();
    for (driver, case, suffix) in INTEROP_CASES {
        assert!(suffix.len() <= 5);
        let name = scratch_namespace(&context, driver, case);
        assert!(name.starts_with(&format!("{run}_")) && name.len() <= 63);
        assert!(
            names.insert(name),
            "case ordinals must be globally unique per run"
        );
    }
    let mut custom = context.clone();
    custom["identity"]["scope_root"] =
        json!(format!("s3://custom-bucket/another/prefix/scratch/{run}"));
    assert_eq!(
        scratch_namespace(&custom, "NR_IRU5", "shapes"),
        format!("{run}_j501")
    );
    let base = format!("s3://custom-bucket/another/prefix/scratch/{run}");
    assert!(interop_descendant(&base, &format!("{base}/rest/ns/table")));
    for location in [
        format!("{base}-sibling/t"),
        format!("{base}/../escaped"),
        format!("{base}/%2e%2e/escaped"),
        format!("{base}//table"),
        format!("s3://custom-bucket//another/prefix/scratch/{run}/table"),
        format!("{base}/table?query"),
        format!("{base}/table#fragment"),
        format!("{base}/table{}invalid", char::from(10)),
    ] {
        assert!(
            !interop_descendant(&base, &location),
            "noncanonical or sibling locations are not descendants"
        );
    }
    custom["identity"]["scope_root"] = json!(format!("s3://custom-bucket/../scratch/{run}"));
    assert!(std::panic::catch_unwind(|| scratch_namespace(&custom, "NR_IRU5", "shapes")).is_err());
    let mut malformed = context.clone();
    malformed["identity"]["run_id"] = json!(format!("{run}_truncated"));
    assert!(
        std::panic::catch_unwind(|| scratch_namespace(&malformed, "NR_IRU5", "shapes")).is_err()
    );
}

#[test]
fn shared_interop_storage_membership_matches_fixture_scope_vectors() {
    let vectors: Value = serde_json::from_str(include_str!(
        "../../../../../../docker/iceberg-rest/tests/run_storage_scope_vectors.json"
    ))
    .unwrap();
    let context = json!({"identity":vectors["identity"]});
    for case in vectors["membership"].as_array().unwrap() {
        assert_eq!(
            interop_storage_belongs(
                &vectors["projection"],
                &context,
                case["uri"].as_str().unwrap()
            ),
            case["belongs"].as_bool().unwrap_or(false),
            "{}",
            case["uri"]
        );
    }
}
