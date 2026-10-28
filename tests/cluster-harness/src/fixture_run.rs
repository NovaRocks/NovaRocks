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

//! Run identity and cumulative deadlines shared by fixture consumers.
//! These values own no process and grant no service administration authority.
use anyhow::{Context, Result, ensure};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

pub const RUN_CONTEXT_ENV: &str = "NOVA_FIXTURE_RUN_CONTEXT";
pub const RUN_STORAGE_ENV: &str = "NOVA_FIXTURE_RUN_STORAGE";

#[derive(Debug)]
pub struct FixtureBudgetRejected(anyhow::Error);
impl std::fmt::Display for FixtureBudgetRejected {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "BLOCKED: {}", self.0)
    }
}
impl std::error::Error for FixtureBudgetRejected {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.0.as_ref())
    }
}
pub fn rejected(error: anyhow::Error) -> anyhow::Error {
    if error.is::<FixtureBudgetExpired>() || error.is::<FixtureBudgetRejected>() {
        error
    } else {
        FixtureBudgetRejected(error).into()
    }
}

#[derive(Debug)]
pub struct FixtureBudgetExpired;
impl std::fmt::Display for FixtureBudgetExpired {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("FixtureBudgetExpired: original fixture work deadline expired")
    }
}
impl std::error::Error for FixtureBudgetExpired {}

#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
pub struct FixturePolicy {
    pub scratch_ttl_seconds: u64,
    pub max_run_seconds: u64,
    pub cleanup_grace_seconds: u64,
}

impl Default for FixturePolicy {
    fn default() -> Self {
        Self {
            scratch_ttl_seconds: 604800,
            max_run_seconds: 86400,
            cleanup_grace_seconds: 300,
        }
    }
}

impl FixturePolicy {
    pub fn validate(self) -> Result<Self> {
        ensure!(
            self.cleanup_grace_seconds > 0 && self.max_run_seconds > self.cleanup_grace_seconds,
            "FixtureBudgetRejected: invalid cleanup reserve or maximum run budget"
        );
        let minimum = self
            .max_run_seconds
            .checked_add(self.cleanup_grace_seconds)
            .and_then(|n| n.checked_mul(2))
            .context("FixtureBudgetRejected: lifetime policy overflow")?;
        ensure!(
            self.scratch_ttl_seconds > minimum,
            "FixtureBudgetRejected: scratch TTL is too short"
        );
        Ok(self)
    }

    pub fn from_environment() -> Result<Self> {
        let names = [
            "NOVA_FIXTURE_SCRATCH_TTL_SECONDS",
            "NOVA_FIXTURE_MAX_RUN_SECONDS",
            "NOVA_FIXTURE_CLEANUP_GRACE_SECONDS",
        ];
        let values = names.map(|name| std::env::var(name).ok());
        if values.iter().all(Option::is_none) {
            ensure!(
                std::env::var_os("NOVA_FIXTURE_MACHINE_ID").is_none(),
                "FixtureBudgetRejected: shared machine omitted its lifetime policy"
            );
            return Self::default().validate();
        }
        let read = |index: usize| -> Result<u64> {
            values[index]
                .as_ref()
                .with_context(|| format!("FixtureBudgetRejected: missing {}", names[index]))?
                .parse()
                .with_context(|| format!("FixtureBudgetRejected: invalid {}", names[index]))
        };
        Self {
            scratch_ttl_seconds: read(0)?,
            max_run_seconds: read(1)?,
            cleanup_grace_seconds: read(2)?,
        }
        .validate()
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ScratchIdentity {
    pub schema: u8,
    pub run_id: String,
    pub created_unix_ms: u64,
    pub scope_root: String,
}

pub fn canonical_s3(value: &str) -> Result<String> {
    ensure!(
        !value.strip_prefix("s3://").unwrap_or(value).contains("//"),
        "FixtureBudgetRejected: invalid empty S3 segment"
    );
    let value = value.trim_end_matches('/');
    let rest = value
        .strip_prefix("s3://")
        .context("FixtureBudgetRejected: scratch root requires s3 URI")?;
    let (bucket, key) = rest.split_once('/').unwrap_or((rest, ""));
    ensure!(
        (3..=63).contains(&bucket.len())
            && bucket
                .bytes()
                .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || b".-".contains(&c))
            && bucket
                .as_bytes()
                .first()
                .is_some_and(u8::is_ascii_alphanumeric)
            && bucket
                .as_bytes()
                .last()
                .is_some_and(u8::is_ascii_alphanumeric)
            && !value.bytes().any(|c| c <= 32 || b"%*?#".contains(&c))
            && (key.is_empty()
                || key
                    .split('/')
                    .all(|p| !p.is_empty() && p != "." && p != "..")),
        "FixtureBudgetRejected: invalid scratch S3 scope"
    );
    Ok(value.to_owned())
}

impl ScratchIdentity {
    pub fn create(worktree_root: &str) -> Result<Self> {
        let now = unix_ms()?;
        let key = rcgen::KeyPair::generate().context("generate fixture run identity")?;
        let token = format!("{:x}", Sha256::digest(key.serialize_der()));
        Self::create_at(worktree_root, now, &token[..32])
    }

    pub fn create_at(worktree_root: &str, created_unix_ms: u64, token: &str) -> Result<Self> {
        ensure!(
            (1_000_000_000_000..10_000_000_000_000).contains(&created_unix_ms)
                && token.len() == 32
                && token
                    .bytes()
                    .all(|c| c.is_ascii_digit() || (b'a'..=b'f').contains(&c)),
            "FixtureBudgetRejected: invalid scratch creation time or token"
        );
        let run_id = format!("nr_scratch_{created_unix_ms:013}_{token}");
        Ok(Self {
            schema: 1,
            scope_root: format!("{}/scratch/{run_id}", canonical_s3(worktree_root)?),
            run_id,
            created_unix_ms,
        })
    }

    pub fn validate(&self) -> Result<()> {
        ensure!(
            self.schema == 1,
            "FixtureBudgetRejected: invalid scratch schema"
        );
        let prefix = format!("nr_scratch_{:013}_", self.created_unix_ms);
        let token = self
            .run_id
            .strip_prefix(&prefix)
            .context("FixtureBudgetRejected: scratch timestamp contradicts name")?;
        let root = canonical_s3(&self.scope_root)?;
        let suffix = format!("/scratch/{}", self.run_id);
        let worktree = root
            .strip_suffix(&suffix)
            .context("FixtureBudgetRejected: scratch root contradicts run identity")?;
        ensure!(
            *self == Self::create_at(worktree, self.created_unix_ms, token)?,
            "FixtureBudgetRejected: scratch identity differs"
        );
        Ok(())
    }

    pub fn resource_name(&self, purpose: &str) -> Result<String> {
        self.validate()?;
        validate_purpose(purpose)?;
        let result = format!("{}_{}", self.run_id, purpose);
        ensure!(
            result.len() <= 63,
            "FixtureBudgetRejected: scratch resource name exceeds 63 characters"
        );
        Ok(result)
    }

    pub fn iam_name(&self, purpose: &str) -> Result<String> {
        self.validate()?;
        validate_purpose(purpose)?;
        let seconds = u32::try_from(self.created_unix_ms / 1000)
            .context("FixtureBudgetRejected: IAM timestamp is out of range")?;
        let suffix = format!("{:x}", Sha256::digest(format!("{}:{purpose}", self.run_id)));
        Ok(format!("nr{seconds:08x}{}", &suffix[..10]))
    }
}

fn validate_purpose(value: &str) -> Result<()> {
    ensure!(
        !value.is_empty()
            && value.len() <= 12
            && value.as_bytes()[0].is_ascii_lowercase()
            && value
                .bytes()
                .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit()),
        "FixtureBudgetRejected: invalid scratch purpose"
    );
    Ok(())
}

/// Fixed run storage facts. An inherited projection must be checked against the
/// original machine publication before granting access or deleting objects.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RunStorageScope {
    schema: u8,
    identity: ScratchIdentity,
    machine: StorageMachine,
    storage_owner: StorageOwner,
    ranges: StorageRanges,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct StorageMachine {
    id: String,
    control_root: String,
    daemon_id: String,
    version: String,
    generation: String,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
enum StorageOwner {
    Minio(MinioStorageOwner),
    Oss(OssStorageOwner),
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct MinioStorageOwner {
    daemon_id: String,
    minio_volume: String,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct OssStorageOwner {
    provider: String,
    endpoint: String,
    region: String,
    bucket: String,
    root_prefix: String,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct OssMachineConfig {
    kind: String,
    region: String,
    bucket: String,
    role_name: String,
    root_prefix: String,
}
impl OssStorageOwner {
    fn root(&self) -> Result<String> {
        ensure!(
            self.provider == "aliyun-oss"
                && !self.region.is_empty()
                && self
                    .region
                    .bytes()
                    .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
                && self.endpoint == format!("https://s3.oss-{}-internal.aliyuncs.com", self.region)
                && !self.root_prefix.is_empty()
                && self.root_prefix.ends_with('/'),
            "FixtureBudgetRejected: invalid OSS storage owner"
        );
        let root = canonical_s3(&format!("s3://{}/{}", self.bucket, self.root_prefix))?;
        ensure!(
            format!("{root}/") == format!("s3://{}/{}", self.bucket, self.root_prefix),
            "FixtureBudgetRejected: invalid OSS storage root"
        );
        Ok(root)
    }
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct StorageRanges {
    run_root: String,
    rest: NamespaceRange,
    rest_mv: NamespaceRange,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct NamespaceRange {
    warehouse: String,
    namespace_prefix: String,
}

fn contains_s3(root: &str, value: &str) -> bool {
    root == value
        || value
            .strip_prefix(root)
            .is_some_and(|suffix| suffix.starts_with('/'))
}

impl RunStorageScope {
    pub fn from_record(identity: &ScratchIdentity, record: &serde_json::Value) -> Result<Self> {
        let field = |value: &serde_json::Value, key: &str| -> Result<String> {
            value[key]
                .as_str()
                .filter(|s| !s.is_empty())
                .map(str::to_owned)
                .with_context(|| format!("FixtureBudgetRejected: machine omitted storage {key}"))
        };
        let range = |service: &str| -> Result<NamespaceRange> {
            Ok(NamespaceRange {
                warehouse: format!(
                    "{}/",
                    canonical_s3(&field(&record["server_warehouses"], service)?)?
                ),
                namespace_prefix: format!("{}_", identity.run_id),
            })
        };
        let storage_owner = match record["config"].get("object_store") {
            Some(value)
                if value.get("kind").and_then(serde_json::Value::as_str) == Some("aliyun-oss") =>
            {
                let config: OssMachineConfig = serde_json::from_value(value.clone())
                    .context("FixtureBudgetRejected: invalid machine OSS configuration")?;
                ensure!(
                    config.kind == "aliyun-oss" && !config.role_name.is_empty(),
                    "FixtureBudgetRejected: invalid machine OSS provider or role"
                );
                StorageOwner::Oss(OssStorageOwner {
                    provider: config.kind,
                    endpoint: format!("https://s3.oss-{}-internal.aliyuncs.com", config.region),
                    region: config.region,
                    bucket: config.bucket,
                    root_prefix: config.root_prefix,
                })
            }
            None => StorageOwner::Minio(MinioStorageOwner {
                daemon_id: field(record, "daemon_id")?,
                minio_volume: field(&record["config"], "minio_volume")?,
            }),
            Some(_) => anyhow::bail!("FixtureBudgetRejected: unsupported machine storage provider"),
        };
        let result = Self {
            schema: if matches!(storage_owner, StorageOwner::Oss(_)) {
                2
            } else {
                1
            },
            identity: identity.clone(),
            machine: StorageMachine {
                id: field(record, "id")?,
                control_root: field(record, "control_root")?,
                daemon_id: field(record, "daemon_id")?,
                version: field(record, "version")?,
                generation: field(record, "generation")?,
            },
            storage_owner,
            ranges: StorageRanges {
                run_root: format!("{}/", identity.scope_root),
                rest: range("rest")?,
                rest_mv: range("rest-mv")?,
            },
        };
        result.validate()?;
        let mut protected: Vec<String> =
            serde_json::from_value(record["protected_benchmark_roots"].clone())
                .context("FixtureBudgetRejected: machine omitted protected benchmark roots")?;
        protected.push(field(&record["config"], "benchmark_root")?);
        for root in result.roots() {
            let root = canonical_s3(root)?;
            for other in &protected {
                let other = canonical_s3(other)?;
                ensure!(
                    !contains_s3(&root, &other) && !contains_s3(&other, &root),
                    "FixtureBudgetRejected: run storage overlaps protected data"
                );
            }
        }
        Ok(result)
    }

    pub fn from_json(value: &str) -> Result<Self> {
        let result: Self = serde_json::from_str(value)
            .context("FixtureBudgetRejected: invalid inherited run storage")?;
        result.validate()?;
        Ok(result)
    }

    pub fn from_environment(
        identity: &ScratchIdentity,
        record: &serde_json::Value,
    ) -> Result<Self> {
        let expected = Self::from_record(identity, record)?;
        if let Some(value) = std::env::var_os(RUN_STORAGE_ENV) {
            let value = value
                .to_str()
                .context("FixtureBudgetRejected: non-UTF8 inherited run storage")?;
            let inherited = Self::from_json(value)?;
            inherited.validate_against(identity, record)?;
            return Ok(inherited);
        }
        Ok(expected)
    }

    pub fn from_publication(env_file: &Path, budget: &RunBudget) -> Result<Self> {
        let manifest: serde_json::Value = serde_json::from_slice(&std::fs::read(
            env_file
                .parent()
                .context("fixture publication directory")?
                .join("manifest.json"),
        )?)?;
        ensure!(
            manifest["ready"] == true
                && manifest["shared_docker"] == true
                && manifest["runtime"]["machine"]["phase"] == "ready",
            "FixtureBudgetRejected: storage requires a ready shared machine publication"
        );
        let record = &manifest["runtime"]["machine"];
        budget.validate_machine(record)?;
        Self::from_environment(budget.identity(), record)
    }

    fn roots(&self) -> [&str; 3] {
        [
            &self.ranges.run_root,
            &self.ranges.rest.warehouse,
            &self.ranges.rest_mv.warehouse,
        ]
    }

    fn validate(&self) -> Result<()> {
        self.identity.validate()?;
        ensure!(
            self.ranges.run_root == format!("{}/", self.identity.scope_root)
                && [
                    &self.machine.id,
                    &self.machine.control_root,
                    &self.machine.daemon_id,
                    &self.machine.version,
                    &self.machine.generation
                ]
                .iter()
                .all(|v| !v.is_empty()),
            "FixtureBudgetRejected: invalid run storage owner or ranges"
        );
        match &self.storage_owner {
            StorageOwner::Minio(owner) => ensure!(
                self.schema == 1
                    && owner.daemon_id == self.machine.daemon_id
                    && !owner.minio_volume.is_empty(),
                "FixtureBudgetRejected: invalid MinIO storage owner or schema"
            ),
            StorageOwner::Oss(owner) => {
                ensure!(
                    self.schema == 2,
                    "FixtureBudgetRejected: invalid OSS storage schema"
                );
                let owner_root = owner.root()?;
                for root in self.roots() {
                    ensure!(
                        contains_s3(&owner_root, &canonical_s3(root)?),
                        "FixtureBudgetRejected: run storage leaves its OSS owner root"
                    );
                }
            }
        }
        for range in [&self.ranges.rest, &self.ranges.rest_mv] {
            ensure!(
                range.namespace_prefix == format!("{}_", self.identity.run_id)
                    && range.warehouse == format!("{}/", canonical_s3(&range.warehouse)?),
                "FixtureBudgetRejected: invalid warehouse namespace range"
            );
        }
        let roots = self.roots().map(canonical_s3);
        for (index, root) in roots.iter().enumerate() {
            let root = root.as_ref().map_err(|e| anyhow::anyhow!("{e:#}"))?;
            for other in &roots[index + 1..] {
                let other = other.as_ref().map_err(|e| anyhow::anyhow!("{e:#}"))?;
                ensure!(
                    !contains_s3(root, other) && !contains_s3(other, root),
                    "FixtureBudgetRejected: independent run storage roots overlap"
                );
            }
        }
        Ok(())
    }

    pub fn validate_against(
        &self,
        identity: &ScratchIdentity,
        record: &serde_json::Value,
    ) -> Result<()> {
        ensure!(
            *self == Self::from_record(identity, record)?,
            "FixtureBudgetRejected: inherited storage projection differs from the machine"
        );
        Ok(())
    }

    pub fn identity(&self) -> &ScratchIdentity {
        &self.identity
    }
    pub fn to_json(&self) -> Result<String> {
        Ok(serde_json::to_string(self)?)
    }

    pub fn owns_namespace(&self, namespace: &str) -> bool {
        namespace
            .strip_prefix(&format!("{}_", self.identity.run_id))
            .is_some_and(|purpose| namespace.len() <= 63 && validate_purpose(purpose).is_ok())
    }

    pub fn belongs(&self, uri: &str) -> Result<bool> {
        self.validate()?;
        // Scheme aliases are comparisons only and require the exact common
        // storage owner captured in this validated projection.
        let alias;
        let uri = if let Some(rest) = uri.strip_prefix("s3a://") {
            alias = format!("s3://{rest}");
            &alias
        } else {
            uri
        };
        let uri = canonical_s3(uri)?;
        if contains_s3(&self.identity.scope_root, &uri) {
            return Ok(true);
        }
        for range in [&self.ranges.rest, &self.ranges.rest_mv] {
            if let Some(child) = uri.strip_prefix(&range.warehouse)
                && self.owns_namespace(child.split('/').next().unwrap_or(""))
            {
                return Ok(true);
            }
        }
        Ok(false)
    }

    pub fn iam_policy(&self) -> Result<serde_json::Value> {
        self.validate()?;
        ensure!(
            matches!(self.storage_owner, StorageOwner::Minio(_)),
            "FixtureBudgetRejected: MinIO IAM policies are unavailable for OSS node-local storage"
        );
        let prefixes = [
            self.ranges.run_root.clone(),
            format!(
                "{}{}",
                self.ranges.rest.warehouse, self.ranges.rest.namespace_prefix
            ),
            format!(
                "{}{}",
                self.ranges.rest_mv.warehouse, self.ranges.rest_mv.namespace_prefix
            ),
        ];
        let mut buckets = std::collections::BTreeMap::<String, Vec<String>>::new();
        let mut objects = Vec::new();
        for uri in prefixes {
            let (bucket, key) = uri
                .strip_prefix("s3://")
                .unwrap()
                .split_once('/')
                .context("S3 policy requires a key prefix")?;
            buckets
                .entry(bucket.to_owned())
                .or_default()
                .push(format!("{key}*"));
            objects.push(format!("arn:aws:s3:::{bucket}/{key}*"));
        }
        let mut statements = Vec::new();
        for (bucket, values) in buckets {
            statements.push(serde_json::json!({"Effect":"Allow","Action":["s3:GetBucketLocation"],"Resource":[format!("arn:aws:s3:::{bucket}")]}));
            statements.push(serde_json::json!({"Effect":"Allow","Action":["s3:ListBucket"],"Resource":[format!("arn:aws:s3:::{bucket}")],"Condition":{"StringLike":{"s3:prefix":values}}}));
        }
        statements.push(serde_json::json!({"Effect":"Allow","Action":["s3:GetObject","s3:GetObjectVersion","s3:PutObject","s3:DeleteObject","s3:DeleteObjectVersion","s3:AbortMultipartUpload","s3:ListMultipartUploadParts"],"Resource":objects}));
        Ok(serde_json::json!({"Version":"2012-10-17","Statement":statements}))
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FixtureRunContext {
    pub schema: u8,
    pub identity: ScratchIdentity,
    pub started_unix_ms: u64,
    pub deadline_unix_ms: u64,
    pub cleanup_grace_seconds: u64,
}

#[derive(Clone, Debug)]
pub struct RunBudget {
    context: FixtureRunContext,
    deadline: Instant,
    work_limit: Option<Instant>,
    startup_sweep: Arc<Mutex<Option<(PathBuf, serde_json::Value)>>>,
}

impl RunBudget {
    pub fn admit(
        policy: FixturePolicy,
        identity: ScratchIdentity,
        seconds: u64,
        inherited: Option<FixtureRunContext>,
    ) -> Result<Self> {
        Self::admit_at(
            policy,
            identity,
            seconds,
            inherited,
            unix_ms()?,
            Instant::now(),
        )
        .map_err(rejected)
    }

    pub fn admit_at(
        policy: FixturePolicy,
        identity: ScratchIdentity,
        seconds: u64,
        inherited: Option<FixtureRunContext>,
        now_unix_ms: u64,
        now: Instant,
    ) -> Result<Self> {
        let policy = policy.validate()?;
        identity.validate()?;
        ensure!(
            inherited.is_some()
                || (seconds > policy.cleanup_grace_seconds && seconds <= policy.max_run_seconds),
            "FixtureBudgetRejected: invalid or excessive complete fixture budget"
        );
        ensure!(
            identity.created_unix_ms <= now_unix_ms.saturating_add(5000),
            "FixtureBudgetRejected: scratch creation time is in the future"
        );
        let effective_seconds = if inherited.is_some() { 0 } else { seconds };
        let end = now_unix_ms
            .checked_add(
                effective_seconds
                    .checked_mul(1000)
                    .context("FixtureBudgetRejected: deadline overflow")?,
            )
            .context("FixtureBudgetRejected: deadline overflow")?;
        let context = inherited.unwrap_or(FixtureRunContext {
            schema: 1,
            identity: identity.clone(),
            started_unix_ms: now_unix_ms,
            deadline_unix_ms: end,
            cleanup_grace_seconds: policy.cleanup_grace_seconds,
        });
        ensure!(
            context.schema == 1
                && context.identity == identity
                && context.cleanup_grace_seconds == policy.cleanup_grace_seconds
                && context.started_unix_ms <= now_unix_ms.saturating_add(5000),
            "FixtureBudgetRejected: inherited run context mismatch"
        );
        let duration = context
            .deadline_unix_ms
            .checked_sub(context.started_unix_ms)
            .context("FixtureBudgetRejected: invalid inherited deadline")?;
        ensure!(
            duration > policy.cleanup_grace_seconds.saturating_mul(1000)
                && duration <= policy.max_run_seconds.saturating_mul(1000)
                && context.deadline_unix_ms
                    <= identity
                        .created_unix_ms
                        .saturating_add(policy.max_run_seconds.saturating_mul(1000))
                        .saturating_add(5000),
            "FixtureBudgetRejected: invalid inherited complete deadline"
        );
        if context.deadline_unix_ms
            <= now_unix_ms.saturating_add(policy.cleanup_grace_seconds.saturating_mul(1000))
        {
            return Err(FixtureBudgetExpired.into());
        }
        let deadline = now
            .checked_add(Duration::from_millis(
                context.deadline_unix_ms - now_unix_ms,
            ))
            .context("FixtureBudgetRejected: monotonic deadline overflow")?;
        Ok(Self {
            context,
            deadline,
            work_limit: None,
            startup_sweep: Arc::new(Mutex::new(None)),
        })
    }

    pub fn from_environment(seconds: u64) -> Result<Self> {
        (|| -> Result<Self> {
            let inherited = std::env::var(RUN_CONTEXT_ENV)
                .ok()
                .map(|value| serde_json::from_str::<FixtureRunContext>(&value))
                .transpose()
                .context("FixtureBudgetRejected: invalid inherited fixture run context")?;
            let identity = match &inherited {
                Some(context) => context.identity.clone(),
                None => ScratchIdentity::create(
                    &std::env::var("CATALOG_WAREHOUSE_URI")
                        .unwrap_or_else(|_| "s3://novarocks/system-tests".to_string()),
                )?,
            };
            Self::admit(
                FixturePolicy::from_environment()?,
                identity,
                seconds,
                inherited,
            )
        })()
        .map_err(rejected)
    }

    /// Validate the original context against the actual publication before any
    /// shared fixture side effect. This never renews the stored monotonic clock.
    pub fn validate_machine(&self, snapshot: &serde_json::Value) -> Result<()> {
        (|| -> Result<()> {
            let policy: FixturePolicy = serde_json::from_value(snapshot["policy"].clone())
                .context("machine snapshot omitted its lifetime policy")?;
            Self::admit(
                policy,
                self.identity().clone(),
                0,
                Some(self.context.clone()),
            )?;
            let mut protected: Vec<String> =
                serde_json::from_value(snapshot["protected_benchmark_roots"].clone())
                    .context("machine snapshot omitted protected benchmark roots")?;
            protected.push(
                snapshot["config"]["benchmark_root"]
                    .as_str()
                    .context("machine snapshot omitted benchmark root")?
                    .to_string(),
            );
            self.validate_protected_roots(&protected)
        })()
        .map_err(rejected)
    }

    pub fn validate_protected_roots(&self, roots: &[String]) -> Result<()> {
        let scope = canonical_s3(&self.identity().scope_root)?;
        for root in roots {
            let root = canonical_s3(root)?;
            ensure!(
                scope != root
                    && !scope.starts_with(&format!("{root}/"))
                    && !root.starts_with(&format!("{scope}/")),
                "FixtureBudgetRejected: scratch overlaps a protected benchmark root"
            );
        }
        Ok(())
    }

    pub fn context(&self) -> &FixtureRunContext {
        &self.context
    }
    pub fn identity(&self) -> &ScratchIdentity {
        &self.context.identity
    }
    pub fn deadline(&self) -> Instant {
        self.deadline
    }
    pub fn work_deadline(&self) -> Instant {
        let original = self.deadline - Duration::from_secs(self.context.cleanup_grace_seconds);
        self.work_limit
            .map_or(original, |limit| original.min(limit))
    }
    /// Narrow an existing local work clock without changing the inherited wire
    /// context or extending its complete cleanup deadline.
    pub fn with_work_deadline_limit(mut self, deadline: Instant) -> Self {
        self.work_limit = Some(self.work_deadline().min(deadline));
        self
    }
    pub fn remaining(&self) -> Duration {
        self.deadline.saturating_duration_since(Instant::now())
    }
    pub fn work_remaining(&self) -> Duration {
        self.work_deadline()
            .saturating_duration_since(Instant::now())
    }
    pub fn check_work(&self) -> Result<()> {
        if self.work_remaining().is_zero() {
            return Err(FixtureBudgetExpired.into());
        }
        Ok(())
    }
    pub fn to_json(&self) -> Result<String> {
        Ok(serde_json::to_string(&self.context)?)
    }
}

/// Sweep once for this outer run and its clones, after admission and before
/// shared writes. Python owns the five-second enumeration cap and only reports
/// a partial timeout after its native cleanup has actually settled.
pub fn startup_sweep(
    repo_root: &Path,
    pinned_env_file: &Path,
    budget: &RunBudget,
) -> Result<serde_json::Value> {
    budget.check_work()?;
    let env_file = pinned_env_file
        .canonicalize()
        .context("resolve startup sweep immutable publication")?;
    let storage = RunStorageScope::from_publication(&env_file, budget)?;
    let mut completed = budget
        .startup_sweep
        .lock()
        .map_err(|_| anyhow::anyhow!("startup sweep result lock poisoned"))?;
    budget.check_work()?;
    if let Some((original, report)) = completed.as_ref() {
        ensure!(
            original == &env_file,
            "startup sweep publication changed within one run"
        );
        return Ok(report.clone());
    }
    let repo_root = repo_root
        .canonicalize()
        .context("resolve startup sweep repository")?;
    let mut command = novarocks_test_support::ManagedCommand::new("python3");
    command
        .env_clear()
        .envs(crate::isolated_iceberg_rest::controlled_environment_values(
            std::env::vars_os(),
        ))
        .current_dir(&repo_root)
        .arg(repo_root.join("docker/iceberg-rest/fixture_run.py"))
        .arg("sweep-start")
        .arg("--env-file")
        .arg(&env_file)
        .env(RUN_CONTEXT_ENV, budget.to_json()?)
        .env(RUN_STORAGE_ENV, storage.to_json()?)
        .terminate_at(budget.work_deadline(), budget.deadline());
    let outcome = novarocks_test_support::capture_command_until(
        command,
        tempfile::tempfile()?,
        tempfile::tempfile()?,
        budget.work_deadline(),
    )?;
    ensure!(
        !outcome.timed_out && outcome.failures.is_empty() && outcome.output.status.success(),
        "fixture startup sweep failed with {}; stderr={}; diagnostics retained at {}",
        outcome.output.status,
        String::from_utf8_lossy(&outcome.output.stderr),
        outcome.diagnostic_root().display()
    );
    let report: serde_json::Value =
        serde_json::from_slice(&outcome.output.stdout).with_context(|| {
            format!(
                "invalid fixture startup sweep report; diagnostics retained at {}",
                outcome.diagnostic_root().display()
            )
        })?;
    ensure!(
        report["complete"].is_boolean(),
        "startup sweep omitted completion fact; diagnostics retained at {}",
        outcome.diagnostic_root().display()
    );
    budget.check_work()?;
    eprintln!(
        "fixture startup sweep: {}",
        String::from_utf8_lossy(&outcome.output.stdout).trim()
    );
    outcome.release_diagnostics()?;
    *completed = Some((env_file, report.clone()));
    Ok(report)
}

fn unix_ms() -> Result<u64> {
    u64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .context("read fixture run wall clock")?
            .as_millis(),
    )
    .context("fixture time exceeds u64")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sweep_fixture(script: &str) -> (tempfile::TempDir, PathBuf, RunBudget) {
        let directory = tempfile::tempdir().unwrap();
        let scripts = directory.path().join("docker/iceberg-rest");
        std::fs::create_dir_all(&scripts).unwrap();
        std::fs::write(scripts.join("fixture_run.py"), script).unwrap();
        let publication = directory.path().join("env.sh");
        std::fs::write(&publication, "# immutable publication\n").unwrap();
        let vectors = storage_vectors();
        let mut record = vectors["record"].clone();
        record["phase"] = serde_json::json!("ready");
        std::fs::write(directory.path().join("manifest.json"), serde_json::to_vec(&serde_json::json!({"ready":true,"shared_docker":true,"runtime":{"machine":record}})).unwrap()).unwrap();
        let budget = RunBudget::admit(
            FixturePolicy::default(),
            ScratchIdentity::create("s3://novarocks/worktree").unwrap(),
            3600,
            None,
        )
        .unwrap();
        (directory, publication, budget)
    }

    #[test]
    fn startup_sweep_runs_once_across_clones_with_original_context_and_pinned_publication() {
        let (directory, publication, budget) = sweep_fixture(
            "import json,os,pathlib,sys\np=pathlib.Path('calls.json')\np.write_text(json.dumps({'args':sys.argv[1:],'context':json.loads(os.environ['NOVA_FIXTURE_RUN_CONTEXT']),'storage':json.loads(os.environ['NOVA_FIXTURE_RUN_STORAGE'])}))\nprint(json.dumps({'complete':False,'errors':[{'code':'ScratchSweepTimeout'}],'enumeration_complete':False}))\n",
        );
        let report = startup_sweep(directory.path(), &publication, &budget).unwrap();
        assert_eq!(report["complete"], false);
        let recorded: serde_json::Value =
            serde_json::from_slice(&std::fs::read(directory.path().join("calls.json")).unwrap())
                .unwrap();
        assert_eq!(
            recorded["context"],
            serde_json::to_value(budget.context()).unwrap()
        );
        let scope = RunStorageScope::from_publication(&publication, &budget).unwrap();
        assert_eq!(recorded["storage"], serde_json::to_value(scope).unwrap());
        assert_eq!(
            recorded["args"],
            serde_json::json!([
                "sweep-start",
                "--env-file",
                publication.canonicalize().unwrap()
            ])
        );
        std::fs::remove_file(directory.path().join("docker/iceberg-rest/fixture_run.py")).unwrap();
        assert_eq!(
            startup_sweep(directory.path(), &publication, &budget.clone()).unwrap(),
            report
        );
        let other = directory.path().join("different-env.sh");
        std::fs::write(&other, "# other publication").unwrap();
        assert!(startup_sweep(directory.path(), &other, &budget).is_err());
    }

    #[test]
    fn startup_sweep_native_failure_is_not_partial_success() {
        let (directory, publication, budget) = sweep_fixture(
            "import sys\nprint('AccessDenied: native S3 failure',file=sys.stderr)\nsys.exit(17)\n",
        );
        let error = startup_sweep(directory.path(), &publication, &budget).unwrap_err();
        let message = format!("{error:#}");
        assert!(message.contains("17"));
        assert!(message.contains("AccessDenied: native S3 failure"));
        assert!(message.contains("diagnostics retained at"));
    }

    #[test]
    fn expired_startup_sweep_does_not_dispatch_or_resolve_publication() {
        let (directory, publication, budget) =
            sweep_fixture("raise RuntimeError('must not run')\n");
        let expired = budget.with_work_deadline_limit(Instant::now() - Duration::from_secs(1));
        std::fs::remove_file(&publication).unwrap();
        assert!(
            startup_sweep(directory.path(), &publication, &expired)
                .unwrap_err()
                .is::<FixtureBudgetExpired>()
        );
    }

    #[test]
    fn invalid_publication_storage_is_rejected_before_startup_dispatch() {
        let (directory, publication, budget) =
            sweep_fixture("raise RuntimeError('must not dispatch')\n");
        let path = directory.path().join("manifest.json");
        let mut manifest: serde_json::Value =
            serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
        manifest["runtime"]["machine"]["server_warehouses"]["rest"] =
            serde_json::json!(&budget.identity().scope_root);
        std::fs::write(&path, serde_json::to_vec(&manifest).unwrap()).unwrap();
        assert!(
            startup_sweep(directory.path(), &publication, &budget)
                .unwrap_err()
                .to_string()
                .contains("overlap")
        );
        assert!(!directory.path().join("calls.json").exists());
    }

    fn storage_vectors() -> serde_json::Value {
        serde_json::from_str(include_str!(
            "../../../docker/iceberg-rest/tests/run_storage_scope_vectors.json"
        ))
        .unwrap()
    }

    #[test]
    fn storage_scope_matches_shared_python_vectors() {
        let vectors = storage_vectors();
        let identity: ScratchIdentity =
            serde_json::from_value(vectors["identity"].clone()).unwrap();
        let scope = RunStorageScope::from_record(&identity, &vectors["record"]).unwrap();
        assert_eq!(serde_json::to_value(&scope).unwrap(), vectors["projection"]);
        assert_eq!(scope.iam_policy().unwrap(), vectors["policy"]);
        for case in vectors["membership"].as_array().unwrap() {
            let actual = scope.belongs(case["uri"].as_str().unwrap());
            if case.get("error").is_some() {
                assert!(actual.is_err(), "{case}");
            } else {
                assert_eq!(
                    actual.unwrap(),
                    case["belongs"].as_bool().unwrap(),
                    "{case}"
                );
            }
        }
        for case in vectors["namespaces"].as_array().unwrap() {
            assert_eq!(
                scope.owns_namespace(case["name"].as_str().unwrap()),
                case["owned"].as_bool().unwrap(),
                "{case}"
            );
        }
    }

    fn oss_storage_fixture() -> (ScratchIdentity, serde_json::Value) {
        let mut record = storage_vectors()["record"].clone();
        let identity = ScratchIdentity::create_at(
            "s3://novarocks-ci/fixture/worktree",
            1_800_000_000_000,
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        )
        .unwrap();
        record["config"]
            .as_object_mut()
            .unwrap()
            .remove("minio_volume");
        record["config"]["object_store"] = serde_json::json!({"kind":"aliyun-oss", "region":"cn-beijing", "bucket":"novarocks-ci", "role_name":"fixture-role", "root_prefix":"fixture/"});
        record["config"]["benchmark_root"] =
            serde_json::json!("s3://novarocks-ci/fixture/benchmarks");
        record["protected_benchmark_roots"] =
            serde_json::json!(["s3://novarocks-ci/fixture/benchmarks"]);
        record["server_warehouses"] = serde_json::json!({"rest":"s3://novarocks-ci/fixture/service/rest", "rest-mv":"s3://novarocks-ci/fixture/service/rest-mv"});
        (identity, record)
    }

    #[test]
    fn oss_storage_scope_has_closed_owner_without_a_minio_volume() {
        let (identity, record) = oss_storage_fixture();
        let scope = RunStorageScope::from_record(&identity, &record).unwrap();
        let wire = serde_json::to_value(&scope).unwrap();
        assert_eq!(wire["schema"], 2);
        assert_eq!(
            wire["storage_owner"],
            serde_json::json!({"provider":"aliyun-oss", "endpoint":"https://s3.oss-cn-beijing-internal.aliyuncs.com", "region":"cn-beijing", "bucket":"novarocks-ci", "root_prefix":"fixture/"})
        );
        let restored = RunStorageScope::from_json(&scope.to_json().unwrap()).unwrap();
        restored.validate_against(&identity, &record).unwrap();
        assert!(
            restored
                .belongs(&format!(
                    "s3a://novarocks-ci/fixture/service/rest/{}_table/data/a",
                    identity.run_id
                ))
                .unwrap()
        );
        assert!(
            !restored
                .belongs("s3://novarocks-ci/fixture/service/rest/foreign_table/data/a")
                .unwrap()
        );
        assert!(
            !restored
                .belongs("s3://other-bucket/fixture/outside")
                .unwrap()
        );
        assert!(restored.iam_policy().is_err());
    }

    #[test]
    fn oss_storage_scope_rejects_schema_owner_and_root_mismatches() {
        let (identity, record) = oss_storage_fixture();
        let scope = RunStorageScope::from_record(&identity, &record).unwrap();
        let wire = serde_json::to_value(&scope).unwrap();
        for (key, replacement) in [
            ("provider", "minio"),
            ("endpoint", "https://foreign.invalid"),
            ("region", "cn-shanghai"),
            ("bucket", "other-bucket"),
            ("root_prefix", "other/"),
            ("minio_volume", "invented-volume"),
        ] {
            let mut changed = wire.clone();
            changed["storage_owner"][key] = serde_json::json!(replacement);
            assert!(
                RunStorageScope::from_json(&changed.to_string())
                    .and_then(|s| s.validate_against(&identity, &record))
                    .is_err(),
                "{key}"
            );
        }
        let mut changed = wire.clone();
        changed["schema"] = serde_json::json!(1);
        assert!(RunStorageScope::from_json(&changed.to_string()).is_err());
        for warehouse in [
            "s3://other-bucket/fixture/service/rest",
            "s3://novarocks-ci/fixture-sibling/service/rest",
        ] {
            let mut changed = record.clone();
            changed["server_warehouses"]["rest"] = serde_json::json!(warehouse);
            assert!(RunStorageScope::from_record(&identity, &changed).is_err());
        }
        let mut changed = record.clone();
        changed["config"]["object_store"]["unexpected"] = serde_json::json!(true);
        assert!(RunStorageScope::from_record(&identity, &changed).is_err());
        let mut changed = record.clone();
        changed["generation"] = serde_json::json!("next");
        assert!(scope.validate_against(&identity, &changed).is_err());
    }

    #[test]
    fn storage_scope_rejects_changed_machine_owner_and_ranges() {
        let vectors = storage_vectors();
        let identity: ScratchIdentity =
            serde_json::from_value(vectors["identity"].clone()).unwrap();
        for (section, field, replacement) in [
            ("machine", "generation", "next"),
            ("storage_owner", "daemon_id", "other"),
            ("storage_owner", "minio_volume", "other"),
            ("ranges", "run_root", "s3://elsewhere/root/"),
        ] {
            let mut value = vectors["projection"].clone();
            value[section][field] = serde_json::json!(replacement);
            let result = RunStorageScope::from_json(&value.to_string())
                .and_then(|scope| scope.validate_against(&identity, &vectors["record"]));
            assert!(result.is_err(), "{section}/{field}");
        }
        for change in ["overlap", "prefix", "unknown"] {
            let mut value = vectors["projection"].clone();
            match change {
                "overlap" => {
                    value["ranges"]["rest_mv"]["warehouse"] =
                        value["ranges"]["rest"]["warehouse"].clone()
                }
                "prefix" => {
                    value["ranges"]["rest"]["namespace_prefix"] = serde_json::json!("foreign_")
                }
                _ => value["machine"]["unexpected"] = serde_json::json!(true),
            }
            assert!(
                RunStorageScope::from_json(&value.to_string()).is_err(),
                "{change}"
            );
        }
        let mut record = vectors["record"].clone();
        record["config"]["benchmark_root"] = serde_json::json!(&identity.scope_root);
        assert!(RunStorageScope::from_record(&identity, &record).is_err());
    }

    fn identity() -> ScratchIdentity {
        ScratchIdentity::create_at(
            "s3://novarocks/worktree",
            1_792_540_800_000,
            "0123456789abcdef0123456789abcdef",
        )
        .unwrap()
    }
    #[test]
    fn names_and_wire_match_the_cross_language_contract() {
        let identity = identity();
        assert_eq!(
            identity.resource_name("db").unwrap(),
            "nr_scratch_1792540800000_0123456789abcdef0123456789abcdef_db"
        );
        assert_eq!(identity.iam_name("initial").unwrap().len(), 20);
        assert_ne!(
            identity.iam_name("initial").unwrap(),
            identity.iam_name("rotated").unwrap()
        );
        let now = Instant::now();
        let run = RunBudget::admit_at(
            FixturePolicy::default(),
            identity.clone(),
            3600,
            None,
            identity.created_unix_ms,
            now,
        )
        .unwrap();
        let context: FixtureRunContext = serde_json::from_str(&run.to_json().unwrap()).unwrap();
        assert_eq!(context.identity, identity);
        assert_eq!(
            context.deadline_unix_ms - context.started_unix_ms,
            3_600_000
        );
        assert_eq!(run.work_deadline(), now + Duration::from_secs(3300));
    }
    #[test]
    fn nesting_inherits_original_deadline_and_rejects_exhausted_work() {
        let identity = identity();
        let now = Instant::now();
        let first = RunBudget::admit_at(
            FixturePolicy::default(),
            identity.clone(),
            600,
            None,
            identity.created_unix_ms,
            now,
        )
        .unwrap();
        let nested = RunBudget::admit_at(
            FixturePolicy::default(),
            identity.clone(),
            3600,
            Some(first.context.clone()),
            identity.created_unix_ms + 100_000,
            now + Duration::from_secs(100),
        )
        .unwrap();
        assert_eq!(nested.deadline(), first.deadline());
        assert!(
            RunBudget::admit_at(
                FixturePolicy::default(),
                identity.clone(),
                3600,
                Some(first.context.clone()),
                identity.created_unix_ms + 300_000,
                now
            )
            .is_err()
        );
    }
    #[test]
    fn invalid_admission_is_rejected_before_any_owner_operation() {
        let identity = identity();
        let now = Instant::now();
        for seconds in [0, 300, 86401, u64::MAX] {
            assert!(
                RunBudget::admit_at(
                    FixturePolicy::default(),
                    identity.clone(),
                    seconds,
                    None,
                    identity.created_unix_ms,
                    now
                )
                .is_err()
            );
        }
        for root in [
            "s3://novarocks/a/../b",
            "s3://novarocks//root",
            "s3://novarocks/a?b",
            "s3://novarocks/a%2fb",
        ] {
            assert!(
                ScratchIdentity::create_at(
                    root,
                    identity.created_unix_ms,
                    "0123456789abcdef0123456789abcdef"
                )
                .is_err()
            );
        }
    }
    #[test]
    fn inherited_short_machine_budget_ignores_new_default_but_preserves_expiry_failure() {
        let identity = identity();
        let now = Instant::now();
        let policy = FixturePolicy {
            scratch_ttl_seconds: 2000,
            max_run_seconds: 500,
            cleanup_grace_seconds: 30,
        };
        let first = RunBudget::admit_at(
            policy,
            identity.clone(),
            400,
            None,
            identity.created_unix_ms,
            now,
        )
        .unwrap();
        let nested = RunBudget::admit_at(
            policy,
            identity.clone(),
            3600,
            Some(first.context.clone()),
            identity.created_unix_ms + 100_000,
            now + Duration::from_secs(100),
        )
        .unwrap();
        assert_eq!(first.deadline(), nested.deadline());
        let error = RunBudget::admit_at(
            policy,
            identity.clone(),
            3600,
            Some(first.context.clone()),
            identity.created_unix_ms + 370_000,
            now,
        )
        .unwrap_err();
        assert!(error.is::<FixtureBudgetExpired>());
        assert!(!rejected(error).is::<FixtureBudgetRejected>());
    }

    #[test]
    fn benchmark_overlap_is_rejected_by_segments_and_never_by_similar_prefix() {
        let identity = identity();
        let run = RunBudget::admit_at(
            FixturePolicy::default(),
            identity.clone(),
            600,
            None,
            identity.created_unix_ms,
            Instant::now(),
        )
        .unwrap();
        for root in [
            "s3://novarocks/worktree".to_string(),
            identity.scope_root.clone(),
            format!("{}/nested", identity.scope_root),
        ] {
            assert!(run.validate_protected_roots(&[root]).is_err());
        }
        run.validate_protected_roots(&[
            "s3://novarocks/worktree-peer".into(),
            format!("{}peer", identity.scope_root),
        ])
        .unwrap();
    }
    #[test]
    fn earlier_local_deadline_never_changes_or_renews_the_wire_context() {
        let identity = identity();
        let now = Instant::now();
        let run = RunBudget::admit_at(
            FixturePolicy::default(),
            identity.clone(),
            600,
            None,
            identity.created_unix_ms,
            now,
        )
        .unwrap();
        let original = run.to_json().unwrap();
        let limited = run
            .clone()
            .with_work_deadline_limit(now + Duration::from_secs(20));
        assert_eq!(limited.work_deadline(), now + Duration::from_secs(20));
        assert_eq!(limited.to_json().unwrap(), original);
        assert_eq!(limited.deadline(), run.deadline());
        assert_eq!(
            limited
                .with_work_deadline_limit(now + Duration::from_secs(100))
                .work_deadline(),
            now + Duration::from_secs(20)
        );
    }
}
