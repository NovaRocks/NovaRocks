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

//! An explicitly isolated Iceberg REST plus MinIO fixture for system scenarios.
//!
//! The ordinary `docker/iceberg-rest` environment intentionally shares Docker
//! services across worktrees, which also means one REST Catalog database and
//! one namespace listing for the whole machine.  Some tests need the opposite:
//! a unique compose project, volume, warehouse, runtime entry, and teardown.
//! Credential-lease scenarios need it because they mint and expire identities
//! on the fixture's own MinIO; a suite that restarts a frontend and lets it
//! rediscover its materialized views needs it because otherwise it discovers
//! every other worktree's views too.  This module is therefore test-only and
//! deliberately drives the existing fixture scripts with
//! `NOVA_ENV_SHARED_DOCKER=false`; it never starts or falls back to the shared
//! project.

use anyhow::{Context, Result, bail, ensure};
use hmac::{Hmac, Mac};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::ffi::{OsStr, OsString};
use std::fmt;
use std::fs;
use std::io::{Read, Seek, SeekFrom};
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::process::{Command, Output, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;

const FIXTURE_PREFIX: &str = "isolated-rest";
/// Every Docker project this fixture creates.  Creation always uses this
/// prefix, and both the per-fixture teardown and the startup sweep refuse to
/// address anything that is not a fixture project by [`is_fixture_project`].
const FIXTURE_PROJECT_PREFIX: &str = "nr-isolated-rest-";
/// Every generated runtime entry this fixture creates.
const FIXTURE_ENTRY_PREFIX: &str = "isolated-rest-";
/// The prefixes this fixture used before it served more than one scenario.
///
/// Nothing is created under them any more; they exist so the startup sweep
/// still reclaims a project or entry an older build left behind, which would
/// otherwise have no owner left to remove it.
const LEGACY_FIXTURE_PROJECT_PREFIX: &str = "nr-cca1-vended-rest-";
const LEGACY_FIXTURE_ENTRY_PREFIX: &str = "cca1-vended-rest-";

/// Whether a Docker project name is one this fixture owns, current or legacy.
fn is_fixture_project(name: &str) -> bool {
    name.starts_with(FIXTURE_PROJECT_PREFIX) || name.starts_with(LEGACY_FIXTURE_PROJECT_PREFIX)
}

/// Whether a generated runtime entry name is one this fixture owns.
fn is_fixture_entry(name: &str) -> bool {
    name.starts_with(FIXTURE_ENTRY_PREFIX) || name.starts_with(LEGACY_FIXTURE_ENTRY_PREFIX)
}
const MAX_DIAGNOSTIC_BYTES: usize = 8 * 1024;
const MINIO_STS_DURATION_SECONDS: u32 = 900;
static NEXT_FIXTURE_ID: AtomicU64 = AtomicU64::new(1);

/// How long one `up.sh` or `down.sh` run may take before the fixture stops
/// waiting on it and reclaims its Docker project directly.
///
/// This covers local BOM verification, isolated service creation and readiness.
/// Fixture inputs are provisioned separately; `up.sh` never builds or pulls.
/// Creating the publication-hook profile also uses this bound for its explicit
/// test image build before any fixture service starts. The bound exists
/// to convert a wedged script into a reported failure the fixture can clean up
/// after, not to police how long a cold machine takes.
const FIXTURE_SCRIPT_TIMEOUT: Duration = Duration::from_secs(20 * 60);
/// How long one direct `docker` or `docker compose` invocation may take.
///
/// Every such call here is a small control-plane operation against containers
/// that are already running, or a reclaim of a project that already exists.
const FIXTURE_DOCKER_TIMEOUT: Duration = Duration::from_secs(120);
/// How long one HTTP request against the fixture's own MinIO or REST Catalog
/// may take. Both are local containers that `up.sh` already proved ready.
const FIXTURE_HTTP_TIMEOUT: Duration = Duration::from_secs(60);
/// How often a bounded external command is checked for completion.
const FIXTURE_COMMAND_POLL_INTERVAL: Duration = Duration::from_millis(20);
/// How long a local probe that answers from this host alone may take.
///
/// Neither `ps` nor `date` can legitimately block, so this exists only so
/// that every external wait in this fixture has a bound.
const FIXTURE_PROBE_TIMEOUT: Duration = Duration::from_secs(30);
/// How long a command that already exited may keep its output pipes open.
///
/// A command whose own output this fixture parses closes both pipes as it
/// exits; a longer hold means something it spawned outlived it, and the
/// capture can no longer be trusted to be the whole answer.
const MAX_COMMAND_CAPTURE_BYTES: u64 = 16 * 1024 * 1024;

/// The non-secret endpoint facts consumed by a vended REST scenario.
#[derive(Clone, Eq, PartialEq)]
pub struct IsolatedIcebergRestEndpoints {
    pub rest_uri: String,
    pub rest_warehouse: String,
    pub minio_endpoint: String,
    pub compose_project: String,
}

/// Safe, immutable identity of one image actually running in the fixture.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct IsolatedIcebergRestImageIdentity {
    pub image_id: String,
    pub image_reference: String,
}

/// Provider facts that are safe to place in a benchmark artifact.
///
/// This deliberately contains neither the compose project nor endpoints,
/// ports, warehouses, access keys, or other generated configuration. Image
/// identities come from live containers, while the three hashes bind the
/// checked-in fixture implementation that started them.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct IsolatedIcebergRestRuntimeIdentity {
    pub schema_version: u32,
    pub images: BTreeMap<String, IsolatedIcebergRestImageIdentity>,
    pub compose_sha256: String,
    pub scripts_sha256: String,
    pub model_sha256: String,
    pub rest_version: String,
    pub minio_version: String,
    pub capabilities: BTreeSet<String>,
}

impl fmt::Debug for IsolatedIcebergRestEndpoints {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("IsolatedIcebergRestEndpoints")
            .field("rest_uri", &self.rest_uri)
            .field("rest_warehouse", &self.rest_warehouse)
            .field("minio_endpoint", &self.minio_endpoint)
            .field("compose_project", &self.compose_project)
            .finish()
    }
}

/// One test-only S3 access-key identity provisioned by the isolated MinIO.
///
/// The secret is needed to construct a vended credential response, but its
/// `Debug` representation remains redacted so it cannot leak through scenario
/// diagnostics.
#[derive(Clone, Eq, PartialEq)]
pub struct IsolatedS3Identity {
    pub access_key_id: String,
    pub secret_access_key: String,
}

impl fmt::Debug for IsolatedS3Identity {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("IsolatedS3Identity")
            .field("access_key_id", &self.access_key_id)
            .field("secret_access_key", &"<redacted>")
            .finish()
    }
}

/// One short-lived S3 identity issued by the fixture's own MinIO STS endpoint.
///
/// A vended Iceberg response carries all three AWS STS scalars, so a normal
/// MinIO access key plus an invented token is not a valid test substitute.
#[derive(Clone, Eq, PartialEq)]
pub struct IsolatedStsS3Identity {
    pub access_key_id: String,
    pub secret_access_key: String,
    pub session_token: String,
    pub not_after_unix_ms: u64,
}

impl fmt::Debug for IsolatedStsS3Identity {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("IsolatedStsS3Identity")
            .field("access_key_id", &self.access_key_id)
            .field("secret_access_key", &"<redacted>")
            .field("session_token", &"<redacted>")
            .field("not_after_unix_ms", &self.not_after_unix_ms)
            .finish()
    }
}

/// The two distinct usable identities required to verify initial credential
/// use and the subsequent refresh.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IsolatedVendedS3Identities {
    pub initial: IsolatedStsS3Identity,
    pub rotated: IsolatedStsS3Identity,
}

/// A per-scenario REST Catalog and object-store environment.
///
/// The fixture owns exactly one generated workspace root below the supplied
/// scenario runtime root.  `shutdown` is idempotent and `Drop` makes a best
/// effort to destroy only that fixture's compose project and runtime entry.
pub struct IsolatedIcebergRestFixture {
    repo_root: PathBuf,
    scenario_root: PathBuf,
    workspace_root: PathBuf,
    config_file: PathBuf,
    compose_project: String,
    /// The generated runtime entry this fixture owns, recorded as soon as
    /// `up.sh` creates it.  Teardown addresses it directly instead of
    /// re-deriving it from a workspace path that may already be gone.
    runtime_entry: Option<RuntimeEntry>,
    endpoints: IsolatedIcebergRestEndpoints,
    minio_root_identity: IsolatedS3Identity,
    vended_s3_identities: Option<IsolatedVendedS3Identities>,
    publication_control_uri: Option<String>,
    profile: FixtureProfile,
    active: bool,
}

/// The identity of one generated `docker/iceberg-rest/runtime/<id>` entry.
#[derive(Clone, Debug)]
struct RuntimeEntry {
    id: String,
    directory: PathBuf,
    configuration_directory: PathBuf,
}

#[derive(Debug)]
enum FixtureProfile {
    Stock,
    PublicationHook { image: String, control_port: u16 },
}

impl FixtureProfile {
    fn name(&self) -> &'static str {
        match self {
            Self::Stock => "stock",
            Self::PublicationHook { .. } => "publication-hook",
        }
    }

    fn apply(&self, command: &mut Command) {
        command.args(["--profile", self.name()]);
        if let Self::PublicationHook {
            image,
            control_port,
        } = self
        {
            command.args(["--hook-image", image]).env(
                "NOVA_ENV_PUBLICATION_HOOK_CONTROL_PORT",
                control_port.to_string(),
            );
        }
    }
}

impl IsolatedIcebergRestFixture {
    /// Starts a fresh REST Catalog and MinIO compose project below
    /// `scenario_root`.  The caller must retain the fixture for the entire
    /// lifetime of any cluster that uses the returned endpoints.
    pub fn start(scenario_root: impl AsRef<Path>) -> Result<Self> {
        Self::start_profile(
            repository_root()?,
            scenario_root.as_ref(),
            FixtureProfile::Stock,
        )
    }

    /// Creates the REST service with its publication hook already installed.
    /// The returned control URI belongs to this fixture's initial REST container.
    pub fn start_with_publication_hook(scenario_root: impl AsRef<Path>) -> Result<(Self, String)> {
        let repo_root = repository_root()?;
        let image = build_publication_hook_image(&repo_root)?;
        let reservation = TcpListener::bind("127.0.0.1:0")
            .context("reserve isolated publication control port")?;
        let control_port = reservation.local_addr()?.port();
        // Docker must bind the port itself. A competing bind is a startup
        // failure, never permission to switch an existing fixture's port.
        drop(reservation);
        let mut fixture = Self::start_profile(
            repo_root,
            scenario_root.as_ref(),
            FixtureProfile::PublicationHook {
                image,
                control_port,
            },
        )?;
        let control_uri = fixture.observe_publication_hook()?;
        fixture.publication_control_uri = Some(control_uri.clone());
        Ok((fixture, control_uri))
    }

    fn start_profile(
        repo_root: PathBuf,
        scenario_root: &Path,
        profile: FixtureProfile,
    ) -> Result<Self> {
        // A previous run that crashed, timed out, or was interrupted leaves its
        // Docker project and generated runtime entry behind with nothing left to
        // reclaim them.  Sweep those before adding another one, so one bad run
        // cannot accumulate into an unusable machine.
        sweep_stale_fixtures(&repo_root);
        let scenario_root = ensure_absolute_directory(scenario_root)?;
        let fixture_id = unique_fixture_id();
        let workspace_root = scenario_root.join(&fixture_id);
        let config_file = workspace_root.join("isolated-compose.env");
        let compose_project = format!("nr-{fixture_id}");
        let (access_key_id, secret_access_key) = fixture_credentials(&fixture_id);

        fs::create_dir_all(&workspace_root).with_context(|| {
            format!("create isolated fixture root {}", workspace_root.display())
        })?;
        write_config(
            &config_file,
            &compose_project,
            &access_key_id,
            &secret_access_key,
        )?;

        let mut fixture = Self {
            repo_root,
            scenario_root,
            workspace_root,
            config_file,
            compose_project,
            runtime_entry: None,
            endpoints: IsolatedIcebergRestEndpoints {
                rest_uri: String::new(),
                rest_warehouse: String::new(),
                minio_endpoint: String::new(),
                compose_project: String::new(),
            },
            minio_root_identity: IsolatedS3Identity {
                access_key_id,
                secret_access_key,
            },
            vended_s3_identities: None,
            publication_control_uri: None,
            profile,
            active: true,
        };

        let started = fixture.run_script("up.sh", &[]);
        // Record the generated entry before inspecting the outcome: a partially
        // created environment still has to be reclaimed, and only the manifest
        // knows which entry `up.sh` chose.
        fixture.record_runtime_entry();
        if let Err(error) = started {
            let cleanup = fixture.shutdown();
            return match cleanup {
                Ok(()) => Err(error),
                Err(cleanup_error) => Err(error.context(format!(
                    "isolated fixture cleanup also failed: {cleanup_error:#}"
                ))),
            };
        }

        let endpoints = fixture.read_endpoints().with_context(|| {
            format!(
                "read isolated Iceberg REST manifest for compose project {}",
                fixture.compose_project
            )
        });
        match endpoints {
            Ok((endpoints, minio_root_identity)) => {
                fixture.endpoints = endpoints;
                fixture.minio_root_identity = minio_root_identity;
            }
            Err(error) => {
                let cleanup = fixture.shutdown();
                return match cleanup {
                    Ok(()) => Err(error),
                    Err(cleanup_error) => Err(error.context(format!(
                        "isolated fixture cleanup also failed: {cleanup_error:#}"
                    ))),
                };
            }
        }
        Ok(fixture)
    }

    pub fn endpoints(&self) -> &IsolatedIcebergRestEndpoints {
        &self.endpoints
    }

    fn observe_publication_hook(&self) -> Result<String> {
        let FixtureProfile::PublicationHook { control_port, .. } = &self.profile else {
            bail!("isolated fixture does not have a publication hook profile");
        };
        let manifest = read_manifest(&self.find_manifest()?)?;
        self.assert_isolated_manifest(&manifest)?;
        let container = live_service_container(&self.repo_root, &self.compose_project, "rest")?;
        let port_output = run_docker(&self.repo_root, &["port", &container, "8182/tcp"])?;
        ensure!(
            port_output.status.success(),
            "publication hook control port is not published"
        );
        let binding = String::from_utf8(port_output.stdout)
            .context("publication hook control port is not UTF-8")?;
        let port = binding
            .lines()
            .next()
            .and_then(|line| line.rsplit_once(':'))
            .and_then(|(_, port)| port.parse::<u16>().ok())
            .context("publication hook control port has no valid host binding")?;
        ensure!(
            port == *control_port,
            "publication hook control port differs from its creation request"
        );
        let control_uri = format!("http://127.0.0.1:{port}");
        ensure!(
            manifest.runtime.control_uri.as_deref() == Some(control_uri.as_str()),
            "publication hook manifest control URI differs from its live binding"
        );
        let client = fixture_http_client()?;
        let deadline = Instant::now() + Duration::from_secs(60);
        loop {
            if client
                .get(format!("{control_uri}/health"))
                .send()
                .is_ok_and(|response| response.status().is_success())
                && client
                    .get(format!("{}/v1/config", self.endpoints.rest_uri))
                    .send()
                    .is_ok_and(|response| response.status().is_success())
            {
                break;
            }
            ensure!(
                Instant::now() < deadline,
                "publication hook REST service did not become ready"
            );
            thread::sleep(Duration::from_millis(100));
        }
        Ok(control_uri)
    }

    /// The fixture's exact generated environment for Spark helpers. Using
    /// this entry keeps cross-engine writes on this private REST and MinIO
    /// project instead of the worktree's shared `runtime/current` entry.
    pub fn runtime_env_file(&self) -> Result<PathBuf> {
        self.assert_owned_paths()?;
        ensure!(self.active, "isolated provider runtime is no longer active");
        let entry = self
            .runtime_entry
            .as_ref()
            .context("isolated provider runtime has no generated entry")?;
        let path = entry.configuration_directory.join("env.sh");
        ensure!(path.is_file(), "isolated provider environment is missing");
        Ok(path)
    }

    pub fn workspace_root(&self) -> &Path {
        &self.workspace_root
    }

    /// Observes the exact live provider runtime without returning generated
    /// network or credential material.
    pub fn runtime_identity(&self) -> Result<IsolatedIcebergRestRuntimeIdentity> {
        self.assert_owned_paths()?;
        ensure!(self.active, "isolated provider runtime is no longer active");
        let mut images = ["minio", "rest", "spark"]
            .into_iter()
            .map(|service| {
                Ok((
                    service.to_string(),
                    live_service_image(&self.repo_root, &self.compose_project, service)?,
                ))
            })
            .collect::<Result<BTreeMap<_, _>>>()?;
        let mc_container =
            completed_service_container(&self.repo_root, &self.compose_project, "mc")?;
        images.insert(
            "mc".to_string(),
            container_image(&self.repo_root, &mc_container, "mc")?,
        );

        let minio_container =
            live_service_container(&self.repo_root, &self.compose_project, "minio")?;
        let minio_version = minio_runtime_version(&self.repo_root, &minio_container)?;
        let rest_container =
            live_service_container(&self.repo_root, &self.compose_project, "rest")?;
        let rest_environment = container_string_array(
            &self.repo_root,
            &rest_container,
            "{{json .Config.Env}}",
            "REST environment",
        )?;
        let expected_io = if self.publication_control_uri.is_some() {
            "CATALOG_IO__IMPL=org.apache.iceberg.rest.fixture.TracingFileIO"
        } else {
            "CATALOG_IO__IMPL=org.apache.iceberg.aws.s3.S3FileIO"
        };
        ensure!(
            rest_environment.iter().any(|value| value == expected_io)
                && rest_environment
                    .iter()
                    .any(|value| value == "CATALOG_S3_PATH__STYLE__ACCESS=true"),
            "isolated REST runtime does not expose the required FileIO path-style capability"
        );
        let rest_image = images
            .get("rest")
            .context("isolated REST image identity is absent")?;
        let rest_version = image_tag(&rest_image.image_reference)
            .context("isolated REST image reference has no immutable version tag")?;
        ensure!(
            rest_version != "latest",
            "isolated REST image does not expose a concrete runtime version"
        );

        let fixture_root = self.repo_root.join("docker/iceberg-rest");
        let templates = fixture_template_paths(&self.repo_root);
        let compose_sha256 = hash_named_files(&self.repo_root, &templates)?;
        let manifest = read_manifest(&self.find_manifest()?)?;
        self.assert_isolated_manifest(&manifest)?;
        let mut scripts = Vec::new();
        for entry in fs::read_dir(&fixture_root)
            .with_context(|| format!("read fixture scripts from {}", fixture_root.display()))?
        {
            let path = entry.context("read fixture script entry")?.path();
            if path
                .extension()
                .is_some_and(|extension| extension == "sh" || extension == "py")
            {
                scripts.push(path);
            }
        }
        scripts.sort();
        ensure!(
            !scripts.is_empty(),
            "isolated fixture has no checked-in scripts"
        );
        let scripts_sha256 = hash_named_files(&self.repo_root, &scripts)?;
        let model_sha256 = fixture_template_model_hash(&self.repo_root)?;
        let capabilities = [
            "iceberg-rest-v1",
            "minio-s3",
            "path-style-s3",
            "spark-iceberg-bootstrap",
        ]
        .into_iter()
        .map(str::to_string)
        .collect();
        Ok(IsolatedIcebergRestRuntimeIdentity {
            schema_version: 1,
            images,
            compose_sha256,
            scripts_sha256,
            model_sha256,
            rest_version,
            minio_version,
            capabilities,
        })
    }

    /// Returns the static object-store identity owned by this isolated
    /// fixture. Test scenarios use it to install the same explicit credential
    /// generation in every process before creating a REST catalog.
    pub fn static_s3_identity(&self) -> IsolatedS3Identity {
        self.minio_root_identity.clone()
    }

    /// Creates two distinct, valid MinIO STS identities. The fixture first
    /// provisions ordinary users and then signs AssumeRole requests as those
    /// users, yielding the access key, secret, and session token that MinIO
    /// itself will verify on the S3 data plane.
    pub fn provision_vended_s3_identities(&mut self) -> Result<IsolatedVendedS3Identities> {
        if let Some(identities) = &self.vended_s3_identities {
            return Ok(identities.clone());
        }
        self.assert_owned_paths()?;
        let initial = self.new_sts_identity("ccai")?;
        let rotated = self.new_sts_identity("ccar")?;
        ensure!(
            initial.access_key_id != rotated.access_key_id,
            "isolated fixture generated duplicate vended access keys"
        );
        let identities = IsolatedVendedS3Identities { initial, rotated };
        self.vended_s3_identities = Some(identities.clone());
        Ok(identities)
    }

    /// Creates an empty Iceberg table through the isolated fixture's own
    /// privileged REST Catalog before a vended client is admitted.
    ///
    /// This is intentionally fixture setup rather than a NovaRocks DDL helper:
    /// a vended catalog must not receive the fixture's MinIO root credential.
    /// The resulting table is subsequently accessed only through the vended
    /// REST proxy.
    ///
    /// The two REST calls below replace one `spark-sql` run inside the
    /// fixture's Spark container. That run produced the same empty table, but
    /// the generated `spark.master local[*]` gave its JVM one task slot per
    /// logical core of the Docker VM, so preparing a table for a 1FE+3BE
    /// scenario saturated the machine that cluster was about to start on.
    /// Creating a table is a catalog fact, and the catalog states it directly.
    pub fn provision_empty_table(&self, namespace: &str, table: &str) -> Result<()> {
        self.assert_owned_paths()?;
        ensure!(
            self.active,
            "refusing to provision a table in an isolated fixture that is no longer active"
        );
        validate_catalog_identifier("namespace", namespace)?;
        validate_catalog_identifier("table", table)?;

        let catalog = self.rest_catalog_base()?;
        create_rest_namespace(&catalog, namespace)
            .with_context(|| format!("provision isolated Iceberg namespace {namespace}"))?;
        create_rest_empty_table(&catalog, namespace, table)
            .with_context(|| format!("provision empty isolated Iceberg table {namespace}.{table}"))
    }

    /// The fixture's own REST Catalog root, without a trailing separator.
    ///
    /// This is the privileged endpoint `up.sh` generated and proved ready. It
    /// is never the vended proxy a scenario later puts in front of it.
    fn rest_catalog_base(&self) -> Result<String> {
        let base = self.endpoints.rest_uri.trim_end_matches('/');
        ensure!(
            base.starts_with("http://") || base.starts_with("https://"),
            "isolated fixture REST Catalog endpoint is not an HTTP(S) URL"
        );
        Ok(base.to_string())
    }

    /// Stops the exact compose project created by this fixture and removes its
    /// generated runtime entry.  It never addresses `nr-iceberg-rest`.
    ///
    /// Teardown is idempotent and does its best to leave nothing behind: the
    /// orderly script path runs first, and a direct label-driven reclaim runs
    /// unconditionally after it.  The script can fail for reasons unrelated to
    /// whether anything actually leaked — a workspace root that no longer
    /// exists, a half-written manifest, a Compose hiccup — and a scenario must
    /// not strand a Docker project on the machine because of one.
    pub fn shutdown(&mut self) -> Result<()> {
        if !self.active {
            return Ok(());
        }
        self.assert_owned_paths()?;
        // Claim the teardown before running it.  A second attempt against a
        // project that is already gone has nothing to do, and the reclaim below
        // is what actually guarantees removal.
        self.active = false;
        let ordered = self.run_script("down.sh", &["--docker", "--purge"]);
        let reclaimed = reclaim_fixture(
            &self.repo_root,
            &self.compose_project,
            self.runtime_entry
                .as_ref()
                .map(|entry| entry.directory.as_path()),
        );
        match (ordered, reclaimed) {
            (Ok(()), Ok(())) => Ok(()),
            (Err(script), Ok(())) => {
                // Nothing leaked, so this is a diagnostic rather than a
                // scenario failure.
                eprintln!(
                    "isolated Iceberg REST fixture teardown script failed for {}, \
                     but its Docker project and runtime entry were reclaimed \
                     directly: {script:#}",
                    self.compose_project
                );
                Ok(())
            }
            (Ok(()), Err(reclaim)) => Err(reclaim),
            (Err(script), Err(reclaim)) => Err(reclaim.context(format!(
                "isolated fixture teardown script also failed: {script:#}"
            ))),
        }
    }

    /// Remembers which generated runtime entry `up.sh` produced for this
    /// fixture.  Best effort: an unreadable manifest only costs teardown its
    /// shortcut, because the label-driven reclaim does not depend on it.
    fn record_runtime_entry(&mut self) {
        let Ok(manifest_path) = self.find_manifest() else {
            return;
        };
        let Ok(manifest) = read_manifest(&manifest_path) else {
            return;
        };
        // The manifest carries the stable entry identity. A publication
        // directory name is never a worktree id or a cleanup target.
        if let Ok(entry) = runtime_entry_from_manifest(&self.repo_root, &manifest) {
            self.runtime_entry = Some(entry);
        }
    }

    fn read_endpoints(&self) -> Result<(IsolatedIcebergRestEndpoints, IsolatedS3Identity)> {
        self.assert_owned_paths()?;
        let manifest_path = self.find_manifest()?;
        let contents = fs::read_to_string(&manifest_path).with_context(|| {
            format!(
                "read generated fixture manifest {}",
                manifest_path.display()
            )
        })?;
        let manifest: Manifest = serde_json::from_str(&contents).with_context(|| {
            format!(
                "decode generated fixture manifest {}",
                manifest_path.display()
            )
        })?;
        self.assert_isolated_manifest(&manifest)?;
        ensure!(
            !manifest.iceberg_rest.uri.trim().is_empty()
                && !manifest.iceberg_rest.warehouse.trim().is_empty()
                && !manifest.minio.endpoint.trim().is_empty()
                && !manifest.minio.access_key_id.trim().is_empty()
                && !manifest.minio.secret_access_key.trim().is_empty(),
            "isolated fixture manifest is missing REST or MinIO endpoint facts"
        );
        let endpoints = IsolatedIcebergRestEndpoints {
            rest_uri: manifest.iceberg_rest.uri,
            rest_warehouse: manifest.iceberg_rest.warehouse,
            minio_endpoint: manifest.minio.endpoint,
            compose_project: manifest.compose_project,
        };
        let root_identity = IsolatedS3Identity {
            access_key_id: manifest.minio.access_key_id,
            secret_access_key: manifest.minio.secret_access_key,
        };
        Ok((endpoints, root_identity))
    }

    fn find_manifest(&self) -> Result<PathBuf> {
        let runtime_base = self.repo_root.join("docker/iceberg-rest/runtime");
        let expected_workspace_root = self.workspace_root.to_string_lossy();
        let mut matches = Vec::new();
        for entry in fs::read_dir(&runtime_base)
            .with_context(|| format!("read fixture runtime base {}", runtime_base.display()))?
        {
            let entry = entry.context("read fixture runtime entry")?;
            if !entry
                .file_type()
                .context("read fixture runtime entry type")?
                .is_dir()
            {
                continue;
            }
            let Ok(manifest_path) = entry.path().join("manifest.json").canonicalize() else {
                continue;
            };
            let Ok(contents) = fs::read_to_string(&manifest_path) else {
                continue;
            };
            let Ok(manifest) = serde_json::from_str::<Manifest>(&contents) else {
                continue;
            };
            if manifest.workspace_root == expected_workspace_root
                && manifest.compose_project == self.compose_project
                && !manifest.shared_docker
                && entry.file_name().to_str() == Some(manifest.env_id.as_str())
            {
                matches.push(manifest_path);
            }
        }
        match matches.len() {
            1 => Ok(matches.remove(0)),
            0 => bail!(
                "no isolated fixture manifest exists for compose project {}",
                self.compose_project
            ),
            _ => bail!(
                "multiple isolated fixture manifests exist for compose project {}",
                self.compose_project
            ),
        }
    }

    fn new_sts_identity(&self, prefix: &str) -> Result<IsolatedStsS3Identity> {
        let user = self.new_builtin_user_identity(prefix)?;
        mint_minio_sts_identity(&self.endpoints.minio_endpoint, &user)
            .context("mint isolated MinIO STS credential")
    }

    fn new_builtin_user_identity(&self, prefix: &str) -> Result<IsolatedS3Identity> {
        let identity = IsolatedS3Identity {
            access_key_id: access_key(prefix, &self.compose_project),
            // MinIO built-in-user secrets are bounded to 8..=40 bytes.
            // Derive a compact test-only value rather than embedding the
            // unbounded unique compose-project name.
            secret_access_key: secret_key(prefix, &self.compose_project),
        };
        let manifest_path = self.find_manifest()?;
        let manifest = read_manifest(&manifest_path)?;
        self.assert_isolated_manifest(&manifest)
            .context("isolated fixture manifest changed before access-key provisioning")?;
        let mut command = controlled_command("docker");
        command
            .current_dir(&self.repo_root)
            .arg("compose")
            .arg("--env-file")
            .arg(&manifest.compose_env)
            .arg("-p")
            .arg(&self.compose_project)
            .arg("-f")
            .arg(&manifest.compose_file)
            .args(["run", "--rm", "--no-deps", "-T"])
            .args(["-e", "MINIO_ROOT_USER", "-e", "MINIO_ROOT_PASSWORD"])
            .args(["-e", "VENDED_ACCESS_KEY", "-e", "VENDED_SECRET_KEY"])
            .args(["--entrypoint", "/bin/sh", "mc", "-c"])
            .arg(
                "set -eu; \\
                 /usr/bin/mc alias set minio http://minio:9000 \"$MINIO_ROOT_USER\" \"$MINIO_ROOT_PASSWORD\" >/dev/null; \\
                 /usr/bin/mc admin user add minio \"$VENDED_ACCESS_KEY\" \"$VENDED_SECRET_KEY\" >/dev/null; \\
                 /usr/bin/mc admin policy attach minio readwrite --user \"$VENDED_ACCESS_KEY\" >/dev/null",
            )
            .env("MINIO_ROOT_USER", &self.minio_root_identity.access_key_id)
            .env("MINIO_ROOT_PASSWORD", &self.minio_root_identity.secret_access_key)
            .env("VENDED_ACCESS_KEY", &identity.access_key_id)
            .env("VENDED_SECRET_KEY", &identity.secret_access_key);
        let output = run_bounded_command(
            command,
            FIXTURE_DOCKER_TIMEOUT,
            "provision isolated MinIO built-in user",
            &[
                &self.minio_root_identity.secret_access_key,
                &identity.secret_access_key,
            ],
        )?;
        if !output.status.success() {
            bail!(
                "provision isolated MinIO built-in user exited with {}; diagnostics: {}",
                output.status,
                safe_diagnostics(
                    &output,
                    &[
                        &self.minio_root_identity.secret_access_key,
                        &identity.secret_access_key,
                    ],
                )
            );
        }
        Ok(identity)
    }

    fn assert_isolated_manifest(&self, manifest: &Manifest) -> Result<()> {
        ensure!(
            manifest.compose_project == self.compose_project
                && !manifest.shared_docker
                && manifest.workspace_root == self.workspace_root.to_string_lossy(),
            "isolated fixture manifest does not belong to this fixture"
        );
        let entry = runtime_entry_from_manifest(&self.repo_root, manifest)?;
        for file in [&manifest.compose_file, &manifest.compose_env] {
            let path = Path::new(file)
                .canonicalize()
                .with_context(|| format!("resolve generated fixture file {file}"))?;
            ensure!(
                path.is_file() && path.starts_with(&entry.directory),
                "isolated fixture manifest references a compose file outside its generated entry"
            );
        }
        ensure!(
            manifest.runtime.profile == self.profile.name(),
            "isolated fixture manifest profile differs from its creation request"
        );
        match &self.profile {
            FixtureProfile::Stock => ensure!(
                manifest.runtime.control_uri.is_none(),
                "stock fixture unexpectedly publishes a hook control URI"
            ),
            FixtureProfile::PublicationHook { control_port, .. } => {
                let expected = format!("http://127.0.0.1:{control_port}");
                ensure!(
                    manifest.runtime.control_uri.as_deref() == Some(expected.as_str()),
                    "publication hook manifest control URI differs from its creation request"
                );
            }
        }
        ensure!(
            manifest.runtime.template_model_sha256 == fixture_template_model_hash(&self.repo_root)?,
            "isolated fixture template model differs from its generated manifest"
        );
        Ok(())
    }

    fn run_script(&self, script: &str, args: &[&str]) -> Result<()> {
        let script_path = self.repo_root.join("docker/iceberg-rest").join(script);
        let mut command = fixture_command(
            &script_path,
            &self.repo_root,
            &self.workspace_root,
            &self.config_file,
            &self.compose_project,
            args,
        );
        if script == "up.sh" {
            self.profile.apply(&mut command);
        }
        // Once the entry is known, address it by name.  The scripts otherwise
        // derive it by hashing the workspace root, which stops resolving as soon
        // as that temporary directory is removed.
        if let Some(entry) = &self.runtime_entry {
            command.env("NOVA_ENV_ID", &entry.id);
        }
        let output = run_bounded_command(
            command,
            FIXTURE_SCRIPT_TIMEOUT,
            &format!(
                "isolated Iceberg REST fixture script {}",
                script_path.display()
            ),
            &[&self.minio_root_identity.secret_access_key],
        )?;
        if output.status.success() {
            return Ok(());
        }
        bail!(
            "isolated Iceberg REST fixture script {} exited with {}; diagnostics: {}",
            script_path.display(),
            output.status,
            safe_diagnostics(&output, &[&self.minio_root_identity.secret_access_key])
        );
    }

    fn assert_owned_paths(&self) -> Result<()> {
        ensure!(
            self.workspace_root.starts_with(&self.scenario_root),
            "refusing to operate isolated fixture outside its scenario root"
        );
        ensure!(
            self.config_file.starts_with(&self.workspace_root),
            "refusing to operate isolated fixture config outside its workspace root"
        );
        ensure!(
            // Strict on purpose: this fixture created its own project, so it
            // can only carry the current prefix.
            self.compose_project.starts_with(FIXTURE_PROJECT_PREFIX),
            "refusing to operate unexpected compose project"
        );
        Ok(())
    }
}

impl Drop for IsolatedIcebergRestFixture {
    fn drop(&mut self) {
        if let Err(error) = self.shutdown() {
            eprintln!("isolated Iceberg REST fixture cleanup failed: {error:#}");
        }
    }
}

/// Removes one fixture's Docker project and generated runtime entry without
/// depending on any generated file.
///
/// The compose file and env file may already be gone by the time teardown runs,
/// so this addresses containers, volumes, and networks through the
/// `com.docker.compose.project` label that Compose itself writes.  That makes
/// the reclaim safe to repeat and immune to a partially created environment.
fn reclaim_fixture(
    repo_root: &Path,
    compose_project: &str,
    runtime_dir: Option<&Path>,
) -> Result<()> {
    ensure!(
        is_fixture_project(compose_project),
        "refusing to reclaim unexpected Docker project {compose_project}"
    );
    let mut failures = Vec::new();

    if let Err(error) = remove_project_docker_state(repo_root, compose_project) {
        failures.push(format!("{error:#}"));
    }
    if let Some(runtime_dir) = runtime_dir
        && let Err(error) = remove_runtime_entry(repo_root, runtime_dir)
    {
        failures.push(format!("{error:#}"));
    }

    if failures.is_empty() {
        Ok(())
    } else {
        bail!(
            "reclaim isolated Iceberg REST fixture {compose_project}: {}",
            failures.join("; ")
        )
    }
}

/// Force-removes every container, volume, and network Compose labelled with
/// this project. Returns an error only if something survives.
fn remove_project_docker_state(repo_root: &Path, compose_project: &str) -> Result<()> {
    let label = format!("label=com.docker.compose.project={compose_project}");

    let containers = docker_ids(repo_root, &["ps", "-aq", "--filter", &label])?;
    if !containers.is_empty() {
        let mut args = vec!["rm", "-f", "-v"];
        args.extend(containers.iter().map(String::as_str));
        let _ = run_docker(repo_root, &args);
    }
    let networks = docker_ids(repo_root, &["network", "ls", "-q", "--filter", &label])?;
    for network in &networks {
        let _ = run_docker(repo_root, &["network", "rm", network]);
    }
    // Volumes must go last: Docker refuses to remove one that a container still
    // references.
    let volumes = docker_ids(repo_root, &["volume", "ls", "-q", "--filter", &label])?;
    for volume in &volumes {
        let _ = run_docker(repo_root, &["volume", "rm", volume]);
    }

    let mut remaining = Vec::new();
    if let Ok(left) = docker_ids(repo_root, &["ps", "-aq", "--filter", &label])
        && !left.is_empty()
    {
        remaining.push(format!("{} container(s)", left.len()));
    }
    if let Ok(left) = docker_ids(repo_root, &["volume", "ls", "-q", "--filter", &label])
        && !left.is_empty()
    {
        remaining.push(format!("volume(s) {}", left.join(", ")));
    }
    if remaining.is_empty() {
        Ok(())
    } else {
        bail!(
            "Docker project {compose_project} still holds {}",
            remaining.join(" and ")
        )
    }
}

/// Removes one generated `runtime/<id>` entry, refusing anything that is not a
/// fixture entry inside this repository's runtime base.
fn remove_runtime_entry(repo_root: &Path, runtime_dir: &Path) -> Result<()> {
    let runtime_base = repo_root.join("docker/iceberg-rest/runtime");
    ensure!(
        runtime_dir.parent() == Some(runtime_base.as_path()),
        "refusing to remove runtime entry outside {}: {}",
        runtime_base.display(),
        runtime_dir.display()
    );
    let name = runtime_dir
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or_default();
    ensure!(
        is_fixture_entry(name),
        "refusing to remove unexpected runtime entry {}",
        runtime_dir.display()
    );
    match fs::remove_dir_all(runtime_dir) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => {
            Err(error).with_context(|| format!("remove runtime entry {}", runtime_dir.display()))
        }
    }
}

/// Reclaims fixture Docker projects and runtime entries left behind by runs
/// that never reached teardown.
///
/// Ownership is decided by the process id embedded in the fixture id: an entry
/// whose creating process is gone can no longer be cleaned up by anyone else,
/// while a live process id is left strictly alone.  That keeps the sweep safe
/// for concurrent runners and for process-id reuse, at the cost of deferring a
/// reclaim to a later run.
fn sweep_stale_fixtures(repo_root: &Path) {
    static SWEPT: std::sync::Once = std::sync::Once::new();
    SWEPT.call_once(|| {
        for project in stale_docker_projects(repo_root) {
            eprintln!("reclaiming stale isolated Iceberg REST Docker project {project}");
            if let Err(error) = remove_project_docker_state(repo_root, &project) {
                eprintln!("could not reclaim stale Docker project {project}: {error:#}");
            }
        }
        for entry in stale_runtime_entries(repo_root) {
            eprintln!(
                "reclaiming stale isolated Iceberg REST runtime entry {}",
                entry.display()
            );
            if let Err(error) = remove_runtime_entry(repo_root, &entry) {
                eprintln!(
                    "could not reclaim stale runtime entry {}: {error:#}",
                    entry.display()
                );
            }
        }
        remove_dangling_fixture_current_link(repo_root);
    });
}

/// Every fixture Docker project whose creating process is gone, gathered from
/// container, volume, and network labels so a partially removed project is
/// still found.
fn stale_docker_projects(repo_root: &Path) -> Vec<String> {
    let mut projects = BTreeSet::new();
    let listings: [&[&str]; 3] = [
        &[
            "ps",
            "-a",
            "--format",
            "{{.Label \"com.docker.compose.project\"}}",
        ],
        &[
            "volume",
            "ls",
            "--filter",
            "label=com.docker.compose.project",
            "--format",
            "{{.Label \"com.docker.compose.project\"}}",
        ],
        &[
            "network",
            "ls",
            "--filter",
            "label=com.docker.compose.project",
            "--format",
            "{{.Label \"com.docker.compose.project\"}}",
        ],
    ];
    for args in listings {
        let Ok(values) = docker_ids(repo_root, args) else {
            continue;
        };
        for value in values {
            if is_fixture_project(&value) && !fixture_owner_is_alive(&value) {
                projects.insert(value);
            }
        }
    }
    projects.into_iter().collect()
}

/// Every generated fixture runtime entry whose creating process is gone.
fn stale_runtime_entries(repo_root: &Path) -> Vec<PathBuf> {
    let runtime_base = repo_root.join("docker/iceberg-rest/runtime");
    let Ok(entries) = fs::read_dir(&runtime_base) else {
        return Vec::new();
    };
    let mut stale = Vec::new();
    for entry in entries.flatten() {
        let path = entry.path();
        // `current` is a symlink; only real generated directories qualify.
        if !entry.file_type().map(|kind| kind.is_dir()).unwrap_or(false) {
            continue;
        }
        let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
            continue;
        };
        if !is_fixture_entry(name) {
            continue;
        }
        // Prefer the manifest's untruncated project name: the directory name is
        // a shortened slug plus a path hash, so it carries less identity.
        let owner = read_manifest(&path.join("manifest.json"))
            .ok()
            .filter(|manifest| {
                !manifest.shared_docker && is_fixture_project(&manifest.compose_project)
            })
            .map(|manifest| manifest.compose_project)
            .unwrap_or_else(|| name.to_owned());
        if !fixture_owner_is_alive(&owner) {
            stale.push(path);
        }
    }
    stale
}

/// Removes `runtime/current` only when it is a symlink to a fixture entry that
/// no longer exists.
///
/// A live worktree link is never touched.  A dangling link into a removed
/// fixture entry is this fixture's own litter from before it stopped claiming
/// the link, and leaving it in place makes every later `source
/// runtime/current/env.sh` fail with an error that points nowhere near the
/// cause.
fn remove_dangling_fixture_current_link(repo_root: &Path) {
    let link = repo_root.join("docker/iceberg-rest/runtime/current");
    let Ok(metadata) = fs::symlink_metadata(&link) else {
        return;
    };
    if !metadata.file_type().is_symlink() {
        return;
    }
    let Ok(target) = fs::read_link(&link) else {
        return;
    };
    let Some(name) = target.file_name().and_then(|name| name.to_str()) else {
        return;
    };
    if !is_fixture_entry(name) {
        return;
    }
    if link.exists() {
        // Still resolves, so a fixture entry is genuinely present; the sweep
        // above owns removing it and will run before this.
        return;
    }
    eprintln!(
        "removing dangling runtime/current symlink left pointing at removed fixture entry {name}"
    );
    let _ = fs::remove_file(&link);
}

/// Whether the process that created a fixture project or entry is still
/// running.  An unparsable name is treated as alive so the sweep never removes
/// something it does not understand.
fn fixture_owner_is_alive(name: &str) -> bool {
    let Some(pid) = fixture_owner_pid(name) else {
        return true;
    };
    // `ps -p` reports existence regardless of the owning user, unlike `kill -0`,
    // which cannot distinguish "gone" from "not permitted".
    let mut command = Command::new("ps");
    command.args(["-p", &pid.to_string()]);
    run_bounded_command(command, FIXTURE_PROBE_TIMEOUT, "ps -p", &[])
        .map(|output| output.status.success())
        // An unanswerable probe leaves the owner alone, exactly as an
        // unparsable name does.
        .unwrap_or(true)
}

/// Extracts the creating process id from `nr-isolated-rest-<pid>-...` or
/// `isolated-rest-<pid>-...`, and from either legacy spelling.
fn fixture_owner_pid(name: &str) -> Option<u32> {
    let rest = name
        .strip_prefix(FIXTURE_PROJECT_PREFIX)
        .or_else(|| name.strip_prefix(FIXTURE_ENTRY_PREFIX))
        .or_else(|| name.strip_prefix(LEGACY_FIXTURE_PROJECT_PREFIX))
        .or_else(|| name.strip_prefix(LEGACY_FIXTURE_ENTRY_PREFIX))?;
    let digits = rest.split('-').next()?;
    digits.parse().ok()
}

fn live_service_container(
    repo_root: &Path,
    compose_project: &str,
    service: &str,
) -> Result<String> {
    let project_label = format!("label=com.docker.compose.project={compose_project}");
    let service_label = format!("label=com.docker.compose.service={service}");
    let containers = docker_ids(
        repo_root,
        &[
            "ps",
            "-q",
            "--filter",
            &project_label,
            "--filter",
            &service_label,
        ],
    )?;
    ensure!(
        containers.len() == 1,
        "isolated fixture service {service} has {} live containers",
        containers.len()
    );
    Ok(containers[0].clone())
}

fn completed_service_container(
    repo_root: &Path,
    compose_project: &str,
    service: &str,
) -> Result<String> {
    let project_label = format!("label=com.docker.compose.project={compose_project}");
    let service_label = format!("label=com.docker.compose.service={service}");
    let containers = docker_ids(
        repo_root,
        &[
            "ps",
            "-aq",
            "--filter",
            &project_label,
            "--filter",
            &service_label,
        ],
    )?;
    ensure!(
        containers.len() == 1,
        "isolated fixture service {service} has {} containers",
        containers.len()
    );
    let container = containers[0].clone();
    ensure!(
        docker_inspect_scalar(
            repo_root,
            &container,
            "{{.State.Status}}",
            "completed service state"
        )? == "exited"
            && docker_inspect_scalar(
                repo_root,
                &container,
                "{{.State.ExitCode}}",
                "completed service exit code"
            )? == "0",
        "isolated fixture service {service} did not complete successfully"
    );
    Ok(container)
}

fn live_service_image(
    repo_root: &Path,
    compose_project: &str,
    service: &str,
) -> Result<IsolatedIcebergRestImageIdentity> {
    let container = live_service_container(repo_root, compose_project, service)?;
    container_image(repo_root, &container, service)
}

fn container_image(
    repo_root: &Path,
    container: &str,
    service: &str,
) -> Result<IsolatedIcebergRestImageIdentity> {
    let image_id = docker_inspect_scalar(repo_root, container, "{{.Image}}", "image id")?;
    ensure!(
        image_id.strip_prefix("sha256:").is_some_and(
            |digest| digest.len() == 64 && digest.bytes().all(|byte| byte.is_ascii_hexdigit())
        ),
        "isolated fixture service {service} did not report an immutable image id"
    );
    let image_reference =
        docker_inspect_scalar(repo_root, container, "{{.Config.Image}}", "image reference")?;
    ensure!(
        !image_reference.trim().is_empty(),
        "isolated fixture service {service} has an empty image reference"
    );
    Ok(IsolatedIcebergRestImageIdentity {
        image_id,
        image_reference,
    })
}

fn docker_inspect_scalar(
    repo_root: &Path,
    object: &str,
    format: &str,
    fact: &str,
) -> Result<String> {
    let output = run_docker(repo_root, &["inspect", "--format", format, object])?;
    ensure!(
        output.status.success(),
        "docker inspect for {fact} exited with {}",
        output.status
    );
    let value = String::from_utf8(output.stdout)
        .with_context(|| format!("decode Docker {fact}"))?
        .trim()
        .to_string();
    ensure!(!value.is_empty(), "Docker {fact} is empty");
    Ok(value)
}

fn container_string_array(
    repo_root: &Path,
    container: &str,
    format: &str,
    fact: &str,
) -> Result<Vec<String>> {
    let json = docker_inspect_scalar(repo_root, container, format, fact)?;
    serde_json::from_str(&json).with_context(|| format!("decode Docker {fact}"))
}

fn minio_runtime_version(repo_root: &Path, container: &str) -> Result<String> {
    let output = run_docker(repo_root, &["exec", container, "minio", "--version"])?;
    ensure!(
        output.status.success(),
        "read isolated MinIO version exited with {}",
        output.status
    );
    let stdout = String::from_utf8(output.stdout).context("decode isolated MinIO version")?;
    let version = stdout
        .lines()
        .next()
        .and_then(|line| line.trim().strip_prefix("minio version "))
        .map(str::trim)
        .filter(|version| !version.is_empty())
        .context("isolated MinIO version output has no version")?;
    Ok(version.to_string())
}

fn image_tag(reference: &str) -> Option<String> {
    let (repository, tag) = reference.rsplit_once(':')?;
    (!repository.is_empty()
        && repository != "sha256"
        && !reference.contains('@')
        && !tag.is_empty()
        && !tag.contains('/'))
    .then(|| tag.to_string())
}

fn fixture_template_paths(repo_root: &Path) -> Vec<PathBuf> {
    ["object-store.yml", "catalog.yml"]
        .into_iter()
        .map(|name| repo_root.join("docker/iceberg-rest/templates").join(name))
        .collect()
}

fn fixture_template_model_hash(repo_root: &Path) -> Result<String> {
    let mut paths = fixture_template_paths(repo_root);
    paths.push(repo_root.join("docker/iceberg-rest/spark/Dockerfile"));
    hash_named_files(repo_root, &paths)
}

fn build_publication_hook_image(repo_root: &Path) -> Result<String> {
    let hook_root = repo_root.join("tests/fixtures/iceberg-rest-publication");
    let mut inputs = vec![hook_root.join("Dockerfile")];
    let source_root = hook_root.join("src/main/java/org/apache/iceberg/rest/fixture");
    for entry in fs::read_dir(&source_root).context("read publication hook sources")? {
        let path = entry?.path();
        if path
            .extension()
            .is_some_and(|extension| extension == "java")
        {
            inputs.push(path);
        }
    }
    let source_hash = hash_named_files(repo_root, &inputs)?;
    let image = format!(
        "novarocks/iceberg-rest-publication-fixture:uea7-{}",
        &source_hash[..16]
    );
    let mut command = controlled_command("docker");
    command
        .current_dir(repo_root)
        .args(["build", "--pull=false", "-t", &image])
        .arg(&hook_root);
    let output = run_bounded_command(
        command,
        FIXTURE_SCRIPT_TIMEOUT,
        "build publication hook image before fixture creation",
        &[],
    )?;
    ensure!(
        output.status.success(),
        "build publication hook image {image}: {}",
        safe_diagnostics(&output, &[])
    );
    Ok(image)
}

fn hash_named_files(repo_root: &Path, paths: &[PathBuf]) -> Result<String> {
    ensure!(!paths.is_empty(), "fixture identity file set is empty");
    let mut paths = paths.to_vec();
    paths.sort();
    paths.dedup();
    let mut hasher = Sha256::new();
    for path in paths {
        ensure!(
            path.starts_with(repo_root),
            "fixture identity refuses a file outside the repository"
        );
        let relative = path
            .strip_prefix(repo_root)
            .context("derive fixture identity relative path")?;
        let bytes = fs::read(&path)
            .with_context(|| format!("read fixture identity input {}", path.display()))?;
        ensure!(
            !bytes.is_empty(),
            "fixture identity input {} is empty",
            path.display()
        );
        let relative = relative.to_string_lossy();
        hasher.update((relative.len() as u64).to_le_bytes());
        hasher.update(relative.as_bytes());
        hasher.update((bytes.len() as u64).to_le_bytes());
        hasher.update(bytes);
    }
    Ok(hex_encode(&hasher.finalize()))
}

/// Runs one `docker` command and returns its non-empty output lines.
fn docker_ids(repo_root: &Path, args: &[&str]) -> Result<Vec<String>> {
    let output = run_docker(repo_root, args)?;
    ensure!(
        output.status.success(),
        "docker {} exited with {}",
        args.join(" "),
        output.status
    );
    Ok(String::from_utf8_lossy(&output.stdout)
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .map(str::to_owned)
        .collect())
}

fn run_docker(repo_root: &Path, args: &[&str]) -> Result<Output> {
    let mut command = controlled_command("docker");
    command.current_dir(repo_root).args(args);
    // Reclaim runs through here, so an unbounded wait would strand exactly the
    // Docker project this fixture exists to remove.
    run_bounded_command(
        command,
        FIXTURE_DOCKER_TIMEOUT,
        &format!("docker {}", args.join(" ")),
        &[],
    )
}

/// Runs one external command to completion, or kills it once `timeout` elapses.
///
/// `Command::output` and `Child::wait_with_output` have no timeout, so a wedged
/// Docker command would otherwise hang the run with nothing left to reclaim
/// this fixture's project.
///
/// Killing the direct child does not reach a script's own grandchildren.
/// Capture in temporary files so the direct child's exit does not require a
/// pipe EOF from another process before the fixture can inspect its output.
/// Whatever the script left running is addressed by the caller's failure path,
/// which reclaims the compose project by label.
fn run_bounded_command(
    mut command: Command,
    timeout: Duration,
    what: &str,
    secrets: &[&str],
) -> Result<Output> {
    let mut stdout = tempfile::tempfile().context("create bounded command stdout capture")?;
    let mut stderr = tempfile::tempfile().context("create bounded command stderr capture")?;
    command
        .stdin(Stdio::null())
        .stdout(Stdio::from(
            stdout.try_clone().context("clone command stdout capture")?,
        ))
        .stderr(Stdio::from(
            stderr.try_clone().context("clone command stderr capture")?,
        ));
    let deadline = Instant::now() + timeout;
    let mut child = command.spawn().with_context(|| format!("start {what}"))?;

    let mut expired = false;
    let status = loop {
        match child
            .try_wait()
            .with_context(|| format!("wait for {what}"))?
        {
            Some(status) => break status,
            None if Instant::now() >= deadline => {
                expired = true;
                let _ = child.kill();
                break child.wait().with_context(|| format!("reap {what}"))?;
            }
            None => thread::sleep(FIXTURE_COMMAND_POLL_INTERVAL),
        }
    };

    let stdout_len = stdout
        .metadata()
        .context("stat command stdout capture")?
        .len();
    let stderr_len = stderr
        .metadata()
        .context("stat command stderr capture")?
        .len();
    let output = Output {
        status,
        stdout: read_command_capture(&mut stdout)?,
        stderr: read_command_capture(&mut stderr)?,
    };
    if expired {
        bail!(
            "{what} did not finish within {}s and was killed; diagnostics: {}",
            timeout.as_secs(),
            safe_diagnostics(&output, secrets)
        );
    }
    ensure!(
        stdout_len <= MAX_COMMAND_CAPTURE_BYTES && stderr_len <= MAX_COMMAND_CAPTURE_BYTES,
        "{what} exited with {status} but exceeded the bounded output capture of {MAX_COMMAND_CAPTURE_BYTES} bytes per stream"
    );
    Ok(output)
}

fn read_command_capture(file: &mut fs::File) -> Result<Vec<u8>> {
    file.seek(SeekFrom::Start(0))
        .context("rewind bounded command capture")?;
    let mut bytes = Vec::new();
    file.take(MAX_COMMAND_CAPTURE_BYTES)
        .read_to_end(&mut bytes)
        .context("read bounded command capture")?;
    Ok(bytes)
}

/// One HTTP client for every call this fixture makes against its own
/// containers, with an explicit bound rather than a library default.
///
/// The fixture must never route to these local endpoints through an ambient
/// proxy: a desktop shell that exports `HTTP_PROXY` would otherwise send
/// loopback catalog and STS traffic somewhere that cannot answer it.
fn fixture_http_client() -> Result<reqwest::blocking::Client> {
    reqwest::blocking::Client::builder()
        .timeout(FIXTURE_HTTP_TIMEOUT)
        .no_proxy()
        .build()
        .context("build isolated fixture HTTP client")
}

/// Creates one namespace in the fixture's own REST Catalog, tolerating a
/// namespace that already exists.
fn create_rest_namespace(catalog_base: &str, namespace: &str) -> Result<()> {
    let response = fixture_http_client()?
        .post(format!("{catalog_base}/v1/namespaces"))
        .header("content-type", "application/json")
        .body(rest_create_namespace_body(namespace))
        .send()
        .context("send isolated Iceberg REST create-namespace request")?;
    let status = response.status();
    // `CREATE NAMESPACE IF NOT EXISTS` was the previous semantic, and several
    // scenarios provision more than one table into the same namespace.
    if status == reqwest::StatusCode::CONFLICT {
        return Ok(());
    }
    ensure!(
        status.is_success(),
        "isolated Iceberg REST create-namespace returned HTTP {status}: {}",
        rest_failure_body(response)
    );
    Ok(())
}

/// Creates one empty, unpartitioned Iceberg table with a single optional
/// `BIGINT` column.
///
/// An existing table is a failure rather than a no-op: the previous Spark
/// `CREATE TABLE` carried no `IF NOT EXISTS`, and a scenario that provisions
/// the same table twice has lost track of its own fixture.
fn create_rest_empty_table(catalog_base: &str, namespace: &str, table: &str) -> Result<()> {
    let response = fixture_http_client()?
        .post(format!("{catalog_base}/v1/namespaces/{namespace}/tables"))
        .header("content-type", "application/json")
        .body(rest_create_table_body(table))
        .send()
        .context("send isolated Iceberg REST create-table request")?;
    let status = response.status();
    ensure!(
        status.is_success(),
        "isolated Iceberg REST create-table returned HTTP {status}: {}",
        rest_failure_body(response)
    );
    Ok(())
}

fn rest_create_namespace_body(namespace: &str) -> String {
    serde_json::json!({ "namespace": [namespace], "properties": {} }).to_string()
}

fn rest_create_table_body(table: &str) -> String {
    serde_json::json!({
        "name": table,
        "schema": {
            "type": "struct",
            "schema-id": 0,
            "fields": [
                { "id": 1, "name": "v", "required": false, "type": "long" }
            ]
        }
    })
    .to_string()
}

/// A bounded, printable rendering of a failed REST response.
///
/// The fixture's REST Catalog answers with its own error model, which carries
/// no credential material; only the response size needs bounding.
fn rest_failure_body(response: reqwest::blocking::Response) -> String {
    match response.text() {
        Ok(body) => truncate_for_diagnostics(body.trim()),
        Err(error) => format!("<unreadable response body: {error}>"),
    }
}

/// Mints a real temporary MinIO credential through its AWS-compatible STS
/// endpoint. This fixture uses a deliberately small SigV4 implementation so
/// the test harness does not need to introduce an AWS SDK only to issue two
/// local credentials.
fn mint_minio_sts_identity(
    endpoint: &str,
    user: &IsolatedS3Identity,
) -> Result<IsolatedStsS3Identity> {
    let endpoint = endpoint
        .parse::<reqwest::Url>()
        .context("parse isolated MinIO STS endpoint")?;
    ensure!(
        endpoint.scheme() == "http" || endpoint.scheme() == "https",
        "isolated MinIO STS endpoint must be HTTP(S)"
    );
    ensure!(
        endpoint.path() == "/" && endpoint.query().is_none(),
        "isolated MinIO STS endpoint must not include a path or query"
    );
    let host = endpoint
        .host_str()
        .context("isolated MinIO STS endpoint has no host")?;
    let canonical_host = endpoint
        .port()
        .map(|port| format!("{host}:{port}"))
        .unwrap_or_else(|| host.to_owned());
    let timestamp = aws_amz_timestamp()?;
    let date = &timestamp[..8];
    let body = format!(
        "Action=AssumeRole&Version=2011-06-15&DurationSeconds={MINIO_STS_DURATION_SECONDS}&RoleArn=arn%3Aaws%3Aiam%3A%3A123456789012%3Arole%2Fcca1-vended&RoleSessionName=cca1-vended"
    );
    let payload_hash = sha256_hex(body.as_bytes());
    let canonical_headers = format!(
        "content-type:application/x-www-form-urlencoded\nhost:{canonical_host}\nx-amz-content-sha256:{payload_hash}\nx-amz-date:{timestamp}\n"
    );
    let signed_headers = "content-type;host;x-amz-content-sha256;x-amz-date";
    let canonical_request =
        format!("POST\n/\n\n{canonical_headers}\n{signed_headers}\n{payload_hash}");
    let scope = format!("{date}/us-east-1/sts/aws4_request");
    let string_to_sign = format!(
        "AWS4-HMAC-SHA256\n{timestamp}\n{scope}\n{}",
        sha256_hex(canonical_request.as_bytes())
    );
    let signing_key = aws_v4_signing_key(&user.secret_access_key, date, "us-east-1", "sts");
    let signature = hex_encode(&hmac_sha256(&signing_key, string_to_sign.as_bytes()));
    let authorization = format!(
        "AWS4-HMAC-SHA256 Credential={}/{scope}, SignedHeaders={signed_headers}, Signature={signature}",
        user.access_key_id
    );

    let response = fixture_http_client()?
        .post(endpoint)
        .header("content-type", "application/x-www-form-urlencoded")
        .header("host", canonical_host)
        .header("x-amz-content-sha256", payload_hash)
        .header("x-amz-date", timestamp)
        .header("authorization", authorization)
        .body(body)
        .send()
        .context("send isolated MinIO STS AssumeRole request")?;
    let status = response.status();
    ensure!(
        status.is_success(),
        "isolated MinIO STS AssumeRole returned HTTP {status}"
    );
    let xml = response
        .text()
        .context("read isolated MinIO STS AssumeRole response")?;
    let expiration = sts_xml_value(&xml, "Expiration")?;
    let not_after_unix_ms = OffsetDateTime::parse(&expiration, &Rfc3339)
        .context("parse isolated MinIO STS credential expiration")?
        .unix_timestamp_nanos()
        .checked_div(1_000_000)
        .and_then(|milliseconds| u64::try_from(milliseconds).ok())
        .context("isolated MinIO STS credential expiration is before the Unix epoch")?;
    Ok(IsolatedStsS3Identity {
        access_key_id: sts_xml_value(&xml, "AccessKeyId")?,
        secret_access_key: sts_xml_value(&xml, "SecretAccessKey")?,
        session_token: sts_xml_value(&xml, "SessionToken")?,
        not_after_unix_ms,
    })
}

fn aws_amz_timestamp() -> Result<String> {
    let mut command = Command::new("date");
    command.args(["-u", "+%Y%m%dT%H%M%SZ"]);
    let output = run_bounded_command(
        command,
        FIXTURE_PROBE_TIMEOUT,
        "read UTC time for isolated MinIO STS request",
        &[],
    )?;
    ensure!(
        output.status.success(),
        "read UTC time for isolated MinIO STS request exited with {}",
        output.status
    );
    let value = String::from_utf8(output.stdout)
        .context("decode UTC time for isolated MinIO STS request")?
        .trim()
        .to_owned();
    ensure!(
        value.len() == 16
            && value.as_bytes()[8] == b'T'
            && value.as_bytes()[15] == b'Z'
            && value
                .bytes()
                .enumerate()
                .all(|(index, byte)| { index == 8 || index == 15 || byte.is_ascii_digit() }),
        "UTC time for isolated MinIO STS request has an unexpected format"
    );
    Ok(value)
}

fn aws_v4_signing_key(secret: &str, date: &str, region: &str, service: &str) -> [u8; 32] {
    let mut initial = b"AWS4".to_vec();
    initial.extend_from_slice(secret.as_bytes());
    let date_key = hmac_sha256(&initial, date.as_bytes());
    let region_key = hmac_sha256(&date_key, region.as_bytes());
    let service_key = hmac_sha256(&region_key, service.as_bytes());
    hmac_sha256(&service_key, b"aws4_request")
}

fn hmac_sha256(key: &[u8], message: &[u8]) -> [u8; 32] {
    let mut mac = Hmac::<Sha256>::new_from_slice(key).expect("HMAC accepts arbitrary key sizes");
    mac.update(message);
    mac.finalize().into_bytes().into()
}

fn sha256_hex(value: &[u8]) -> String {
    hex_encode(&Sha256::digest(value))
}

fn hex_encode(value: &[u8]) -> String {
    value.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn sts_xml_value(xml: &str, element: &str) -> Result<String> {
    let open = format!("<{element}>");
    let close = format!("</{element}>");
    let (_, after_open) = xml
        .split_once(&open)
        .with_context(|| format!("isolated MinIO STS response has no {element}"))?;
    let (value, _) = after_open
        .split_once(&close)
        .with_context(|| format!("isolated MinIO STS response has no closing {element}"))?;
    ensure!(
        !value.is_empty(),
        "isolated MinIO STS response has an empty {element}"
    );
    Ok(value.to_owned())
}

#[derive(Deserialize)]
struct Manifest {
    env_id: String,
    runtime: ManifestRuntime,
    workspace_root: String,
    shared_docker: bool,
    compose_project: String,
    compose_file: String,
    compose_env: String,
    runtime_dir: String,
    minio: ManifestMinio,
    iceberg_rest: ManifestIcebergRest,
}

#[derive(Deserialize)]
struct ManifestRuntime {
    profile: String,
    control_uri: Option<String>,
    template_model_sha256: String,
    publication_dir: String,
    entry_root: String,
}

fn runtime_entry_from_manifest(repo_root: &Path, manifest: &Manifest) -> Result<RuntimeEntry> {
    ensure!(
        manifest.env_id.starts_with(FIXTURE_ENTRY_PREFIX)
            && Path::new(&manifest.env_id).components().count() == 1,
        "isolated fixture manifest has an unexpected entry identity"
    );
    let expected = repo_root
        .join("docker/iceberg-rest/runtime")
        .join(&manifest.env_id);
    ensure!(
        Path::new(&manifest.runtime.entry_root) == expected
            && Path::new(&manifest.runtime_dir) == expected,
        "isolated fixture manifest entry root differs from its worktree identity"
    );
    let directory = expected
        .canonicalize()
        .context("resolve isolated runtime entry")?;
    ensure!(
        directory == expected,
        "isolated fixture entry root must not redirect outside its owner"
    );
    let configuration_directory = Path::new(&manifest.runtime.publication_dir)
        .canonicalize()
        .context("resolve isolated fixture publication")?;
    ensure!(
        configuration_directory.is_dir() && configuration_directory.starts_with(&directory),
        "isolated fixture publication is outside its stable entry"
    );
    Ok(RuntimeEntry {
        id: manifest.env_id.clone(),
        directory,
        configuration_directory,
    })
}

fn read_manifest(manifest_path: &Path) -> Result<Manifest> {
    let contents = fs::read_to_string(manifest_path).with_context(|| {
        format!(
            "read generated fixture manifest {}",
            manifest_path.display()
        )
    })?;
    serde_json::from_str(&contents).with_context(|| {
        format!(
            "decode generated fixture manifest {}",
            manifest_path.display()
        )
    })
}

#[derive(Deserialize)]
struct ManifestMinio {
    endpoint: String,
    access_key_id: String,
    secret_access_key: String,
}

#[derive(Deserialize)]
struct ManifestIcebergRest {
    uri: String,
    warehouse: String,
}

fn repository_root() -> Result<PathBuf> {
    let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
    manifest_dir
        .ancestors()
        .nth(2)
        .context("resolve repository root from cluster-harness manifest directory")?
        .canonicalize()
        .context("canonicalize repository root")
}

fn ensure_absolute_directory(path: &Path) -> Result<PathBuf> {
    fs::create_dir_all(path)
        .with_context(|| format!("create scenario runtime root {}", path.display()))?;
    path.canonicalize()
        .with_context(|| format!("canonicalize scenario runtime root {}", path.display()))
}

fn unique_fixture_id() -> String {
    let sequence = NEXT_FIXTURE_ID.fetch_add(1, Ordering::Relaxed);
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    format!("{FIXTURE_PREFIX}-{}-{nanos}-{sequence}", std::process::id())
}

fn fixture_credentials(fixture_id: &str) -> (String, String) {
    (
        access_key("cca", fixture_id),
        format!("cca1-root-secret-{fixture_id}"),
    )
}

fn access_key(prefix: &str, value: &str) -> String {
    debug_assert!(prefix.len() <= 4);
    let mut hash = 0xcbf29ce484222325_u64;
    for byte in value.bytes() {
        hash ^= u64::from(byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    format!("{prefix}{hash:016x}")
}

fn secret_key(prefix: &str, value: &str) -> String {
    format!("s{}", access_key(prefix, value))
}

/// Bounds one namespace or table name to a lower-case identifier.
///
/// The name now lands in a REST Catalog URL path rather than in SQL text, so
/// this keeps a fixture name from having to be escaped, percent-encoded, or
/// reinterpreted as another path segment.
fn validate_catalog_identifier(kind: &str, value: &str) -> Result<()> {
    let mut characters = value.bytes();
    let Some(first) = characters.next() else {
        bail!("isolated fixture {kind} must not be empty");
    };
    if !(first.is_ascii_lowercase() || first == b'_')
        || !characters.all(|character| {
            character.is_ascii_lowercase() || character.is_ascii_digit() || character == b'_'
        })
    {
        bail!("isolated fixture {kind} must be a lower-case identifier, got {value:?}");
    }
    Ok(())
}

fn write_config(
    config_file: &Path,
    compose_project: &str,
    access_key_id: &str,
    secret_access_key: &str,
) -> Result<()> {
    let contents = format!(
        "# Generated by the isolated Iceberg REST fixture.\nNOVA_ENV_SHARED_DOCKER=false\nNOVA_ENV_COMPOSE_PROJECT={}\nMINIO_ROOT_USER={}\nMINIO_ROOT_PASSWORD={}\n",
        shell_literal(compose_project),
        shell_literal(access_key_id),
        shell_literal(secret_access_key),
    );
    fs::write(config_file, contents)
        .with_context(|| format!("write isolated fixture config {}", config_file.display()))
}

fn shell_literal(value: &str) -> String {
    format!("'{}'", value.replace('\'', "'\\\"'\\\"'"))
}

fn apply_controlled_environment(
    command: &mut Command,
    source: impl IntoIterator<Item = (OsString, OsString)>,
) {
    // Keep this transport/tooling allowlist aligned with fixture_runtime.py.
    // Generated endpoints, credentials and Compose overrides are never inputs.
    const PRESERVED: &[&str] = &[
        "PATH",
        "HOME",
        "TMPDIR",
        "LANG",
        "LC_ALL",
        "SSL_CERT_FILE",
        "DOCKER_CONTEXT",
        "DOCKER_HOST",
        "DOCKER_TLS_VERIFY",
        "DOCKER_CERT_PATH",
        "DOCKER_CONFIG",
        "XDG_CONFIG_HOME",
        "XDG_CACHE_HOME",
        "XDG_STATE_HOME",
        "NOVA_FIXTURE_STORE",
        "NOVA_FIXTURE_RUNTIME_DIR",
    ];
    command.env_clear();
    command.envs(
        source
            .into_iter()
            .filter(|(key, _)| key.to_str().is_some_and(|key| PRESERVED.contains(&key))),
    );
}

fn controlled_command(program: impl AsRef<OsStr>) -> Command {
    let mut command = Command::new(program);
    apply_controlled_environment(&mut command, std::env::vars_os());
    command
}

fn fixture_command(
    script_path: &Path,
    repo_root: &Path,
    workspace_root: &Path,
    config_file: &Path,
    compose_project: &str,
    args: &[&str],
) -> Command {
    let mut command = controlled_command(script_path);
    command
        .current_dir(repo_root)
        .args(args)
        .env("NOVAROCKS_WORKSPACE_ROOT", workspace_root)
        .env("NOVA_ENV_CONFIG_FILE", config_file)
        .env("NOVA_ENV_SHARED_DOCKER", "false")
        .env("NOVA_ENV_COMPOSE_PROJECT", compose_project)
        // `runtime/current` is the worktree's documented environment entrypoint
        // and belongs to whoever created that worktree.  This fixture is
        // throwaway, so it must neither repoint the link on the way up nor
        // remove it on the way down.
        .env("NOVA_ENV_UPDATE_CURRENT", "false")
        // The fixture creates this exact non-shared project and no other.
        // `down.sh --docker --purge` requires both pieces of this proof before
        // it will remove the project's MinIO volume.
        .env("NOVA_ENV_ALLOW_VOLUME_DELETE", "true")
        .env("NOVA_ENV_EXPECTED_COMPOSE_PROJECT", compose_project)
        .env(
            "NOVA_ENV_EXPECTED_MINIO_VOLUME",
            format!("{compose_project}_minio-data"),
        );
    command
}

fn safe_diagnostics(output: &Output, secrets: &[&str]) -> String {
    let mut text = String::from_utf8_lossy(&output.stderr).into_owned();
    if !output.stdout.is_empty() {
        if !text.is_empty() {
            text.push(' ');
        }
        text.push_str(&String::from_utf8_lossy(&output.stdout));
    }
    let mut text = text.replace('\n', " ");
    for secret in secrets.iter().copied().filter(|secret| !secret.is_empty()) {
        text = text.replace(secret, "<redacted>");
    }
    truncate_for_diagnostics(&text)
}

/// Bounds one diagnostic string without splitting a character.
///
/// Child output is lossily decoded before it reaches here, so the byte at the
/// cap can land inside a multi-byte character; slicing there would panic while
/// reporting some other failure.
fn truncate_for_diagnostics(text: &str) -> String {
    if text.len() <= MAX_DIAGNOSTIC_BYTES {
        return text.to_string();
    }
    let mut end = MAX_DIAGNOSTIC_BYTES;
    while end > 0 && !text.is_char_boundary(end) {
        end -= 1;
    }
    format!("{}...<truncated>", &text[..end])
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::process::ExitStatusExt;

    #[test]
    fn controlled_environment_preserves_transport_and_private_supply_only() {
        let source = [
            ("PATH", "/usr/bin:/bin"),
            ("HOME", "/tmp/fixture-home"),
            ("DOCKER_CONTEXT", "test-daemon"),
            ("DOCKER_HOST", "unix:///tmp/fixture.sock"),
            ("XDG_CACHE_HOME", "/tmp/fixture-cache"),
            ("NOVA_FIXTURE_STORE", "/tmp/private-bom"),
            ("NOVA_FIXTURE_RUNTIME_DIR", "/tmp/private-owner"),
            ("NOVA_ENV_REST_PORT", "8181"),
            ("NOVA_ENV_UNKNOWN_FUTURE_OVERRIDE", "foreign"),
            ("NOVAROCKS_WORKSPACE_ROOT", "/foreign/worktree"),
            ("COMPOSE_PROJECT_NAME", "nr-iceberg-rest"),
            ("ICEBERG_REST_IMAGE", "foreign-image"),
            ("AWS_S3_SECRET_ACCESS_KEY", "foreign-secret"),
        ];
        let mut command = Command::new("/usr/bin/env");
        apply_controlled_environment(
            &mut command,
            source
                .into_iter()
                .map(|(key, value)| (key.into(), value.into())),
        );
        let output = run_bounded_command(command, FIXTURE_PROBE_TIMEOUT, "environment probe", &[])
            .expect("read controlled environment");
        assert!(output.status.success());
        let actual = String::from_utf8(output.stdout).unwrap();
        for expected in [
            "DOCKER_CONTEXT=test-daemon",
            "DOCKER_HOST=unix:///tmp/fixture.sock",
            "XDG_CACHE_HOME=/tmp/fixture-cache",
            "NOVA_FIXTURE_STORE=/tmp/private-bom",
            "NOVA_FIXTURE_RUNTIME_DIR=/tmp/private-owner",
        ] {
            assert!(actual.lines().any(|line| line == expected), "{actual}");
        }
        for forbidden in [
            "NOVA_ENV_",
            "NOVAROCKS_WORKSPACE_ROOT",
            "COMPOSE_",
            "ICEBERG_REST_IMAGE",
            "AWS_",
        ] {
            assert!(!actual.contains(forbidden), "{actual}");
        }
    }

    #[test]
    fn publication_profile_is_an_initial_creation_request() {
        let mut command = fixture_command(
            Path::new("/repo/docker/iceberg-rest/up.sh"),
            Path::new("/repo"),
            Path::new("/tmp/isolated-rest-test"),
            Path::new("/tmp/isolated-rest-test/config.env"),
            "nr-isolated-rest-test",
            &[],
        );
        FixtureProfile::PublicationHook {
            image: "novarocks/hook:exact-test-inputs".into(),
            control_port: 38182,
        }
        .apply(&mut command);
        assert_eq!(
            command.get_args().collect::<Vec<_>>(),
            vec![
                OsStr::new("--profile"),
                OsStr::new("publication-hook"),
                OsStr::new("--hook-image"),
                OsStr::new("novarocks/hook:exact-test-inputs"),
            ]
        );
        assert_eq!(
            command
                .get_envs()
                .find(|(key, _)| *key == "NOVA_ENV_PUBLICATION_HOOK_CONTROL_PORT")
                .and_then(|(_, value)| value),
            Some(OsStr::new("38182"))
        );
        assert!(
            !command
                .get_envs()
                .any(|(key, _)| key == "NOVA_ENV_REST_PORT")
        );
    }

    fn manifest_fixture() -> (
        tempfile::TempDir,
        IsolatedIcebergRestFixture,
        serde_json::Value,
    ) {
        let temp = tempfile::tempdir().expect("create manifest test root");
        let repo_root = temp.path().canonicalize().unwrap();
        let env_id = "isolated-rest-1-test";
        let entry = repo_root.join("docker/iceberg-rest/runtime").join(env_id);
        fs::create_dir_all(&entry).unwrap();
        for file in ["compose.yml", "compose.env", "env.sh"] {
            fs::write(entry.join(file), "fixture test\n").unwrap();
        }
        let mut model = fixture_template_paths(&repo_root);
        model.push(repo_root.join("docker/iceberg-rest/spark/Dockerfile"));
        for file in model {
            fs::create_dir_all(file.parent().unwrap()).unwrap();
            fs::write(file, "fixture model\n").unwrap();
        }
        let scenario_root = repo_root.join("scenario");
        let workspace_root = scenario_root.join(env_id);
        fs::create_dir_all(&workspace_root).unwrap();
        let project = format!("nr-{env_id}");
        let manifest = serde_json::json!({
            "env_id": env_id,
            "workspace_root": workspace_root,
            "shared_docker": false,
            "compose_project": project,
            "compose_file": entry.join("compose.yml"),
            "compose_env": entry.join("compose.env"),
            "runtime_dir": entry,
            "runtime": {
                "profile": "stock", "control_uri": null,
                "template_model_sha256": fixture_template_model_hash(&repo_root).unwrap(),
                "publication_dir": entry, "entry_root": entry,
            },
            "minio": {"endpoint":"http://127.0.0.1:38000", "access_key_id":"test-key", "secret_access_key":"test-secret"},
            "iceberg_rest": {"uri":"http://127.0.0.1:38001", "warehouse":"s3://warehouse/test/rest"},
        });
        let fixture = IsolatedIcebergRestFixture {
            repo_root,
            scenario_root,
            config_file: workspace_root.join("isolated-compose.env"),
            workspace_root,
            compose_project: project,
            runtime_entry: None,
            endpoints: IsolatedIcebergRestEndpoints {
                rest_uri: String::new(),
                rest_warehouse: String::new(),
                minio_endpoint: String::new(),
                compose_project: String::new(),
            },
            minio_root_identity: IsolatedS3Identity {
                access_key_id: "test-key".into(),
                secret_access_key: "test-secret".into(),
            },
            vended_s3_identities: None,
            publication_control_uri: None,
            profile: FixtureProfile::Stock,
            // These tests own files only. Drop must never contact Docker.
            active: false,
        };
        (temp, fixture, manifest)
    }

    #[test]
    fn manifest_requires_owned_generated_compose_and_the_exact_model() {
        let (_temp, fixture, mut value) = manifest_fixture();
        let manifest: Manifest = serde_json::from_value(value.clone()).unwrap();
        fixture
            .assert_isolated_manifest(&manifest)
            .expect("generated compose is owned");
        let old_compose = fixture.repo_root.join("docker/iceberg-rest/compose.yml");
        fs::write(&old_compose, "old model\n").unwrap();
        value["compose_file"] = serde_json::to_value(old_compose).unwrap();
        let manifest: Manifest = serde_json::from_value(value.clone()).unwrap();
        assert!(fixture.assert_isolated_manifest(&manifest).is_err());
        value["compose_file"] = serde_json::to_value(
            Path::new(value["runtime_dir"].as_str().unwrap()).join("compose.yml"),
        )
        .unwrap();
        value["runtime"]["template_model_sha256"] = serde_json::json!("wrong-model");
        let manifest: Manifest = serde_json::from_value(value).unwrap();
        assert!(fixture.assert_isolated_manifest(&manifest).is_err());
    }

    #[test]
    fn publication_directory_does_not_replace_the_stable_entry_identity() {
        let (_temp, mut fixture, mut value) = manifest_fixture();
        let entry = PathBuf::from(value["runtime_dir"].as_str().unwrap());
        let publication = entry.join("publications/pub-random-id");
        fs::create_dir_all(&publication).unwrap();
        value["runtime"]["publication_dir"] = serde_json::to_value(&publication).unwrap();
        fs::write(
            publication.join("manifest.json"),
            serde_json::to_vec(&value).unwrap(),
        )
        .unwrap();
        std::os::unix::fs::symlink(
            "publications/pub-random-id/manifest.json",
            entry.join("manifest.json"),
        )
        .unwrap();
        assert_eq!(
            fixture.find_manifest().unwrap(),
            publication.join("manifest.json")
        );
        fixture.record_runtime_entry();
        let recorded = fixture.runtime_entry.as_ref().expect("record stable entry");
        assert_eq!(recorded.id, "isolated-rest-1-test");
        assert_eq!(recorded.directory, entry);
        assert_eq!(recorded.configuration_directory, publication);
        assert!(
            fixture
                .assert_isolated_manifest(
                    &read_manifest(&fixture.find_manifest().unwrap()).unwrap()
                )
                .is_ok()
        );
    }

    #[test]
    fn manifest_profile_and_control_uri_must_match_creation() {
        let (_temp, mut fixture, mut value) = manifest_fixture();
        fixture.profile = FixtureProfile::PublicationHook {
            image: "hook:test".into(),
            control_port: 38182,
        };
        let stock: Manifest = serde_json::from_value(value.clone()).unwrap();
        assert!(fixture.assert_isolated_manifest(&stock).is_err());
        value["runtime"]["profile"] = serde_json::json!("publication-hook");
        value["runtime"]["control_uri"] = serde_json::json!("http://127.0.0.1:38182");
        let hook: Manifest = serde_json::from_value(value.clone()).unwrap();
        fixture.assert_isolated_manifest(&hook).unwrap();
        value["runtime"]["control_uri"] = serde_json::json!("http://127.0.0.1:38183");
        let wrong: Manifest = serde_json::from_value(value).unwrap();
        assert!(fixture.assert_isolated_manifest(&wrong).is_err());
    }

    #[test]
    fn isolated_command_forces_unique_non_shared_environment() {
        let root = PathBuf::from("/tmp/cca1-scenario/cca1-vended-rest-1-2-3");
        let config = root.join("isolated-compose.env");
        let command = fixture_command(
            Path::new("/repo/docker/iceberg-rest/up.sh"),
            Path::new("/repo"),
            &root,
            &config,
            "nr-cca1-vended-rest-1-2-3",
            &[],
        );
        let environments = command
            .get_envs()
            .map(|(key, value)| {
                (
                    key.to_string_lossy().into_owned(),
                    value.map(|value| value.to_string_lossy().into_owned()),
                )
            })
            .collect::<std::collections::BTreeMap<_, _>>();
        assert_eq!(
            environments.get("NOVA_ENV_SHARED_DOCKER"),
            Some(&Some("false".to_string()))
        );
        assert_eq!(
            environments.get("NOVA_ENV_COMPOSE_PROJECT"),
            Some(&Some("nr-cca1-vended-rest-1-2-3".to_string()))
        );
        assert_eq!(
            environments.get("NOVAROCKS_WORKSPACE_ROOT"),
            Some(&Some(root.to_string_lossy().into_owned()))
        );
        assert_eq!(
            environments.get("NOVA_ENV_CONFIG_FILE"),
            Some(&Some(config.to_string_lossy().into_owned()))
        );
        assert_eq!(
            environments.get("NOVA_ENV_ALLOW_VOLUME_DELETE"),
            Some(&Some("true".to_string()))
        );
        assert_eq!(
            environments.get("NOVA_ENV_EXPECTED_COMPOSE_PROJECT"),
            Some(&Some("nr-cca1-vended-rest-1-2-3".to_string()))
        );
        assert_eq!(
            environments.get("NOVA_ENV_EXPECTED_MINIO_VOLUME"),
            Some(&Some("nr-cca1-vended-rest-1-2-3_minio-data".to_string()))
        );
    }

    #[test]
    fn generated_config_has_no_shared_docker_fallback() {
        let config = std::env::temp_dir().join(format!("{FIXTURE_PREFIX}-config-test"));
        let _ = fs::remove_dir_all(&config);
        fs::create_dir_all(&config).expect("create fixture config test directory");
        let config_file = config.join("isolated-compose.env");
        write_config(&config_file, "nr-cca1-vended-rest-test", "key", "secret")
            .expect("write config");
        let contents = fs::read_to_string(&config_file).expect("read config");
        assert!(contents.contains("NOVA_ENV_SHARED_DOCKER=false"));
        assert!(contents.contains("NOVA_ENV_COMPOSE_PROJECT='nr-cca1-vended-rest-test'"));
        assert!(!contents.contains("shared.env"));
        fs::remove_dir_all(config).expect("remove fixture config test directory");
    }

    #[test]
    fn generated_access_keys_fit_minio_s3_limits_and_debug_is_redacted() {
        let initial = IsolatedS3Identity {
            access_key_id: access_key("ccai", "fixture"),
            secret_access_key: "do-not-print-this".to_string(),
        };
        assert_eq!(initial.access_key_id.len(), 20);
        assert!(!format!("{initial:?}").contains("do-not-print-this"));
    }

    #[test]
    fn diagnostics_redact_fixture_secrets() {
        let output = Output {
            status: std::process::ExitStatus::from_raw(1),
            stdout: b"first-secret".to_vec(),
            stderr: b"second-secret".to_vec(),
        };
        let diagnostics = safe_diagnostics(&output, &["first-secret", "second-secret"]);
        assert!(!diagnostics.contains("first-secret"));
        assert!(!diagnostics.contains("second-secret"));
        assert!(diagnostics.contains("<redacted>"));
    }

    #[test]
    fn fixture_table_identifiers_are_strictly_bounded() {
        validate_catalog_identifier("namespace", "vended_rest_db").expect("valid namespace");
        validate_catalog_identifier("table", "vended_rest_data").expect("valid table");
        assert!(validate_catalog_identifier("table", "vended-rest").is_err());
        assert!(validate_catalog_identifier("table", "vended_rest; DROP TABLE t").is_err());
        assert!(validate_catalog_identifier("table", "1vended").is_err());
        // The name reaches a URL path now, so anything that could open a new
        // path segment or a query has to be refused before it is formatted in.
        assert!(validate_catalog_identifier("table", "vended/../other").is_err());
        assert!(validate_catalog_identifier("table", "vended?purgeRequested=true").is_err());
    }

    #[test]
    fn provisioning_requests_are_exactly_the_iceberg_rest_create_payloads() {
        let namespace: serde_json::Value =
            serde_json::from_str(&rest_create_namespace_body("vended_refresh_db"))
                .expect("namespace body is JSON");
        assert_eq!(
            namespace,
            serde_json::json!({ "namespace": ["vended_refresh_db"], "properties": {} })
        );

        let table: serde_json::Value =
            serde_json::from_str(&rest_create_table_body("vended_refresh_data"))
                .expect("table body is JSON");
        // One optional BIGINT column and no partition spec: exactly the table
        // the Spark `CREATE TABLE ... (v BIGINT) USING iceberg` used to make.
        assert_eq!(
            table,
            serde_json::json!({
                "name": "vended_refresh_data",
                "schema": {
                    "type": "struct",
                    "schema-id": 0,
                    "fields": [
                        { "id": 1, "name": "v", "required": false, "type": "long" }
                    ]
                }
            })
        );
    }

    #[test]
    fn a_bounded_command_returns_the_output_of_a_command_that_finishes() {
        let mut command = Command::new("/bin/sh");
        command.args(["-c", "printf out; printf err >&2; exit 3"]);
        let output = run_bounded_command(command, Duration::from_secs(30), "probe", &[])
            .expect("a finished command is not a timeout");
        assert_eq!(output.status.code(), Some(3));
        assert_eq!(output.stdout, b"out");
        assert_eq!(output.stderr, b"err");
    }

    #[test]
    fn a_bounded_command_kills_a_child_that_outlives_its_deadline() {
        let started = Instant::now();
        let mut command = Command::new("/bin/sh");
        command.args(["-c", "sleep 600"]);
        let error = run_bounded_command(command, Duration::from_millis(200), "probe", &[])
            .expect_err("a command that outlives its deadline must fail");
        assert!(
            format!("{error:#}").contains("did not finish within"),
            "unexpected error: {error:#}"
        );
        // The point of the bound is that the wait returns; a `wait_with_output`
        // here would still be blocked ten minutes from now.
        assert!(started.elapsed() < Duration::from_secs(30));
    }

    #[test]
    fn a_bounded_command_expires_even_while_its_child_is_still_writing() {
        // The child writes more than a pipe buffer before it sleeps. Capture
        // must not block its exit or wait for inherited handles after timeout.
        let started = Instant::now();
        let mut command = Command::new("/bin/sh");
        command.args(["-c", "yes novarocks | head -c 4000000; sleep 600"]);
        let error = run_bounded_command(command, Duration::from_millis(500), "probe", &[])
            .expect_err("a command that outlives its deadline must fail");
        assert!(
            format!("{error:#}").contains("did not finish within"),
            "unexpected error: {error:#}"
        );
        assert!(started.elapsed() < Duration::from_secs(30));
    }

    #[test]
    fn a_bounded_command_timeout_redacts_fixture_secrets() {
        let mut command = Command::new("/bin/sh");
        command.args(["-c", "echo leaked-fixture-secret; sleep 600"]);
        let error = run_bounded_command(
            command,
            Duration::from_millis(200),
            "probe",
            &["leaked-fixture-secret"],
        )
        .expect_err("a command that outlives its deadline must fail");
        let rendered = format!("{error:#}");
        assert!(!rendered.contains("leaked-fixture-secret"));
        assert!(rendered.contains("<redacted>"));
    }

    #[test]
    fn diagnostics_are_bounded_without_splitting_a_character() {
        let long = "\u{4e2d}".repeat(MAX_DIAGNOSTIC_BYTES);
        let truncated = truncate_for_diagnostics(&long);
        assert!(truncated.ends_with("...<truncated>"));
        assert!(truncated.len() <= MAX_DIAGNOSTIC_BYTES + "...<truncated>".len());
        assert!(truncate_for_diagnostics("short").eq("short"));
    }

    #[test]
    fn a_new_fixture_is_created_under_the_current_prefix_and_the_old_one_is_still_reclaimable() {
        let id = unique_fixture_id();
        assert!(id.starts_with(FIXTURE_ENTRY_PREFIX), "{id}");
        assert!(is_fixture_entry(&id), "{id}");
        assert!(is_fixture_project(&format!("nr-{id}")), "{id}");

        // Nothing is created under the legacy spelling any more, but a project
        // or entry an older build left behind still has to be reclaimable --
        // otherwise it has no owner left to remove it.
        assert!(is_fixture_project("nr-cca1-vended-rest-11940-1-1"));
        assert!(is_fixture_entry("cca1-vended-rest-11940-1-1"));
        for project in [
            "nr-iceberg-rest",
            "nr-iceberg-hive",
            "nr-fx-a1b2c3-cat-0123456789ab",
        ] {
            assert!(!is_fixture_project(project));
        }
        assert!(!is_fixture_entry("novarocks-5e0a3e29"));
    }

    #[test]
    fn fixture_owner_pid_is_read_from_both_project_and_entry_names() {
        // The compose project keeps the full fixture id.
        assert_eq!(
            fixture_owner_pid("nr-cca1-vended-rest-11940-1788320173510947000-1"),
            Some(11940)
        );
        // The generated runtime entry truncates the id to a slug plus a path
        // hash, so the process id must still be read from the shortened form.
        assert_eq!(
            fixture_owner_pid("cca1-vended-rest-11940-1-264f2c9f"),
            Some(11940)
        );
    }

    #[test]
    fn fixture_owner_pid_refuses_names_this_fixture_does_not_own() {
        // An unparsable owner makes the sweep treat the project as live, so a
        // foreign project must never yield a process id.
        assert_eq!(fixture_owner_pid("nr-iceberg-rest"), None);
        assert_eq!(fixture_owner_pid("nr-tst10-ce44a1d2c2"), None);
        assert_eq!(fixture_owner_pid("cca1-vended-rest-notapid-1"), None);
    }

    #[test]
    fn an_unparsable_owner_is_treated_as_alive_so_the_sweep_leaves_it_alone() {
        assert!(fixture_owner_is_alive("nr-iceberg-rest"));
        // Process id 1 always exists, standing in for a concurrent runner.
        assert!(fixture_owner_is_alive("nr-cca1-vended-rest-1-1-1"));
    }

    #[test]
    fn reclaim_refuses_shared_versioned_and_hive_projects_before_docker_access() {
        let repo_root = repository_root().expect("repository root");
        for project in [
            "nr-iceberg-rest",
            "nr-iceberg-hive",
            "nr-fx-a1b2c3-cat-0123456789ab",
        ] {
            let error = reclaim_fixture(&repo_root, project, None)
                .expect_err("a foreign project must never be reclaimable");
            assert!(format!("{error:#}").contains("refusing to reclaim"));
        }
    }

    #[test]
    fn removing_a_runtime_entry_refuses_paths_this_fixture_does_not_own() {
        let repo_root = repository_root().expect("repository root");
        let runtime_base = repo_root.join("docker/iceberg-rest/runtime");
        let error = remove_runtime_entry(&repo_root, &runtime_base.join("some-worktree-entry"))
            .expect_err("a worktree entry must never be reclaimable");
        assert!(
            format!("{error:#}").contains("refusing to remove unexpected runtime entry"),
            "unexpected error: {error:#}"
        );
        let error = remove_runtime_entry(&repo_root, Path::new("/tmp/cca1-vended-rest-1-2-3"))
            .expect_err("an entry outside the runtime base must never be reclaimable");
        assert!(
            format!("{error:#}").contains("refusing to remove runtime entry outside"),
            "unexpected error: {error:#}"
        );
    }

    #[test]
    fn removing_a_missing_runtime_entry_succeeds_so_teardown_can_repeat() {
        let repo_root = repository_root().expect("repository root");
        let missing = repo_root
            .join("docker/iceberg-rest/runtime")
            .join("cca1-vended-rest-0-0-0");
        remove_runtime_entry(&repo_root, &missing).expect("removing a missing entry is a no-op");
    }

    #[test]
    fn provider_runtime_identity_serialization_is_safe_and_exact() {
        let identity = IsolatedIcebergRestRuntimeIdentity {
            schema_version: 1,
            images: BTreeMap::from([(
                "rest".to_string(),
                IsolatedIcebergRestImageIdentity {
                    image_id: format!("sha256:{}", "a".repeat(64)),
                    image_reference: "apache/iceberg-rest-fixture:1.10.1".to_string(),
                },
            )]),
            compose_sha256: "b".repeat(64),
            scripts_sha256: "c".repeat(64),
            model_sha256: "d".repeat(64),
            rest_version: "1.10.1".to_string(),
            minio_version: "RELEASE.2025-09-07T16-13-09Z".to_string(),
            capabilities: BTreeSet::from(["iceberg-rest-v1".to_string()]),
        };
        let value = serde_json::to_value(identity).expect("serialize runtime identity");
        let keys = value
            .as_object()
            .expect("runtime identity object")
            .keys()
            .map(String::as_str)
            .collect::<BTreeSet<_>>();
        assert_eq!(
            keys,
            BTreeSet::from([
                "capabilities",
                "compose_sha256",
                "images",
                "minio_version",
                "model_sha256",
                "rest_version",
                "schema_version",
                "scripts_sha256",
            ])
        );
        let text = value.to_string();
        for forbidden in ["endpoint", "port", "project", "secret", "warehouse"] {
            assert!(!text.to_ascii_lowercase().contains(forbidden));
        }
    }

    #[test]
    fn provider_model_hash_covers_both_templates_and_spark_definition() {
        let root = tempfile::tempdir().expect("create model test root");
        let mut paths = fixture_template_paths(root.path());
        paths.push(root.path().join("docker/iceberg-rest/spark/Dockerfile"));
        for path in &paths {
            fs::create_dir_all(path.parent().unwrap()).expect("create model directory");
            fs::write(path, "original").expect("write model input");
        }
        let forward = fixture_template_model_hash(root.path()).expect("hash model");
        let mut reversed = paths.clone();
        reversed.reverse();
        assert_eq!(forward, hash_named_files(root.path(), &reversed).unwrap());
        for path in &paths {
            fs::write(path, "changed").expect("change model input");
            assert_ne!(forward, fixture_template_model_hash(root.path()).unwrap());
            fs::write(path, "original").expect("restore model input");
        }
        assert_eq!(forward.len(), 64);
    }

    #[test]
    fn provider_version_requires_an_explicit_image_tag() {
        assert_eq!(
            image_tag("apache/iceberg-rest-fixture:1.10.1").as_deref(),
            Some("1.10.1")
        );
        assert_eq!(
            image_tag("registry:5000/team/rest:2.0").as_deref(),
            Some("2.0")
        );
        assert_eq!(image_tag("apache/iceberg-rest-fixture"), None);
        assert_eq!(image_tag("sha256:abcdef"), None);
    }

    #[test]
    #[ignore = "requires Docker and locally provisioned REST and Spark base images"]
    fn publication_hook_is_selected_before_the_private_rest_service_starts() -> Result<()> {
        let scenario_root = std::env::temp_dir().join(format!(
            "uea7-publication-hook-{}-{}",
            std::process::id(),
            unique_fixture_id()
        ));
        let (mut fixture, control_uri) =
            IsolatedIcebergRestFixture::start_with_publication_hook(&scenario_root)?;
        let project = fixture.compose_project.clone();
        let initial_rest = live_service_container(&fixture.repo_root, &project, "rest")?;
        let outcome = (|| -> Result<()> {
            ensure!(
                fixture_http_client()?
                    .get(format!("{control_uri}/health"))
                    .send()?
                    .status()
                    .is_success(),
                "publication control endpoint is unavailable"
            );
            fixture.runtime_identity()?;
            fixture.provision_empty_table("probe_db", "probe_data")?;
            ensure!(
                live_service_container(&fixture.repo_root, &project, "rest")? == initial_rest,
                "publication hook replaced the initial REST container"
            );
            Ok(())
        })();
        let shutdown = fixture.shutdown();
        let _ = fs::remove_dir_all(&scenario_root);
        outcome?;
        shutdown?;
        ensure!(
            live_project_docker_state(&repository_root()?, &project).is_empty(),
            "publication hook fixture left Docker state behind"
        );
        Ok(())
    }

    /// Proves the whole provisioning path against the real fixture without
    /// launching a cluster.
    ///
    /// This is the verification that belongs to the fixture rather than to a
    /// scenario: the vended scenarios that consume `provision_empty_table` run
    /// 1FE+3BE against Docker, so reaching for one of them to check a table
    /// creation is both slow and the reason this fixture became a resource
    /// problem in the first place.
    #[test]
    #[ignore = "requires Docker; starts an isolated Iceberg REST and MinIO project"]
    fn an_empty_table_is_provisioned_through_the_rest_catalog_alone() -> Result<()> {
        let scenario_root = std::env::temp_dir().join(format!(
            "cca1-provision-probe-{}-{}",
            std::process::id(),
            unique_fixture_id()
        ));
        let mut fixture = IsolatedIcebergRestFixture::start(&scenario_root)?;
        let outcome = (|| -> Result<()> {
            let catalog = fixture.rest_catalog_base()?;
            fixture.provision_empty_table("probe_db", "probe_data")?;

            let table = fixture_http_client()?
                .get(format!(
                    "{catalog}/v1/namespaces/probe_db/tables/probe_data"
                ))
                .send()
                .context("load the provisioned table")?;
            ensure!(
                table.status().is_success(),
                "loading the provisioned table returned HTTP {}",
                table.status()
            );
            let table: serde_json::Value = table.json().context("decode the provisioned table")?;
            let metadata = &table["metadata"];
            assert_eq!(metadata["format-version"], serde_json::json!(2));
            assert_eq!(
                metadata["schemas"][0]["fields"],
                serde_json::json!([
                    { "id": 1, "name": "v", "required": false, "type": "long" }
                ])
            );
            // Empty means empty: an accidental write here would give a vended
            // scenario data it never asked for.
            assert_eq!(metadata["snapshots"], serde_json::json!([]));
            // The table lives under the REST Catalog's own warehouse, which is
            // what the vended proxy scopes its credentials to.
            let location = metadata["location"]
                .as_str()
                .context("provisioned table has no location")?;
            assert!(
                location.starts_with("s3://"),
                "unexpected table location {location}"
            );

            // The previous Spark statement carried no `IF NOT EXISTS`, so a
            // repeat has to stay a failure.
            let repeated = fixture.provision_empty_table("probe_db", "probe_data");
            assert!(repeated.is_err(), "provisioning the same table twice");
            // A second table in the same namespace, however, is ordinary setup,
            // so the namespace call has to stay tolerant of one that exists.
            fixture.provision_empty_table("probe_db", "probe_other")?;
            Ok(())
        })();
        let shutdown = fixture.shutdown();
        let _ = fs::remove_dir_all(&scenario_root);
        outcome?;
        shutdown?;

        // Containers alone do not measure a leak: Compose labels the volume and
        // the network with the project too, and each outlives the containers.
        let leaked = live_project_docker_state(&repository_root()?, &fixture.compose_project);
        assert!(
            leaked.is_empty(),
            "fixture left Docker state behind for {}: {leaked:?}",
            fixture.compose_project
        );
        Ok(())
    }

    /// Every container, volume, and network Docker still labels with `project`.
    fn live_project_docker_state(repo_root: &Path, project: &str) -> Vec<String> {
        let filter = format!("label=com.docker.compose.project={project}");
        let listings: [&[&str]; 3] = [
            &["ps", "-a", "--filter", &filter, "--format", "{{.Names}}"],
            &["volume", "ls", "--filter", &filter, "--format", "{{.Name}}"],
            &[
                "network",
                "ls",
                "--filter",
                &filter,
                "--format",
                "{{.Name}}",
            ],
        ];
        listings
            .into_iter()
            .filter_map(|args| docker_ids(repo_root, args).ok())
            .flatten()
            .collect()
    }
}
