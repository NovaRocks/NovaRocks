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

//! Ten original exact inputs, one original role set per case.
//! Source and binaries require explicit frozen prelaunch admission.
//! Never registered automatically or admitted from the old runnable=false inputs.
use super::exact_mysql_native_oracle::{ActualBinding, RowInput, compare_cancel};
use super::independent_root_target::{IndependentRootObserver, IndependentRootTarget};
use super::result_delivery::root_census;
use super::result_delivery_support::metric;
use crate::actors::exact_mysql_control_v2::{Command, GatePhase, ReplyFacts, UnixControlClient};
use crate::actors::mysql_stream::exact_result_reader::{ExactReadSnapshot, ExactResultReader};
use crate::exact_mysql_target_binding;
use crate::scenario::{Scenario, ScenarioContext, ScenarioLaunchConfig};
use anyhow::{Context, Result, bail, ensure};
use novarocks_cluster_harness::{CrossProcessChildEnvironment, LaunchProfile, TestClusterOwner};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::fs::{self, File};
use std::io::{Read, Write};
use std::os::unix::fs::{DirBuilderExt, MetadataExt};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

#[path = "exact_mysql_prepared_config.rs"]
mod prepared_config;
use prepared_config::OriginalPreparedConfig;

#[path = "m07_variant_test_session.rs"]
pub(super) mod variant_sessions;

#[path = "result_delivery_held_late_ack.rs"]
mod held_late_ack;
/// Only the separate admitted new selector may call this. Old ten cases are unchanged.
pub(crate) fn held_late_ack_from_admitted(
    run: AdmittedExactNativeRun,
    live_fences: bool,
) -> Result<Box<dyn Scenario>> {
    held_late_ack::from_admitted(run, live_fences)
}

const S: u64 = 1_048_576;
const SOCKET_ENV: &str = "NOVAROCKS_MEM_1_M07_MYSQL_WRITE_SOCKET";
const NONCE_ENV: &str = "NOVAROCKS_MEM_1_M07_MYSQL_WRITE_NONCE_HEX";
const HEALTH_SQL: &str = "SELECT SUM(generate_series) AS total FROM generate_series(1, 100)";
const HEALTH_ROW_SHA: &str = "6dab454b19ecc06d337eb421a7ecc69aaa272a74fdf3a6bf7b81900ca94758b1";
const HTTP_BYTES: u64 = 1_048_576;
const HELD_SNAPSHOTS: usize = 6;
const RESUME_SNAPSHOTS: usize = 6;
const FINAL_EXIT_SNAPSHOTS: usize = 2;
const CONTROL_REPLY_SLOTS: usize = 16;
const IDLE_SAMPLES: usize = 51;
const RECEIPT_BYTES: usize = 131_072;

/// Produced only by root's mandatory prelaunch frozen/admitted provenance gate.
/// These strings are safe identities, never arbitrary command output/config values.
/// The runner re-admits these facts before every original scene launch.
#[derive(Clone, PartialEq, Eq)]
pub(crate) struct AdmittedExactNativeRun {
    pub clean_revision: String,
    pub source_tree_sha256: String,
    pub server_binary_sha256: String,
    pub server_build_identity: String,
    pub runner_binary_sha256: String,
    pub base_config_sha256: String,
    pub frozen_execution_binding_sha256: String,
    pub large_input_sha256: String,
    pub tiny_input_sha256: String,
}
impl AdmittedExactNativeRun {
    fn validate_shape(&self) -> Result<()> {
        ensure!(
            self.clean_revision.len() == 40
                && self.clean_revision.bytes().all(|b| b.is_ascii_hexdigit()),
            "invalid admitted clean revision"
        );
        for hash in [
            &self.source_tree_sha256,
            &self.server_binary_sha256,
            &self.runner_binary_sha256,
            &self.base_config_sha256,
            &self.frozen_execution_binding_sha256,
            &self.large_input_sha256,
            &self.tiny_input_sha256,
        ] {
            hash32(hash)?;
        }
        ensure!(
            !self.server_build_identity.is_empty() && self.server_build_identity.len() <= 256,
            "invalid admitted original binary build identity"
        );
        ensure!(
            self.large_input_sha256 == LARGE_INPUT_SHA && self.tiny_input_sha256 == TINY_INPUT_SHA,
            "admission changed immutable original matrix bytes"
        );
        Ok(())
    }
}

#[derive(Clone, Copy)]
struct FrozenCase {
    name: &'static str,
    sql: &'static str,
    sql_sha: &'static str,
    row_sha: &'static str,
    prefix_sha: &'static str,
    input: RowInput,
}
const LARGE_INPUT_SHA: &str = "d4bbd8c0cd3c3d647c6a0c948692db381360406584f25e3f6653fc91b73feff2";
const TINY_INPUT_SHA: &str = "a250875085fd54bc3c35e0c1e7a08173d2f36920bc7dc6ab83ba6dbf2745d127";
const CASES: [FrozenCase; 10] = [
    FrozenCase {
        name: "exact-native-resident-cut-1048575",
        sql: "SELECT REPEAT('x', 1048576) AS payload FROM generate_series(1, 1)",
        sql_sha: "3cf4353ce6c28949b09b6cb3ed0dc22447c500b0cb187f58e9e4918daad78fd2",
        row_sha: "e5a05e54f4636fe6e87eb8094fceda8e002bff799f3de76413a2a77c52fd50b8",
        prefix_sha: "3f295505e16bf4878e87e4f309298156482c30e5ebca6f9935518dca1a96ca9d",
        input: RowInput {
            columns: 1,
            value_bytes: 1048576,
            repeated_byte: b'x',
            cut: 1048575,
            expect_complete_tail: true,
        },
    },
    FrozenCase {
        name: "exact-native-resident-cut-1048576",
        sql: "SELECT REPEAT('x', 1048576) AS payload FROM generate_series(1, 1)",
        sql_sha: "3cf4353ce6c28949b09b6cb3ed0dc22447c500b0cb187f58e9e4918daad78fd2",
        row_sha: "e5a05e54f4636fe6e87eb8094fceda8e002bff799f3de76413a2a77c52fd50b8",
        prefix_sha: "d6a7901bbaaaa42ba83d32f88d197c5da12280b23236f916090ded124215a5ae",
        input: RowInput {
            columns: 1,
            value_bytes: 1048576,
            repeated_byte: b'x',
            cut: 1048576,
            expect_complete_tail: true,
        },
    },
    FrozenCase {
        name: "exact-native-resident-cut-1048577",
        sql: "SELECT REPEAT('x', 1048576) AS payload FROM generate_series(1, 1)",
        sql_sha: "3cf4353ce6c28949b09b6cb3ed0dc22447c500b0cb187f58e9e4918daad78fd2",
        row_sha: "e5a05e54f4636fe6e87eb8094fceda8e002bff799f3de76413a2a77c52fd50b8",
        prefix_sha: "7fba88713c8a50edfd46263480e04c0e1b7646f46d1c629a531584a08a3cec17",
        input: RowInput {
            columns: 1,
            value_bytes: 1048576,
            repeated_byte: b'x',
            cut: 1048577,
            expect_complete_tail: true,
        },
    },
    FrozenCase {
        name: "exact-native-missing-tail-cut-1048577",
        sql: "SELECT REPEAT('q', 1048575 + generate_series) AS c0, REPEAT('q', 1048575 + generate_series) AS c1, REPEAT('q', 1048575 + generate_series) AS c2, REPEAT('q', 1048575 + generate_series) AS c3, REPEAT('q', 1048575 + generate_series) AS c4, REPEAT('q', 1048575 + generate_series) AS c5, REPEAT('q', 1048575 + generate_series) AS c6, REPEAT('q', 1048575 + generate_series) AS c7, REPEAT('q', 1048575 + generate_series) AS c8, REPEAT('q', 1048575 + generate_series) AS c9, REPEAT('q', 1048575 + generate_series) AS c10, REPEAT('q', 1048575 + generate_series) AS c11, REPEAT('q', 1048575 + generate_series) AS c12, REPEAT('q', 1048575 + generate_series) AS c13, REPEAT('q', 1048575 + generate_series) AS c14, REPEAT('q', 1048575 + generate_series) AS c15, REPEAT('q', 1048575 + generate_series) AS c16 FROM generate_series(1, 1)",
        sql_sha: "6366beea12d3c9acf30f1f0ed89424734cbd63b790ba86a527ce07cc82f47236",
        row_sha: "51433c7930844aec5dca09a6dcb7e13e1a98801a483a30b04f9f3be064c9ffb4",
        prefix_sha: "859f61de598f4227197d13f83ebcdbb0a0f5289d983e8dce9975db7a56bf18fb",
        input: RowInput {
            columns: 17,
            value_bytes: 1048576,
            repeated_byte: b'q',
            cut: 1048577,
            expect_complete_tail: false,
        },
    },
    FrozenCase {
        name: "exact-native-resident-cut-1",
        sql: "SELECT 'abc' AS payload FROM generate_series(1, 1)",
        sql_sha: "5bfdb7c3b0d51d81b665e38055b7cf5e298501b35c12d897393087cb38cfd140",
        row_sha: "658094aec4a81dfc97cb374a907ee41f74b1faf9a02f62bbf4f98d6d48c1219f",
        prefix_sha: "e52d9c508c502347344d8c07ad91cbd6068afc75ff6292f062a09ca381c89e71",
        input: RowInput {
            columns: 1,
            value_bytes: 3,
            repeated_byte: b'a',
            cut: 1,
            expect_complete_tail: true,
        },
    },
    FrozenCase {
        name: "exact-native-resident-cut-2",
        sql: "SELECT 'abc' AS payload FROM generate_series(1, 1)",
        sql_sha: "5bfdb7c3b0d51d81b665e38055b7cf5e298501b35c12d897393087cb38cfd140",
        row_sha: "658094aec4a81dfc97cb374a907ee41f74b1faf9a02f62bbf4f98d6d48c1219f",
        prefix_sha: "c0ba8a33ac67f44abff5984dfbb6f56c46b880ac2b86e1f23e7fa9c402c53ae7",
        input: RowInput {
            columns: 1,
            value_bytes: 3,
            repeated_byte: b'a',
            cut: 2,
            expect_complete_tail: true,
        },
    },
    FrozenCase {
        name: "exact-native-resident-cut-3",
        sql: "SELECT 'abc' AS payload FROM generate_series(1, 1)",
        sql_sha: "5bfdb7c3b0d51d81b665e38055b7cf5e298501b35c12d897393087cb38cfd140",
        row_sha: "658094aec4a81dfc97cb374a907ee41f74b1faf9a02f62bbf4f98d6d48c1219f",
        prefix_sha: "3ee0d7c44a58950b18c1e01b912ed0e08549ff905fbf7010318a44d3e2efb8db",
        input: RowInput {
            columns: 1,
            value_bytes: 3,
            repeated_byte: b'a',
            cut: 3,
            expect_complete_tail: true,
        },
    },
    FrozenCase {
        name: "exact-native-resident-cut-4",
        sql: "SELECT 'abc' AS payload FROM generate_series(1, 1)",
        sql_sha: "5bfdb7c3b0d51d81b665e38055b7cf5e298501b35c12d897393087cb38cfd140",
        row_sha: "658094aec4a81dfc97cb374a907ee41f74b1faf9a02f62bbf4f98d6d48c1219f",
        prefix_sha: "8a3044222ef06194ca6ec01b989000752c076c1c1aae1a0d81c82a5c94288b6d",
        input: RowInput {
            columns: 1,
            value_bytes: 3,
            repeated_byte: b'a',
            cut: 4,
            expect_complete_tail: true,
        },
    },
    FrozenCase {
        name: "exact-native-resident-cut-5",
        sql: "SELECT 'abc' AS payload FROM generate_series(1, 1)",
        sql_sha: "5bfdb7c3b0d51d81b665e38055b7cf5e298501b35c12d897393087cb38cfd140",
        row_sha: "658094aec4a81dfc97cb374a907ee41f74b1faf9a02f62bbf4f98d6d48c1219f",
        prefix_sha: "2bd5620604a8d6a6c1b0e76d94714b6b389b580b625632d1a00e1387a077ec9d",
        input: RowInput {
            columns: 1,
            value_bytes: 3,
            repeated_byte: b'a',
            cut: 5,
            expect_complete_tail: true,
        },
    },
    FrozenCase {
        name: "exact-native-resident-cut-6",
        sql: "SELECT 'abc' AS payload FROM generate_series(1, 1)",
        sql_sha: "5bfdb7c3b0d51d81b665e38055b7cf5e298501b35c12d897393087cb38cfd140",
        row_sha: "658094aec4a81dfc97cb374a907ee41f74b1faf9a02f62bbf4f98d6d48c1219f",
        prefix_sha: "36dcb689dea90ebe81b1937491eeb7beec678b5b8bdaf36cd0d0724079900c14",
        input: RowInput {
            columns: 1,
            value_bytes: 3,
            repeated_byte: b'a',
            cut: 6,
            expect_complete_tail: true,
        },
    },
];

/// Explicit root-owned constructor; does not add a permissive registry/default mode.
pub(crate) fn scenarios_from_admitted(
    run: AdmittedExactNativeRun,
) -> Result<Vec<Box<dyn Scenario>>> {
    run.validate_shape()?;
    let run = Arc::new(run);
    CASES
        .iter()
        .map(|case| {
            ensure!(
                hash32(case.sql_sha)? == <[u8; 32]>::from(Sha256::digest(case.sql.as_bytes())),
                "original exact SQL identity differs"
            );
            ensure!(
                case.input.prefix_hash()? == hash32(case.prefix_sha)?,
                "original literal prefix differs"
            );
            case.input.payload_bytes()?;
            Ok(Box::new(ExactNativeCase {
                case: *case,
                run: Arc::clone(&run),
                private: Mutex::new(None),
            }) as Box<dyn Scenario>)
        })
        .collect()
}

struct PrivateControlOwner {
    parent: PathBuf,
    socket: PathBuf,
    nonce: [u8; 16],
    directory_created: bool,
    identity: Option<(u64, u64)>,
    prepared: Option<OriginalPreparedConfig>,
}
impl PrivateControlOwner {
    fn reserve() -> Result<Self> {
        let mut random = [0; 32];
        File::open("/dev/urandom")?.read_exact(&mut random)?;
        let nonce: [u8; 16] = random[..16].try_into().unwrap();
        ensure!(nonce != [0; 16], "zero private control nonce");
        // Locator uses independent random bytes, never the authentication nonce.
        let parent = Path::new("/tmp").join(format!("nr-exact-{}", hex(&random[16..])));
        let socket = parent.join("control.sock");
        ensure!(
            socket.as_os_str().as_encoded_bytes().len() <= 90,
            "private Unix locator is too long"
        );
        Ok(Self {
            parent,
            socket,
            nonce,
            directory_created: false,
            identity: None,
            prepared: None,
        })
    }
    fn initialize(&mut self) -> Result<()> {
        ensure!(
            !self.directory_created,
            "original private parent already created"
        );
        fs::DirBuilder::new().mode(0o700).create(&self.parent)?;
        self.directory_created = true;
        let metadata = fs::symlink_metadata(&self.parent)?;
        ensure!(
            metadata.is_dir() && metadata.mode() & 0o777 == 0o700,
            "private control parent differs"
        );
        self.identity = Some((metadata.dev(), metadata.ino()));
        Ok(())
    }
    fn environment(&self) -> CrossProcessChildEnvironment {
        let mut environment = CrossProcessChildEnvironment::default();
        environment.fe.insert(
            SOCKET_ENV.into(),
            self.socket.to_string_lossy().into_owned(),
        );
        environment.fe.insert(NONCE_ENV.into(), hex(&self.nonce));
        environment
    }
    fn remove_empty_parent(&self) -> Result<()> {
        if !self.directory_created {
            return Ok(());
        }
        let (dev, ino) = self
            .identity
            .context("original private parent identity unavailable; retain owner")?;
        let metadata = fs::symlink_metadata(&self.parent)?;
        ensure!(
            metadata.is_dir()
                && metadata.dev() == dev
                && metadata.ino() == ino
                && metadata.mode() & 0o777 == 0o700,
            "original private parent was replaced"
        );
        // Original FE owns socket removal. A leftover socket is retained, never unlinked here.
        fs::remove_dir(&self.parent).context("remove original empty private control parent")
    }
}
struct ExactNativeCase {
    case: FrozenCase,
    run: Arc<AdmittedExactNativeRun>,
    private: Mutex<Option<PrivateControlOwner>>,
}
impl Scenario for ExactNativeCase {
    fn name(&self) -> &'static str {
        self.case.name
    }
    fn is_explicit_stage(&self) -> bool {
        true
    }
    fn validate_runner_inputs(&self, profile: LaunchProfile) -> Result<()> {
        ensure!(
            profile == LaunchProfile::FaultScenario,
            "exact native fixture requires the fault-scenario launch profile"
        );
        self.run.validate_shape()
    }
    fn launch_config(&self, _root: &Path) -> Result<ScenarioLaunchConfig> {
        let mut owner = self
            .private
            .lock()
            .map_err(|_| anyhow::anyhow!("private launch owner poisoned"))?;
        ensure!(
            owner.is_none(),
            "original private control owner already allocated"
        );
        *owner = Some(PrivateControlOwner::reserve()?);
        owner.as_mut().unwrap().initialize()?;
        Ok(ScenarioLaunchConfig {
            child_environment: owner.as_ref().unwrap().environment(),
            ..Default::default()
        })
    }
    fn test_session_plan(
        &self,
        launch: &ScenarioLaunchConfig,
        _scenario_root: &Path,
        cluster_size: usize,
        binary: &Path,
        original_deadline: Instant,
    ) -> Result<Option<novarocks_cluster_harness::test_sessions::ClusterTestSessionPlan>> {
        self.run.validate_shape()?;
        let private = self
            .private
            .lock()
            .map_err(|_| anyhow::anyhow!("original exact private owner poisoned"))?;
        let owner = private
            .as_ref()
            .context("original exact private owner is absent")?;
        ensure!(
            launch.child_environment == owner.environment(),
            "original exact private launch inputs changed before declaration"
        );
        let target_token = format!("{}-target", self.case.name);
        let health_token = format!("{}-health", self.case.name);
        let mut tokens = vec![target_token.as_str()];
        if self.case.input.expect_complete_tail {
            tokens.push(health_token.as_str());
        }
        Ok(Some(variant_sessions::fixture_plan(
            binary,
            cluster_size,
            original_deadline,
            vec![variant_sessions::grant(
                "mysql-fixture",
                "original-exact-write",
                json!({"action":"exact-write", "socket":owner.socket,
                    "nonce_hex":hex(&owner.nonce)}),
            )],
            "mem-1-m07-exact-mysql-write",
            &[
                "novarocks-server",
                "novarocks-frontend-application",
                "novarocks-mysql-adapter",
            ],
            &tokens,
        )?))
    }
    fn freeze_prepared_exact_config(
        &self,
        artifact: &novarocks_cluster_harness::EffectiveLaunchConfigEvidence,
        root: &Path,
        deadline: Instant,
    ) -> Result<()> {
        let mut owner = self
            .private
            .lock()
            .map_err(|_| anyhow::anyhow!("private launch owner poisoned"))?;
        let owner = owner
            .as_mut()
            .context("original private owner absent before config freeze")?;
        ensure!(
            owner.prepared.is_none(),
            "original prepared config already frozen"
        );
        owner.prepared = Some(OriginalPreparedConfig::freeze(artifact, root, deadline)?);
        Ok(())
    }
    fn run(&self, context: &mut ScenarioContext) -> Result<()> {
        let deadline = context.exact_mysql_deadline()?;
        let original_roles = context.recheck_live_process_launch_identities()?;
        ensure!(
            original_roles.len() == 4,
            "exact native scene requires original four roles"
        );
        let owner = self
            .private
            .lock()
            .map_err(|_| anyhow::anyhow!("private launch owner poisoned"))?;
        let owner = owner.as_ref().context("original private owner absent")?;
        owner
            .prepared
            .as_ref()
            .context("original prepared config was never frozen before role spawn")?
            .verify(context.effective_launch_config_evidence())?;
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;
        context.retain_artifacts();
        let result = runtime.block_on(run_case(context, self.case, owner, deadline, &self.run));
        // Runner, not this future, owns same-FE successful exit then all three BE stops.
        result
    }
    fn teardown(&self) -> Result<()> {
        let mut owner = self
            .private
            .lock()
            .map_err(|_| anyhow::anyhow!("private launch owner poisoned"))?;
        let mut failure = None;
        if let Some(value) = owner.as_ref() {
            if let Some(prepared) = &value.prepared
                && let Err(error) = prepared.verify_original_file()
            {
                retain_secondary(&mut failure, error);
            }
            if let Err(error) = value.remove_empty_parent() {
                retain_secondary(&mut failure, error);
            }
        }
        if let Some(error) = failure {
            return Err(error);
        }
        *owner = None;
        Ok(())
    }
}

/// Fixed test-evidence errors. These are not the FE application's four-box verdict.
struct SceneFailure {
    errors: [Option<anyhow::Error>; 5],
}
impl std::fmt::Debug for SceneFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SceneFailure")
            .field(
                "failed_stages",
                &self.errors.each_ref().map(Option::is_some),
            )
            .finish()
    }
}
impl std::fmt::Display for SceneFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "exact native matrix failed; original primary and cleanup sources retained"
        )
    }
}
impl std::error::Error for SceneFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.errors
            .iter()
            .flatten()
            .next()
            .map(|error| error.as_ref())
    }
}
#[derive(Default)]
struct Receipt {
    controls: [Option<String>; CONTROL_REPLY_SLOTS],
    control_count: usize,
    control_prefixes: [Option<String>; CONTROL_REPLY_SLOTS],
    final_control_prefixes: Option<String>,
    held_roots: Option<[BTreeMap<String, u64>; 3]>,
    root: Option<String>,
    raw_binding: Option<String>,
    reader: Option<ExactReadSnapshot>,
    kill_started_us: Option<u128>,
    kill_returned_us: Option<u128>,
    resumed_us: Option<u128>,
    recovery_before: Option<[u64; 3]>,
    recovery_after: Option<[u64; 3]>,
}
impl Receipt {
    fn reply(&mut self, value: ReplyFacts, client: &UnixControlClient) -> Result<()> {
        ensure!(
            self.control_count < self.controls.len(),
            "fixed control receipt overflow"
        );
        self.controls[self.control_count] = Some(format!("{value:?}"));
        self.control_prefixes[self.control_count] = Some(format!("{:?}", client.last_prefixes()));
        self.control_count += 1;
        Ok(())
    }
}

async fn run_case(
    context: &mut ScenarioContext,
    case: FrozenCase,
    owner: &PrivateControlOwner,
    deadline: Instant,
    run: &AdmittedExactNativeRun,
) -> Result<()> {
    let epoch = Instant::now();
    let mut receipt = Receipt::default();
    let mut reader: Option<ExactResultReader> = None;
    let mut control: Option<UnixControlClient> = None;
    let mut submitted = false;
    let mut kill_attempted = false;
    let mut errors: [Option<anyhow::Error>; 5] = std::array::from_fn(|_| None);
    let primary = async {
        check(deadline)?;
        await_idle(context, deadline).await?;
        let frontend = context
            .handle()
            .original_exact_mysql_frontend_identity(deadline)?;
        let observer = IndependentRootObserver::capture_baseline(context, frontend)?;
        reader = Some(
            match ExactResultReader::connect(
                context.mysql_user(),
                context.mysql_port(),
                case.input,
                deadline,
            )
            .await
            {
                Ok(reader) => reader,
                Err(error) => {
                    receipt.reader = Some(error.observation);
                    return Err(error.into());
                }
            },
        );
        let cid = reader.as_ref().unwrap().connection_id();
        ensure!(cid != 0, "actual handshake connection ID is zero");
        control =
            Some(UnixControlClient::connect(&owner.socket, frontend, owner.nonce, deadline).await?);
        let armed = exchange_until(
            control.as_mut().unwrap(),
            Command::Arm {
                connection_id: cid,
                exact_sql_sha256: hash32(case.sql_sha)?,
                cut_bytes: case.input.cut,
            },
            deadline,
        )
        .await?;
        receipt.reply(armed, control.as_ref().unwrap())?;
        ensure!(
            armed.failure.is_none() && armed.accepted_peers == 1 && armed.used_arm,
            "original Arm failed"
        );
        variant_sessions::arm_statement_until(
            context,
            &format!("{}-target", case.name),
            cid,
            case.sql,
            deadline,
        )?;
        submitted = true; // Even a partially written original command must not skip actual cancellation.
        reader
            .as_mut()
            .unwrap()
            .send_original_query(case.sql)
            .await?;
        let metadata_deadline = phase(deadline, Duration::from_secs(5));
        let metadata = tokio::time::timeout_at(
            metadata_deadline.into(),
            reader.as_mut().unwrap().read_metadata(),
        )
        .await??;
        check(metadata_deadline)?;
        ensure!(
            metadata.columns == case.input.columns
                && metadata.actual_next_sequence == case.input.columns + 3,
            "actual metadata does not precede the frozen row sequence"
        );
        let held_deadline = phase(deadline, Duration::from_secs(5));
        tokio::time::timeout_at(
            held_deadline.into(),
            reader.as_mut().unwrap().read_cut_prefix(),
        )
        .await??;
        let mut target = None;
        let mut consecutive = 0;
        for _ in 0..HELD_SNAPSHOTS {
            let held =
                exchange_until(control.as_mut().unwrap(), Command::Snapshot, held_deadline).await?;
            receipt.reply(held, control.as_ref().unwrap())?;
            ensure!(held.failure.is_none(), "original held control failure");
            let gate = held.gate.context("original gate absent while held")?;
            if gate.phase == GatePhase::Rows
                && gate.blocked_after_acceptance
                && gate.accepted_prefix_bytes == case.input.cut
                && gate.accepted_prefix_sha256 == hash32(case.prefix_sha)?
            {
                if target.is_none() {
                    target = Some(observer.observe(context)?);
                }
                let census = census(context, held_deadline).await?;
                let qualifies = held_census(case.input, target.as_ref().unwrap(), &census.roots)?;
                receipt.held_roots = Some(census.roots);
                consecutive = if qualifies { consecutive + 1 } else { 0 };
                if consecutive == 2 {
                    break;
                }
            } else {
                consecutive = 0;
            }
            sleep_until_sample(held_deadline).await?;
        }
        ensure!(
            consecutive == 2,
            "original held root census never qualified twice"
        );
        let target = target.context("independent fresh actual root absent")?;
        let raw =
            context
                .handle()
                .with_original_frontend_log_snapshot(deadline, |reader, length| {
                    exact_mysql_target_binding::scan(
                        reader,
                        length,
                        frontend,
                        cid,
                        hash32(case.sql_sha)?,
                        deadline,
                    )
                })?;
        let binding = ActualBinding::from_original_sources(raw, &target)?;
        receipt.root = Some(format!("{:?}", target.root));
        receipt.raw_binding = Some(format!("{raw:?}"));
        receipt.kill_started_us = Some(epoch.elapsed().as_micros());
        kill_attempted = true; // Unknown/failed KILL outcome never admits a second KILL attempt.
        context.handle().kill_query_until(cid, deadline)?;
        receipt.kill_returned_us = Some(epoch.elapsed().as_micros());
        check(deadline)?;
        // Same original scene clock; 2s observes after KILL return, not production Closing origin.
        let resume_deadline = phase(deadline, Duration::from_secs(2));
        let mut resumed = None;
        for _ in 0..RESUME_SNAPSHOTS {
            let reply = exchange_until(
                control.as_mut().unwrap(),
                Command::Snapshot,
                resume_deadline,
            )
            .await?;
            receipt.reply(reply, control.as_ref().unwrap())?;
            ensure!(
                reply.failure.is_none(),
                "original cancellation control failure"
            );
            if reply
                .gate
                .is_some_and(|gate| gate.phase == GatePhase::Resumed)
                && reply.original_freeze.is_some()
            {
                compare_cancel(case.input, &binding, &reply)?;
                receipt.resumed_us = Some(epoch.elapsed().as_micros());
                resumed = Some(reply);
                break;
            }
            sleep_until_sample(resume_deadline).await?;
        }
        ensure!(
            resumed.is_some(),
            "original local Resume/freeze absent within the original observation clock"
        );
        if case.input.expect_complete_tail {
            reader
                .as_mut()
                .unwrap()
                .complete_row_then_interrupted(hash32(case.row_sha)?)
                .await?;
        } else {
            let observed = reader.as_mut().unwrap().read_missing_tail_to_eof().await?;
            ensure!(
                observed.missing_tail_read_eof
                    && !observed.row_complete
                    && observed.row_wire_bytes >= case.input.cut
                    && observed.row_wire_bytes > 4,
                "original missing-tail did not produce a validated partial row and physical EOF"
            );
        }
        // Root census zero proves only context-held owner convergence, never final allocator alias.
        await_idle(context, deadline).await?;
        let before = census(context, deadline).await?.created;
        receipt.recovery_before = Some(before);
        if case.input.expect_complete_tail {
            variant_sessions::arm_statement_until(
                context,
                &format!("{}-health", case.name),
                reader.as_ref().unwrap().connection_id(),
                HEALTH_SQL,
                deadline,
            )?;
            health(reader.as_mut().unwrap()).await?;
            await_idle(context, deadline).await?;
            let after = census(context, deadline).await?.created;
            ensure!(
                after
                    .iter()
                    .zip(before)
                    .all(|(after, before)| *after >= before)
                    && after
                        .iter()
                        .zip(before)
                        .any(|(after, before)| *after > before),
                "same-socket health had no actual native task growth"
            );
            receipt.recovery_after = Some(after);
        } else {
            let followup = reader
                .as_mut()
                .unwrap()
                .probe_zero_response_followup(HEALTH_SQL)
                .await?;
            ensure!(
                followup.followup_write_complete
                    && followup.followup_read_eof
                    && followup.followup_response_bytes == 0,
                "same-socket missing-tail followup was not exact zero-response EOF"
            );
            let after = census(context, deadline).await?.created;
            ensure!(
                after == before,
                "poisoned original socket created new native tasks"
            );
            receipt.recovery_after = Some(after);
        }
        check(deadline)?;
        Ok::<(), anyhow::Error>(())
    }
    .await;
    errors[0] = primary.err();
    // A failed held probe still asks original query control to cancel this actual handshake.
    // No external Resume command exists; original cancellation records and locally resumes.
    if submitted
        && !kill_attempted
        && let Some(reader) = reader.as_ref()
        && let Err(error) = context
            .handle()
            .kill_query_until(reader.connection_id(), deadline)
    {
        errors[1] = Some(error);
    }
    if let Some(reader) = reader.as_mut() {
        receipt.reader = Some(reader.snapshot());
        if let Err(error) = reader.shutdown_original_socket().await {
            errors[2] = Some(error.into());
        }
        receipt.reader = Some(reader.snapshot());
    }
    // Drop the one original socket even when the original clock is already expired.
    drop(reader.take());
    if let Some(control) = control.as_mut() {
        // At most two remaining actual snapshots; reserved Stop always stays within sixteen.
        // This observes original writer exit, never treats client-side shutdown as that fact.
        if errors[0].is_none() {
            let exited = async {
                for _ in 0..FINAL_EXIT_SNAPSHOTS {
                    let reply = exchange_until(control, Command::Snapshot, deadline).await?;
                    receipt.reply(reply, control)?;
                    ensure!(
                        reply.failure.is_none(),
                        "original writer exit observation failed"
                    );
                    if reply.original_writer_exited {
                        return Ok::<(), anyhow::Error>(());
                    }
                    sleep_until_sample(deadline).await?;
                }
                bail!("original writer had not exited in fixed final observation positions")
            }
            .await;
            if let Err(error) = exited {
                errors[3] = Some(error);
            }
        }
        match exchange_until(control, Command::Stop, deadline).await {
            Ok(reply) => {
                if let Err(error) = (|| -> Result<()> {
                    receipt.reply(reply, control)?;
                    ensure!(
                        reply.explicit_stop
                            && reply.stopped
                            && reply.used_arm
                            && reply.failure.is_none()
                            && reply.original_writer_exited
                            && reply.accepted_peers == 1,
                        "original Stop did not preserve successful actual writer exit"
                    );
                    Ok(())
                })() {
                    retain_secondary(&mut errors[3], error);
                }
            }
            Err(error) => retain_secondary(&mut errors[3], error),
        }
        // This is a physical close, never a fabricated successful Stop/reply.
        if let Some(error) = control.close_incomplete() {
            retain_secondary(&mut errors[3], error.into());
        }
        receipt.final_control_prefixes = Some(format!("{:?}", control.last_prefixes()));
    }
    drop(control.take());
    if let Err(error) = save_receipt(context, case, run, &receipt, &errors, deadline) {
        errors[4] = Some(error);
    }
    if errors.iter().any(Option::is_some) {
        Err(SceneFailure { errors }.into())
    } else {
        Ok(())
    }
}

async fn exchange_until(
    client: &mut UnixControlClient,
    command: Command,
    deadline: Instant,
) -> Result<ReplyFacts> {
    check(deadline)?;
    let result = tokio::time::timeout_at(deadline.into(), client.exchange(command)).await;
    match result {
        Ok(result) => {
            let reply = result?;
            check(deadline)?;
            Ok(reply)
        }
        Err(elapsed) => {
            let primary: anyhow::Error = elapsed.into();
            match client.close_incomplete() {
                Some(secondary) => Err(SecondaryFailure {
                    primary,
                    secondary: secondary.into(),
                }
                .into()),
                None => Err(primary),
            }
        }
    }
}
fn check(deadline: Instant) -> Result<()> {
    ensure!(
        Instant::now() < deadline,
        "original prelaunch exact native clock expired"
    );
    Ok(())
}
fn phase(original: Instant, length: Duration) -> Instant {
    original.min(Instant::now() + length)
}
async fn sleep_until_sample(deadline: Instant) -> Result<()> {
    check(deadline)?;
    tokio::time::sleep(
        Duration::from_millis(100).min(deadline.saturating_duration_since(Instant::now())),
    )
    .await;
    check(deadline)
}
fn hash32(value: &str) -> Result<[u8; 32]> {
    ensure!(
        value.len() == 64
            && value
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b)),
        "invalid fixed SHA256 identity"
    );
    let mut bytes = [0; 32];
    for (index, byte) in bytes.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&value[index * 2..index * 2 + 2], 16)?;
    }
    Ok(bytes)
}
fn hex(value: &[u8]) -> String {
    value.iter().map(|byte| format!("{byte:02x}")).collect()
}

struct Census {
    roots: [BTreeMap<String, u64>; 3],
    created: [u64; 3],
    backend_idle: [bool; 3],
}
async fn http_json(port: u16, path: &str, deadline: Instant) -> Result<Value> {
    check(deadline)?;
    let request_deadline = deadline.min(Instant::now() + Duration::from_millis(500));
    let result = tokio::time::timeout_at(request_deadline.into(), async {
        let client = reqwest::Client::builder()
            .timeout(request_deadline.saturating_duration_since(Instant::now()))
            .build()?;
        let mut response = client
            .get(format!("http://127.0.0.1:{port}{path}"))
            .send()
            .await?
            .error_for_status()?;
        if let Some(length) = response.content_length() {
            ensure!(
                length <= HTTP_BYTES,
                "public observation body exceeds fixed bound"
            );
        }
        let mut bytes = Vec::new();
        while let Some(chunk) = response.chunk().await? {
            ensure!(
                chunk.len() as u64 <= HTTP_BYTES - bytes.len() as u64,
                "public observation body exceeds fixed bound"
            );
            bytes.extend_from_slice(&chunk);
            check(request_deadline)?;
        }
        check(request_deadline)?;
        Ok::<_, anyhow::Error>(serde_json::from_slice(&bytes)?)
    })
    .await??;
    check(deadline)?;
    Ok(result)
}
async fn census(context: &mut ScenarioContext, deadline: Instant) -> Result<Census> {
    let ports: [u16; 3] = context
        .handle()
        .runtime()
        .be
        .iter()
        .map(|backend| backend.http)
        .collect::<Vec<_>>()
        .try_into()
        .map_err(|_| anyhow::anyhow!("public census requires actual three backends"))?;
    let mut roots: [BTreeMap<String, u64>; 3] = std::array::from_fn(|_| BTreeMap::new());
    let mut created = [0; 3];
    let mut backend_idle = [false; 3];
    for index in 0..3 {
        let value = http_json(ports[index], "/metrics?type=json", deadline).await?;
        let rows = value
            .as_array()
            .context("public backend observation is not an array")?;
        roots[index] = root_census(rows)?.context("public context-held census unavailable/busy")?;
        created[index] = metric(
            rows,
            "novarocks_backend_task_execution_tasks_created_total",
            &[],
        )?;
        let mut idle = metric(
            rows,
            "novarocks_backend_worker_context_reservations",
            &[("dimension", "used")],
        )? == 0
            && metric(
                rows,
                "novarocks_backend_worker_reservation_last_published_unixtime_seconds",
                &[],
            )? > 0;
        for class in ["ordinary", "control", "root_result"] {
            for phase in ["running", "waiting"] {
                idle &= metric(
                    rows,
                    "novarocks_backend_native_ingress_slots",
                    &[("class", class), ("phase", phase), ("dimension", "used")],
                )? == 0;
            }
        }
        backend_idle[index] = idle;
    }
    check(deadline)?;
    Ok(Census {
        roots,
        created,
        backend_idle,
    })
}
fn held_census(
    input: RowInput,
    target: &IndependentRootTarget,
    roots: &[BTreeMap<String, u64>; 3],
) -> Result<bool> {
    ensure!(
        target.root_backend_index < 3,
        "independent root backend is invalid"
    );
    for (index, root) in roots.iter().enumerate() {
        if index != target.root_backend_index && root.values().any(|value| *value != 0) {
            return Ok(false);
        }
    }
    let root = &roots[target.root_backend_index];
    qualifies_root(input, root)
}
fn qualifies_root(input: RowInput, root: &BTreeMap<String, u64>) -> Result<bool> {
    let common = root["channels"] == 1 && root["ends_acknowledged"] == 0 && root["sealed"] == 0;
    if !common {
        return Ok(false);
    }
    if !input.expect_complete_tail {
        return Ok(root["data_positions"] == 2
            && root["payload_bytes"] == 2 * S
            && root["producers_running"] == 1
            && root["producers_exited"] == 0
            && root["ends_published"] == 0
            && root["segments"] >= 2);
    }
    let native_bytes = input.payload_bytes()? + 4;
    // Original tiny row is exactly 8 native bytes, so a synthetic second Data is forbidden.
    // Native and MySQL row prefixes both occupy four bytes for these original
    // one-column inputs. At S the first Data has been consumed and retired;
    // W=2 remains the capacity, not a requirement to retain that consumed item.
    let retired = u64::from(native_bytes > S && input.cut >= S);
    let data = if native_bytes <= S { 1 } else { 2 - retired };
    let retained_bytes = native_bytes - retired * S;
    Ok(root["data_positions"] == data
        && root["payload_bytes"] == retained_bytes
        && root["segments"] == data
        && root["producers_running"] == 0
        && root["producers_exited"] == 1
        && root["terminal_task_records"] == 1
        && root["ends_published"] == 1)
}
async fn await_idle(context: &mut ScenarioContext, deadline: Instant) -> Result<()> {
    for _ in 0..IDLE_SAMPLES {
        let state = http_json(context.fe_http_port(), "/v1/frontend/state", deadline).await?;
        let governance = &state["workload"]["governance"];
        let windows = governance["result_window_positions"]
            .as_array()
            .context("missing public window dimensions")?;
        ensure!(windows.len() == 4, "invalid public window dimensions");
        let mut idle = windows
            .iter()
            .map(|value| value.as_u64().context("invalid public window count"))
            .collect::<Result<Vec<_>>>()?
            .iter()
            .all(|value| *value == 0);
        for name in [
            "root_responsibilities",
            "admitted_queries",
            "preparation",
            "execution",
            "old_attempts",
            "unknown_creates",
            "obligations",
            "waiting_records",
            "control_ready",
            "control_inflight",
        ] {
            idle &= governance[name]
                .as_u64()
                .context("missing public owner dimension")?
                == 0;
        }
        for name in ["statement", "background"] {
            idle &= state["workload"]["active"][name]
                .as_u64()
                .context("missing public active-work dimension")?
                == 0;
        }
        let backends = census(context, deadline).await?;
        idle &= backends.backend_idle.iter().all(|value| *value)
            && backends
                .roots
                .iter()
                .all(|root| root.values().all(|value| *value == 0));
        check(deadline)?;
        if idle {
            return Ok(());
        }
        sleep_until_sample(deadline).await?;
    }
    bail!("public owners did not converge in the original bounded sample positions")
}

fn field<'a>(payload: &'a [u8], cursor: &mut usize) -> Result<&'a [u8]> {
    let prefix = *payload
        .get(*cursor)
        .context("truncated health field length")?;
    *cursor += 1;
    let length = match prefix {
        0..=250 => prefix as usize,
        252 => {
            let raw = payload
                .get(*cursor..*cursor + 2)
                .context("truncated health field length")?;
            *cursor += 2;
            let value = u16::from_le_bytes(raw.try_into().unwrap()) as usize;
            ensure!(value >= 251, "noncanonical health field length");
            value
        }
        _ => bail!("unsupported health metadata field length"),
    };
    let value = payload
        .get(
            *cursor
                ..cursor
                    .checked_add(length)
                    .context("health field length overflow")?,
        )
        .context("truncated health metadata field")?;
    *cursor += length;
    Ok(value)
}
fn health_column(payload: &[u8]) -> Result<()> {
    let mut cursor = 0;
    let mut fields = [&[][..]; 6];
    for value in &mut fields {
        *value = field(payload, &mut cursor)?;
    }
    ensure!(
        fields[0] == b"def" && fields[4] == b"total",
        "health schema name/catalog differs"
    );
    let fixed = payload
        .get(cursor..)
        .context("health fixed metadata absent")?;
    ensure!(
        fixed.len() == 13 && fixed[0] == 12 && fixed[7] == 8 && fixed[11..] == [0, 0],
        "health ColumnDefinition41 type/structure differs"
    );
    Ok(())
}
fn health_eof(payload: &[u8]) -> Result<()> {
    ensure!(
        payload.len() == 5 && payload[0] == 0xfe,
        "health EOF framing differs"
    );
    let status = u16::from_le_bytes([payload[3], payload[4]]);
    ensure!(
        status & 8 == 0,
        "health result unexpectedly has more results"
    );
    Ok(())
}
async fn health(reader: &mut ExactResultReader) -> Result<()> {
    reader.begin_original_health_query(HEALTH_SQL).await?;
    let mut total = 0;
    let mut row_sha = None;
    for sequence in 1..=5 {
        let packet = reader.read_original_health_packet().await?;
        ensure!(
            packet.sequence == sequence,
            "health packet sequence differs"
        );
        total += packet.payload().len() + 4;
        ensure!(
            total <= 4096,
            "health total wire exceeds original small-result bound"
        );
        match sequence {
            1 => ensure!(packet.payload() == [1], "health column count differs"),
            2 => health_column(packet.payload())?,
            3 | 5 => health_eof(packet.payload())?,
            4 => {
                ensure!(
                    packet.payload() == b"\x045050",
                    "health row is not literal 5050"
                );
                let mut hash = Sha256::new();
                hash.update(packet.payload());
                hash.update(5u64.to_le_bytes());
                row_sha = Some(<[u8; 32]>::from(hash.finalize()));
            }
            _ => unreachable!(),
        }
    }
    ensure!(
        row_sha == Some(hash32(HEALTH_ROW_SHA)?),
        "independent native health row hash differs"
    );
    Ok(())
}

/// Retain both actual errors without formatting either source into a new string.
struct SecondaryFailure {
    primary: anyhow::Error,
    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "Retain the original secondary cleanup failure without formatting it."
        )
    )]
    secondary: anyhow::Error,
}
impl std::fmt::Debug for SecondaryFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("SecondaryFailure { actual_sources_retained: true }")
    }
}
impl std::fmt::Display for SecondaryFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("original primary and secondary cleanup errors retained")
    }
}
impl std::error::Error for SecondaryFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.primary.as_ref())
    }
}
fn retain_secondary(slot: &mut Option<anyhow::Error>, secondary: anyhow::Error) {
    *slot = Some(match slot.take() {
        Some(primary) => SecondaryFailure { primary, secondary }.into(),
        None => secondary,
    });
}
/// Finite type facts only. Never call arbitrary source Display/Debug while saving.
fn safe_error_fact(error: &anyhow::Error) -> Value {
    if let Some(failure) =
        error.downcast_ref::<crate::actors::exact_mysql_control_v2::ClientFailure>()
    {
        return json!({"class":"UnixControlRetained", "finite_facts":format!("{failure:?}")});
    }
    if let Some(failure) =
        error.downcast_ref::<crate::actors::mysql_stream::exact_result_reader::ExactReadFailure>()
    {
        return json!({"class":"OriginalMysqlReadRetained", "observation":format!("{:?}",failure.observation)});
    }
    if let Some(io) = error.downcast_ref::<std::io::Error>() {
        return json!({"class":"OriginalIoRetained", "kind":format!("{:?}",io.kind()), "raw_os_error":io.raw_os_error()});
    }
    json!({"class":"OtherActualSourceRetained", "raw_formatter_invoked":false})
}
fn save_receipt(
    context: &mut ScenarioContext,
    case: FrozenCase,
    run: &AdmittedExactNativeRun,
    receipt: &Receipt,
    errors: &[Option<anyhow::Error>; 5],
    deadline: Instant,
) -> Result<()> {
    settle_original_receipt(deadline, |within_original_clock| {
        let roles = context.process_launch_identities();
        let value = json!({
            "schema_version":1, "case":case.name, "native_acceptance":false,
        "effective_launch_config_bytes":context.effective_launch_config_evidence().artifact_bytes().len(),
        "effective_launch_config_sha256":context.effective_launch_config_evidence().artifact_sha256(),
        "effective_launch_config_semantics_sha256":context.effective_launch_config_evidence().semantics_sha256(),
        "original_prelaunch_config_artifact":"exact-native-effective-launch-config.json",
            "operation_before_receipt_pass":errors.iter().all(Option::is_none) && within_original_clock,
            "receipt_settlement":"requires successful original deadline postcheck after write and sync",
            "all_four_role_exit":"requires runner's later settled successful schema5 evidence",
            "admitted_provenance":{
                "clean_revision":run.clean_revision, "source_tree_sha256":run.source_tree_sha256,
                "server_binary_sha256":run.server_binary_sha256, "server_build_identity":run.server_build_identity,
                "runner_binary_sha256":run.runner_binary_sha256, "base_config_sha256":run.base_config_sha256,
                "frozen_execution_binding_sha256":run.frozen_execution_binding_sha256,
                "large_input_sha256":run.large_input_sha256, "tiny_input_sha256":run.tiny_input_sha256 },
            "original_launch_identities":{"frontend":roles.0,"backends":roles.1},
            "exact_sql_sha256":case.sql_sha, "cut_row_wire_bytes":case.input.cut,
            "expected_row_sha256":case.row_sha, "expected_prefix_sha256":case.prefix_sha,
            "controls":receipt.controls, "control_prefixes":receipt.control_prefixes,
            "final_control_prefixes":receipt.final_control_prefixes, "held_roots":receipt.held_roots,
            "independent_root":receipt.root, "raw_original_binding":receipt.raw_binding,
            "reader":receipt.reader.map(|reader| format!("{reader:?}")),
            "kill_started_us":receipt.kill_started_us,"kill_returned_us":receipt.kill_returned_us,
            "resumed_us":receipt.resumed_us,"timing_origin":"scene operation entry; all deadlines use original prelaunch absolute Instant",
            "recovery_before_tasks_created":receipt.recovery_before,"recovery_after_tasks_created":receipt.recovery_after,
            "errors":errors.each_ref().map(|error|error.as_ref().map(safe_error_fact)),
            "scope":"original framing/source facts + wire + context-held/public owner convergence; excludes allocator last alias, fullClosing64, lateACKalias"
        });
        let bytes = serde_json::to_vec_pretty(&value)?;
        ensure!(
            bytes.len() <= RECEIPT_BYTES,
            "fixed scene evidence exceeds bound"
        );
        let mut file = File::create(
            context
                .scenario_root()
                .join("exact-native-case-observation.json"),
        )?;
        file.write_all(&bytes)?;
        file.sync_all()?;
        Ok(())
    })
}

fn settle_original_receipt(
    deadline: Instant,
    write: impl FnOnce(bool) -> Result<()>,
) -> Result<()> {
    let mut failure = check(deadline).err();
    // Preserve failed diagnostic receipts after expiry, but never admit late success.
    if let Err(error) = write(failure.is_none()) {
        retain_secondary(&mut failure, error);
    }
    if let Err(error) = check(deadline) {
        retain_secondary(&mut failure, error);
    }
    match failure {
        Some(error) => Err(error),
        None => Ok(()),
    }
}

#[cfg(test)]
#[path = "exact_mysql_native_driver_tests.rs"]
mod tests;
