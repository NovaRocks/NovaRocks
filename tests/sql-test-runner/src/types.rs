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

use crate::suite_manifest::SuiteManifest;
use std::collections::HashMap;
use std::path::PathBuf;
use std::time::Duration;

#[derive(Debug, Clone)]
pub struct SuiteConfig {
    pub name: String,
    pub sql_dir: PathBuf,
    pub result_dir: Option<PathBuf>,
    pub sql_glob: String,
    pub default_catalog: String,
    pub default_db: String,
    pub auto_case_db: bool,
    pub verify_default: bool,
    pub init_sql: Option<PathBuf>,
    pub cleanup_sql: Option<PathBuf>,
    pub manifest: SuiteManifest,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum ImvStatelessLevel {
    Baseline,
    Package,
    Provenance,
    Full,
}

impl ImvStatelessLevel {
    /// Render as the string form expected by the server-side
    /// `novarocks_imv_stateless_rebuild` procedure's `level` argument, and
    /// returned (case-insensitively) as its `AvailableLevel` result column.
    pub fn as_sql(&self) -> &'static str {
        match self {
            ImvStatelessLevel::Baseline => "baseline",
            ImvStatelessLevel::Package => "package",
            ImvStatelessLevel::Provenance => "provenance",
            ImvStatelessLevel::Full => "full",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ImvStatelessDirective {
    pub mv: String,
    pub level: ImvStatelessLevel,
    /// Catalog that hosts the `system.novarocks_imv_stateless_rebuild`
    /// procedure and the target MV. Defaults to `ice` when unset, so
    /// REST-catalog cases can omit it; per-case hadoop catalogs (e.g.
    /// `mv_ice_${uuid0}`) must set it explicitly.
    pub catalog: Option<String>,
}

#[derive(Debug, Default, Clone)]
pub struct QueryMeta {
    pub order_sensitive: Option<bool>,
    pub float_epsilon: Option<f64>,
    pub db: Option<String>,
    pub expect_error: Option<String>,
    pub expect_error_code: Option<String>,
    pub result_contains: Vec<String>,
    pub result_contains_any: Vec<String>,
    pub result_not_contains: Vec<String>,
    pub explain_contains: Vec<String>,
    pub explain_not_contains: Vec<String>,
    pub normalize_explain_timing: bool,
    pub tags: Vec<String>,
    pub skip_result_check: bool,
    pub retry_count: Option<usize>,
    pub retry_interval_ms: Option<u64>,
    pub kill_be_index: Option<usize>,
    pub kill_be_after_fragment_start: Option<usize>,
    pub fail_fragment_after_start_be_index: Option<usize>,
    pub network_partition_be: Option<usize>,
    pub heartbeat_delay_ms: Option<u64>,
    pub restart_be_delay_ms: Option<u64>,
    /// Restart the runner-owned frontend after this statement has succeeded.
    /// This is intentionally a post-statement control-plane failure, not a
    /// query-lifecycle injection, so durable background work can be recovered.
    pub restart_fe_after_step: bool,
    pub drop_next_init_ack_be_index: Option<usize>,
    pub stop_query_control_heartbeat_be_index: Option<usize>,
    pub kill_fe_after_control_ready_count: Option<usize>,
    pub restart_be_after_init_ack_index: Option<usize>,
    /// Execute KILL QUERY from a separate client after this query's Nth ControlReady.
    pub kill_query_after_control_ready_count: Option<usize>,
    /// Fail the local StageFragments build at this one-based fragment ordinal.
    pub fail_stage_prepare_ordinal: Option<usize>,
    pub drop_next_stage_ack_be_index: Option<usize>,
    pub drop_next_start_ack_be_index: Option<usize>,
    pub suppress_start_ack_be_index: Option<usize>,
    /// Store the immutable terminal snapshot but deliberately omit the stream
    /// ACK for this participant, requiring BE unary fallback delivery.
    pub drop_next_terminal_ack_be_index: Option<usize>,
    /// Close one BE's control stream immediately before TerminalSnapshot so
    /// the immutable payload can only reach FE through unary fallback.
    pub drop_terminal_snapshot_stream_be_index: Option<usize>,
    /// Inject a second valid-but-different participant snapshot at FE ingress
    /// before ACK, proving same-identity conflicts fail closed.
    pub terminal_snapshot_conflict_be_index: Option<usize>,
    pub kill_query_at_lifecycle_phase: Option<QueryLifecyclePhase>,
    pub kill_fe_at_lifecycle_phase: Option<QueryLifecyclePhase>,
    pub stop_query_control_heartbeat_after_stage_be_index: Option<usize>,
    pub hold_start_until_early_ingress: bool,
    pub query_control_fragment_backend_limit: Option<usize>,
    /// After the step SQL executes, poll `SHOW ALTER TABLE COLUMN` until FINISHED.
    /// Value is the table name.
    pub wait_alter_column: Option<String>,
    /// After the step SQL executes, poll `SHOW ALTER TABLE ROLLUP` until FINISHED.
    /// Value is the table name.
    pub wait_alter_rollup: Option<String>,
    /// After the step SQL executes, poll `SHOW ALTER TABLE OPTIMIZE` until FINISHED.
    /// Value is the table name.
    pub wait_alter_optimize: Option<String>,
    /// After the step SQL executes (verify mode), assert the named MV's
    /// incremental contents equal a full recompute derived by running the MV's
    /// SelectText (from `SHOW MATERIALIZED VIEWS`) directly against its base
    /// tables. Value is the MV name (qualified by the step/case db like
    /// wait_alter_*).
    pub imv_equivalence_check: Option<String>,
    /// After the step SQL executes (verify mode), assert that the named MV
    /// can be rebuilt statelessly at the requested fidelity level (default
    /// `Package`) — i.e. its lake-native metadata is sufficient to reproduce
    /// current contents without relying on in-process incremental state.
    pub imv_stateless_rebuild: Option<ImvStatelessDirective>,
    /// Require a substring to occur in at least one runner-owned BE log.
    pub be_log_contains: Vec<String>,
    /// Reject a substring if it occurs in any runner-owned BE log after this step began.
    pub be_log_not_contains: Vec<String>,
    /// Require the total non-overlapping substring count across all BE logs.
    pub be_log_count_at_least: Vec<(String, usize)>,
    /// Require a substring to appear in at least this many distinct BE logs.
    pub be_log_be_count_at_least: Vec<(String, usize)>,
    /// Prove exact accepted/cancelled fragment identity equality for the injected query.
    ///
    /// The value is the exact number of runner-owned BE logs that must contribute
    /// at least one accepted fragment for that query.
    pub be_log_exact_fragment_cancellation: Option<usize>,
}

/// Runner-visible QLC-3 startup boundaries. The backend and frontend consume
/// the matching runner-owned fault files; the runner keeps this enum typed so
/// malformed SQL directives fail during parsing instead of at execution time.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueryLifecyclePhase {
    Staging,
    Staged,
    Starting,
    Running,
    TerminalRetained,
}

impl QueryLifecyclePhase {
    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "staging" => Some(Self::Staging),
            "staged" => Some(Self::Staged),
            "starting" => Some(Self::Starting),
            "running" => Some(Self::Running),
            "terminal-retained" => Some(Self::TerminalRetained),
            _ => None,
        }
    }

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Staging => "staging",
            Self::Staged => "staged",
            Self::Starting => "starting",
            Self::Running => "running",
            Self::TerminalRetained => "terminal-retained",
        }
    }
}

impl QueryMeta {
    pub fn has_be_log_directives(&self) -> bool {
        !self.be_log_contains.is_empty()
            || !self.be_log_not_contains.is_empty()
            || !self.be_log_count_at_least.is_empty()
            || !self.be_log_be_count_at_least.is_empty()
            || self.be_log_exact_fragment_cancellation.is_some()
    }
}

#[derive(Debug, Clone)]
pub struct SqlStep {
    pub query_number: usize,
    pub sql: String,
    pub meta: QueryMeta,
}

#[derive(Debug, Clone)]
pub struct SqlCase {
    pub source_file: PathBuf,
    pub case_id: String,
    pub steps: Vec<SqlStep>,
    /// Resolved per-case database names detected from `${case_db}` / `${case_db_N}` placeholders.
    /// Index 0 is the primary (`${case_db}`), subsequent entries are `${case_db_2}`, etc.
    /// Empty when the case does not use per-case database isolation.
    pub case_dbs: Vec<String>,
    /// When true, this case must run sequentially (not in parallel with other cases).
    /// Set by file-level `@sequential = true` metadata or a legacy `-- name: ... @sequential` tag.
    pub sequential: bool,
}

#[derive(Debug, Clone)]
pub struct ConnectionConfig {
    pub mysql: String,
    pub host: String,
    pub port: String,
    pub user: String,
    pub password: Option<String>,
    pub catalog: Option<String>,
    pub db: Option<String>,
}


#[derive(Debug, Clone)]
pub struct QueryExecution {
    pub header: Vec<String>,
    pub rows: Vec<Vec<String>>,
    pub text_output: String,
    pub elapsed: Duration,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ResultSet {
    pub header: Vec<String>,
    pub rows: Vec<Vec<String>>,
}

#[derive(Debug, Clone)]
pub struct SuiteHook {
    pub path: PathBuf,
    pub sql: String,
    pub catalog: Option<String>,
    pub db: Option<String>,
}

#[derive(Debug, Default, Clone)]
pub struct RunnerConfig {
    pub path: Option<PathBuf>,
    pub values: HashMap<String, String>,
    pub cluster: HashMap<String, String>,
}
