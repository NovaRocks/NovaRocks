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
}

#[derive(Debug, Default, Clone)]
pub struct QueryMeta {
    pub order_sensitive: Option<bool>,
    pub float_epsilon: Option<f64>,
    pub db: Option<String>,
    pub expect_error: Option<String>,
    pub result_contains: Vec<String>,
    pub result_contains_any: Vec<String>,
    pub result_not_contains: Vec<String>,
    pub tags: Vec<String>,
    pub skip_result_check: bool,
    pub retry_count: Option<usize>,
    pub retry_interval_ms: Option<u64>,
    /// After the step SQL executes, poll `SHOW ALTER TABLE COLUMN` until FINISHED.
    /// Value is the table name.
    pub wait_alter_column: Option<String>,
    /// After the step SQL executes, poll `SHOW ALTER TABLE ROLLUP` until FINISHED.
    /// Value is the table name.
    pub wait_alter_rollup: Option<String>,
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
