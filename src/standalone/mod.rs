use std::sync::OnceLock;

pub(crate) mod catalog;
pub(crate) mod coordinator;
mod engine;
pub(crate) mod hadoop_catalog;
pub(crate) mod iceberg;
pub(crate) mod iceberg_add_files;
pub(crate) mod iceberg_s3_storage;
mod server;
pub(crate) mod store;

pub use crate::sql::catalog::{ColumnDef, TableDef, TableStorage};
pub(crate) use engine::StandaloneStreamLoadRequest;
pub use engine::{
    QueryResult, QueryResultColumn, StandaloneNovaRocks, StandaloneOptions, StandaloneSession,
};
pub use server::{StandaloneServerOptions, StandaloneTableConfig, run_standalone_server};

fn stream_load_engine_cell() -> &'static OnceLock<StandaloneNovaRocks> {
    static ENGINE: OnceLock<StandaloneNovaRocks> = OnceLock::new();
    &ENGINE
}

pub(crate) fn register_stream_load_engine(engine: StandaloneNovaRocks) {
    let _ = stream_load_engine_cell().set(engine);
}

pub(crate) fn current_stream_load_engine() -> Option<StandaloneNovaRocks> {
    stream_load_engine_cell().get().cloned()
}
