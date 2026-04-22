use std::sync::OnceLock;

mod engine;
pub(crate) mod iceberg;
pub(crate) mod lake;
mod server;

pub use crate::runtime::query_result::{QueryResult, QueryResultColumn};
pub use crate::sql::catalog::{ColumnDef, TableDef, TableStorage};
pub(crate) use engine::StandaloneStreamLoadRequest;
pub use engine::{
    StandaloneManagedTableInfo, StandaloneManagedTabletInfo, StandaloneNovaRocks,
    StandaloneOptions, StandaloneSession,
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
