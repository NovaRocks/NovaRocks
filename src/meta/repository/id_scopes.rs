use crate::meta::{IdScope, MetaError};

pub fn starrocks_db() -> IdScope {
    stable("starrocks.db")
}

pub fn starrocks_table() -> IdScope {
    stable("starrocks.table")
}

pub fn starrocks_partition() -> IdScope {
    stable("starrocks.partition")
}

pub fn starrocks_index() -> IdScope {
    stable("starrocks.index")
}

pub fn starrocks_tablet() -> IdScope {
    stable("starrocks.tablet")
}

pub fn starrocks_txn() -> IdScope {
    stable("starrocks.txn")
}

pub fn mv_id() -> IdScope {
    stable("mv.id")
}

pub fn refresh_id() -> IdScope {
    stable("refresh.id")
}

pub fn erase_job() -> IdScope {
    stable("job.erase")
}

pub fn iceberg_optimize_job() -> IdScope {
    stable("job.iceberg_optimize")
}

pub fn iceberg_operation() -> IdScope {
    stable("iceberg.operation")
}

pub fn dictionary_snapshot() -> IdScope {
    stable("dictionary.snapshot")
}

pub fn custom(value: impl Into<String>) -> Result<IdScope, MetaError> {
    IdScope::new(value)
}

fn stable(value: &'static str) -> IdScope {
    IdScope::new(value).expect("stable metadata id scope must be valid")
}
