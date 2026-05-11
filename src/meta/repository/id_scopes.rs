use crate::meta::{IdScope, MetaError};

pub fn managed_db() -> IdScope {
    stable("managed.db")
}

pub fn managed_table() -> IdScope {
    stable("managed.table")
}

pub fn managed_partition() -> IdScope {
    stable("managed.partition")
}

pub fn managed_index() -> IdScope {
    stable("managed.index")
}

pub fn managed_tablet() -> IdScope {
    stable("managed.tablet")
}

pub fn managed_txn() -> IdScope {
    stable("managed.txn")
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

pub fn custom(value: impl Into<String>) -> Result<IdScope, MetaError> {
    IdScope::new(value)
}

fn stable(value: &'static str) -> IdScope {
    IdScope::new(value).expect("stable metadata id scope must be valid")
}
