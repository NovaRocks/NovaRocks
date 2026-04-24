//! DEPRECATED in favor of `crate::connector::starrocks::managed`. This
//! shim exists only during the standalone/connector decoupling refactor
//! so existing call sites keep compiling while imports are rewritten one
//! caller at a time. Will be deleted at the end of Phase 1 (Task 1.10).

pub(crate) use crate::connector::starrocks::managed::{
    ManagedLakeCatalog, ManagedLakeConfig, reconcile_on_open, register_managed_table_in_catalog,
    register_managed_tables_in_catalog, runtime_registered,
};

pub(crate) mod catalog {
    pub(crate) use crate::connector::starrocks::managed::catalog::*;
}
pub(crate) mod config {
    pub(crate) use crate::connector::starrocks::managed::config::*;
}
pub(crate) mod ddl {
    pub(crate) use crate::connector::starrocks::managed::ddl::*;
}
pub(crate) mod erase {
    pub(crate) use crate::connector::starrocks::managed::erase::*;
}
pub(crate) mod mv_ddl {
    pub(crate) use crate::connector::starrocks::managed::mv_ddl::*;
}
pub(crate) mod mv_refresh {
    pub(crate) use crate::connector::starrocks::managed::mv_refresh::*;
}
pub(crate) mod mv_shape {
    pub(crate) use crate::connector::starrocks::managed::mv_shape::*;
}
pub(crate) mod store {
    pub(crate) use crate::connector::starrocks::managed::store::*;
}
pub(crate) mod txn {
    pub(crate) use crate::connector::starrocks::managed::txn::*;
}
