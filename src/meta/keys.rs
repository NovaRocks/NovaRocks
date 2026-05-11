pub const NS_MANAGED: &str = "managed";
pub const NS_MANAGED_TXN: &str = "managed.txn";
pub const NS_MV: &str = "mv";
pub const NS_ICEBERG_CATALOG: &str = "iceberg_catalog";
pub const NS_JOB: &str = "job";

pub fn normalize_lookup_name(value: &str) -> String {
    value.to_ascii_lowercase()
}
