pub const NS_STARROCKS: &str = "starrocks";
pub const NS_STARROCKS_TXN: &str = "starrocks.txn";
pub const NS_MV: &str = "mv";
pub const NS_ICEBERG_CATALOG: &str = "iceberg_catalog";
pub const NS_ICEBERG_OPERATION: &str = "iceberg_operation";
pub const NS_JOB: &str = "job";
pub const NS_DICTIONARY: &str = "dictionary";

pub fn normalize_lookup_name(value: &str) -> String {
    value.to_ascii_lowercase()
}
