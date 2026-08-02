//! NovaRocks-native protobuf schema artifacts.
//!
//! This crate owns generated DTOs, the descriptor set, and schema-ledger
//! metadata only. Transport and FE/BE semantic conversion remain outside this
//! package.

pub const SCHEMA_LEDGER_VERSION: u32 = 1;

/// File descriptor set generated from the canonical repository-level IDL.
pub const FILE_DESCRIPTOR_SET: &[u8] =
    include_bytes!(concat!(env!("OUT_DIR"), "/novarocks_descriptor.bin"));

#[allow(clippy::len_without_is_empty)]
pub mod common {
    include!(concat!(env!("OUT_DIR"), "/novarocks.common.rs"));
}

#[allow(clippy::module_inception)]
pub mod expr {
    include!(concat!(env!("OUT_DIR"), "/novarocks.expr.rs"));
}

pub mod filter {
    include!(concat!(env!("OUT_DIR"), "/novarocks.filter.rs"));
}

#[allow(clippy::large_enum_variant)]
pub mod plan {
    include!(concat!(env!("OUT_DIR"), "/novarocks.plan.rs"));
}

#[allow(clippy::large_enum_variant)]
pub mod novarocks {
    use super::{common, filter, plan};

    include!(concat!(env!("OUT_DIR"), "/novarocks.rs"));
}
