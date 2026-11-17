//! NovaRocks-native protobuf generated schema artifacts.
//!
//! This crate is the sole owner of repository-level generated DTOs, the
//! descriptor set, and the schema ledger. Wire codecs, transport, role-local
//! state, and FE/BE execution conversion are intentionally outside this crate.

pub const SCHEMA_LEDGER_VERSION: u32 = 1;

pub mod resource_layout;

include!(concat!(env!("OUT_DIR"), "/resource_layout_registry.rs"));

/// File descriptor set generated from the canonical repository-level IDL.
pub const FILE_DESCRIPTOR_SET: &[u8] =
    include_bytes!(concat!(env!("OUT_DIR"), "/novarocks_descriptor.bin"));

pub mod catalog {
    include!(concat!(env!("OUT_DIR"), "/novarocks.catalog.rs"));
}

#[allow(clippy::len_without_is_empty)]
pub mod common {
    include!(concat!(env!("OUT_DIR"), "/novarocks.common.rs"));
}

pub mod connector_read {
    include!(concat!(env!("OUT_DIR"), "/novarocks.connector_read.rs"));
}

pub mod connector_common {
    include!(concat!(env!("OUT_DIR"), "/novarocks.connector_common.rs"));
}

pub mod connector_write {
    include!(concat!(env!("OUT_DIR"), "/novarocks.connector_write.rs"));
}

pub mod filter {
    include!(concat!(env!("OUT_DIR"), "/novarocks.filter.rs"));
}

/// Sole Arrow field, layout and source-attribute vocabulary.
pub mod arrow_type {
    include!(concat!(env!("OUT_DIR"), "/novarocks.arrow_type.rs"));
}

/// Original binding requests and IPC constants shared by explicit plan owners.
pub mod call_request {
    include!(concat!(env!("OUT_DIR"), "/novarocks.call_request.rs"));
}

/// Shared logical value definitions for compiled packages and runtime filters.
pub mod value_type {
    include!(concat!(env!("OUT_DIR"), "/novarocks.value_type.rs"));
}

/// Flat type-table component of the v2 physical package vocabulary.
pub mod physical_type_v2 {
    include!(concat!(env!("OUT_DIR"), "/novarocks.physical_type_v2.rs"));
}

/// Flat invocation/control component of the v2 physical package vocabulary.
pub mod physical_control_v2 {
    include!(concat!(
        env!("OUT_DIR"),
        "/novarocks.physical_control_v2.rs"
    ));
}

/// Complete frozen call-effects and parameter component of the v2 vocabulary.
pub mod physical_semantics_v2 {
    include!(concat!(
        env!("OUT_DIR"),
        "/novarocks.physical_semantics_v2.rs"
    ));
}

/// Complete flat fragment carrier. Generated DTOs remain codec-owned;
/// structural and installed-capability validation precede local compilation.
pub mod physical_package_v2 {
    // Expansion aliases; the shared generated vocabulary is the sole owner.
    pub use super::call_request::{
        CallRequestDefinition, ConstantReference, FragmentCallRequests, IpcCompression,
        IpcConstantPool, LambdaArgumentType, OriginalCallRequest, OriginalFunctionArgument,
        OriginalValueArgument, SourceConstantPolicy, UnitCallRequest, call_request_definition,
        original_function_argument,
    };
    include!(concat!(
        env!("OUT_DIR"),
        "/novarocks.physical_package_v2.rs"
    ));
}

/// Shared execution type namespaces and runtime-filter contracts.
#[allow(clippy::large_enum_variant)]
pub mod plan {
    // Expansion aliases; physical Arrow descriptors have one generated owner.
    pub use super::arrow_type::{
        ArrowDecimalType, ArrowDictionaryType, ArrowFieldMetadataEntry, ArrowFixedSizeListType,
        ArrowIntervalUnit, ArrowMapType, ArrowPhysicalColumn, ArrowPhysicalField,
        ArrowPhysicalSchema, ArrowPhysicalType, ArrowPrimitiveType, ArrowRunEndEncodedType,
        ArrowStructType, ArrowTimeType, ArrowTimeUnit, ArrowTimestampType, ArrowUnionField,
        ArrowUnionMode, ArrowUnionType, arrow_physical_type,
    };
    include!(concat!(env!("OUT_DIR"), "/novarocks.plan.rs"));
}

#[allow(clippy::large_enum_variant)]
pub mod result {
    include!(concat!(env!("OUT_DIR"), "/novarocks.result.rs"));
}

#[allow(clippy::large_enum_variant)]
pub mod novarocks {
    pub use super::{
        catalog, common, connector_common, connector_read, connector_write, filter, plan, result,
    };

    include!(concat!(env!("OUT_DIR"), "/novarocks.rs"));
}
