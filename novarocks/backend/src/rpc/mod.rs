//! Backend-owned RPC infrastructure.
//!
//! This module owns generated Tonic stubs, codecs, role-local outbound channel
//! mechanics, listener composition, and data-plane handlers. Domain adapters
//! remain owned by fragment, query lifecycle, runtime filter, and connector.

pub(crate) mod client;
pub(crate) mod codec;
pub(crate) mod data_plane;
pub(crate) mod data_plane_handlers;
pub(crate) mod runtime;
pub(crate) mod server;

pub(crate) mod transport {
    include!(concat!(env!("OUT_DIR"), "/novarocks.rs"));
}

#[cfg(test)]
mod tests {
    #[test]
    fn generated_rpc_stubs_reference_protocol_dtos() {
        let generated = include_str!(concat!(env!("OUT_DIR"), "/novarocks.rs"));
        assert!(generated.contains("nova_rocks_grpc_client"));
        assert!(generated.contains("nova_rocks_grpc_server"));
        assert!(generated.contains("::novarocks_proto::novarocks::HeartbeatRequest"));
        assert!(generated.contains("::novarocks_proto::filter::LookupRequest"));
    }
}
