//! Frontend-owned native transport.
//!
//! Generated Tonic stubs are deliberately private: Frontend implements Core's
//! carrier-neutral ports but does not re-export a role-neutral transport API.

pub(crate) mod codec;
pub(crate) mod data_runtime;
pub(crate) mod fragment_encoder;
pub(crate) mod fragment_transport;
pub(crate) mod query_lifecycle;
pub(crate) mod report_server;
pub(crate) mod task_transport;
pub(crate) mod transport;

pub(crate) mod generated {
    include!(concat!(env!("OUT_DIR"), "/novarocks.rs"));
}
