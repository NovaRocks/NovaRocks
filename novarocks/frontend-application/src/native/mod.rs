//! Frontend-owned native transport.
//!
//! Generated Tonic stubs are deliberately private: Frontend implements Core's
//! carrier-neutral ports but does not re-export a role-neutral transport API.

pub(crate) mod apply_send_owner;
pub(crate) mod data_runtime;
pub(crate) mod fragment_encoder;
pub(crate) mod fragment_transport;
mod original_retirement;
pub(crate) mod report_server;
mod root_result_codec;
mod subscription_owner;
#[cfg(test)]
pub(crate) use subscription_owner::NativeSubscriptionOwner;
pub(crate) mod task_transport;
pub(crate) mod transport;
pub(crate) mod transport_supervisor;
