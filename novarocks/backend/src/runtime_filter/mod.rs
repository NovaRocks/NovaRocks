//! Backend-owned runtime-filter participant state.

pub(crate) mod artifact;
pub(crate) mod artifact_query;
pub(crate) mod codec;
pub(crate) mod domain;
pub(crate) mod ingress;
pub(crate) mod install_decode;
pub(crate) mod materializer;
pub(crate) mod membership_contract_decode;
pub(crate) mod observation;
pub(crate) mod participant;
pub(crate) mod reliable_transport;
pub(crate) mod rpc;
pub(crate) mod terminal_contribution;
#[cfg(test)]
pub(crate) mod test_support;
pub(crate) mod transport;
// The typed scan cannot bind this filter yet: `RuntimeFilterSession::subscribe`
// needs the fragment's decoded consumer contract, which reaches the scan node
// only after `lower_typed_connector_scan` has already frozen its source. The
// filter itself is complete and covered by tests; only that seam is missing.
#[allow(
    dead_code,
    reason = "Awaiting the fragment seam that carries decoded consumer contracts into typed scan lowering."
)]
pub(crate) mod typed_dynamic_filter;
