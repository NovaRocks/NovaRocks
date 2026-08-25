//! Validated provider-scoped native carriers.

mod execution_binding;

pub use execution_binding::{
    EnsureConnectorExecutionBindingOutcome, EnsureConnectorExecutionBindingRejection,
    EnsureConnectorExecutionBindingRejectionReason, EnsureConnectorExecutionBindingResult,
    RetireConnectorExecutionBindingOutcome, RetireConnectorExecutionBindingResult,
    connector_execution_binding_declaration_digest,
};
