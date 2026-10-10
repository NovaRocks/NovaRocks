//! Fragment-lifecycle contracts owned by the execution kernel.

pub mod error;
pub mod exchange;
pub mod execution_failure;
pub mod fact;
pub mod handle;
pub mod instance;
pub mod io;
pub mod resources;
pub mod runtime_state;
pub mod scan;
pub mod sink;
pub mod submission;

pub use error::{
    FragmentExecutionError, FragmentExecutionErrorKind, FragmentLaunchError,
    FragmentLaunchErrorKind, FragmentLaunchStage,
};
pub use fact::{FragmentCancelReason, FragmentOutcome, FragmentTerminalFact};
pub use handle::{
    CompiledFragmentSubmission, CompiledWriterBindings, DormantFragmentHandle,
    FragmentPrepareContext, RunningFragmentHandle, compiled_sink_kind, prepare_compiled_fragment,
    prepare_compiled_fragment_with_metadata_host, prepare_fragment,
};
pub use instance::*;
pub use submission::FragmentSubmission;

pub use execution_failure::{
    ExecutionFailure, ExecutionFailureCause, ExecutionFailureContext, ExecutionResult,
    PipelineOperation, RequiredExpressionRowError,
};
