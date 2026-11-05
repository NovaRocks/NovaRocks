//! Fragment-lifecycle contracts owned by the execution kernel.

pub mod connector_failure;
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
mod sink_failure;

pub use error::{
    LocalTaskExecutionError, LocalTaskExecutionErrorKind, LocalTaskLaunchError,
    LocalTaskLaunchErrorKind, LocalTaskLaunchStage,
};
pub use fact::{LocalTaskOutcome, LocalTaskTerminalFact, TaskCancelDetail};
pub use handle::{
    CompiledTaskSubmission, CompiledWriterBindings, LocalTaskPrepareContext, PreparedTaskExecution,
    RunningTaskExecution, compiled_sink_kind, prepare_compiled_task,
    prepare_compiled_task_with_metadata_host,
};
pub use instance::*;
pub use resources::ExchangeReceiverRetirement;

mod domain_failure;
pub use domain_failure::SharedDomainFailure;

pub use execution_failure::{
    ExecutionFailure, ExecutionFailureCause, ExecutionFailureReporter, ExecutionResult,
    OperatorFailureSite, PipelineOperation, RequiredExpressionRowError,
};

mod execution_reason;
pub(crate) use execution_reason::ExecutionReason;
