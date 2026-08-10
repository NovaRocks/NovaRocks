//! Fragment-lifecycle contracts owned by the execution kernel.

pub mod error;
pub mod exchange;
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
    DormantFragmentHandle, FragmentPrepareContext, RunningFragmentHandle, prepare_fragment,
};
pub use instance::*;
pub use submission::FragmentSubmission;
