// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Execution-owned failure payloads. These facts own no terminal latch.

use crate::exec::pipeline::operator::input_construction::ConstructionFailure;
use novarocks_functions::{KernelDiagnostic, KernelFailure, RowDataError, Selection};
use novarocks_local_program::ProgramExpressionRootSite;
use std::{
    error::Error,
    fmt,
    sync::{Arc, Mutex},
};

pub type ExecutionResult<T> = Result<T, ExecutionFailure>;

/// Reports a known originating failure independently of the owner's exit.
/// Implementations decide acceptance; producers never wait for sibling cleanup
/// before submitting the cause and never infer a terminal state from this call.
pub trait ExecutionFailureReporter: Send + Sync {
    fn report_failure(&self, error: ExecutionFailure);
    fn report_cleanup(&self, cause: &ExecutionFailure);
    fn report_unresolved_exit(&self, cause: &ExecutionFailure);
}

/// A required root's remaining journal error, after its legal guards ran.
/// The original selected ordinal and message remain in the original payload.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RequiredExpressionRowError {
    root: ProgramExpressionRootSite,
    batch_row: usize,
    error: RowDataError,
}

impl RequiredExpressionRowError {
    pub fn try_new(
        root: ProgramExpressionRootSite,
        selection: Selection<'_>,
        error: RowDataError,
    ) -> Result<Self, KernelFailure> {
        let batch_row = selection.row(error.selected_ordinal()).ok_or_else(|| {
            KernelFailure::Internal(KernelDiagnostic::new(
                "required expression error ordinal is outside its selection",
            ))
        })?;
        Ok(Self {
            root,
            batch_row,
            error,
        })
    }

    pub const fn root(&self) -> ProgramExpressionRootSite {
        self.root
    }
    pub const fn batch_row(&self) -> usize {
        self.batch_row
    }
    pub const fn error(&self) -> &RowDataError {
        &self.error
    }
}

impl fmt::Display for RequiredExpressionRowError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "required expression {:?} failed at batch row {} (selected ordinal {}): {}",
            self.root,
            self.batch_row,
            self.error.selected_ordinal(),
            self.error.message()
        )
    }
}
impl Error for RequiredExpressionRowError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(&self.error)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ExecutionFailureCause {
    Domain(super::SharedDomainFailure),
    MemoryAccounting(crate::runtime::mem_tracker::MemoryAccountingFailure),
    Connector(super::connector_failure::ConnectorCallFailure),
    Writer(crate::exec::pipeline::writer_failure::WriterOwnerFailure),
    Construction(ConstructionFailure),
    Kernel(KernelFailure),
    RuntimeFilter(crate::runtime_filter::RuntimeFilterFailure),
    RuntimeScalarSource(novarocks_functions::ScalarResourceError),
    RuntimeScalarMemory(crate::runtime::scalar_memory::RuntimeScalarMemoryRefusal),
    InvocationData(novarocks_functions::InvocationData),
    ScalarInvocationData(novarocks_functions::ScalarInvocationData),
    WindowInvocationData(novarocks_functions::WindowInvocationData),
    RequiredRow(RequiredExpressionRowError),
    PreparationMetadata(crate::runtime::preparation_metadata::PreparationMetadataFailure),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PipelineOperation {
    Control,
    Preparation,
    Push,
    Pull,
    Finishing,
    Admission,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct OperatorFailureSite {
    pub operator_ordinal: usize,
    pub operation: PipelineOperation,
}

/// One shared Execution occurrence. Value-owned causes retain custody here;
/// a shared domain cause retains it in its canonical source node instead, so
/// an independently retained inner handle cannot leave its source unguarded.
struct FailureOccurrence {
    cause: ExecutionFailureCause,
    retention: Mutex<Vec<Box<dyn novarocks_execution_contract::SourceRetentionCapability>>>,
}
impl FailureOccurrence {
    fn new(cause: ExecutionFailureCause) -> Arc<Self> {
        Arc::new(Self {
            cause,
            retention: Mutex::new(Vec::new()),
        })
    }
}
impl fmt::Debug for FailureOccurrence {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(&self.cause, f)
    }
}
impl PartialEq for FailureOccurrence {
    fn eq(&self, other: &Self) -> bool {
        self.cause == other.cause
    }
}
impl Eq for FailureOccurrence {}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExecutionFailure {
    // Rich source material is allocated only when a failure occurs. Clones
    // and added call-site context retain this same local occurrence identity.
    cause: Arc<FailureOccurrence>,
    context: Option<OperatorFailureSite>,
}

impl ExecutionFailure {
    pub fn from_domain<E>(cause: E) -> Self
    where
        E: Error + novarocks_failure_contract::FailureProjection + Send + Sync + 'static,
    {
        Self {
            cause: FailureOccurrence::new(ExecutionFailureCause::Domain(
                super::SharedDomainFailure::new(cause),
            )),
            context: None,
        }
    }

    /// Attach one already funded source-capture obligation. This creates no
    /// Work, callback or capacity. Shared domain handles attach to their
    /// canonical node, including aliases made before this Execution wrapper.
    /// Known Kernel, construction and metadata wrappers forward through their
    /// explicit typed owners. Other causes retain their Execution occurrence;
    /// their independent resource owners must still supervise shared captures.
    /// Destruction remains supervised if the original source unwinds.
    pub fn retain_source_lifetime(
        &self,
        custody: Box<dyn novarocks_execution_contract::SourceRetentionCapability>,
    ) {
        use crate::exec::chunk::CompiledSchemaRequestFailure as Schema;
        use crate::runtime::preparation_memory::PreparationMemoryRefusal as Host;
        use crate::runtime::preparation_metadata::PreparationMetadataFailure as Metadata;
        use novarocks_type_contract::MetadataRequestError as Request;
        let custody = match &self.cause.cause {
            ExecutionFailureCause::Domain(source)
            | ExecutionFailureCause::Construction(ConstructionFailure::Source(source))
            | ExecutionFailureCause::PreparationMetadata(Metadata::SchemaRequest(
                Schema::Projection(source),
            )) => {
                source.retain_source_lifetime(custody);
                return;
            }
            ExecutionFailureCause::Kernel(source) => source.try_retain_source_lifetime(custody),
            ExecutionFailureCause::Construction(ConstructionFailure::Control(source))
            | ExecutionFailureCause::PreparationMetadata(
                Metadata::SchemaRequest(Schema::Request(Request::Control(source)))
                | Metadata::Request(Request::Control(source)),
            ) => source.try_retain_source_lifetime(custody),
            ExecutionFailureCause::Construction(ConstructionFailure::Caller(source)) => {
                source.retain_source_lifetime(custody);
                return;
            }
            ExecutionFailureCause::PreparationMetadata(Metadata::Host(Host::Interrupted(
                source,
            ))) => {
                source.retain_source_lifetime(custody);
                return;
            }
            ExecutionFailureCause::Construction(
                ConstructionFailure::MissingOwner
                | ConstructionFailure::Memory(_)
                | ConstructionFailure::InvalidSource(_)
                | ConstructionFailure::Resource(_),
            )
            | ExecutionFailureCause::PreparationMetadata(
                Metadata::MissingCompiledSite
                | Metadata::SchemaRequest(Schema::Request(
                    Request::Arithmetic
                    | Request::SourceModel(_)
                    | Request::Metadata(_)
                    | Request::ValueType(_),
                ))
                | Metadata::Request(
                    Request::Arithmetic
                    | Request::SourceModel(_)
                    | Request::Metadata(_)
                    | Request::ValueType(_),
                )
                | Metadata::Host(
                    Host::MissingQueryMemory | Host::SharedShortage(_) | Host::Capacity(_),
                ),
            )
            | ExecutionFailureCause::MemoryAccounting(_)
            | ExecutionFailureCause::Connector(_)
            | ExecutionFailureCause::Writer(_)
            | ExecutionFailureCause::RuntimeFilter(_)
            | ExecutionFailureCause::RuntimeScalarSource(_)
            | ExecutionFailureCause::RuntimeScalarMemory(_)
            | ExecutionFailureCause::InvocationData(_)
            | ExecutionFailureCause::ScalarInvocationData(_)
            | ExecutionFailureCause::WindowInvocationData(_)
            | ExecutionFailureCause::RequiredRow(_) => Err(custody),
        };
        let Err(custody) = custody else {
            return;
        };
        self.cause
            .retention
            .lock()
            .expect("source lifetime custody")
            .push(custody);
    }

    pub fn cause(&self) -> &ExecutionFailureCause {
        &self.cause.cause
    }
    /// A repeated observation of one originating failure is distinct from two
    /// independent failures with equal facts or text. Identity is local only;
    /// role boundaries project neutral facts and never encode this pointer.
    pub(crate) fn same_occurrence(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.cause, &other.cause)
    }
    pub const fn site(&self) -> Option<OperatorFailureSite> {
        self.context
    }

    pub fn detail(&self) -> &str {
        match &self.cause.cause {
            ExecutionFailureCause::Domain(error) => error.detail(),
            ExecutionFailureCause::MemoryAccounting(error) => error.detail(),
            ExecutionFailureCause::Connector(error) => error.detail(),
            ExecutionFailureCause::Writer(error) => error.detail(),
            ExecutionFailureCause::Construction(error) => error.detail(),
            ExecutionFailureCause::RuntimeFilter(error) => error.detail(),
            ExecutionFailureCause::RequiredRow(error) => error.error().message(),
            ExecutionFailureCause::InvocationData(error) => error.message(),
            ExecutionFailureCause::ScalarInvocationData(error) => error.message(),
            ExecutionFailureCause::WindowInvocationData(error) => error.message(),
            ExecutionFailureCause::PreparationMetadata(error) => error.detail(),
            ExecutionFailureCause::RuntimeScalarSource(_) => "scalar runtime source request failed",
            ExecutionFailureCause::RuntimeScalarMemory(_) => "scalar runtime memory refused",
            ExecutionFailureCause::Kernel(error) => match error {
                KernelFailure::Source(source) => source.detail(),
                KernelFailure::CompileControl(source) => source.detail(),
                KernelFailure::Capacity(_) => "kernel byte capacity was refused",
                KernelFailure::Cancelled => "kernel evaluation was cancelled",
                KernelFailure::DeadlineExceeded => "kernel evaluation deadline was exceeded",
                KernelFailure::ResourceExhausted => "kernel evaluation resources were exhausted",
                KernelFailure::InstanceFailed => "kernel instance has already failed",
                KernelFailure::InvalidProgram(message)
                | KernelFailure::Internal(message)
                | KernelFailure::Operational(message) => message.message(),
            },
        }
    }

    /// Render only neutral facts and typed execution location for status and
    /// secondary diagnostics. A provider's arbitrary Display is local source
    /// material, not a redaction guarantee, even when bounded by SafeDetail.
    pub fn safe_detail(&self) -> novarocks_execution_contract::status::SafeDetail {
        use novarocks_failure_contract::FailureProjection;
        self.safe_detail_for_fact(self.failure_fact())
    }

    /// Render one already frozen fact with this carrier's typed location.
    /// This formatting path never projects or renders the original source.
    pub fn safe_detail_for_fact(
        &self,
        fact: novarocks_failure_contract::FailureFact,
    ) -> novarocks_execution_contract::status::SafeDetail {
        struct Summary<'a>(
            &'a ExecutionFailure,
            novarocks_failure_contract::FailureFact,
        );
        impl fmt::Display for Summary<'_> {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                let fact = self.1;
                write!(f, "{:?} failure (reason {})", fact.class(), fact.reason())?;
                if let Some(context) = self.0.context {
                    write!(
                        f,
                        " at operator {} during {:?}",
                        context.operator_ordinal, context.operation
                    )?;
                }
                if let Some(position) = fact.row_position() {
                    write!(
                        f,
                        " at batch row {} (selected ordinal {})",
                        position.batch_row, position.selected_ordinal
                    )?;
                }
                if let Some(resource) = fact.resource() {
                    write!(f, "; {:?}", resource.dimension)?;
                    if let Some(requested) = resource.requested {
                        write!(f, " requested={requested}")?;
                    }
                    if let Some(limit) = resource.limit {
                        write!(f, " limit={limit}")?;
                    }
                    if let Some(committed) = resource.committed {
                        write!(f, " committed={committed}")?;
                    }
                }
                if let Some(representation) = fact.representation() {
                    write!(
                        f,
                        "; {:?} layout {}:{}",
                        representation.boundary,
                        representation.layout.domain,
                        representation.layout.code
                    )?;
                    if let Some(required) = representation.required {
                        write!(f, " required={required}")?;
                    }
                    if let Some(prefix) = representation.extent_limit {
                        write!(f, " extent_limit={prefix}")?;
                    }
                    if let Some(prefix) = representation.representable_prefix {
                        write!(f, " representable_prefix={prefix}")?;
                    }
                }
                Ok(())
            }
        }
        novarocks_execution_contract::status::SafeDetail::truncating_display(Summary(self, fact))
    }

    /// Attach the exact driver-local operation without formatting the cause
    /// into a different failure category or allocating a second diagnostic.
    pub fn at_operator(mut self, operator_ordinal: usize, operation: PipelineOperation) -> Self {
        if self.context.is_none() {
            self.context = Some(OperatorFailureSite {
                operator_ordinal,
                operation,
            });
        }
        self
    }
}

impl From<crate::runtime::preparation_metadata::PreparationMetadataFailure> for ExecutionFailure {
    fn from(error: crate::runtime::preparation_metadata::PreparationMetadataFailure) -> Self {
        Self {
            cause: FailureOccurrence::new(ExecutionFailureCause::PreparationMetadata(error)),
            context: None,
        }
    }
}

impl From<super::connector_failure::ConnectorCallFailure> for ExecutionFailure {
    fn from(error: super::connector_failure::ConnectorCallFailure) -> Self {
        Self {
            cause: FailureOccurrence::new(ExecutionFailureCause::Connector(error)),
            context: None,
        }
    }
}
impl From<crate::exec::pipeline::writer_failure::WriterFailure> for ExecutionFailure {
    fn from(error: crate::exec::pipeline::writer_failure::WriterFailure) -> Self {
        Self {
            cause: FailureOccurrence::new(ExecutionFailureCause::Writer(error.into())),
            context: None,
        }
    }
}

impl From<crate::runtime::mem_tracker::MemoryAccountingFailure> for ExecutionFailure {
    fn from(error: crate::runtime::mem_tracker::MemoryAccountingFailure) -> Self {
        Self {
            cause: FailureOccurrence::new(ExecutionFailureCause::MemoryAccounting(error)),
            context: None,
        }
    }
}
impl From<novarocks_functions::generate_series_core::SeriesError> for ExecutionFailure {
    fn from(cause: novarocks_functions::generate_series_core::SeriesError) -> Self {
        Self::from_domain(cause)
    }
}

impl From<KernelFailure> for ExecutionFailure {
    fn from(error: KernelFailure) -> Self {
        Self {
            cause: FailureOccurrence::new(ExecutionFailureCause::Kernel(error)),
            context: None,
        }
    }
}
impl From<ConstructionFailure> for ExecutionFailure {
    fn from(error: ConstructionFailure) -> Self {
        match error {
            // Preserve the originating failure, including its existing driver
            // context, rather than burying its category under construction.
            ConstructionFailure::Caller(error) => *error,
            error => Self {
                cause: FailureOccurrence::new(ExecutionFailureCause::Construction(error)),
                context: None,
            },
        }
    }
}
impl From<crate::runtime::exchange::ExchangeFailure> for ExecutionFailure {
    fn from(source: crate::runtime::exchange::ExchangeFailure) -> Self {
        match source {
            crate::runtime::exchange::ExchangeFailure::Source(source) => Self {
                cause: FailureOccurrence::new(ExecutionFailureCause::Domain(source)),
                context: None,
            },
            crate::runtime::exchange::ExchangeFailure::Execution(source) => source,
        }
    }
}
impl From<crate::runtime_filter::RuntimeFilterFailure> for ExecutionFailure {
    fn from(error: crate::runtime_filter::RuntimeFilterFailure) -> Self {
        Self {
            cause: FailureOccurrence::new(ExecutionFailureCause::RuntimeFilter(error)),
            context: None,
        }
    }
}
impl From<novarocks_functions::EvaluationFailure> for ExecutionFailure {
    fn from(error: novarocks_functions::EvaluationFailure) -> Self {
        match error {
            novarocks_functions::EvaluationFailure::Kernel(cause) => cause.into(),
            novarocks_functions::EvaluationFailure::InvocationData(cause) => Self {
                cause: FailureOccurrence::new(ExecutionFailureCause::InvocationData(cause)),
                context: None,
            },
        }
    }
}
impl From<novarocks_functions::ScalarInvocationFailure> for ExecutionFailure {
    fn from(error: novarocks_functions::ScalarInvocationFailure) -> Self {
        match error {
            novarocks_functions::ScalarInvocationFailure::Kernel(cause) => cause.into(),
            novarocks_functions::ScalarInvocationFailure::Data(cause) => Self {
                cause: FailureOccurrence::new(ExecutionFailureCause::ScalarInvocationData(cause)),
                context: None,
            },
        }
    }
}
impl From<crate::runtime::scalar_memory::RuntimeScalarEvaluationFailure> for ExecutionFailure {
    fn from(error: crate::runtime::scalar_memory::RuntimeScalarEvaluationFailure) -> Self {
        use crate::runtime::scalar_memory::RuntimeScalarEvaluationFailure;
        let cause = match error {
            RuntimeScalarEvaluationFailure::Kernel(cause) => return cause.into(),
            RuntimeScalarEvaluationFailure::Data(cause) => {
                ExecutionFailureCause::ScalarInvocationData(cause)
            }
            RuntimeScalarEvaluationFailure::Source(cause) => {
                ExecutionFailureCause::RuntimeScalarSource(cause)
            }
            RuntimeScalarEvaluationFailure::Host(cause) => {
                ExecutionFailureCause::RuntimeScalarMemory(cause)
            }
        };
        Self {
            cause: FailureOccurrence::new(cause),
            context: None,
        }
    }
}
impl From<novarocks_functions::WindowEvaluationFailure> for ExecutionFailure {
    fn from(error: novarocks_functions::WindowEvaluationFailure) -> Self {
        match error {
            novarocks_functions::WindowEvaluationFailure::Kernel(cause) => cause.into(),
            novarocks_functions::WindowEvaluationFailure::InvocationData(cause) => Self {
                cause: FailureOccurrence::new(ExecutionFailureCause::WindowInvocationData(cause)),
                context: None,
            },
        }
    }
}
impl From<RequiredExpressionRowError> for ExecutionFailure {
    fn from(error: RequiredExpressionRowError) -> Self {
        Self {
            cause: FailureOccurrence::new(ExecutionFailureCause::RequiredRow(error)),
            context: None,
        }
    }
}
impl fmt::Display for ExecutionFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(context) = self.context {
            write!(
                f,
                "operator[{}] {:?}: ",
                context.operator_ordinal, context.operation
            )?;
        }
        match &self.cause.cause {
            ExecutionFailureCause::Domain(error) => error.fmt(f),
            ExecutionFailureCause::MemoryAccounting(error) => error.fmt(f),
            ExecutionFailureCause::Connector(error) => error.fmt(f),
            ExecutionFailureCause::Writer(error) => error.fmt(f),
            ExecutionFailureCause::Construction(error) => error.fmt(f),
            ExecutionFailureCause::Kernel(error) => error.fmt(f),
            ExecutionFailureCause::RuntimeFilter(error) => error.fmt(f),
            ExecutionFailureCause::RuntimeScalarSource(error) => error.fmt(f),
            ExecutionFailureCause::RuntimeScalarMemory(error) => error.fmt(f),
            ExecutionFailureCause::RequiredRow(error) => error.fmt(f),
            ExecutionFailureCause::InvocationData(error) => error.fmt(f),
            ExecutionFailureCause::ScalarInvocationData(error) => error.fmt(f),
            ExecutionFailureCause::WindowInvocationData(error) => error.fmt(f),
            ExecutionFailureCause::PreparationMetadata(error) => error.fmt(f),
        }
    }
}
impl Error for ExecutionFailure {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match &self.cause.cause {
            ExecutionFailureCause::Domain(error) => Some(error),
            ExecutionFailureCause::MemoryAccounting(error) => Some(error),
            ExecutionFailureCause::Connector(error) => Some(error),
            ExecutionFailureCause::Writer(error) => Some(error),
            ExecutionFailureCause::Construction(error) => Some(error),
            ExecutionFailureCause::Kernel(error) => Some(error),
            ExecutionFailureCause::RuntimeFilter(error) => Some(error),
            ExecutionFailureCause::RuntimeScalarSource(error) => Some(error),
            ExecutionFailureCause::RuntimeScalarMemory(error) => Some(error),
            ExecutionFailureCause::RequiredRow(error) => Some(error),
            ExecutionFailureCause::InvocationData(error) => Some(error),
            ExecutionFailureCause::ScalarInvocationData(error) => Some(error),
            ExecutionFailureCause::WindowInvocationData(error) => Some(error),
            ExecutionFailureCause::PreparationMetadata(error) => Some(error),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::exec::operators::OwnedTopNBoundaryError;
    use crate::runtime_filter::contribution::{
        ContributionCodecError, ControlledContributionCodecError, ControlledOrderedTupleError,
        OrderedTupleError,
    };
    use crate::runtime_filter::{
        AggregateTopNBoundaryError, RuntimeFilterContractViolation,
        RuntimeFilterContractViolationKind, RuntimeFilterFailure,
    };

    /// Short owner-local presentation/source-chain oracle; messages are literal
    /// original contract texts. There is no service, fixture, or controller.
    #[test]
    fn typed_failure_runtime_filter_keeps_original_raw_source_formatter_and_context() {
        let contract = RuntimeFilterContractViolation::new(
            RuntimeFilterContractViolationKind::ContractMismatch,
            "ResourceExhausted: kernel evaluation was cancelled",
        );
        let unsupported = novarocks_local_program::FilterMembershipSchemaError::UnsupportedType;
        let corrupt = novarocks_local_program::FilterMembershipSchemaError::DigestMismatch;
        let cases: [(ExecutionFailure, RuntimeFilterFailure, &str); 6] = [
            (
                contract.clone().into(),
                RuntimeFilterFailure::Contract(contract),
                "runtime-filter contract violation ContractMismatch: ResourceExhausted: kernel evaluation was cancelled",
            ),
            (
                unsupported.into(),
                RuntimeFilterFailure::Schema(unsupported),
                unsupported.detail(),
            ),
            (
                corrupt.into(),
                RuntimeFilterFailure::Schema(corrupt),
                corrupt.detail(),
            ),
            (
                AggregateTopNBoundaryError::BoundLoosened.into(),
                RuntimeFilterFailure::Boundary(AggregateTopNBoundaryError::BoundLoosened),
                "aggregate TopN cumulative boundary unexpectedly loosened",
            ),
            (
                OrderedTupleError::ArityMismatch.into(),
                RuntimeFilterFailure::Tuple(OrderedTupleError::ArityMismatch),
                "ArityMismatch",
            ),
            (
                ContributionCodecError::Malformed.into(),
                RuntimeFilterFailure::Codec(ContributionCodecError::Malformed),
                "invalid runtime filter contribution: Malformed",
            ),
        ];
        for (failure, original, text) in cases {
            assert_eq!(
                failure.cause(),
                &ExecutionFailureCause::RuntimeFilter(original.clone())
            );
            assert_eq!(failure.site(), None);
            assert_eq!(failure.to_string(), text);
            let failure = failure
                .at_operator(7, PipelineOperation::Push)
                .at_operator(41, PipelineOperation::Pull);
            assert_eq!(failure.to_string(), format!("operator[7] Push: {text}"));
            let source = failure
                .source()
                .unwrap()
                .downcast_ref::<RuntimeFilterFailure>()
                .unwrap();
            assert_eq!(source, &original);
            let raw = source.source().unwrap();
            match original {
                RuntimeFilterFailure::Contract(original) => {
                    assert_eq!(
                        raw.downcast_ref::<RuntimeFilterContractViolation>(),
                        Some(&original)
                    );
                    assert_eq!(failure.detail(), original.detail());
                }
                RuntimeFilterFailure::Schema(original) => {
                    assert_eq!(
                        raw.downcast_ref::<novarocks_local_program::FilterMembershipSchemaError>(),
                        Some(&original)
                    );
                    assert_eq!(
                        novarocks_failure_contract::FailureProjection::failure_fact(&failure),
                        novarocks_failure_contract::FailureProjection::failure_fact(&original)
                    );
                    assert_eq!(failure.detail(), original.detail());
                }
                RuntimeFilterFailure::Boundary(original) => assert_eq!(
                    raw.downcast_ref::<AggregateTopNBoundaryError>(),
                    Some(&original)
                ),
                RuntimeFilterFailure::Tuple(original) => {
                    assert_eq!(raw.downcast_ref::<OrderedTupleError>(), Some(&original))
                }
                RuntimeFilterFailure::Codec(original) => assert_eq!(
                    raw.downcast_ref::<ContributionCodecError>(),
                    Some(&original)
                ),
            }
        }
    }

    #[test]
    fn typed_failure_runtime_filter_control_refusals_remain_the_original_kernel_category() {
        for original in [
            KernelFailure::Cancelled,
            KernelFailure::DeadlineExceeded,
            KernelFailure::ResourceExhausted,
        ] {
            let failures: [ExecutionFailure; 3] = [
                OwnedTopNBoundaryError::Kernel(original.clone()).into(),
                ControlledOrderedTupleError::Kernel(original.clone()).into(),
                ControlledContributionCodecError::Kernel(original.clone()).into(),
            ];
            for failure in failures {
                assert_eq!(
                    failure.cause(),
                    &ExecutionFailureCause::Kernel(original.clone())
                );
                assert_eq!(
                    failure.source().unwrap().downcast_ref::<KernelFailure>(),
                    Some(&original)
                );
            }
        }
        let nested =
            AggregateTopNBoundaryError::CandidateContractMismatch(OrderedTupleError::TypeMismatch);
        let failure: ExecutionFailure = OwnedTopNBoundaryError::Boundary(nested.clone()).into();
        assert_eq!(
            failure.cause(),
            &ExecutionFailureCause::RuntimeFilter(RuntimeFilterFailure::Boundary(nested))
        );
        assert_eq!(
            failure.to_string(),
            "aggregate TopN candidate does not match its order contract: TypeMismatch"
        );
        let tuple: ExecutionFailure =
            ControlledOrderedTupleError::Tuple(OrderedTupleError::TypeMismatch).into();
        assert_eq!(
            tuple.cause(),
            &ExecutionFailureCause::RuntimeFilter(RuntimeFilterFailure::Tuple(
                OrderedTupleError::TypeMismatch
            ))
        );
        let codec: ExecutionFailure =
            ControlledContributionCodecError::Codec(ContributionCodecError::ResourceLimit).into();
        assert_eq!(
            codec.cause(),
            &ExecutionFailureCause::RuntimeFilter(RuntimeFilterFailure::Codec(
                ContributionCodecError::ResourceLimit
            ))
        );
    }

    #[test]
    fn typed_failure_text_cannot_impersonate_a_kernel_category_or_replace_context() {
        let failure = ExecutionFailure::from(novarocks_functions::KernelFailure::Internal(
            novarocks_functions::KernelDiagnostic::new(
                "ResourceExhausted: kernel evaluation was cancelled",
            ),
        ))
        .at_operator(7, PipelineOperation::Push)
        .at_operator(41, PipelineOperation::Pull);
        assert!(matches!(
            failure.cause(),
            ExecutionFailureCause::Kernel(KernelFailure::Internal(_))
        ));
        assert_eq!(
            novarocks_failure_contract::FailureProjection::failure_fact(&failure).class(),
            novarocks_failure_contract::FailureClass::Invariant
        );
        assert_eq!(
            failure.site(),
            Some(OperatorFailureSite {
                operator_ordinal: 7,
                operation: PipelineOperation::Push
            })
        );
        assert!(failure.source().unwrap().is::<KernelFailure>());
    }
    #[test]
    fn failure_observers_share_one_occurrence_while_equal_new_failures_remain_distinct() {
        let first: ExecutionFailure =
            KernelFailure::Internal(KernelDiagnostic::new("same local message")).into();
        let observed = first.clone().at_operator(7, PipelineOperation::Push);
        assert!(first.same_occurrence(&observed));
        assert!(std::ptr::eq(first.cause(), observed.cause()));
        assert!(first.site().is_none());
        assert_eq!(observed.site().unwrap().operator_ordinal, 7);
        let independent: ExecutionFailure =
            KernelFailure::Internal(KernelDiagnostic::new("same local message")).into();
        assert_eq!(
            first, independent,
            "value comparison remains available for diagnostic oracles"
        );
        assert!(
            !first.same_occurrence(&independent),
            "text equality cannot deduplicate failures"
        );
        let forwarded: ExecutionFailure =
            ConstructionFailure::Caller(Box::new(observed.clone())).into();
        assert!(first.same_occurrence(&forwarded));
        assert_eq!(forwarded.site(), observed.site());
        assert!(
            forwarded
                .source()
                .unwrap()
                .downcast_ref::<KernelFailure>()
                .is_some()
        );
    }

    #[test]
    fn status_diagnostic_uses_facts_without_copying_arbitrary_provider_text() {
        use super::super::connector_failure::{ConnectorCallFailure, ConnectorOperation};
        use novarocks_spi::connector::{ConnectorError, ConnectorErrorKind};
        let secret = "private-provider-credential-marker";
        let source = ConnectorError::new(ConnectorErrorKind::PermissionDenied, secret);
        let failure: ExecutionFailure =
            ConnectorCallFailure::new(ConnectorOperation::ReadPage, source.clone()).into();
        let failure = failure.at_operator(7, PipelineOperation::Pull);
        let safe = failure.safe_detail();
        assert!(!safe.as_str().contains(secret));
        assert!(safe.as_str().contains("reason 4:3"));
        assert!(safe.as_str().contains("operator 7"));
        let ExecutionFailureCause::Connector(call) = failure.cause() else {
            panic!("connector source must remain nominal");
        };
        assert_eq!(call.cause(), &source);
        assert_eq!(call.cause().message(), secret);
    }
    #[test]
    fn connector_domain_reasons_preserve_the_source_and_only_render_safe_facts() {
        use super::super::connector_failure::{ConnectorCallFailure, ConnectorOperation};
        use novarocks_failure_contract::{
            FailureClass, FailureFact, FailureProjection, FailureReason, domain,
        };
        use novarocks_spi::connector::{ConnectorError, ConnectorErrorKind};
        use std::num::NonZeroU16;

        #[derive(Debug)]
        struct ProducerCause(u16);
        impl std::fmt::Display for ProducerCause {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str("private-provider-url-principal-credential-marker")
            }
        }
        impl std::error::Error for ProducerCause {}
        impl FailureProjection for ProducerCause {
            fn failure_fact(&self) -> FailureFact {
                FailureFact::new(
                    FailureClass::Dependency,
                    FailureReason::new(domain::STORAGE_AUTHORITY, NonZeroU16::new(self.0).unwrap()),
                )
            }
        }

        for (code, kind) in [
            (1, ConnectorErrorKind::Unavailable),
            (2, ConnectorErrorKind::PermissionDenied),
            (3, ConnectorErrorKind::Unavailable),
            (4, ConnectorErrorKind::PermissionDenied),
        ] {
            let source = ConnectorError::from_domain(kind, ProducerCause(code));
            let expected = source.failure_fact();
            let failure: ExecutionFailure =
                ConnectorCallFailure::new(ConnectorOperation::ReadPage, source.clone()).into();
            let failure = failure.at_operator(7, PipelineOperation::Pull);
            assert_eq!(failure.failure_fact(), expected);
            let safe = failure.safe_detail();
            assert_eq!(
                safe.as_str(),
                format!("Dependency failure (reason 23:{code}) at operator 7 during Pull")
            );
            assert!(!safe.as_str().contains("private-provider"));
            let ExecutionFailureCause::Connector(call) = failure.cause() else {
                panic!("connector must retain its original source");
            };
            assert_eq!(call.cause(), &source);
            assert!(call.cause().message().contains("private-provider"));
        }
    }

    #[test]
    fn typed_failure_launch_cleanup_keeps_original_kernel_payload_and_source_chain() {
        use crate::runtime::task::{
            LocalTaskLaunchError, LocalTaskLaunchErrorKind, LocalTaskLaunchStage,
        };
        let original = KernelFailure::Internal(KernelDiagnostic::new("original internal cause"));
        let error = LocalTaskLaunchError::from_failure(
            LocalTaskLaunchStage::BuildPipelines,
            LocalTaskLaunchErrorKind::PipelineBuild,
            original.clone().into(),
        )
        .with_cleanup_diagnostics(vec![
            KernelFailure::Internal(KernelDiagnostic::new("secondary cleanup failure")).into(),
        ]);
        assert_eq!(
            error.cause().cause(),
            &ExecutionFailureCause::Kernel(original.clone())
        );
        assert_eq!(
            error.cleanup_diagnostics()[0].detail(),
            "secondary cleanup failure"
        );
        assert_eq!(
            novarocks_failure_contract::FailureProjection::failure_fact(
                &error.cleanup_diagnostics()[0]
            ),
            novarocks_failure_contract::FailureProjection::failure_fact(&KernelFailure::Internal(
                KernelDiagnostic::new("different diagnostic")
            ))
        );
        assert_eq!(
            error
                .source()
                .unwrap()
                .source()
                .unwrap()
                .downcast_ref::<KernelFailure>(),
            Some(&original)
        );
    }
}

impl novarocks_failure_contract::FailureProjection for RequiredExpressionRowError {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        self.error.failure_fact().with_row_position(
            novarocks_failure_contract::FailureRowPosition {
                selected_ordinal: u64::try_from(self.error.selected_ordinal())
                    .expect("supported pointer widths fit u64"),
                batch_row: u64::try_from(self.batch_row).expect("supported pointer widths fit u64"),
            },
        )
    }
}
impl novarocks_failure_contract::FailureProjection for ExecutionFailure {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        self.cause.cause.failure_fact()
    }
}
impl novarocks_failure_contract::FailureProjection for ExecutionFailureCause {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        match self {
            ExecutionFailureCause::Domain(cause) => cause.failure_fact(),
            ExecutionFailureCause::MemoryAccounting(cause) => cause.failure_fact(),
            ExecutionFailureCause::Connector(cause) => cause.failure_fact(),
            ExecutionFailureCause::Writer(cause) => cause.failure_fact(),
            ExecutionFailureCause::Construction(cause) => cause.failure_fact(),
            ExecutionFailureCause::Kernel(cause) => cause.failure_fact(),
            ExecutionFailureCause::RuntimeFilter(cause) => cause.failure_fact(),
            ExecutionFailureCause::RuntimeScalarSource(cause) => cause.failure_fact(),
            ExecutionFailureCause::RuntimeScalarMemory(cause) => cause.failure_fact(),
            ExecutionFailureCause::InvocationData(cause) => cause.failure_fact(),
            ExecutionFailureCause::ScalarInvocationData(cause) => cause.failure_fact(),
            ExecutionFailureCause::WindowInvocationData(cause) => cause.failure_fact(),
            ExecutionFailureCause::RequiredRow(cause) => cause.failure_fact(),
            ExecutionFailureCause::PreparationMetadata(cause) => cause.failure_fact(),
        }
    }
}

impl From<novarocks_local_program::StaticExpressionError> for ExecutionFailure {
    fn from(source: novarocks_local_program::StaticExpressionError) -> Self {
        Self::from_domain(source)
    }
}
impl From<novarocks_local_program::StaticSinkError> for ExecutionFailure {
    fn from(source: novarocks_local_program::StaticSinkError) -> Self {
        Self::from_domain(source)
    }
}

impl From<novarocks_types::value::variant::VariantFailure> for ExecutionFailure {
    fn from(source: novarocks_types::value::variant::VariantFailure) -> Self {
        Self::from_domain(source)
    }
}
impl From<novarocks_types::value::variant_encode::VariantEncodeFailure> for ExecutionFailure {
    fn from(source: novarocks_types::value::variant_encode::VariantEncodeFailure) -> Self {
        Self::from_domain(source)
    }
}

impl From<novarocks_type_contract::VariantTargetTypeError> for ExecutionFailure {
    fn from(source: novarocks_type_contract::VariantTargetTypeError) -> Self {
        Self::from_domain(source)
    }
}

impl From<crate::runtime::mem_tracker::MemTrackerLimitExceeded> for ExecutionFailure {
    fn from(source: crate::runtime::mem_tracker::MemTrackerLimitExceeded) -> Self {
        Self::from_domain(source)
    }
}
impl From<novarocks_functions::AggregateInputBatchError> for ExecutionFailure {
    fn from(source: novarocks_functions::AggregateInputBatchError) -> Self {
        Self::from_domain(source)
    }
}

impl From<novarocks_functions::largeint::LargeIntCarrierFailure> for ExecutionFailure {
    fn from(cause: novarocks_functions::largeint::LargeIntCarrierFailure) -> Self {
        Self::from_domain(cause)
    }
}

impl From<novarocks_functions::array_projection::ArrayProjectionFailure> for ExecutionFailure {
    fn from(source: novarocks_functions::array_projection::ArrayProjectionFailure) -> Self {
        Self::from_domain(source)
    }
}

#[cfg(test)]
mod required_row_source_properties {
    use super::*;
    use novarocks_failure_contract::FailureProjection;
    #[test]
    fn required_root_preserves_the_row_error_source_and_selection_address() {
        let rows = [1, 4];
        let selection = Selection::try_sparse(5, &rows).unwrap();
        let row = RowDataError::with_reason(
            1,
            novarocks_functions::DataFailureReason::NumericOutOfRange,
            "out of range",
        );
        let expected = row.failure_fact();
        let required = RequiredExpressionRowError::try_new(
            ProgramExpressionRootSite::SinkPartition { branch: 0, key: 1 },
            selection,
            row,
        )
        .unwrap();
        assert_eq!(required.batch_row(), 4);
        let failure = ExecutionFailure::from(required);
        assert_eq!(
            failure.failure_fact(),
            expected.with_row_position(novarocks_failure_contract::FailureRowPosition {
                selected_ordinal: 1,
                batch_row: 4,
            })
        );
        let mut source: &(dyn Error + 'static) = &failure;
        loop {
            if let Some(row) = source.downcast_ref::<RowDataError>() {
                assert_eq!(row.selected_ordinal(), 1);
                assert_eq!(row.failure_fact(), expected);
                assert_eq!(row.message(), "out of range");
                break;
            }
            source = source
                .source()
                .expect("required root must keep its original row error");
        }
    }
}

#[cfg(test)]
mod source_retention_tests {
    use super::*;
    use novarocks_execution_contract::SourceRetentionCapability;
    use novarocks_failure_contract::{
        FailureClass, FailureFact, FailureProjection, FailureReason, domain,
    };
    use std::{
        num::NonZeroU16,
        sync::atomic::{AtomicBool, AtomicUsize, Ordering},
    };

    #[derive(Debug)]
    struct Source {
        drops: Arc<AtomicUsize>,
        panic_on_drop: bool,
    }
    impl fmt::Display for Source {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str("source retention probe")
        }
    }
    impl Error for Source {}
    impl novarocks_failure_contract::OuterFailureProjection for Source {}
    impl FailureProjection for Source {
        fn failure_fact(&self) -> FailureFact {
            FailureFact::new(
                FailureClass::Data,
                FailureReason::new(domain::EXECUTION, NonZeroU16::new(65000).unwrap()),
            )
        }
    }
    impl Drop for Source {
        fn drop(&mut self) {
            self.drops.fetch_add(1, Ordering::SeqCst);
            assert!(!self.panic_on_drop, "injected original source Drop panic");
        }
    }
    #[derive(Debug)]
    struct Retention {
        source_drops: Arc<AtomicUsize>,
        exits: Arc<AtomicUsize>,
        unwound: Arc<AtomicBool>,
    }
    impl SourceRetentionCapability for Retention {}
    impl Drop for Retention {
        fn drop(&mut self) {
            assert_eq!(
                self.source_drops.load(Ordering::SeqCst),
                1,
                "the original source must drop before any capture marker"
            );
            self.unwound
                .store(std::thread::panicking(), Ordering::SeqCst);
            self.exits.fetch_add(1, Ordering::SeqCst);
        }
    }
    #[test]
    fn typed_wrapper_aliases_keep_their_original_canonical_source_and_unwind_observation() {
        use crate::exec::chunk::CompiledSchemaRequestFailure as Schema;
        use crate::runtime::preparation_memory::PreparationMemoryRefusal as Host;
        use crate::runtime::preparation_metadata::PreparationMetadataFailure as Metadata;
        use novarocks_type_contract::{CompileControlError, MetadataRequestError};
        for route in 0..9 {
            for early in [false, true] {
                for panic_on_drop in [false, true] {
                    let source_drops = Arc::new(AtomicUsize::new(0));
                    let exits = Arc::new(AtomicUsize::new(0));
                    let unwound = Arc::new(AtomicBool::new(false));
                    let source = Source {
                        drops: source_drops.clone(),
                        panic_on_drop,
                    };
                    let failure: ExecutionFailure = match route {
                        0 => KernelFailure::from_domain(source).into(),
                        1 => {
                            KernelFailure::CompileControl(CompileControlError::from_domain(source))
                                .into()
                        }
                        2 => ConstructionFailure::Source(super::super::SharedDomainFailure::new(
                            source,
                        ))
                        .into(),
                        3 => ConstructionFailure::Control(CompileControlError::from_domain(source))
                            .into(),
                        4 => Metadata::SchemaRequest(Schema::Projection(
                            super::super::SharedDomainFailure::new(source),
                        ))
                        .into(),
                        5 => Metadata::Request(MetadataRequestError::Control(
                            CompileControlError::from_domain(source),
                        ))
                        .into(),
                        6 => Metadata::SchemaRequest(Schema::Request(
                            MetadataRequestError::Control(CompileControlError::from_domain(source)),
                        ))
                        .into(),
                        7 => {
                            Metadata::Host(Host::Interrupted(ExecutionFailure::from_domain(source)))
                                .into()
                        }
                        8 => ConstructionFailure::Caller(Box::new(ExecutionFailure::from_domain(
                            source,
                        )))
                        .into(),
                        _ => unreachable!(),
                    };
                    // This is a typed inner alias, not an Execution occurrence
                    // clone. It can outlive every outer ExecutionFailure.
                    let alias = early.then(|| failure.cause().clone());
                    failure.retain_source_lifetime(Box::new(Retention {
                        source_drops: source_drops.clone(),
                        exits: exits.clone(),
                        unwound: unwound.clone(),
                    }));
                    let alias = alias.unwrap_or_else(|| failure.cause().clone());
                    drop(failure);
                    assert_eq!(source_drops.load(Ordering::SeqCst), 0, "route {route}");
                    assert_eq!(exits.load(Ordering::SeqCst), 0, "route {route}");
                    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                        drop(alias);
                    }));
                    assert_eq!(result.is_err(), panic_on_drop, "route {route}");
                    assert_eq!(source_drops.load(Ordering::SeqCst), 1);
                    assert_eq!(exits.load(Ordering::SeqCst), 1);
                    assert_eq!(unwound.load(Ordering::SeqCst), panic_on_drop);
                }
            }
        }
    }

    #[test]
    fn source_retention_includes_clones_created_before_and_after_admission() {
        let source_drops = Arc::new(AtomicUsize::new(0));
        let exits = Arc::new(AtomicUsize::new(0));
        let unwound = Arc::new(AtomicBool::new(false));
        let failure = ExecutionFailure::from_domain(Source {
            drops: source_drops.clone(),
            panic_on_drop: false,
        });
        let early = failure.clone();
        let original_address = failure.cause() as *const ExecutionFailureCause;
        for _ in 0..2 {
            failure.retain_source_lifetime(Box::new(Retention {
                source_drops: source_drops.clone(),
                exits: exits.clone(),
                unwound: unwound.clone(),
            }));
        }
        let late = failure.clone();
        assert!(failure.same_occurrence(&early));
        assert_eq!(failure, early);
        assert_eq!(
            original_address,
            early.cause() as *const ExecutionFailureCause
        );
        assert_eq!(
            original_address,
            late.cause() as *const ExecutionFailureCause
        );
        drop(failure);
        drop(late);
        assert_eq!(
            exits.load(Ordering::SeqCst),
            0,
            "an earlier clone still holds the shared original source"
        );
        assert_eq!(source_drops.load(Ordering::SeqCst), 0);
        drop(early);
        assert_eq!(source_drops.load(Ordering::SeqCst), 1);
        assert_eq!(exits.load(Ordering::SeqCst), 2);
        assert!(!unwound.load(Ordering::SeqCst));
    }

    #[test]
    fn a_domain_handle_from_either_public_read_path_keeps_original_source_custody() {
        for alias_before_admission in [false, true] {
            for through_error_source in [false, true] {
                let source_drops = Arc::new(AtomicUsize::new(0));
                let exits = Arc::new(AtomicUsize::new(0));
                let unwound = Arc::new(AtomicBool::new(false));
                let failure = ExecutionFailure::from_domain(Source {
                    drops: source_drops.clone(),
                    panic_on_drop: false,
                });
                let inner = || {
                    if through_error_source {
                        failure
                            .source()
                            .unwrap()
                            .downcast_ref::<super::super::SharedDomainFailure>()
                            .unwrap()
                            .clone()
                    } else {
                        let ExecutionFailureCause::Domain(inner) = failure.cause() else {
                            panic!("original domain source");
                        };
                        inner.clone()
                    }
                };
                let early = alias_before_admission.then(inner);
                failure.retain_source_lifetime(Box::new(Retention {
                    source_drops: source_drops.clone(),
                    exits: exits.clone(),
                    unwound: unwound.clone(),
                }));
                let alias = early.unwrap_or_else(inner);
                let original_pointer = alias.downcast_ref::<Source>().unwrap() as *const Source;
                drop(failure);
                assert_eq!(source_drops.load(Ordering::SeqCst), 0);
                assert_eq!(
                    exits.load(Ordering::SeqCst),
                    0,
                    "an independently retained inner handle still owns the original source"
                );
                assert_eq!(
                    alias.downcast_ref::<Source>().unwrap() as *const Source,
                    original_pointer
                );
                drop(alias);
                assert_eq!(source_drops.load(Ordering::SeqCst), 1);
                assert_eq!(exits.load(Ordering::SeqCst), 1);
                assert!(!unwound.load(Ordering::SeqCst));
            }
        }
    }

    #[test]
    fn rewrapping_a_shared_domain_handle_preserves_its_original_source_custody() {
        let source_drops = Arc::new(AtomicUsize::new(0));
        let exits = Arc::new(AtomicUsize::new(0));
        let unwound = Arc::new(AtomicBool::new(false));
        let original = super::super::SharedDomainFailure::new(Source {
            drops: source_drops.clone(),
            panic_on_drop: false,
        });
        let source_pointer = original.downcast_ref::<Source>().unwrap() as *const Source;
        let failure = ExecutionFailure::from_domain(original.clone());
        failure.retain_source_lifetime(Box::new(Retention {
            source_drops: source_drops.clone(),
            exits: exits.clone(),
            unwound: unwound.clone(),
        }));
        let replacement = ExecutionFailure::from_domain(original.clone());
        let ExecutionFailureCause::Domain(replacement_source) = replacement.cause() else {
            panic!("rewrapped original domain source");
        };
        assert_eq!(replacement_source, &original);
        assert_eq!(
            replacement_source.downcast_ref::<Source>().unwrap() as *const Source,
            source_pointer
        );
        drop(failure);
        drop(original);
        assert_eq!(source_drops.load(Ordering::SeqCst), 0);
        assert_eq!(exits.load(Ordering::SeqCst), 0);
        drop(replacement);
        assert_eq!(source_drops.load(Ordering::SeqCst), 1);
        assert_eq!(exits.load(Ordering::SeqCst), 1);
        assert!(!unwound.load(Ordering::SeqCst));
    }
    #[test]
    fn a_panicking_original_source_reaches_its_capture_marker_during_unwind() {
        let source_drops = Arc::new(AtomicUsize::new(0));
        let exits = Arc::new(AtomicUsize::new(0));
        let unwound = Arc::new(AtomicBool::new(false));
        let failure = ExecutionFailure::from_domain(Source {
            drops: source_drops.clone(),
            panic_on_drop: true,
        });
        failure.retain_source_lifetime(Box::new(Retention {
            source_drops: source_drops.clone(),
            exits: exits.clone(),
            unwound: unwound.clone(),
        }));
        assert!(std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| drop(failure))).is_err());
        assert_eq!(source_drops.load(Ordering::SeqCst), 1);
        assert_eq!(exits.load(Ordering::SeqCst), 1);
        assert!(
            unwound.load(Ordering::SeqCst),
            "the host must observe unknown source exit rather than fabricate success"
        );
    }
}
