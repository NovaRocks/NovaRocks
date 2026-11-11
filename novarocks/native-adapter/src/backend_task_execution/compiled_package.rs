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

//! The compiled-package static plan interpreter.
//!
//! A host composes exactly one static plan interpreter. When it composes this
//! one, a task's static carrier is a physical package that is received,
//! provider-validated and compiled into a LocalProgram during preparation;
//! the plan-tree decoder is never consulted. The host-owned decode model,
//! limits and catalogs are built once at composition, never per task.

// Design: ADR-0185 (docs/adr/ADR-0185-compiled-packages-are-the-sole-static-plan-path.md)

use novarocks_execution_contract::failure::{FailureProjection, SharedFailureCause};
use std::error::Error;
use std::fmt;
use std::num::NonZeroUsize;
use std::sync::Arc;

use novarocks_connector_contract::PureProviderProgramCatalog;
use novarocks_functions::{ConstantPolicy, PureEngineFunctionCatalog};
use novarocks_local_compiler::{
    FragmentCreationError, LocalCompileOptions, ProviderPreparationError,
    compile_fragment_with_creation, compile_fragment_with_creation_and_project_metadata_host,
    validate_fragment_providers,
};
use novarocks_local_program::{KernelAbiVersion, LocalProgram};
use novarocks_plan_codec::host_projection_v2::ProjectionFailure;
use novarocks_plan_codec::physical_package_v2::{
    PackageDecodeError, PackageDecodeLimits, decode_fragment_package_with_factory,
    decode_fragment_package_with_factory_and_funding,
};
use novarocks_plan_codec::resource_preflight_v2::FragmentDecodeResourceModel;
use novarocks_spi::connector::ConnectorStopView;
use novarocks_type_contract::{
    CompileControlError, CompilePhase, PureCompileControl, ValueSchemaCreation,
};

pub use crate::native_type_construction::NativeTypeConstructionError;

use crate::compiled_runtime_filter::CompiledRuntimeFilterEndpoints;

use super::project_materialization::ProjectMetadataHost;
use super::type_materialization::{TypeMaterializationHost, TypeMaterializationRefusal};
use novarocks_execution::runtime::preparation_memory::PreparationMemoryRefusal;
use novarocks_type_contract::MetadataRequestError;

/// The task facts a compiled program is specialized for.
#[derive(Clone, Copy, Debug)]
pub struct CompiledTaskOptions {
    pub pipeline_dop: NonZeroUsize,
}

/// Why a task's package did not become a LocalProgram.
#[derive(Debug)]
pub enum CompiledPackageError {
    /// The task's own control stopped the work; the cause stays primary.
    Control(CompileControlError),
    /// The original BE construction owner refused an actual request.
    Construction(NativeTypeConstructionError),
    /// The original receiver cause, including its source/resource model.
    Decode(PackageDecodeError),
    RuntimeFilter(crate::compiled_runtime_filter::CompiledRuntimeFilterBindingError),
    Provider(SharedFailureCause),
    Compile(novarocks_local_compiler::FragmentCompileError),
    TypeMaterialization(TypeMaterializationRefusal),
    ProjectMetadata(PreparationMemoryRefusal),
    ProjectMetadataRequest(MetadataRequestError),
}

impl fmt::Display for CompiledPackageError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Control(cause) => cause.fmt(f),
            Self::Construction(cause) => cause.fmt(f),
            Self::Decode(cause) => cause.fmt(f),
            Self::RuntimeFilter(source) => source.fmt(f),
            Self::Provider(source) => source.fmt(f),
            Self::Compile(source) => source.fmt(f),
            Self::TypeMaterialization(cause) => cause.fmt(f),
            Self::ProjectMetadata(cause) => cause.fmt(f),
            Self::ProjectMetadataRequest(cause) => cause.fmt(f),
        }
    }
}

impl Error for CompiledPackageError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Control(cause) => Some(cause),
            Self::Construction(cause) => Some(cause),
            Self::Decode(cause) => Some(cause),
            Self::RuntimeFilter(source) => Some(source),
            Self::Provider(source) => Some(source),
            Self::Compile(source) => Some(source),
            Self::TypeMaterialization(cause) => Some(cause),
            Self::ProjectMetadata(cause) => Some(cause),
            Self::ProjectMetadataRequest(cause) => Some(cause),
        }
    }
}

impl FailureProjection for CompiledPackageError {
    fn failure_fact(&self) -> novarocks_execution_contract::failure::FailureFact {
        match self {
            Self::Control(source) => source.failure_fact(),
            Self::Construction(source) => source.failure_fact(),
            Self::Decode(source) => source.failure_fact(),
            Self::RuntimeFilter(source) => source.failure_fact(),
            Self::Provider(source) => source.failure_fact(),
            Self::Compile(source) => source.failure_fact(),
            Self::TypeMaterialization(source) => source.failure_fact(),
            Self::ProjectMetadata(source) => source.failure_fact(),
            Self::ProjectMetadataRequest(source) => source.failure_fact(),
        }
    }
}

fn provider_preparation_failure<E: Error + FailureProjection + Send + Sync + 'static>(
    error: ProviderPreparationError<E>,
) -> CompiledPackageError {
    use novarocks_connector_contract::PureProviderProgramError;
    if let ProviderPreparationError::Control(cause) = error {
        return CompiledPackageError::Control(cause);
    }
    let (ProviderPreparationError::Read { error: cause, .. }
    | ProviderPreparationError::Write { error: cause, .. }) = &error
    else {
        unreachable!("control cause handled above");
    };
    if let PureProviderProgramError::Control(cause) = cause {
        return CompiledPackageError::Control(cause.clone());
    }
    CompiledPackageError::Provider(SharedFailureCause::new(error))
}

/// One task's compiled program and the runtime-filter bindings its package
/// numbers.
///
/// The program names each runtime-filter site by its plan-global binding
/// identity only; which filter and endpoint a binding is stays in the
/// package's cuts, so it is projected here, before the package is consumed.
#[derive(Debug)]
pub struct CompiledTaskProgram {
    output: CompiledProgramOutput,
    pub runtime_filters: CompiledRuntimeFilterEndpoints,
}

#[derive(Debug)]
enum CompiledProgramOutput {
    Native(ValidatedNativeProgram),
    Direct(LocalProgram),
}
#[derive(Debug)]
struct ValidatedNativeProgram {
    program: LocalProgram,
}
impl CompiledTaskProgram {
    /// Independently authored programs retain their original Direct preparation.
    pub fn direct(program: LocalProgram, runtime_filters: CompiledRuntimeFilterEndpoints) -> Self {
        Self {
            output: CompiledProgramOutput::Direct(program),
            runtime_filters,
        }
    }
    pub fn program(&self) -> &LocalProgram {
        match &self.output {
            CompiledProgramOutput::Native(source) => &source.program,
            CompiledProgramOutput::Direct(program) => program,
        }
    }

    /// Transfer the actual received program to a component execution fixture.
    /// This creates no metadata authority and is absent from product builds.
    #[cfg(any(test, novarocks_dev_controls))]
    #[doc(hidden)]
    pub fn into_local_program_for_test(self) -> LocalProgram {
        match self.output {
            CompiledProgramOutput::Native(source) => source.program,
            CompiledProgramOutput::Direct(program) => program,
        }
    }
    fn native(program: LocalProgram, runtime_filters: CompiledRuntimeFilterEndpoints) -> Self {
        Self {
            output: CompiledProgramOutput::Native(ValidatedNativeProgram { program }),
            runtime_filters,
        }
    }
    /// Consume the sealed output at the original Arc allocation handoff.
    /// No owned program or detachable provenance can be recombined by callers.
    pub(crate) fn into_preparation(self) -> PreparedCompiledProgram {
        match self.output {
            CompiledProgramOutput::Native(source) => PreparedCompiledProgram {
                program: Arc::new(source.program),
                source: PreparedCompiledSource::Native,
                _runtime_filters: self.runtime_filters,
            },
            CompiledProgramOutput::Direct(program) => PreparedCompiledProgram {
                program: Arc::new(program),
                source: PreparedCompiledSource::Direct,
                _runtime_filters: self.runtime_filters,
            },
        }
    }
}
enum PreparedCompiledSource {
    Native,
    Direct,
}
pub(crate) struct PreparedCompiledProgram {
    program: Arc<LocalProgram>,
    source: PreparedCompiledSource,
    _runtime_filters: CompiledRuntimeFilterEndpoints,
}
impl PreparedCompiledProgram {
    pub(crate) fn program(&self) -> &Arc<LocalProgram> {
        &self.program
    }
    pub(crate) fn native_metadata_loan(&self) -> Option<NativeCompiledMetadataLoan<'_>> {
        match self.source {
            PreparedCompiledSource::Native => Some(NativeCompiledMetadataLoan {
                program: &self.program,
            }),
            PreparedCompiledSource::Direct => None,
        }
    }
}
pub(crate) struct NativeCompiledMetadataLoan<'a> {
    program: &'a Arc<LocalProgram>,
}
impl NativeCompiledMetadataLoan<'_> {
    pub(crate) fn program(&self) -> &Arc<LocalProgram> {
        self.program
    }
}

/// A sealed, borrowed factory supplied by the actual admitted BE preparation.
/// This public port preserves the compiler's object safety; constructing it is
/// restricted to the Native owner that already has the tracker and controls.
pub struct CompiledTypeScopes<'borrow, 'control> {
    inner: crate::native_type_construction::NativeTypeFactory<'borrow, 'control>,
}
impl<'borrow, 'control> CompiledTypeScopes<'borrow, 'control> {
    pub(crate) fn new(
        creation: &'borrow crate::native_type_construction::NativeTypeConstruction<'control>,
    ) -> Self {
        Self {
            inner: crate::native_type_construction::NativeTypeFactory::new(creation),
        }
    }
}
pub struct CompiledTypeScope<'borrow> {
    inner: crate::native_type_construction::NativeFactoryScope<'borrow>,
}
impl novarocks_type_contract::ValueSchemaCreationScope<NativeTypeConstructionError>
    for CompiledTypeScope<'_>
{
    fn storage_lifetime(&self) -> novarocks_type_contract::ValueTypeStorageLifetime {
        novarocks_type_contract::ValueSchemaCreationScope::storage_lifetime(&self.inner)
    }
    fn request_layout(
        &mut self,
        layout: std::alloc::Layout,
    ) -> Result<(), NativeTypeConstructionError> {
        novarocks_type_contract::ValueSchemaCreationScope::request_layout(&mut self.inner, layout)
    }
    fn observe_opaque_upper_bound(
        &mut self,
        bytes: usize,
    ) -> Result<(), NativeTypeConstructionError> {
        novarocks_type_contract::ValueSchemaCreationScope::observe_opaque_upper_bound(
            &mut self.inner,
            bytes,
        )
    }
}
impl novarocks_type_contract::ValueSchemaCreationFactory<NativeTypeConstructionError>
    for CompiledTypeScopes<'_, '_>
{
    type Scope<'a>
        = CompiledTypeScope<'a>
    where
        Self: 'a;
    fn visit_scope_allocation_layouts<E>(
        &self,
        visit: &mut dyn FnMut(std::alloc::Layout) -> Result<(), E>,
    ) -> Result<(), E>
    where
        E: From<novarocks_type_contract::owned_resources::layout::LayoutResourceError>,
    {
        self.inner.visit_scope_allocation_layouts(visit)
    }
    fn begin_scope(&self) -> Result<Self::Scope<'_>, NativeTypeConstructionError> {
        Ok(CompiledTypeScope {
            inner: self.inner.begin_scope()?,
        })
    }
    fn begin_scope_observed<O>(
        &self,
        observe: &mut dyn FnMut(std::alloc::Layout) -> Result<(), O>,
    ) -> Result<
        Self::Scope<'_>,
        novarocks_type_contract::ValueSchemaScopeBeginFailure<O, NativeTypeConstructionError>,
    > {
        Ok(CompiledTypeScope {
            inner: self.inner.begin_scope_observed(observe)?,
        })
    }
}

/// Turns one task's package bytes into its LocalProgram.
pub trait CompiledPackageCompiler: Send + Sync + 'static {
    /// Explicit Direct construction retains the main typed creator contract.
    /// It grants no Native funding-source provenance.
    fn compile(
        &self,
        package: &[u8],
        options: CompiledTaskOptions,
        creation: &mut ValueSchemaCreation<'_, NativeTypeConstructionError>,
        type_scopes: &CompiledTypeScopes<'_, '_>,
        control: &dyn PureCompileControl,
    ) -> Result<CompiledTaskProgram, CompiledPackageError>;

    /// The admitted Native owner supplies both original funding capabilities.
    /// There is no default delegation to Direct preparation.
    fn compile_with_funding(
        &self,
        package: &[u8],
        options: CompiledTaskOptions,
        creation: &mut ValueSchemaCreation<'_, NativeTypeConstructionError>,
        type_scopes: &CompiledTypeScopes<'_, '_>,
        control: &dyn PureCompileControl,
        type_host: &mut TypeMaterializationHost<'_>,
        project_host: &mut ProjectMetadataHost<'_>,
    ) -> Result<CompiledTaskProgram, CompiledPackageError>;
}

/// Receiver, provider validation and local compiler over host-owned inputs.
pub struct CompiledPackageInterpreter<E: Error + Send + Sync + 'static> {
    /// Shared with the ingress package gate, so one process builds one model.
    model: Arc<FragmentDecodeResourceModel>,
    decode_limits: PackageDecodeLimits,
    functions: Arc<PureEngineFunctionCatalog>,
    providers: Arc<PureProviderProgramCatalog<E>>,
    constants: ConstantPolicy,
}

impl<E: Error + Send + Sync + 'static> CompiledPackageInterpreter<E> {
    pub fn new(
        model: impl Into<Arc<FragmentDecodeResourceModel>>,
        decode_limits: PackageDecodeLimits,
        functions: Arc<PureEngineFunctionCatalog>,
        providers: Arc<PureProviderProgramCatalog<E>>,
        constants: ConstantPolicy,
    ) -> Self {
        Self {
            model: model.into(),
            decode_limits,
            functions,
            providers,
            constants,
        }
    }
}

impl<E: Error + FailureProjection + Send + Sync + 'static> CompiledPackageCompiler
    for CompiledPackageInterpreter<E>
where
    PureProviderProgramCatalog<E>: Send + Sync,
{
    fn compile(
        &self,
        package: &[u8],
        options: CompiledTaskOptions,
        creation: &mut ValueSchemaCreation<'_, NativeTypeConstructionError>,
        type_scopes: &CompiledTypeScopes<'_, '_>,
        control: &dyn PureCompileControl,
    ) -> Result<CompiledTaskProgram, CompiledPackageError> {
        self.compile_body(package, options, creation, type_scopes, control, None, None)
    }
    fn compile_with_funding(
        &self,
        package: &[u8],
        options: CompiledTaskOptions,
        creation: &mut ValueSchemaCreation<'_, NativeTypeConstructionError>,
        type_scopes: &CompiledTypeScopes<'_, '_>,
        control: &dyn PureCompileControl,
        type_host: &mut TypeMaterializationHost<'_>,
        project_host: &mut ProjectMetadataHost<'_>,
    ) -> Result<CompiledTaskProgram, CompiledPackageError> {
        self.compile_body(
            package,
            options,
            creation,
            type_scopes,
            control,
            Some(type_host),
            Some(project_host),
        )
    }
}
impl<E: Error + FailureProjection + Send + Sync + 'static> CompiledPackageInterpreter<E> {
    fn compile_body(
        &self,
        package: &[u8],
        options: CompiledTaskOptions,
        creation: &mut ValueSchemaCreation<'_, NativeTypeConstructionError>,
        type_scopes: &CompiledTypeScopes<'_, '_>,
        control: &dyn PureCompileControl,
        type_host: Option<&mut TypeMaterializationHost<'_>>,
        project_host: Option<&mut ProjectMetadataHost<'_>>,
    ) -> Result<CompiledTaskProgram, CompiledPackageError> {
        let native = type_host.is_some() && project_host.is_some();
        let package = match type_host {
            Some(type_host) => decode_fragment_package_with_factory_and_funding(
                package,
                &self.model,
                &self.decode_limits,
                creation,
                type_scopes,
                control,
                &mut NativeTypeFunding(type_host),
            ),
            None => decode_fragment_package_with_factory(
                package,
                &self.model,
                &self.decode_limits,
                creation,
                type_scopes,
                control,
            ),
        }
        .map_err(package_decode_failure)?;
        let runtime_filters = CompiledRuntimeFilterEndpoints::from_package(&package)
            .map_err(CompiledPackageError::RuntimeFilter)?;
        // The compiled RootResult algorithm gathers its terminal pipeline to
        // one driver; this width is independent of the task pipeline DOP.
        let root_sink_dop = matches!(
            package.fragment().sink(),
            novarocks_physical_plan::FragmentSink::RootResult(_)
        )
        .then_some(NonZeroUsize::MIN);
        let validated = validate_fragment_providers(Arc::new(package), &self.providers, control)
            .map_err(provider_preparation_failure)?;
        use novarocks_type_contract::ValueSchemaCreationFactory;
        let mut temporary = type_scopes
            .begin_scope()
            .map_err(CompiledPackageError::Construction)?;
        let options = LocalCompileOptions {
            pipeline_dop: options.pipeline_dop,
            root_sink_dop,
            kernel_abi: KernelAbiVersion::CURRENT,
            constants: self.constants,
        };
        let program = match project_host {
            Some(project_host) => compile_fragment_with_creation_and_project_metadata_host(
                validated,
                &self.functions,
                options,
                creation,
                &mut temporary,
                type_scopes,
                control,
                project_host,
            ),
            None => compile_fragment_with_creation(
                validated,
                &self.functions,
                options,
                creation,
                &mut temporary,
                type_scopes,
                control,
            ),
        }
        .map_err(|error| match error {
            FragmentCreationError::Control(cause) => CompiledPackageError::Control(cause),
            FragmentCreationError::Creator(cause) => CompiledPackageError::Construction(cause),
            FragmentCreationError::Resource(cause) => {
                CompiledPackageError::Construction(NativeTypeConstructionError::Resource(cause))
            }
            FragmentCreationError::Compile(source) => CompiledPackageError::Compile(source),
        })?;
        Ok(if native {
            CompiledTaskProgram::native(program, runtime_filters)
        } else {
            CompiledTaskProgram::direct(program, runtime_filters)
        })
    }
}

/// Borrow the actual Native creator without exposing a provenance constructor.
#[cfg(any(test, novarocks_dev_controls))]
pub fn with_native_type_construction_for_test<R>(
    tracker: Arc<novarocks_execution::runtime::mem_tracker::MemTracker>,
    control: &dyn PureCompileControl,
    body: impl FnOnce(
        &mut ValueSchemaCreation<'_, NativeTypeConstructionError>,
        &CompiledTypeScopes<'_, '_>,
    ) -> R,
) -> Result<R, NativeTypeConstructionError> {
    let creator = crate::native_type_construction::NativeTypeConstruction::try_with_control(
        tracker, control,
    )?;
    let scopes = CompiledTypeScopes::new(&creator);
    let mut request = |layout| creator.request_layout(layout);
    let mut opaque = |bytes| creator.request_opaque_upper_bound(bytes);
    let mut creation =
        ValueSchemaCreation::new(creator.storage_lifetime(), &mut request, &mut opaque);
    Ok(body(&mut creation, &scopes))
}

/// Test-support composition of the actual admitted creator and funded receiver.
/// The test supplies its real tracker/control and original funding hosts.
#[cfg(any(test, novarocks_dev_controls))]
pub fn compile_native_package_with_funding_for_test(
    compiler: &dyn CompiledPackageCompiler,
    package: &[u8],
    options: CompiledTaskOptions,
    tracker: Arc<novarocks_execution::runtime::mem_tracker::MemTracker>,
    control: &dyn PureCompileControl,
    type_host: &mut TypeMaterializationHost<'_>,
    project_host: &mut ProjectMetadataHost<'_>,
) -> Result<CompiledTaskProgram, CompiledPackageError> {
    let creator =
        crate::native_type_construction::NativeTypeConstruction::try_with_control(tracker, control)
            .map_err(CompiledPackageError::Construction)?;
    let scopes = CompiledTypeScopes::new(&creator);
    let mut request = |layout| creator.request_layout(layout);
    let mut opaque = |bytes| creator.request_opaque_upper_bound(bytes);
    let mut creation =
        ValueSchemaCreation::new(creator.storage_lifetime(), &mut request, &mut opaque);
    compiler.compile_with_funding(
        package,
        options,
        &mut creation,
        &scopes,
        control,
        type_host,
        project_host,
    )
}

struct NativeTypeFunding<'borrow, 'host>(&'borrow mut TypeMaterializationHost<'host>);
impl novarocks_plan_codec::physical_type_v2::PackageTypeFundingScope<NativeTypeConstructionError>
    for NativeTypeFunding<'_, '_>
{
    fn materialize(
        &mut self,
        namespace: novarocks_plan_codec::physical_type_v2::TypeMaterializationNamespace,
        facts: &novarocks_plan_codec::physical_type_v2::PackageTypeProjectionFacts,
        body: &mut dyn FnMut() -> Result<
            novarocks_plan_codec::physical_type_v2::DecodedTypeTable,
            ProjectionFailure<
                novarocks_plan_codec::physical_type_v2::TypeCodecError,
                NativeTypeConstructionError,
            >,
        >,
    ) -> Result<
        novarocks_plan_codec::physical_type_v2::DecodedTypeTable,
        ProjectionFailure<
            novarocks_plan_codec::physical_type_v2::TypeCodecError,
            NativeTypeConstructionError,
        >,
    > {
        use novarocks_plan_codec::physical_type_v2::PackageTypeMaterializationScope;
        match self.0.materialize_namespace(namespace, facts, body) {
            Ok(value) => Ok(value),
            Err(ProjectionFailure::Codec(original)) => Err(original),
            Err(ProjectionFailure::Host(original)) => Err(ProjectionFailure::Host(
                NativeTypeConstructionError::Funding(Box::new(original)),
            )),
        }
    }
}

fn package_decode_failure(
    error: ProjectionFailure<PackageDecodeError, NativeTypeConstructionError>,
) -> CompiledPackageError {
    match error {
        ProjectionFailure::Host(NativeTypeConstructionError::Funding(cause)) => {
            CompiledPackageError::TypeMaterialization(*cause)
        }
        ProjectionFailure::Host(cause) => CompiledPackageError::Construction(cause),
        ProjectionFailure::Codec(PackageDecodeError::Control(cause)) => {
            CompiledPackageError::Control(cause)
        }
        ProjectionFailure::Codec(other) => CompiledPackageError::Decode(other),
    }
}

/// Caller control for one exact Task preparation. Its accepted originating
/// failure or control observation crosses decode, provider validation and
/// compilation unchanged; an observation never creates a cancellation intent.
pub(crate) struct TaskPreparationControl {
    stop: ConnectorStopView,
    original: novarocks_worker::TaskPreparationView,
}

impl TaskPreparationControl {
    pub(crate) fn new(
        stop: ConnectorStopView,
        original: novarocks_worker::TaskPreparationView,
    ) -> Self {
        Self { stop, original }
    }
}

impl PureCompileControl for TaskPreparationControl {
    fn checkpoint(&self, _phase: CompilePhase, _units: u32) -> Result<(), CompileControlError> {
        self.original
            .borrow_control()
            .checkpoint()
            .map_err(|error| {
                CompileControlError::Source(
                    novarocks_execution::runtime::task::SharedDomainFailure::new(error),
                )
            })?;
        if self.stop.is_stopped() {
            return Err(CompileControlError::Cancelled);
        }
        Ok(())
    }
}

#[cfg(test)]
#[path = "compiled_metadata_host_tests.rs"]
mod metadata_host_tests;

#[cfg(test)]
mod capability_tests {
    use super::*;
    use crate::backend_test_support::find_source;
    use novarocks_connector_contract::{
        ConnectorError, ConnectorErrorKind, ConnectorProviderId, PureProviderCatalogError,
        PureProviderProgramError,
    };
    use novarocks_execution::runtime::mem_tracker::{MemTracker, MemTrackerLimitExceeded};
    use novarocks_execution_contract::failure::{FailureClass, RepresentationBoundary, domain};
    use novarocks_physical_plan::NodeId;
    use novarocks_spi::connector::{
        ConnectorCodecError, ConnectorCodecErrorKind, ConnectorFieldPath,
    };
    use novarocks_type_contract::owned_resources::layout::LayoutResourceError;

    fn provider(error: PureProviderProgramError<ConnectorCodecError>) -> CompiledPackageError {
        provider_preparation_failure(ProviderPreparationError::Read {
            node: NodeId::new(1),
            error,
        })
    }

    #[test]
    fn provider_nominal_failures_keep_the_domain_class_reason_and_original_source() {
        for (kind, class, reason) in [
            (
                ConnectorCodecErrorKind::Unsupported,
                FailureClass::Unsupported,
                166,
            ),
            (
                ConnectorCodecErrorKind::Capacity,
                FailureClass::Resource,
                167,
            ),
            (
                ConnectorCodecErrorKind::InvalidValue,
                FailureClass::Invariant,
                162,
            ),
            (
                ConnectorCodecErrorKind::VersionMismatch,
                FailureClass::Invariant,
                168,
            ),
        ] {
            let original = ConnectorCodecError::new(
                ConnectorFieldPath::root("scan"),
                kind,
                "Cancelled ResourceExhausted unsupported diagnostic",
            );
            let expected = original.failure_fact();
            assert_eq!(expected.class(), class);
            assert_eq!(expected.reason().domain(), domain::CONNECTOR.get());
            assert_eq!(expected.reason().code(), reason);
            let error = provider(PureProviderProgramError::Provider(original.clone()));
            assert_eq!(error.failure_fact(), expected);
            assert_eq!(find_source::<ConnectorCodecError>(&error), Some(&original));
            let preparation = find_source::<ProviderPreparationError<ConnectorCodecError>>(&error)
                .expect("original provider preparation author");
            assert!(
                matches!(preparation, ProviderPreparationError::Read { node, .. }
                if *node == NodeId::new(1))
            );
        }
        let id = ConnectorProviderId::parse("iceberg").unwrap();
        for (original, class, reason) in [
            (
                PureProviderCatalogError::ReadUnavailable(id.clone()),
                FailureClass::Unsupported,
                147,
            ),
            (
                PureProviderCatalogError::WriteUnavailable(id.clone()),
                FailureClass::Unsupported,
                148,
            ),
            (
                PureProviderCatalogError::MissingProvider(id),
                FailureClass::Dependency,
                146,
            ),
        ] {
            let expected = original.failure_fact();
            assert_eq!(expected.class(), class);
            assert_eq!(expected.reason().domain(), domain::CONNECTOR.get());
            assert_eq!(expected.reason().code(), reason);
            let error = provider(PureProviderProgramError::Catalog(original.clone()));
            assert_eq!(error.failure_fact(), expected);
            assert_eq!(
                find_source::<PureProviderCatalogError>(&error),
                Some(&original)
            );
        }
        for (kind, class, reason) in [
            (
                ConnectorErrorKind::Unsupported,
                FailureClass::Unsupported,
                4,
            ),
            (
                ConnectorErrorKind::ResourceExhausted,
                FailureClass::Resource,
                7,
            ),
            (ConnectorErrorKind::InvalidRequest, FailureClass::Data, 1),
            (ConnectorErrorKind::Internal, FailureClass::Unexpected, 10),
            (ConnectorErrorKind::Unavailable, FailureClass::Dependency, 8),
        ] {
            let original = ConnectorError::new(kind, "unsupported Cancelled resource diagnostic");
            let expected = original.failure_fact();
            assert_eq!(expected.class(), class);
            assert_eq!(expected.reason().domain(), domain::CONNECTOR.get());
            assert_eq!(expected.reason().code(), reason);
            let error = provider(PureProviderProgramError::Contract(original.clone()));
            assert_eq!(error.failure_fact(), expected);
            assert_eq!(find_source::<ConnectorError>(&error), Some(&original));
        }
    }

    #[test]
    fn unsupported_operation_kind_cannot_replace_resource_representation_or_domain_facts() {
        let tracker = MemTracker::new_root("original provider capacity");
        tracker.install_limit_once(1).unwrap();
        let resource = tracker.consume_and_check_limit_typed(16).unwrap_err();
        tracker.release(16);
        assert_eq!(tracker.current(), 0);
        let resource_fact = resource.failure_fact();
        assert_eq!(resource_fact.class(), FailureClass::Resource);
        assert_eq!(resource_fact.resource().unwrap().requested, Some(16));
        assert_eq!(resource_fact.resource().unwrap().limit, Some(1));
        assert_eq!(resource_fact.resource().unwrap().committed, Some(16));
        let representation = novarocks_functions::aggregate_scalar::checked_list_output_offset(
            0,
            i32::MAX as usize + 1,
        )
        .unwrap_err();
        let representation_fact = representation.failure_fact();
        assert_eq!(representation_fact.class(), FailureClass::Representation);
        let extent = representation_fact.representation().unwrap();
        assert_eq!(extent.boundary, RepresentationBoundary::MinimumUnit);
        assert_eq!(extent.layout.domain, domain::KERNEL);
        assert_eq!(extent.layout.code.get(), 63);
        assert_eq!(extent.required, Some(2147483648));
        assert_eq!(extent.extent_limit, Some(2147483647));
        assert_eq!(extent.representable_prefix, None);
        let invariant = LayoutResourceError::SourceModel;
        assert_eq!(invariant.failure_fact().class(), FailureClass::Invariant);
        for original in [
            ConnectorError::from_domain(ConnectorErrorKind::Unsupported, resource.clone()),
            ConnectorError::from_domain(ConnectorErrorKind::Unsupported, representation.clone()),
            ConnectorError::from_domain(ConnectorErrorKind::Unsupported, invariant),
        ] {
            let expected = original.failure_fact();
            let original_cause = original
                .source()
                .unwrap()
                .downcast_ref::<SharedFailureCause>()
                .unwrap()
                .clone();
            let original =
                original.map_message(|_| "unsupported Cancelled: ResourceExhausted".into());
            for write in [false, true] {
                let error =
                    PureProviderProgramError::<ConnectorCodecError>::Contract(original.clone());
                let preparation = if write {
                    ProviderPreparationError::Write {
                        node: NodeId::new(1),
                        error,
                    }
                } else {
                    ProviderPreparationError::Read {
                        node: NodeId::new(1),
                        error,
                    }
                };
                let projected = provider_preparation_failure(preparation);
                assert_eq!(projected.failure_fact(), expected);
                let retained = find_source::<ConnectorError>(&projected)
                    .expect("actual Connector author through Native preparation");
                assert_eq!(retained, &original);
                assert_eq!(retained.kind(), ConnectorErrorKind::Unsupported);
                assert_eq!(
                    retained
                        .source()
                        .unwrap()
                        .downcast_ref::<SharedFailureCause>(),
                    Some(&original_cause)
                );
                match expected.class() {
                    FailureClass::Resource => assert_eq!(
                        find_source::<MemTrackerLimitExceeded>(&projected)
                            .unwrap()
                            .failure_fact(),
                        resource_fact,
                    ),
                    FailureClass::Representation => assert_eq!(
                        find_source::<novarocks_functions::KernelFailure>(&projected)
                            .unwrap()
                            .failure_fact(),
                        representation_fact,
                    ),
                    FailureClass::Invariant => assert_eq!(
                        find_source::<LayoutResourceError>(&projected),
                        Some(&invariant),
                    ),
                    _ => panic!("the matrix keeps its actual original author"),
                }
            }
        }
    }

    #[test]
    fn original_control_and_source_controls_keep_their_exact_cause_and_fact() {
        let invariant = LayoutResourceError::SourceModel;
        for cause in [
            CompileControlError::Cancelled,
            CompileControlError::DeadlineExceeded,
            CompileControlError::ResourceExhausted,
            CompileControlError::from_domain(invariant),
        ] {
            let expected = cause.failure_fact();
            let direct = provider_preparation_failure::<ConnectorCodecError>(
                ProviderPreparationError::Control(cause.clone()),
            );
            let nested = provider(PureProviderProgramError::Control(cause.clone()));
            for projected in [direct, nested] {
                assert_eq!(projected.failure_fact(), expected);
                assert_eq!(find_source::<CompileControlError>(&projected), Some(&cause));
                assert!(
                    matches!(&projected, CompiledPackageError::Control(actual) if actual == &cause)
                );
                if matches!(&cause, CompileControlError::Source(_)) {
                    assert_eq!(projected.failure_fact().class(), FailureClass::Invariant);
                    assert_eq!(
                        find_source::<LayoutResourceError>(&projected),
                        Some(&invariant)
                    );
                }
            }
        }
    }

    #[test]
    fn invalid_compiler_content_is_not_a_capability_refusal() {
        use novarocks_execution_contract::failure::FailureClass;
        use novarocks_local_compiler::FragmentCompileError;
        let unsupported = CompiledPackageError::Compile(FragmentCompileError::Unsupported {
            node: Some(NodeId::new(7)),
            feature: "FULL OUTER hash join",
        });
        assert_eq!(
            unsupported.failure_fact().class(),
            FailureClass::Unsupported
        );
        assert_eq!(
            unsupported.failure_fact().reason().domain(),
            novarocks_execution_contract::failure::domain::LOCAL_COMPILER.get()
        );
        assert_eq!(unsupported.failure_fact().reason().code(), 1);
        assert!(
            matches!(unsupported.source().unwrap().downcast_ref::<FragmentCompileError>(),
            Some(FragmentCompileError::Unsupported { node: Some(node), feature: "FULL OUTER hash join" }) if *node == NodeId::new(7))
        );
        let invalid =
            CompiledPackageError::Compile(FragmentCompileError::Invalid("unsupported diagnostic"));
        assert_eq!(invalid.failure_fact().class(), FailureClass::Invariant);
        assert!(matches!(
            invalid
                .source()
                .unwrap()
                .downcast_ref::<FragmentCompileError>(),
            Some(FragmentCompileError::Invalid("unsupported diagnostic"))
        ));
    }
}
