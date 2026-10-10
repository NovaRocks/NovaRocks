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

use std::error::Error;
use std::fmt;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::Duration;

use novarocks_connector_contract::PureProviderProgramCatalog;
use novarocks_functions::{ConstantPolicy, PureEngineFunctionCatalog};
use novarocks_local_compiler::{
    FragmentCompileError, LocalCompileOptions, ProviderPreparationError, compile_fragment,
    validate_fragment_providers,
};
use novarocks_local_program::{KernelAbiVersion, LocalProgram};
use novarocks_plan_codec::physical_package_v2::{
    PackageDecodeError, PackageDecodeLimits, decode_fragment_package_with_type_host,
};
use novarocks_plan_codec::resource_preflight_v2::FragmentDecodeResourceModel;
use novarocks_spi::connector::ConnectorStopView;
use novarocks_type_contract::{CompileControlError, CompilePhase, PureCompileControl};

use super::type_materialization::{TypeMaterializationHost, TypeMaterializationRefusal};
use crate::compiled_runtime_filter::CompiledRuntimeFilterEndpoints;
use novarocks_plan_codec::host_projection_v2::ProjectionFailure;

/// The task facts a compiled program is specialized for.
#[derive(Clone, Copy, Debug)]
pub struct CompiledTaskOptions {
    pub pipeline_dop: NonZeroUsize,
    /// Host-admitted receive wait for every compiled exchange source.
    pub exchange_wait: Duration,
}

/// Why a task's package did not become a LocalProgram.
#[derive(Debug)]
pub enum CompiledPackageError {
    /// The task's own control stopped the work; the cause stays primary.
    Control(CompileControlError),
    /// The package is not receivable, its providers refuse it, or the
    /// compiler refuses one of its shapes.
    Refused(String),
    TypeMaterialization(TypeMaterializationRefusal),
}

impl fmt::Display for CompiledPackageError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Control(cause) => cause.fmt(f),
            Self::Refused(detail) => f.write_str(detail),
            Self::TypeMaterialization(cause) => cause.fmt(f),
        }
    }
}

impl Error for CompiledPackageError {}

/// One task's compiled program and the runtime-filter bindings its package
/// numbers.
///
/// The program names each runtime-filter site by its plan-global binding
/// identity only; which filter and endpoint a binding is stays in the
/// package's cuts, so it is projected here, before the package is consumed.
#[derive(Debug)]
pub struct CompiledTaskProgram {
    pub program: LocalProgram,
    pub runtime_filters: CompiledRuntimeFilterEndpoints,
}

/// Turns one task's package bytes into its LocalProgram.
pub trait CompiledPackageCompiler: Send + Sync + 'static {
    fn compile(
        &self,
        package: &[u8],
        options: CompiledTaskOptions,
        control: &dyn PureCompileControl,
        type_host: &mut TypeMaterializationHost<'_>,
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

impl<E: Error + Send + Sync + 'static> CompiledPackageCompiler for CompiledPackageInterpreter<E>
where
    PureProviderProgramCatalog<E>: Send + Sync,
{
    fn compile(
        &self,
        package: &[u8],
        options: CompiledTaskOptions,
        control: &dyn PureCompileControl,
        type_host: &mut TypeMaterializationHost<'_>,
    ) -> Result<CompiledTaskProgram, CompiledPackageError> {
        let package = decode_fragment_package_with_type_host(
            package,
            &self.model,
            &self.decode_limits,
            control,
            type_host,
        )
        .map_err(|error| match error {
            ProjectionFailure::Host(cause) => CompiledPackageError::TypeMaterialization(cause),
            ProjectionFailure::Codec(PackageDecodeError::Control(cause)) => {
                CompiledPackageError::Control(cause)
            }
            ProjectionFailure::Codec(other) => {
                CompiledPackageError::Refused(format!("package is not receivable: {other}"))
            }
        })?;
        let runtime_filters =
            CompiledRuntimeFilterEndpoints::from_package(&package).map_err(|error| {
                CompiledPackageError::Refused(format!(
                    "package runtime-filter bindings are not resolvable: {error}"
                ))
            })?;
        // The compiled RootResult algorithm gathers its terminal pipeline to
        // one driver; this width is independent of the task pipeline DOP.
        let root_sink_dop = matches!(
            package.fragment().sink(),
            novarocks_physical_plan::FragmentSink::RootResult(_)
        )
        .then_some(NonZeroUsize::MIN);
        let validated = validate_fragment_providers(Arc::new(package), &self.providers, control)
            .map_err(|error| match error {
                ProviderPreparationError::Control(cause) => CompiledPackageError::Control(cause),
                other => {
                    CompiledPackageError::Refused(format!("package providers refuse it: {other}"))
                }
            })?;
        let program = compile_fragment(
            validated,
            &self.functions,
            LocalCompileOptions {
                pipeline_dop: options.pipeline_dop,
                root_sink_dop,
                kernel_abi: KernelAbiVersion::CURRENT,
                constants: self.constants,
                exchange_wait: options.exchange_wait,
            },
            control,
        )
        .map_err(|error| match error {
            FragmentCompileError::Control(cause) => CompiledPackageError::Control(cause),
            other => CompiledPackageError::Refused(format!("package does not compile: {other}")),
        })?;
        Ok(CompiledTaskProgram {
            program,
            runtime_filters,
        })
    }
}

/// Compile control for one task's preparation: a stop of the task refuses the
/// next checkpoint, so decode, provider validation and compile all end with
/// the task's own cancellation as their primary cause.
pub(crate) struct TaskPreparationControl<'a> {
    stop: ConnectorStopView,
    preparation: &'a novarocks_worker::PreparationControlLoan<'a>,
}

impl<'a> TaskPreparationControl<'a> {
    pub(crate) fn new(
        stop: ConnectorStopView,
        preparation: &'a novarocks_worker::PreparationControlLoan<'a>,
    ) -> Self {
        Self { stop, preparation }
    }
}

impl PureCompileControl for TaskPreparationControl<'_> {
    fn checkpoint(&self, _phase: CompilePhase, _units: u32) -> Result<(), CompileControlError> {
        if self.stop.is_stopped() || self.preparation.checkpoint().is_err() {
            return Err(CompileControlError::Cancelled);
        }
        Ok(())
    }
}
