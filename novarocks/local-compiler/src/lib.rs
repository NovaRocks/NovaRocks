// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

//! Pure fragment preparation. Provider validation is a distinct intermediate
//! phase; its result is not a complete semantic validation or a LocalProgram.

mod aggregate;
mod assert_rows;
mod change_events;

#[cfg(test)]
mod change_events_lowering_tests;
mod channels;
mod exchange;
mod expressions;
mod generate_series;
mod join;
#[cfg(test)]
mod join_lowering_tests;
mod lowering;
mod original_requests;
mod project_metadata;
mod repeat;
mod runtime_filter;
mod scan;
mod table_function;
mod union;
mod union_flow;
mod unpivot;
mod window;
mod writer;
mod writer_statistics;

#[cfg(test)]
mod window_lowering_tests;

#[cfg(test)]
mod repeat_lowering_tests;

#[cfg(test)]
mod writer_lowering_tests;

#[cfg(test)]
mod unpivot_lowering_tests;

#[cfg(test)]
mod assert_rows_lowering_tests;

pub use lowering::{
    FragmentCompileError, LocalCompileOptions, compile_fragment,
    compile_fragment_with_project_metadata_host,
};
pub use project_metadata::{
    ProjectMetadataFailure, ProjectMetadataOutput, ProjectMetadataScope, ProjectOutputRequestFacts,
};

use std::{collections::BTreeMap, error::Error, fmt, sync::Arc};

use novarocks_connector_contract::{
    ConnectorReadProgramRecipe, ConnectorWriteRecipe, PureProviderProgramCatalog,
    PureProviderProgramError,
};
use novarocks_physical_plan::{FragmentPackage, NodeId};
use novarocks_type_contract::{
    CompileCheckpoints, CompileControlError, CompilePhase, PureCompileControl,
};

/// Exactly one structural package and its validated provider recipes. There is
/// no task, split inventory, driver, runtime binding or retained caller control.
/// Function/effect validation and lowering must still consume this same package.
pub struct ProviderValidatedFragment {
    package: Arc<FragmentPackage>,
    reads: BTreeMap<NodeId, ConnectorReadProgramRecipe>,
    writes: BTreeMap<NodeId, ConnectorWriteRecipe>,
}
impl ProviderValidatedFragment {
    pub fn package(&self) -> &Arc<FragmentPackage> {
        &self.package
    }
    pub fn reads(&self) -> &BTreeMap<NodeId, ConnectorReadProgramRecipe> {
        &self.reads
    }
    pub fn writes(&self) -> &BTreeMap<NodeId, ConnectorWriteRecipe> {
        &self.writes
    }
    /// Lowering moves each validated recipe into its one scan or writer owner
    /// instead of copying it beside the retained package.
    #[allow(clippy::type_complexity)]
    pub(crate) fn into_parts(
        self,
    ) -> (
        Arc<FragmentPackage>,
        BTreeMap<NodeId, ConnectorReadProgramRecipe>,
        BTreeMap<NodeId, ConnectorWriteRecipe>,
    ) {
        (self.package, self.reads, self.writes)
    }
}

/// Keeps the exact sparse physical node and the original error owner. A control
/// refusal is lifted without adding a second callback or replacing its cause.
#[derive(Debug)]
pub enum ProviderPreparationError<E: Error + 'static> {
    Read {
        node: NodeId,
        error: PureProviderProgramError<E>,
    },
    Write {
        node: NodeId,
        error: PureProviderProgramError<E>,
    },
    Control(CompileControlError),
}
impl<E: Error + 'static> fmt::Display for ProviderPreparationError<E> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Read { node, error } => {
                write!(f, "provider read at physical node {}: {error}", node.get())
            }
            Self::Write { node, error } => {
                write!(f, "provider write at physical node {}: {error}", node.get())
            }
            Self::Control(error) => fmt::Display::fmt(error, f),
        }
    }
}
impl<E: Error + 'static> Error for ProviderPreparationError<E> {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Read { error, .. } | Self::Write { error, .. } => Some(error),
            Self::Control(error) => Some(error),
        }
    }
}
impl<E: Error + 'static> From<CompileControlError> for ProviderPreparationError<E> {
    fn from(error: CompileControlError) -> Self {
        Self::Control(error)
    }
}

/// Compiles each exact scan/writer occurrence once through an installed pure
/// port. The structurally checked package establishes exact node coverage;
/// provider seals establish full public/private input consistency. Neither
/// proof authorizes function semantics or obtains execution capabilities.
pub fn validate_fragment_providers<E: Error + 'static>(
    package: Arc<FragmentPackage>,
    catalog: &PureProviderProgramCatalog<E>,
    control: &dyn PureCompileControl,
) -> Result<ProviderValidatedFragment, ProviderPreparationError<E>> {
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::ProviderValidation)?;
    let result =
        (|| {
            let mut reads = BTreeMap::new();
            let mut writes = BTreeMap::new();
            for (&node, frozen) in package.scans() {
                work.flush()?;
                let recipe = catalog
                    .compile_read(frozen, work.control())
                    .map_err(|error| match error {
                        PureProviderProgramError::Control(error) => {
                            ProviderPreparationError::Control(error)
                        }
                        error => ProviderPreparationError::Read { node, error },
                    })?;
                reads.insert(node, recipe);
                work.step()?;
            }
            for (&node, draft) in package.writes() {
                work.flush()?;
                let recipe = catalog
                    .compile_write(draft, work.control())
                    .map_err(|error| match error {
                        PureProviderProgramError::Control(error) => {
                            ProviderPreparationError::Control(error)
                        }
                        error => ProviderPreparationError::Write { node, error },
                    })?;
                writes.insert(node, recipe);
                work.step()?;
            }
            Ok(ProviderValidatedFragment {
                package,
                reads,
                writes,
            })
        })();
    if matches!(&result, Err(ProviderPreparationError::Control(_))) {
        return result;
    }
    work.finish()?;
    result
}

#[cfg(test)]
mod tests;

#[cfg(test)]
mod lowering_tests;

#[cfg(test)]
mod channel_tests;

#[cfg(test)]
mod control_call_tests;

#[cfg(test)]
mod nary_lowering_tests;

#[cfg(test)]
mod unary_lowering_tests;

#[cfg(test)]
mod case_lowering_tests;

#[cfg(test)]
mod equality_lowering_tests;

#[cfg(test)]
mod arithmetic_lowering_tests;

mod sort;
mod stream_sink;
mod topn;
mod values;

#[cfg(test)]
mod exchange_lowering_tests;

#[cfg(test)]
mod scan_lowering_tests;

#[cfg(test)]
mod runtime_filter_lowering_tests;

#[cfg(test)]
mod union_flow_tests;

#[cfg(test)]
mod union_lowering_tests;

#[cfg(test)]
mod table_function_lowering_tests;
