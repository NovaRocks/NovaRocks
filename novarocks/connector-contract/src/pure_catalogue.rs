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

//! Installed pure preparation ports, independent of runtime provider bindings.

use crate::{
    ConnectorError, ConnectorProviderId, ConnectorReadProgramCompileError,
    ConnectorReadProgramCompiler, ConnectorReadProgramRecipe, ConnectorWriteLayoutRecipe,
    ConnectorWriteLayoutRecipeCompiler, ConnectorWriteLayoutRecipeDraft, ConnectorWriteRecipe,
    ConnectorWriteRecipeCompileError, ConnectorWriteRecipeCompiler, ConnectorWriteRecipeDraft,
    FrozenConnectorRead,
};
use novarocks_type_contract::{
    CompileCheckpoints, CompileControlError, CompilePhase, PureCompileControl,
};
use std::{collections::BTreeMap, error::Error, fmt, sync::Arc};

/// Authored by the host's complete provider manifest, independently of which
/// definitions were successfully installed. Facets describe pure port coverage;
/// an individual port still validates every relation kind and input shape.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PureProviderManifestEntry {
    provider: ConnectorProviderId,
    read: bool,
    write: bool,
    write_layout: bool,
}
impl PureProviderManifestEntry {
    pub const fn new(provider: ConnectorProviderId, read: bool, write: bool) -> Self {
        Self {
            provider,
            read,
            write,
            write_layout: false,
        }
    }
    pub const fn with_write_layout(mut self) -> Self {
        self.write_layout = true;
        self
    }
    pub const fn provider(&self) -> &ConnectorProviderId {
        &self.provider
    }
    pub const fn read(&self) -> bool {
        self.read
    }
    pub const fn write(&self) -> bool {
        self.write
    }
}

/// Only the complete-input pure interfaces are admitted here. A payload-only
/// recipe compiler or a runtime factory cannot fill either facet.
pub struct PureProviderProgramDefinition<E: Error + 'static> {
    provider: ConnectorProviderId,
    read: Option<Arc<dyn ConnectorReadProgramCompiler<Error = E>>>,
    write: Option<Arc<dyn ConnectorWriteRecipeCompiler<Error = E>>>,
    write_layout: Option<Arc<dyn ConnectorWriteLayoutRecipeCompiler<Error = E>>>,
}
impl<E: Error + 'static> PureProviderProgramDefinition<E> {
    pub fn new(
        provider: ConnectorProviderId,
        read: Option<Arc<dyn ConnectorReadProgramCompiler<Error = E>>>,
        write: Option<Arc<dyn ConnectorWriteRecipeCompiler<Error = E>>>,
    ) -> Self {
        Self {
            provider,
            read,
            write,
            write_layout: None,
        }
    }
    pub fn with_write_layout(
        mut self,
        layout: Arc<dyn ConnectorWriteLayoutRecipeCompiler<Error = E>>,
    ) -> Self {
        self.write_layout = Some(layout);
        self
    }
    pub const fn provider(&self) -> &ConnectorProviderId {
        &self.provider
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PureProviderCatalogError {
    DuplicateManifest(ConnectorProviderId),
    EmptyManifestEntry(ConnectorProviderId),
    DuplicateDefinition(ConnectorProviderId),
    MissingDefinition(ConnectorProviderId),
    UnexpectedDefinition(ConnectorProviderId),
    CapabilityMismatch(ConnectorProviderId),
    MissingProvider(ConnectorProviderId),
    ReadUnavailable(ConnectorProviderId),
    WriteUnavailable(ConnectorProviderId),
    WriteLayoutUnavailable(ConnectorProviderId),
    Control(CompileControlError),
}
impl fmt::Display for PureProviderCatalogError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let (reason, provider) = match self {
            Self::DuplicateManifest(p) => ("duplicate manifest entry", p),
            Self::EmptyManifestEntry(p) => ("manifest entry has no pure facet", p),
            Self::DuplicateDefinition(p) => ("duplicate installed definition", p),
            Self::MissingDefinition(p) => ("manifest definition is not installed", p),
            Self::UnexpectedDefinition(p) => {
                ("installed definition is absent from the manifest", p)
            }
            Self::CapabilityMismatch(p) => ("installed pure facets differ from the manifest", p),
            Self::MissingProvider(p) => ("provider is absent from the installed pure catalogue", p),
            Self::ReadUnavailable(p) => ("provider has no pure read port", p),
            Self::WriteUnavailable(p) => ("provider has no pure write port", p),
            Self::WriteLayoutUnavailable(p) => ("provider has no pure write layout port", p),
            Self::Control(e) => return fmt::Display::fmt(e, f),
        };
        write!(f, "pure provider '{}': {reason}", provider.as_str())
    }
}
impl Error for PureProviderCatalogError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Control(e) => Some(e),
            _ => None,
        }
    }
}
impl From<CompileControlError> for PureProviderCatalogError {
    fn from(error: CompileControlError) -> Self {
        Self::Control(error)
    }
}

#[derive(Debug)]
pub enum PureProviderProgramError<E: Error + 'static> {
    Catalog(PureProviderCatalogError),
    Contract(ConnectorError),
    Provider(E),
    Control(CompileControlError),
}
impl<E: Error + 'static> fmt::Display for PureProviderProgramError<E> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Catalog(e) => fmt::Display::fmt(e, f),
            Self::Contract(e) => fmt::Display::fmt(e, f),
            Self::Provider(e) => fmt::Display::fmt(e, f),
            Self::Control(e) => fmt::Display::fmt(e, f),
        }
    }
}
impl<E: Error + 'static> Error for PureProviderProgramError<E> {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Catalog(e) => Some(e),
            Self::Contract(e) => Some(e),
            Self::Provider(e) => Some(e),
            Self::Control(e) => Some(e),
        }
    }
}
impl<E: Error + 'static> From<CompileControlError> for PureProviderProgramError<E> {
    fn from(error: CompileControlError) -> Self {
        Self::Control(error)
    }
}
impl<E: Error + 'static> From<PureProviderCatalogError> for PureProviderProgramError<E> {
    fn from(error: PureProviderCatalogError) -> Self {
        match error {
            PureProviderCatalogError::Control(e) => Self::Control(e),
            e => Self::Catalog(e),
        }
    }
}
impl<E: Error + 'static> From<ConnectorReadProgramCompileError<E>> for PureProviderProgramError<E> {
    fn from(error: ConnectorReadProgramCompileError<E>) -> Self {
        match error {
            ConnectorReadProgramCompileError::Contract(e) => Self::Contract(e),
            ConnectorReadProgramCompileError::Provider(e) => Self::Provider(e),
            ConnectorReadProgramCompileError::Control(e) => Self::Control(e),
        }
    }
}
impl<E: Error + 'static> From<ConnectorWriteRecipeCompileError<E>> for PureProviderProgramError<E> {
    fn from(error: ConnectorWriteRecipeCompileError<E>) -> Self {
        match error {
            ConnectorWriteRecipeCompileError::Contract(e) => Self::Contract(e),
            ConnectorWriteRecipeCompileError::Provider(e) => Self::Provider(e),
            ConnectorWriteRecipeCompileError::Control(e) => Self::Control(e),
        }
    }
}

/// Immutable installed definitions shared by task preparation. The catalogue
/// retains neither caller control nor read/write execution capabilities.
pub struct PureProviderProgramCatalog<E: Error + 'static> {
    definitions: BTreeMap<ConnectorProviderId, PureProviderProgramDefinition<E>>,
}
impl<E: Error + 'static> PureProviderProgramCatalog<E> {
    pub fn try_new(
        manifest: &[PureProviderManifestEntry],
        definitions: Vec<PureProviderProgramDefinition<E>>,
        control: &dyn PureCompileControl,
    ) -> Result<Self, PureProviderCatalogError> {
        let mut work = CompileCheckpoints::try_new(control, CompilePhase::ProviderValidation)?;
        let result = (|| {
            let mut expected = BTreeMap::new();
            for entry in manifest {
                let old = expected.insert(
                    entry.provider.clone(),
                    (entry.read, entry.write, entry.write_layout),
                );
                work.step()?;
                if old.is_some() {
                    return Err(PureProviderCatalogError::DuplicateManifest(
                        entry.provider.clone(),
                    ));
                }
                if !entry.read && !entry.write && !entry.write_layout {
                    return Err(PureProviderCatalogError::EmptyManifestEntry(
                        entry.provider.clone(),
                    ));
                }
            }
            let mut installed = BTreeMap::new();
            for definition in definitions {
                let id = definition.provider.clone();
                let expected_facets = expected.get(&id).copied();
                work.step()?;
                let Some((read, write, write_layout)) = expected_facets else {
                    return Err(PureProviderCatalogError::UnexpectedDefinition(id));
                };
                if definition.read.is_some() != read
                    || definition.write.is_some() != write
                    || definition.write_layout.is_some() != write_layout
                {
                    return Err(PureProviderCatalogError::CapabilityMismatch(id));
                }
                let duplicate = installed.insert(id.clone(), definition).is_some();
                work.step()?;
                if duplicate {
                    return Err(PureProviderCatalogError::DuplicateDefinition(id));
                }
            }
            for id in expected.keys() {
                let installed_here = installed.contains_key(id);
                work.step()?;
                if !installed_here {
                    return Err(PureProviderCatalogError::MissingDefinition(id.clone()));
                }
            }
            Ok(Self {
                definitions: installed,
            })
        })();
        if matches!(&result, Err(PureProviderCatalogError::Control(_))) {
            return result;
        }
        work.finish()?;
        result
    }

    pub fn provider_count(&self) -> usize {
        self.definitions.len()
    }

    pub fn compile_read(
        &self,
        frozen: &FrozenConnectorRead,
        control: &dyn PureCompileControl,
    ) -> Result<ConnectorReadProgramRecipe, PureProviderProgramError<E>> {
        let mut work = CompileCheckpoints::try_new(control, CompilePhase::ProviderValidation)?;
        let result = (|| {
            let id = &frozen.scan().recipe().binding().descriptor().provider_id;
            let definition = self.definitions.get(id);
            work.step()?;
            let definition =
                definition.ok_or_else(|| PureProviderCatalogError::MissingProvider(id.clone()))?;
            let port = definition
                .read
                .as_ref()
                .ok_or_else(|| PureProviderCatalogError::ReadUnavailable(id.clone()))?;
            work.flush()?;
            let recipe = ConnectorReadProgramRecipe::try_compile_with_provider(
                frozen,
                port.as_ref(),
                work.control(),
            )?;
            work.step()?;
            Ok(recipe)
        })();
        work.finish_result(result, PureProviderProgramError::Control)
    }

    pub fn compile_write_layout(
        &self,
        draft: &ConnectorWriteLayoutRecipeDraft,
        control: &dyn PureCompileControl,
    ) -> Result<ConnectorWriteLayoutRecipe, PureProviderProgramError<E>> {
        let id = &draft.binding().descriptor().provider_id;
        let definition = self
            .definitions
            .get(id)
            .ok_or_else(|| PureProviderCatalogError::MissingProvider(id.clone()))?;
        let port = definition
            .write_layout
            .as_ref()
            .ok_or_else(|| PureProviderCatalogError::WriteLayoutUnavailable(id.clone()))?;
        ConnectorWriteLayoutRecipe::compile(draft, port.as_ref(), control)
    }

    pub fn compile_write(
        &self,
        draft: &ConnectorWriteRecipeDraft,
        control: &dyn PureCompileControl,
    ) -> Result<ConnectorWriteRecipe, PureProviderProgramError<E>> {
        let mut work = CompileCheckpoints::try_new(control, CompilePhase::ProviderValidation)?;
        let result = (|| {
            let id = &draft.binding().descriptor().provider_id;
            let definition = self.definitions.get(id);
            work.step()?;
            let definition =
                definition.ok_or_else(|| PureProviderCatalogError::MissingProvider(id.clone()))?;
            let port = definition
                .write
                .as_ref()
                .ok_or_else(|| PureProviderCatalogError::WriteUnavailable(id.clone()))?;
            work.flush()?;
            let recipe = ConnectorWriteRecipe::try_compile_with_provider(
                draft,
                port.as_ref(),
                work.control(),
            )?;
            work.step()?;
            Ok(recipe)
        })();
        work.finish_result(result, PureProviderProgramError::Control)
    }
}

#[cfg(test)]
mod tests;
