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

//! Exact provider-output and writer-input links in one checked local graph.
//!
//! This checks borrowed public recipes, not provider private facts again. The
//! compiler still owns physical-source/token correspondence and provenance.

use std::{collections::BTreeMap, fmt};

use novarocks_connector_contract::{ConnectorReadPublicFacts, ConnectorWriteRecipe};
#[cfg(test)]
use novarocks_type_contract::arrow_data_types_exact;
use novarocks_type_contract::{
    CompileCheckpoints, CompileControlError, CompilePhase, FunctionValueType, PureCompileControl,
    ValueTypeError, arrow_fields_exact,
};

use crate::{
    ProgramChannelLayoutRole, ProgramChannelSite, ProgramNodeId, ProgramNodeKind,
    ProgramTypedChannels, StaticLayout,
};

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ProviderLinkError {
    Control(CompileControlError),
    ValueType(ValueTypeError),
    LegacyScan(ProgramNodeId),
    MissingWriterRecipe(ProgramNodeId),
    UnexpectedWriterRecipe(ProgramNodeId),
    ColumnCount(ProgramNodeId),
    SchemaMetadata(ProgramNodeId),
    FieldMismatch { node: ProgramNodeId, ordinal: u32 },
    MissingChannel(ProgramChannelSite),
    TypeMismatch(ProgramChannelSite),
    OrdinalOverflow(ProgramNodeId),
}
impl From<CompileControlError> for ProviderLinkError {
    fn from(error: CompileControlError) -> Self {
        Self::Control(error)
    }
}
impl From<novarocks_type_contract::ExecutionValueTypeError> for ProviderLinkError {
    fn from(error: novarocks_type_contract::ExecutionValueTypeError) -> Self {
        Self::ValueType(ValueTypeError::from(error))
    }
}

impl From<ValueTypeError> for ProviderLinkError {
    fn from(error: ValueTypeError) -> Self {
        Self::ValueType(error)
    }
}
impl fmt::Display for ProviderLinkError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "invalid local provider link: {self:?}")
    }
}
impl std::error::Error for ProviderLinkError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Control(error) => Some(error),
            Self::ValueType(error) => Some(error),
            _ => None,
        }
    }
}

/// Require compiled scan sources and precisely one recipe per actual writer.
/// Arrow/schema helpers below remain opaque bounded-by-owner operations:
/// checkpoints surround them but do not certify their internal work quantum
/// or authorize allocations. No fields, recipes or capabilities are copied.
pub(crate) fn validate_provider_links(
    graph: &crate::LocalProgramGraph,
    channels: &crate::ProgramTypedChannels,
    writes: &BTreeMap<ProgramNodeId, ConnectorWriteRecipe>,
    control: &dyn PureCompileControl,
) -> Result<(), ProviderLinkError> {
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::LowerProgram)?;
    let result = validate_core(graph, channels, writes, &mut work);
    // Preserve a first control refusal; ordinary errors and success must still
    // observe their completed tail before returning to the final program owner.
    if matches!(result, Err(ProviderLinkError::Control(_))) {
        return result;
    }
    work.finish()?;
    result
}

fn validate_core(
    graph: &crate::LocalProgramGraph,
    channels: &crate::ProgramTypedChannels,
    writes: &BTreeMap<ProgramNodeId, ConnectorWriteRecipe>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), ProviderLinkError> {
    for (index, node) in graph.nodes().iter().enumerate() {
        let id = ProgramNodeId::new(index);
        let kind = node.kind();
        work.step()?;
        match kind {
            ProgramNodeKind::Scan { source, .. } => {
                let compiled = source.compiled().ok_or(ProviderLinkError::LegacyScan(id))?;
                validate_read(
                    id,
                    compiled.frozen().public_facts(),
                    node.output_layout(),
                    channels,
                    work,
                )?;
            }
            ProgramNodeKind::TableWriter { projection, .. } => {
                let recipe = writes.get(&id);
                work.step()?;
                let recipe = recipe.ok_or(ProviderLinkError::MissingWriterRecipe(id))?;
                let input = recipe.draft().input();
                let count_matches =
                    input.field_count() == projection.layout.schema().fields().len();
                work.step()?;
                if !count_matches {
                    return Err(ProviderLinkError::ColumnCount(id));
                }
                for (ordinal, (binding, actual)) in input
                    .fields_iter()
                    .zip(projection.layout.schema().fields())
                    .enumerate()
                {
                    let ordinal = ordinal_id(id, ordinal)?;
                    let expected = binding.field();
                    work.step()?;
                    if !projection.layout.is_typed() {
                        return Err(ProviderLinkError::TypeMismatch(
                            ProgramChannelSite::Layout {
                                node: id,
                                role: ProgramChannelLayoutRole::WriterProjection,
                                ordinal,
                            },
                        ));
                    }
                    {
                        let site = ProgramChannelSite::Layout {
                            node: id,
                            role: ProgramChannelLayoutRole::WriterProjection,
                            ordinal,
                        };
                        let ty = channel(channels, site, work)?;
                        let signed = binding
                            .execution_type()
                            .ok_or(ProviderLinkError::TypeMismatch(site))?;
                        // The complete provider-signed value contract owns
                        // semantics. Format attributes remain in the recipe.
                        // Only root NULL differs as a writer row obligation.
                        if actual.is_nullable() != ty.nullable()
                            || !signed.same_physical_binding_ignoring_root_nullability_observed(
                                ty,
                                || {
                                    work.step()?;
                                    Ok::<_, ProviderLinkError>(())
                                },
                            )?
                        {
                            return Err(ProviderLinkError::TypeMismatch(site));
                        }
                        work.step()?;
                        if expected.name().len() != actual.name().len() {
                            return Err(ProviderLinkError::FieldMismatch { node: id, ordinal });
                        }
                        for (expected, actual) in expected
                            .name()
                            .as_bytes()
                            .chunks(1024)
                            .zip(actual.name().as_bytes().chunks(1024))
                        {
                            for _ in expected {
                                work.step()?;
                            }
                            if expected != actual {
                                return Err(ProviderLinkError::FieldMismatch { node: id, ordinal });
                            }
                        }
                        if !signed
                            .typed_binding()
                            .layout()
                            .matches_projection_observed(actual.data_type(), |units| {
                                for _ in 0..units {
                                    work.step()?;
                                }
                                Ok::<_, ProviderLinkError>(())
                            })?
                        {
                            return Err(ProviderLinkError::FieldMismatch { node: id, ordinal });
                        }
                        continue;
                    }
                }
            }
            _ => {}
        }
    }
    // A recipe for a finish, nonwriter or unknown node cannot be hidden by
    // successful validation of the actual writers.
    for id in writes.keys() {
        let matches = graph
            .nodes()
            .get(id.index())
            .is_some_and(|node| matches!(node.kind(), ProgramNodeKind::TableWriter { .. }));
        work.step()?;
        if !matches {
            return Err(ProviderLinkError::UnexpectedWriterRecipe(*id));
        }
    }
    Ok(())
}

fn validate_read(
    node: ProgramNodeId,
    facts: &ConnectorReadPublicFacts,
    layout: &StaticLayout,
    channels: &ProgramTypedChannels,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), ProviderLinkError> {
    let expected = facts.schema();
    let actual = layout.schema();
    let count_matches = expected.fields().len() == actual.fields().len();
    work.step()?;
    if !count_matches {
        return Err(ProviderLinkError::ColumnCount(node));
    }
    // Schema-level metadata is separate from root/nested Field metadata.
    work.flush()?;
    let metadata_matches = expected.metadata() == actual.metadata();
    work.flush()?;
    if !metadata_matches {
        return Err(ProviderLinkError::SchemaMetadata(node));
    }
    for (ordinal, (expected, actual)) in expected.fields().iter().zip(actual.fields()).enumerate() {
        let ordinal = ordinal_id(node, ordinal)?;
        work.step()?;
        require_field(node, ordinal, expected, actual, work)?;
        let site = ProgramChannelSite::Layout {
            node,
            role: ProgramChannelLayoutRole::NodeOutput,
            ordinal,
        };
        let ty = channel(channels, site, work)?;
        // The read owner stores explicit root identities even when the Field
        // has no duplicate root tag. Never substitute Field/carrier inference.
        work.flush()?;
        let matches = facts.matches_value_type(ordinal as usize, ty);
        work.flush()?;
        if !matches {
            return Err(ProviderLinkError::TypeMismatch(site));
        }
    }
    Ok(())
}

fn channel<'a>(
    channels: &'a ProgramTypedChannels,
    site: ProgramChannelSite,
    work: &mut CompileCheckpoints<'_>,
) -> Result<&'a FunctionValueType, ProviderLinkError> {
    let ty = channels.channel_type(site);
    work.step()?;
    ty.ok_or(ProviderLinkError::MissingChannel(site))
}

fn require_field(
    node: ProgramNodeId,
    ordinal: u32,
    expected: &arrow_schema::Field,
    actual: &arrow_schema::Field,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), ProviderLinkError> {
    work.flush()?;
    let matches = arrow_fields_exact(expected, actual);
    work.flush()?;
    if matches {
        Ok(())
    } else {
        Err(ProviderLinkError::FieldMismatch { node, ordinal })
    }
}

#[cfg(test)]
fn require_carrier(
    expected: &arrow_schema::DataType,
    actual: &FunctionValueType,
    site: ProgramChannelSite,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), ProviderLinkError> {
    work.flush()?;
    let matches = arrow_data_types_exact(expected, actual.data_type());
    work.flush()?;
    if matches {
        Ok(())
    } else {
        Err(ProviderLinkError::TypeMismatch(site))
    }
}

fn ordinal_id(node: ProgramNodeId, ordinal: usize) -> Result<u32, ProviderLinkError> {
    u32::try_from(ordinal).map_err(|_| ProviderLinkError::OrdinalOverflow(node))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_schema::{DataType, Field};
    use std::{collections::HashMap, sync::Mutex};

    struct OriginalControl {
        calls: Mutex<Vec<(CompilePhase, u32)>>,
        refuse_at: Option<usize>,
        cause: CompileControlError,
    }
    impl OriginalControl {
        fn admitted() -> Self {
            Self {
                calls: Mutex::new(Vec::new()),
                refuse_at: None,
                cause: CompileControlError::Cancelled,
            }
        }
    }
    impl PureCompileControl for OriginalControl {
        fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
            let mut calls = self.calls.lock().unwrap();
            calls.push((phase, units));
            if self.refuse_at == Some(calls.len()) {
                Err(self.cause.clone())
            } else {
                Ok(())
            }
        }
    }

    #[test]
    #[allow(deprecated)]
    fn provider_link_fields_preserve_dictionary_identity_and_provider_metadata() {
        let owner = OriginalControl::admitted();
        let mut work = CompileCheckpoints::try_new(&owner, CompilePhase::LowerProgram).unwrap();
        let node = ProgramNodeId::new(7);
        let dictionary = DataType::Dictionary(Box::new(DataType::Int8), Box::new(DataType::Utf8));
        let expected = Field::new_dict("provider_name", dictionary.clone(), true, 23, true)
            .with_metadata(HashMap::from([
                ("provider.field-id".into(), "17".into()),
                ("provider.extension".into(), "unchanged".into()),
            ]));
        require_field(node, 0, &expected, &expected.clone(), &mut work).unwrap();
        let wrong_id = Field::new_dict("provider_name", dictionary, true, 24, true)
            .with_metadata(expected.metadata().clone());
        for wrong in [
            wrong_id,
            expected.clone().with_name("assignment_alias"),
            expected.clone().with_nullable(false),
            expected.clone().with_metadata(HashMap::new()),
        ] {
            assert_eq!(
                require_field(node, 0, &expected, &wrong, &mut work),
                Err(ProviderLinkError::FieldMismatch { node, ordinal: 0 })
            );
        }
        work.finish().unwrap();
    }

    #[test]
    #[allow(deprecated)]
    fn provider_link_carriers_retain_nested_dictionary_field_attributes() {
        let owner = OriginalControl::admitted();
        let mut work = CompileCheckpoints::try_new(&owner, CompilePhase::LowerProgram).unwrap();
        let field = |id| {
            Field::new_dict(
                "value",
                DataType::Dictionary(Box::new(DataType::Int8), Box::new(DataType::Utf8)),
                false,
                id,
                true,
            )
        };
        let expected = DataType::Struct(vec![field(11)].into());
        let actual = FunctionValueType::new(DataType::Struct(vec![field(12)].into()), false);
        let site = ProgramChannelSite::Layout {
            node: ProgramNodeId::new(0),
            role: ProgramChannelLayoutRole::WriterProjection,
            ordinal: 0,
        };
        assert_eq!(
            require_carrier(&expected, &actual, site, &mut work),
            Err(ProviderLinkError::TypeMismatch(site))
        );
        work.finish().unwrap();
    }

    #[test]
    fn provider_link_opaque_comparison_exit_preserves_all_typed_control_causes() {
        for cause in [
            CompileControlError::Cancelled,
            CompileControlError::DeadlineExceeded,
            CompileControlError::ResourceExhausted,
        ] {
            // Admission and operation entry succeed; actual comparison's exit
            // refuses even though the fields would produce an ordinary error.
            let owner = OriginalControl {
                refuse_at: Some(3),
                cause: cause.clone(),
                ..OriginalControl::admitted()
            };
            let mut work = CompileCheckpoints::try_new(&owner, CompilePhase::LowerProgram).unwrap();
            let failure = require_field(
                ProgramNodeId::new(0),
                0,
                &Field::new("source", DataType::Int64, false),
                &Field::new("foreign", DataType::Int64, false),
                &mut work,
            )
            .unwrap_err();
            assert_eq!(failure, ProviderLinkError::Control(cause.clone()));
            assert!(std::error::Error::source(&failure).is_some());
            assert_eq!(owner.calls.lock().unwrap().len(), 3);
            assert_eq!(work.flush(), Err(cause));
            assert_eq!(owner.calls.lock().unwrap().len(), 3);
        }
    }
}
