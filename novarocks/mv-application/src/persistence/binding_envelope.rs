// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file distributed
// with this work for additional information regarding copyright ownership.
// The ASF licenses this file to you under the Apache License, Version 2.0.

use novarocks_spi::connector::{
    ConnectorDocumentVersion, DocumentIdentity, KeyedDocumentReadError, KeyedDocumentView,
};

use super::codec::keyed::{BINDING_FORMAT_VERSION, KEYED_INTERPRETATION_FORMAT_VERSION};
use super::codec::{DEFINITION_FORMAT_VERSION, STORAGE_MAPPING_FORMAT_VERSION};
use super::documents::MvDocumentError;
use super::keyed_documents::MV_DOCUMENT_OWNER;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum BindingGeneration {
    Current,
    Retired,
}

pub(crate) struct BindingEnvelope<'a> {
    pub generation: BindingGeneration,
    pub binding: &'a ConnectorDocumentVersion,
    pub definition: &'a ConnectorDocumentVersion,
    pub interpretation: &'a ConnectorDocumentVersion,
    pub mapping: Option<&'a ConnectorDocumentVersion>,
}

impl BindingEnvelope<'_> {
    pub fn require_current(&self) -> Result<(), MvDocumentError> {
        if self.generation == BindingGeneration::Retired {
            return Err(KeyedDocumentReadError::LegacyFormat {
                key: format!("rev.{}", self.binding.identity().to_hex()),
            }
            .into());
        }
        Ok(())
    }
}

/// Inspect identity and references before reading any document payload. DROP
/// uses only these facts; refresh and reads must also decode a current graph.
pub(crate) fn inspect_binding(
    view: &KeyedDocumentView,
    root: DocumentIdentity,
) -> Result<BindingEnvelope<'_>, MvDocumentError> {
    let binding = exact_envelope(view, root, "binding")?;
    let (generation, definition_format, interpretation_format, reference_count) =
        match binding.format().version() {
            1 => (BindingGeneration::Retired, 1, 2, 2),
            BINDING_FORMAT_VERSION => (
                BindingGeneration::Current,
                DEFINITION_FORMAT_VERSION,
                KEYED_INTERPRETATION_FORMAT_VERSION,
                3,
            ),
            version => {
                return Err(
                    crate::persistence::codec::PersistenceCodecError::UnknownFormatVersion {
                        document: "binding",
                        version,
                    }
                    .into(),
                );
            }
        };
    if binding.references().len() != reference_count {
        return Err(invalid("unexpected binding references"));
    }

    let definition_id = required_reference(binding, "definition")?;
    let definition = exact_envelope(view, definition_id, "definition")?;
    let interpretation = exact_envelope(
        view,
        required_reference(binding, "interpretation")?,
        "interpretation",
    )?;
    if definition.format().version() != definition_format
        || interpretation.format().version() != interpretation_format
        || !definition.references().is_empty()
        || interpretation.references().len() != 1
        || required_reference(interpretation, "definition")? != definition_id
    {
        return Err(invalid("incompatible D/L definition reference closure"));
    }

    let mapping = match generation {
        BindingGeneration::Retired => None,
        BindingGeneration::Current => {
            let value = exact_envelope(view, required_reference(binding, "mapping")?, "mapping")?;
            if value.format().version() != STORAGE_MAPPING_FORMAT_VERSION
                || !value.references().is_empty()
            {
                return Err(invalid("invalid storage mapping envelope"));
            }
            Some(value)
        }
    };
    Ok(BindingEnvelope {
        generation,
        binding,
        definition,
        interpretation,
        mapping,
    })
}

fn exact_envelope<'a>(
    view: &'a KeyedDocumentView,
    id: DocumentIdentity,
    name: &str,
) -> Result<&'a ConnectorDocumentVersion, MvDocumentError> {
    let version = view
        .versions
        .get(&id)
        .ok_or_else(|| invalid(format!("missing MV document version {name}")))?;
    if version.owner() != MV_DOCUMENT_OWNER
        || version.name() != name
        || version.format().owner() != MV_DOCUMENT_OWNER
        || version.format().name() != name
        || version.identity() != id
    {
        return Err(invalid(format!("invalid MV document envelope {name}")));
    }
    Ok(version)
}

fn required_reference(
    version: &ConnectorDocumentVersion,
    name: &str,
) -> Result<DocumentIdentity, MvDocumentError> {
    version
        .references()
        .get(name)
        .copied()
        .ok_or_else(|| invalid(format!("{} lacks {name} reference", version.name())))
}

fn invalid(message: impl Into<String>) -> MvDocumentError {
    MvDocumentError::Contract(message.into())
}
