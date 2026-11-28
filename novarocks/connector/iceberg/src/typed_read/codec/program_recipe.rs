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

//! Complete frozen read validation without a runtime adapter or metadata I/O.

use novarocks_connector_contract::{
    ConnectorReadArtifactCoverage, ConnectorReadDistribution, ConnectorReadProgramCompiler,
    ConnectorReadRelationRecipeDraft, FrozenConnectorRead, PureProviderCompileError,
};
use novarocks_spi::connector::{
    ConnectorCodecCategory, ConnectorCodecError, ConnectorCodecErrorKind, ConnectorCodecRevision,
    ConnectorDecodeContext, ConnectorDecodeLedger, ConnectorDecodeLimits, ConnectorEncodedPayload,
    ConnectorFieldPath,
};
use novarocks_type_contract::{CompileCheckpoints, CompilePhase, PureCompileControl};

use super::{
    ICEBERG_READ_CODEC_REVISION, IcebergReadRecipeCompiler, MAX_PRIVATE_READ_BYTES,
    MAX_PRIVATE_RETAINED_BYTES,
};
use crate::provider_types::IcebergReadTypes;
use crate::typed_read::{
    IcebergRuntimeRelation,
    read_static_facts::{ReadStaticFactsError, iceberg_final_static_facts_for_compile},
};

type Failure = PureProviderCompileError<ConnectorCodecError>;

impl ConnectorReadProgramCompiler for IcebergReadRecipeCompiler {
    type Error = ConnectorCodecError;

    fn compile_private(
        &self,
        frozen: &FrozenConnectorRead,
        control: &dyn PureCompileControl,
    ) -> Result<ConnectorReadRelationRecipeDraft, Failure> {
        let mut work = CompileCheckpoints::try_new(control, CompilePhase::ProviderValidation)?;
        let result = compile(frozen, control, &mut work);
        work.finish_result(result, Failure::Control)
    }
}

fn compile(
    frozen: &FrozenConnectorRead,
    control: &dyn PureCompileControl,
    work: &mut CompileCheckpoints<'_>,
) -> Result<ConnectorReadRelationRecipeDraft, Failure> {
    let scan = frozen.scan();
    let draft = scan.recipe();
    if draft.binding().descriptor().provider_id.as_str() != crate::PROVIDER_ID {
        return Err(invalid(
            "binding",
            "Iceberg read has another provider identity",
        ));
    }
    work.step()?;
    let codecs = IcebergReadTypes::wire_codecs();
    let (relation, execution) = decode(
        frozen,
        draft.relation().table(),
        ConnectorCodecCategory::ReadTable,
        control,
        work,
        |bytes, context| codecs.decode_table(bytes, context),
    )?;
    let _view = decode(
        frozen,
        draft.relation().view(),
        ConnectorCodecCategory::ReadView,
        control,
        work,
        |bytes, context| codecs.decode_read_view(bytes, context),
    )?;
    let matching_kind = relation.kind() == draft.relation().kind();
    work.step()?;
    if !matching_kind {
        return Err(invalid(
            "relation",
            "Iceberg public relation kind differs from its private table",
        ));
    }

    work.flush()?;
    let expected =
        iceberg_final_static_facts_for_compile(&relation, control).map_err(
            |error| match error {
                ReadStaticFactsError::Control(error) => Failure::Control(error),
                ReadStaticFactsError::Source(error) => {
                    Failure::Provider(ConnectorCodecError::from_domain(
                        ConnectorFieldPath::root("iceberg_read").field("source"),
                        error,
                    ))
                }
            },
        )?;
    work.step()?;
    let public = frozen.public_facts();
    let source = public.source();
    if !bytes_equal(
        source.input_version().as_bytes(),
        expected.input_version().as_bytes(),
        work,
    )? || !bytes_equal(
        &source.selection_digest(),
        &expected.selection_digest(),
        work,
    )? || !bytes_equal(
        source.coverage_evidence(),
        expected.coverage_evidence(),
        work,
    )? || !coverage_equal(
        source.artifact_coverage(),
        expected.artifact_coverage(),
        work,
    )? {
        return Err(invalid(
            "source",
            "Iceberg frozen source identity, selection or coverage differs from its private relation",
        ));
    }
    let matching_distribution = matches!(
        (
            source.properties().distribution(),
            expected.properties().distribution()
        ),
        (
            ConnectorReadDistribution::Unconstrained,
            ConnectorReadDistribution::Unconstrained
        ) | (
            ConnectorReadDistribution::Singleton,
            ConnectorReadDistribution::Singleton
        )
    );
    let matching_ordering =
        source.properties().ordering().is_empty() && expected.properties().ordering().is_empty();
    work.step()?;
    if !matching_distribution || !matching_ordering {
        return Err(invalid(
            "source",
            "Iceberg frozen read properties differ from its provider guarantee",
        ));
    }
    match &relation {
        IcebergRuntimeRelation::SystemTable(reference) => {
            let matching_metadata = match public.metadata_kind() {
                Some(kind) => bytes_equal(
                    kind.as_str().as_bytes(),
                    reference.system_table_type().suffix().as_bytes(),
                    work,
                )?,
                None => false,
            };
            work.step()?;
            if !matching_metadata {
                return Err(invalid(
                    "metadata",
                    "Iceberg metadata kind differs from its frozen reference",
                ));
            }
        }
        _ => {
            work.step()?;
            if public.metadata_kind().is_some() {
                return Err(invalid(
                    "relation",
                    "Iceberg ordinary relation has metadata facts",
                ));
            }
        }
    }

    let mut columns = Vec::with_capacity(draft.columns().len());
    for (index, payload) in draft.columns().iter().enumerate() {
        let column = decode(
            frozen,
            payload,
            ConnectorCodecCategory::ReadColumn,
            control,
            work,
            |bytes, context| codecs.decode_column(bytes, context),
        )
        .map_err(|error| match error {
            Failure::Provider(error) => Failure::Provider(
                error.with_path(
                    ConnectorFieldPath::root("iceberg_read")
                        .field("columns")
                        .index(index),
                ),
            ),
            Failure::Control(error) => Failure::Control(error),
        })?;
        columns.push(column);
        work.step()?;
    }
    if !super::super::runtime::IcebergReadLayout::applies_to(&relation)
        && (execution.layout != super::super::runtime::IcebergReadLayout::Offsets
            || execution.preserve_top_level_dictionary)
    {
        return Err(invalid(
            "execution",
            "Scan representation selection requires a data relation",
        ));
    }
    super::program_fields::validate_program_columns(
        &relation,
        &columns,
        public,
        execution.layout,
        work,
    )?;
    // Assignments, negotiated predicates, remaining expressions and RF hints
    // retain the original public authority. In particular a private pushed
    // predicate can have been downgraded to pruning-only; it is not an engine
    // residual or an exact public guarantee merely because it was decoded.
    work.flush()?;
    let compiled = draft.clone();
    work.step()?;
    Ok(compiled)
}

fn decode<T>(
    frozen: &FrozenConnectorRead,
    payload: &ConnectorEncodedPayload,
    category: ConnectorCodecCategory,
    control: &dyn PureCompileControl,
    work: &mut CompileCheckpoints<'_>,
    decode_private: impl FnOnce(
        &[u8],
        &mut ConnectorDecodeContext<'_>,
    ) -> Result<T, ConnectorCodecError>,
) -> Result<T, Failure> {
    let binding = frozen.scan().recipe().binding();
    payload
        .header()
        .validate_expected::<ConnectorCodecError>(
            &binding.descriptor().provider_id,
            binding.catalog_handle(),
            category,
            ConnectorCodecRevision::try_new(ICEBERG_READ_CODEC_REVISION)
                .expect("Iceberg read codec revision is non-zero"),
        )
        .map_err(lift)?;
    work.step()?;
    work.flush()?;
    let limits = ConnectorDecodeLimits::try_new(
        MAX_PRIVATE_READ_BYTES,
        MAX_PRIVATE_RETAINED_BYTES,
        MAX_PRIVATE_READ_BYTES,
        1_000_000,
        novarocks_spi::connector::MAX_CONNECTOR_CODEC_FIELD_PATH_DEPTH,
    )
    .expect("Iceberg recipe decode limits are finite");
    let mut ledger = ConnectorDecodeLedger::new(limits);
    let mut context =
        ConnectorDecodeContext::try_new_for_compile(payload.header(), &mut ledger, control)
            .map_err(lift)?;
    let value = decode_private(payload.payload(), &mut context).map_err(lift)?;
    context.flush_compile_control().map_err(lift)?;
    work.step()?;
    Ok(value)
}

fn coverage_equal(
    actual: &ConnectorReadArtifactCoverage,
    expected: &ConnectorReadArtifactCoverage,
    work: &mut CompileCheckpoints<'_>,
) -> Result<bool, Failure> {
    let result = match (actual, expected) {
        (
            ConnectorReadArtifactCoverage::NoArtifactInputs,
            ConnectorReadArtifactCoverage::NoArtifactInputs,
        ) => Ok(true),
        (
            ConnectorReadArtifactCoverage::Exact {
                source_selection_digest: a,
                content_digest: b,
                evidence: c,
            },
            ConnectorReadArtifactCoverage::Exact {
                source_selection_digest: x,
                content_digest: y,
                evidence: z,
            },
        ) => Ok(bytes_equal(a, x, work)? && bytes_equal(b, y, work)? && bytes_equal(c, z, work)?),
        _ => Ok(false),
    };
    if matches!(&result, Err(Failure::Control(_))) {
        return result;
    }
    work.step()?;
    result
}

fn bytes_equal(a: &[u8], b: &[u8], work: &mut CompileCheckpoints<'_>) -> Result<bool, Failure> {
    let same_length = a.len() == b.len();
    work.step()?;
    if !same_length {
        return Ok(false);
    }
    for (a, b) in a.chunks(1024).zip(b.chunks(1024)) {
        let same = a == b;
        work.step()?;
        if !same {
            return Ok(false);
        }
    }
    Ok(true)
}

fn lift(error: ConnectorCodecError) -> Failure {
    match error.compile_control_error() {
        Some(cause) => Failure::Control(cause.clone()),
        None => Failure::Provider(error),
    }
}
fn invalid(path: &'static str, message: impl AsRef<str>) -> Failure {
    Failure::Provider(ConnectorCodecError::new(
        ConnectorFieldPath::root("iceberg_read").field(path),
        ConnectorCodecErrorKind::InconsistentFields,
        message,
    ))
}

#[cfg(test)]
mod tests;
