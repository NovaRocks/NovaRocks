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

//! Provider-owned `ConnectorWriteControl::activate_row_mutation`.
//!
//! Materializes the provider-owned route graph after a durable operation has
//! retained the exact write lease. This is deliberately not a call to
//! `prepare_write`: route preparation is derived from the sealed row-mutation
//! contract and every physical choice stays inside the Iceberg provider.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::Instant;

use arrow::array::{Array, Int8Array, Int64Array, StringArray};
use arrow::datatypes::{Schema, SchemaRef};
use bytes::Bytes;
use sha2::{Digest, Sha256};

use novarocks_spi::connector::{
    ConnectorError, ConnectorErrorKind, ConnectorExecutionBindingKey, ConnectorMutationRouteInput,
    ConnectorMutationTargetField, ConnectorRowMutationActivationRequest,
    ConnectorRowMutationCohortRecipe, ConnectorRowMutationEffect,
    ConnectorRowMutationExecutionPlan, ConnectorRowMutationPreparation, ConnectorRowMutationRoute,
    ConnectorRowMutationScanBinding, ConnectorRowMutationSelection,
    ConnectorRowMutationSelectionOrdinal, ConnectorRowMutationStrategy,
    ConnectorSealedWriteCohortSet, ConnectorTableHandle, ConnectorWriteCohortDescriptor,
    ConnectorWriteCohortId, ConnectorWriteFieldBinding, ConnectorWriteFieldToken,
    ConnectorWriteInputShape, ConnectorWriteIntent, ConnectorWritePreparation,
    ConnectorWriteRouteId,
};

use crate::commit::write_shared::write_target_schema;
use crate::control_provider::IcebergTablePayload;
use crate::control_runtime::IcebergControlRuntime;
use crate::file_reader::execution_payload::decode_payload;
use crate::iceberg::spec::TableMetadata;
use crate::manifest::{DataFileWithStats, data_file_with_stats_to_iceberg_data_file_info};
use crate::row_lineage_synth::{ICEBERG_LAST_UPDATED_SEQ_COL, ICEBERG_ROW_ID_COL};
use crate::row_mutation_payload::encode_cow_recipe;

/// Materialize the Provider-owned route graph after a durable operation has
/// retained the exact write lease.  This is deliberately not a call to
/// `prepare_write`: route preparation is derived from the sealed row-mutation
/// contract and every physical choice remains inside the Iceberg provider.
pub(crate) fn activate_row_mutation(
    request: ConnectorRowMutationActivationRequest,
    owner: &ConnectorExecutionBindingKey,
    runtime: &IcebergControlRuntime,
) -> Result<ConnectorRowMutationExecutionPlan, ConnectorError> {
    request.validate(owner)?;
    if request.context().cancellation().is_cancelled() {
        return Err(ConnectorError::new(
            ConnectorErrorKind::Cancelled,
            "Iceberg row-mutation activation was cancelled before Provider planning",
        ));
    }
    if Instant::now() >= request.context().deadline() {
        return Err(ConnectorError::new(
            ConnectorErrorKind::DeadlineExceeded,
            "Iceberg row-mutation activation deadline elapsed before Provider planning",
        ));
    }
    let preparation = request.preparation().clone();
    match &request {
        ConnectorRowMutationActivationRequest::Direct { .. } => {
            activate_iceberg_direct_row_mutation(&preparation)
        }
        ConnectorRowMutationActivationRequest::CopyOnWrite { selection, .. } => {
            activate_iceberg_cow_row_mutation(&preparation, selection, request.context(), runtime)
        }
    }
}

fn activate_iceberg_direct_row_mutation(
    preparation: &ConnectorRowMutationPreparation,
) -> Result<ConnectorRowMutationExecutionPlan, ConnectorError> {
    let primary = ConnectorWriteCohortId::primary(preparation.operation_id());
    let mut routes = Vec::new();
    match preparation.strategy() {
        ConnectorRowMutationStrategy::PositionDelete
        | ConnectorRowMutationStrategy::DeletionVector => {
            let effects = admitted_effects(preparation, &[ConnectorRowMutationEffect::Delete]);
            if effects.is_empty() {
                return Err(invalid_iceberg_row_mutation_activation(
                    "Iceberg position-delete strategy cannot implement the admitted logical effects",
                ));
            }
            let input = iceberg_position_input(preparation)?;
            routes.push(iceberg_row_mutation_route(
                preparation,
                primary,
                b"direct-position-delete",
                effects,
                input,
                iceberg_position_partition_tokens(preparation)?,
            )?);
        }
        ConnectorRowMutationStrategy::EqualityDelete => {
            let effects = admitted_effects(preparation, &[ConnectorRowMutationEffect::Delete]);
            if effects.is_empty() {
                return Err(invalid_iceberg_row_mutation_activation(
                    "Iceberg equality-delete strategy cannot implement the admitted logical effects",
                ));
            }
            let input = ConnectorWriteInputShape::EqualityDelete {
                equality_fields: target_bindings(preparation.match_contract().before_fields()),
            };
            routes.push(iceberg_row_mutation_route(
                preparation,
                primary,
                b"direct-equality-delete",
                effects,
                input,
                Vec::new(),
            )?);
        }
        ConnectorRowMutationStrategy::MergeOnRead => {
            // A Replace reaches both routes.  The delete route consumes its
            // before-image identity while the data route consumes its
            // after-image values; neither route learns the other's physical
            // Iceberg policy.
            let delete_effects = admitted_effects(
                preparation,
                &[
                    ConnectorRowMutationEffect::Delete,
                    ConnectorRowMutationEffect::Replace,
                ],
            );
            if !delete_effects.is_empty() {
                routes.push(iceberg_row_mutation_route(
                    preparation,
                    iceberg_row_mutation_direct_cohort(preparation, b"mor-delete")?,
                    b"mor-delete",
                    delete_effects,
                    iceberg_mor_delete_input(preparation)?,
                    iceberg_position_partition_tokens(preparation)?,
                )?);
            }
            let replacement_effects =
                admitted_effects(preparation, &[ConnectorRowMutationEffect::Replace]);
            if !replacement_effects.is_empty() {
                routes.push(iceberg_row_mutation_route(
                    preparation,
                    iceberg_row_mutation_direct_cohort(preparation, b"mor-replacement")?,
                    b"mor-replacement",
                    replacement_effects,
                    iceberg_cow_rewrite_input(preparation)?,
                    Vec::new(),
                )?);
            }
            let insert_effects =
                admitted_effects(preparation, &[ConnectorRowMutationEffect::Insert]);
            if !insert_effects.is_empty() {
                routes.push(iceberg_row_mutation_route(
                    preparation,
                    iceberg_row_mutation_direct_cohort(preparation, b"mor-insert")?,
                    b"mor-insert",
                    insert_effects,
                    ConnectorWriteInputShape::Data {
                        fields: target_bindings(preparation.match_contract().after_fields()),
                    },
                    Vec::new(),
                )?);
            }
        }
        ConnectorRowMutationStrategy::CopyOnWrite => {
            return Err(invalid_iceberg_row_mutation_activation(
                "Iceberg Copy-on-Write activation requires the bounded match selection",
            ));
        }
    }
    ConnectorRowMutationExecutionPlan::try_direct(preparation.clone(), routes)
}

fn activate_iceberg_cow_row_mutation(
    preparation: &ConnectorRowMutationPreparation,
    selection: &ConnectorRowMutationSelection,
    context: &novarocks_spi::connector::ConnectorRequestContext,
    runtime: &IcebergControlRuntime,
) -> Result<ConnectorRowMutationExecutionPlan, ConnectorError> {
    if preparation.strategy() != ConnectorRowMutationStrategy::CopyOnWrite {
        return Err(invalid_iceberg_row_mutation_activation(
            "only an Iceberg Copy-on-Write preparation accepts a bounded selection",
        ));
    }
    selection.validate()?;
    let (rewrite_rows, append_ordinals) = iceberg_cow_selection_groups(preparation, selection)?;
    let touched_files = rewrite_rows.keys().cloned().collect::<BTreeSet<_>>();
    let frozen = freeze_iceberg_cow_base(preparation, &touched_files, runtime)?;
    activate_iceberg_cow_row_mutation_from_frozen(
        preparation,
        selection,
        context,
        rewrite_rows,
        append_ordinals,
        frozen,
    )
}

fn activate_iceberg_cow_row_mutation_from_frozen(
    preparation: &ConnectorRowMutationPreparation,
    selection: &ConnectorRowMutationSelection,
    context: &novarocks_spi::connector::ConnectorRequestContext,
    rewrite_rows: BTreeMap<String, Vec<IcebergCowMatchedRow>>,
    append_ordinals: Vec<ConnectorRowMutationSelectionOrdinal>,
    frozen: IcebergFrozenCowBase,
) -> Result<ConnectorRowMutationExecutionPlan, ConnectorError> {
    let mut routes = Vec::new();
    let mut descriptors = Vec::new();
    let mut recipes = Vec::new();
    // A COW rewrite re-emits every live row from an old data file.  Preserve
    // the row lineage alongside the replacement data so the writer retains
    // the stable row-id and sequence-number semantics of the rewritten file.
    let rewrite_preparation = ConnectorWritePreparation::try_new(
        preparation.owner().clone(),
        preparation.table().clone(),
        preparation.target_ref().clone(),
        ConnectorWriteIntent::RowDelta,
        preparation.base_version().clone(),
        iceberg_cow_rewrite_input(preparation)?,
        iceberg_row_mutation_route_payload(preparation, b"cow-rewrite"),
    )?;
    for (old_file, matched_rows) in rewrite_rows {
        let data_file = frozen.files.get(&old_file).ok_or_else(|| {
            invalid_iceberg_row_mutation_activation(format!(
                "Iceberg COW matched data file `{old_file}` is absent from the admitted base"
            ))
        })?;
        validate_matched_rows_against_frozen_file(&old_file, &matched_rows, data_file)?;
        let (source, scan_schema, source_digest) = freeze_iceberg_cow_source(
            preparation,
            &frozen,
            data_file.clone(),
            context.max_handle_payload_bytes(),
        )?;
        let selection_ordinals = matched_rows
            .iter()
            .map(|row| row.ordinal)
            .collect::<Vec<_>>();
        let row_ids = matched_rows
            .iter()
            .map(|row| row.row_id)
            .collect::<Vec<_>>();
        let old_file_digest: [u8; 32] = Sha256::digest(old_file.as_bytes()).into();
        let cohort_id = ConnectorWriteCohortId::derive(
            preparation.operation_id(),
            b"iceberg-cow-rewrite",
            old_file_digest,
        )?;
        let route = iceberg_row_mutation_route_with_preparation(
            preparation,
            cohort_id,
            b"cow-rewrite",
            admitted_effects(
                preparation,
                &[
                    ConnectorRowMutationEffect::Delete,
                    ConnectorRowMutationEffect::Replace,
                ],
            ),
            rewrite_preparation.clone(),
            Vec::new(),
        )?;
        let route_id = route.route_id();
        descriptors.push(ConnectorWriteCohortDescriptor::new(
            cohort_id,
            ConnectorWriteIntent::RowDelta,
            rewrite_preparation.digest(),
        ));
        let (scan_bindings, written_version_token) = iceberg_cow_scan_bindings(
            preparation,
            rewrite_preparation.input(),
            scan_schema.as_ref(),
        )?;
        recipes.push(ConnectorRowMutationCohortRecipe::try_rewrite(
            cohort_id,
            route_id,
            selection,
            selection_ordinals,
            source,
            preparation.base_version().digest(),
            scan_schema,
            scan_bindings,
            preparation.match_contract().uniqueness_tokens().to_vec(),
            written_version_token,
            iceberg_cow_recipe_payload(
                b"rewrite",
                &old_file,
                &row_ids,
                Some(frozen.snapshot_id),
                Some(source_digest),
            )?,
        )?);
        routes.push(route);
    }
    if !append_ordinals.is_empty() {
        let append_digest: [u8; 32] = Sha256::digest(b"iceberg-cow-append").into();
        let cohort_id = ConnectorWriteCohortId::derive(
            preparation.operation_id(),
            b"iceberg-cow-append",
            append_digest,
        )?;
        let append_preparation = ConnectorWritePreparation::try_new(
            preparation.owner().clone(),
            preparation.table().clone(),
            preparation.target_ref().clone(),
            ConnectorWriteIntent::Append,
            preparation.base_version().clone(),
            ConnectorWriteInputShape::Data {
                fields: target_bindings(preparation.match_contract().after_fields()),
            },
            iceberg_row_mutation_route_payload(preparation, b"cow-append"),
        )?;
        let route = iceberg_row_mutation_route_with_preparation(
            preparation,
            cohort_id,
            b"cow-append",
            admitted_effects(preparation, &[ConnectorRowMutationEffect::Insert]),
            append_preparation.clone(),
            Vec::new(),
        )?;
        let route_id = route.route_id();
        descriptors.push(ConnectorWriteCohortDescriptor::new(
            cohort_id,
            ConnectorWriteIntent::Append,
            append_preparation.digest(),
        ));
        recipes.push(ConnectorRowMutationCohortRecipe::try_append(
            cohort_id,
            route_id,
            selection,
            append_ordinals,
            iceberg_cow_recipe_payload(b"append", "", &[], None, None)?,
        )?);
        routes.push(route);
    }
    let sealed = ConnectorSealedWriteCohortSet::try_new(preparation.operation_id(), descriptors)?;
    ConnectorRowMutationExecutionPlan::try_copy_on_write(
        preparation.clone(),
        selection.clone(),
        routes,
        sealed,
        recipes,
        context,
    )
}

fn admitted_effects(
    preparation: &ConnectorRowMutationPreparation,
    candidates: &[ConnectorRowMutationEffect],
) -> Vec<ConnectorRowMutationEffect> {
    candidates
        .iter()
        .copied()
        .filter(|effect| preparation.intent().accepts(*effect))
        .collect()
}

fn iceberg_row_mutation_route(
    preparation: &ConnectorRowMutationPreparation,
    cohort_id: ConnectorWriteCohortId,
    route_kind: &[u8],
    effects: Vec<ConnectorRowMutationEffect>,
    input: ConnectorWriteInputShape,
    partition_fields: Vec<ConnectorWriteFieldToken>,
) -> Result<ConnectorRowMutationRoute, ConnectorError> {
    let route_preparation = ConnectorWritePreparation::try_new(
        preparation.owner().clone(),
        preparation.table().clone(),
        preparation.target_ref().clone(),
        ConnectorWriteIntent::RowDelta,
        preparation.base_version().clone(),
        input,
        iceberg_row_mutation_route_payload(preparation, route_kind),
    )?;
    iceberg_row_mutation_route_with_preparation(
        preparation,
        cohort_id,
        route_kind,
        effects,
        route_preparation,
        partition_fields,
    )
}

fn iceberg_row_mutation_route_with_preparation(
    preparation: &ConnectorRowMutationPreparation,
    cohort_id: ConnectorWriteCohortId,
    route_kind: &[u8],
    effects: Vec<ConnectorRowMutationEffect>,
    route_preparation: ConnectorWritePreparation,
    partition_fields: Vec<ConnectorWriteFieldToken>,
) -> Result<ConnectorRowMutationRoute, ConnectorError> {
    if effects.is_empty() {
        return Err(invalid_iceberg_row_mutation_activation(
            "Iceberg row-mutation route has no admitted logical effects",
        ));
    }
    let route_id = iceberg_row_mutation_route_id(preparation, cohort_id, route_kind);
    let input_ordinals = route_preparation
        .input()
        .fields()
        .into_iter()
        .map(|binding| {
            row_mutation_input_ordinal(preparation, binding.token())
                .map(|ordinal| ConnectorMutationRouteInput::new(binding.token(), ordinal))
        })
        .collect::<Result<Vec<_>, _>>()?;
    ConnectorRowMutationRoute::try_new(
        route_id,
        cohort_id,
        effects,
        route_preparation.input().clone(),
        input_ordinals,
        partition_fields,
        route_preparation,
    )
}

fn iceberg_row_mutation_route_id(
    preparation: &ConnectorRowMutationPreparation,
    cohort_id: ConnectorWriteCohortId,
    route_kind: &[u8],
) -> ConnectorWriteRouteId {
    let mut hasher = Sha256::new();
    hasher.update(b"novarocks.iceberg.row-mutation-route.v1\0");
    hasher.update(preparation.operation_id().to_bytes());
    hasher.update(preparation.digest());
    hasher.update(cohort_id.to_bytes());
    hasher.update((route_kind.len() as u64).to_be_bytes());
    hasher.update(route_kind);
    ConnectorWriteRouteId::from_bytes(hasher.finalize().into())
}

fn iceberg_row_mutation_direct_cohort(
    preparation: &ConnectorRowMutationPreparation,
    route_kind: &[u8],
) -> Result<ConnectorWriteCohortId, ConnectorError> {
    let mut hasher = Sha256::new();
    hasher.update(b"novarocks.iceberg.row-mutation-direct-cohort.v1\0");
    hasher.update(preparation.digest());
    hasher.update((route_kind.len() as u64).to_be_bytes());
    hasher.update(route_kind);
    ConnectorWriteCohortId::derive(
        preparation.operation_id(),
        b"iceberg-row-mutation-direct",
        hasher.finalize().into(),
    )
}

fn iceberg_row_mutation_route_payload(
    preparation: &ConnectorRowMutationPreparation,
    route_kind: &[u8],
) -> Bytes {
    let mut hasher = Sha256::new();
    hasher.update(b"novarocks.iceberg.row-mutation-route-payload.v1\0");
    hasher.update(preparation.operation_id().to_bytes());
    hasher.update(preparation.digest());
    hasher.update((route_kind.len() as u64).to_be_bytes());
    hasher.update(route_kind);
    Bytes::from(format!(
        "iceberg/row-mutation-route/v1/{}",
        lowercase_hex(hasher.finalize())
    ))
}

/// Provider-private lowercase hex. Byte-for-byte identical to `hex::encode`,
/// which the legacy Core implementation used; this crate deliberately does not
/// take a dependency for a bounded 32-byte digest.
fn lowercase_hex(value: impl AsRef<[u8]>) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let value = value.as_ref();
    let mut output = String::with_capacity(value.len() * 2);
    for byte in value {
        output.push(DIGITS[usize::from(byte >> 4)] as char);
        output.push(DIGITS[usize::from(byte & 0x0f)] as char);
    }
    output
}

fn iceberg_cow_recipe_payload(
    role: &[u8],
    old_file: &str,
    row_ids: &[i64],
    base_snapshot_id: Option<i64>,
    frozen_source_digest: Option<[u8; 32]>,
) -> Result<Bytes, ConnectorError> {
    encode_cow_recipe(
        role,
        old_file,
        row_ids,
        base_snapshot_id,
        frozen_source_digest,
    )
    .map_err(invalid_iceberg_row_mutation_activation)
}

fn target_bindings(fields: &[ConnectorMutationTargetField]) -> Vec<ConnectorWriteFieldBinding> {
    fields
        .iter()
        .map(|field| ConnectorWriteFieldBinding::new(field.token(), field.field().clone()))
        .collect()
}

fn iceberg_cow_rewrite_input(
    preparation: &ConnectorRowMutationPreparation,
) -> Result<ConnectorWriteInputShape, ConnectorError> {
    let contract = preparation.match_contract();
    let lineage = [ICEBERG_ROW_ID_COL, ICEBERG_LAST_UPDATED_SEQ_COL]
        .into_iter()
        .map(|name| {
            contract
                .identity_fields()
                .iter()
                .find(|field| field.field().name().eq_ignore_ascii_case(name))
                .map(|field| {
                    ConnectorWriteFieldBinding::new(
                        field.token(),
                        field.field().clone().with_nullable(false),
                    )
                })
                .ok_or_else(|| {
                    invalid_iceberg_row_mutation_activation(format!(
                        "Iceberg COW identity lacks `{name}`"
                    ))
                })
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(ConnectorWriteInputShape::RowLineage {
        data_fields: target_bindings(contract.after_fields()),
        row_identity_fields: lineage,
    })
}

fn iceberg_position_input(
    preparation: &ConnectorRowMutationPreparation,
) -> Result<ConnectorWriteInputShape, ConnectorError> {
    let contract = preparation.match_contract();
    let file = contract
        .identity_fields()
        .iter()
        .find(|field| field.field().name().eq_ignore_ascii_case("_file"))
        .ok_or_else(|| {
            invalid_iceberg_row_mutation_activation("Iceberg row identity lacks `_file`")
        })?;
    let pos = contract
        .identity_fields()
        .iter()
        .find(|field| field.field().name().eq_ignore_ascii_case("_pos"))
        .ok_or_else(|| {
            invalid_iceberg_row_mutation_activation("Iceberg row identity lacks `_pos`")
        })?;
    let partition_source_fields = iceberg_position_partition_bindings(preparation)?;
    Ok(match preparation.strategy() {
        ConnectorRowMutationStrategy::DeletionVector => ConnectorWriteInputShape::DeletionVector {
            identity_fields: vec![
                ConnectorWriteFieldBinding::new(
                    file.token(),
                    file.field().clone().with_nullable(false),
                ),
                ConnectorWriteFieldBinding::new(
                    pos.token(),
                    pos.field().clone().with_nullable(false),
                ),
            ],
            partition_source_fields,
        },
        _ => ConnectorWriteInputShape::PositionDelete {
            identity_fields: vec![
                ConnectorWriteFieldBinding::new(
                    file.token(),
                    file.field().clone().with_nullable(false),
                ),
                ConnectorWriteFieldBinding::new(
                    pos.token(),
                    pos.field().clone().with_nullable(false),
                ),
            ],
            partition_source_fields,
        },
    })
}

fn iceberg_mor_delete_input(
    preparation: &ConnectorRowMutationPreparation,
) -> Result<ConnectorWriteInputShape, ConnectorError> {
    // MOR is admitted only for Iceberg v3 row-lineage tables.  Its delete
    // half therefore uses the v3 deletion-vector writer, while its data half
    // remains an ordinary row-lineage append route.
    match iceberg_position_input(preparation)? {
        ConnectorWriteInputShape::PositionDelete {
            identity_fields,
            partition_source_fields,
        } => Ok(ConnectorWriteInputShape::DeletionVector {
            identity_fields,
            partition_source_fields,
        }),
        input @ ConnectorWriteInputShape::DeletionVector { .. } => Ok(input),
        _ => Err(invalid_iceberg_row_mutation_activation(
            "Iceberg MOR delete route did not derive a position identity input",
        )),
    }
}

fn iceberg_position_partition_bindings(
    preparation: &ConnectorRowMutationPreparation,
) -> Result<Vec<ConnectorWriteFieldBinding>, ConnectorError> {
    let payload: IcebergTablePayload = decode_payload(
        preparation.table().payload(),
        "admitted Iceberg row-mutation table",
    )?;
    let table = payload.table_info.ok_or_else(|| {
        invalid_iceberg_row_mutation_activation(
            "admitted Iceberg row-mutation table is missing frozen metadata",
        )
    })?;
    let metadata: TableMetadata =
        serde_json::from_str(table.serialized_metadata.as_deref().ok_or_else(|| {
            invalid_iceberg_row_mutation_activation(
                "admitted Iceberg row-mutation table has no serialized metadata",
            )
        })?)
        .map_err(|error| {
            ConnectorError::new(
                ConnectorErrorKind::CorruptData,
                format!("decode admitted Iceberg row-mutation metadata: {error}"),
            )
        })?;
    let target_schema = write_target_schema(&metadata, preparation.base_version_ordinal())?;
    metadata
        .default_partition_spec()
        .fields()
        .iter()
        .map(|partition| {
            let source = target_schema
                .field_by_id(partition.source_id)
                .ok_or_else(|| {
                    invalid_iceberg_row_mutation_activation(
                        "Iceberg partition source is absent from the frozen schema",
                    )
                })?;
            let field = preparation
                .match_contract()
                .before_fields()
                .iter()
                .find(|field| field.field().name().eq_ignore_ascii_case(&source.name))
                .ok_or_else(|| {
                    invalid_iceberg_row_mutation_activation(
                        "Iceberg match contract is missing a partition source before-field",
                    )
                })?;
            Ok(ConnectorWriteFieldBinding::new(
                field.token(),
                field.field().clone(),
            ))
        })
        .collect()
}

fn iceberg_position_partition_tokens(
    preparation: &ConnectorRowMutationPreparation,
) -> Result<Vec<ConnectorWriteFieldToken>, ConnectorError> {
    iceberg_position_partition_bindings(preparation).map(|bindings| {
        bindings
            .into_iter()
            .map(|binding| binding.token())
            .collect()
    })
}

fn row_mutation_input_ordinal(
    preparation: &ConnectorRowMutationPreparation,
    token: ConnectorWriteFieldToken,
) -> Result<u32, ConnectorError> {
    let contract = preparation.match_contract();
    if let Some(field) = contract
        .identity_fields()
        .iter()
        .find(|field| field.token() == token)
    {
        return Ok(field.source_ordinal());
    }
    if let Some(field) = contract
        .before_fields()
        .iter()
        .chain(contract.after_fields())
        .find(|field| field.token() == token)
    {
        return Ok(field.target_ordinal());
    }
    Err(invalid_iceberg_row_mutation_activation(
        "Iceberg route input token is foreign to its signed match contract",
    ))
}

#[derive(Clone, Copy)]
struct IcebergCowMatchedRow {
    ordinal: ConnectorRowMutationSelectionOrdinal,
    row_id: i64,
    position: i64,
    last_updated_sequence_number: i64,
}

struct IcebergFrozenCowBase {
    table_payload: IcebergTablePayload,
    metadata: TableMetadata,
    snapshot_id: i64,
    files: BTreeMap<String, DataFileWithStats>,
}

fn iceberg_cow_selection_groups(
    preparation: &ConnectorRowMutationPreparation,
    selection: &ConnectorRowMutationSelection,
) -> Result<
    (
        BTreeMap<String, Vec<IcebergCowMatchedRow>>,
        Vec<ConnectorRowMutationSelectionOrdinal>,
    ),
    ConnectorError,
> {
    let contract = preparation.match_contract();
    let file_ordinal = contract
        .identity_fields()
        .iter()
        .find(|field| field.field().name().eq_ignore_ascii_case("_file"))
        .map(|field| field.source_ordinal() as usize)
        .ok_or_else(|| {
            invalid_iceberg_row_mutation_activation("Iceberg COW identity lacks `_file`")
        })?;
    let row_id_ordinal = contract
        .identity_fields()
        .iter()
        .find(|field| field.field().name().eq_ignore_ascii_case("_row_id"))
        .map(|field| field.source_ordinal() as usize)
        .ok_or_else(|| {
            invalid_iceberg_row_mutation_activation("Iceberg COW identity lacks `_row_id`")
        })?;
    let position_ordinal = contract
        .identity_fields()
        .iter()
        .find(|field| field.field().name().eq_ignore_ascii_case("_pos"))
        .map(|field| field.source_ordinal() as usize)
        .ok_or_else(|| {
            invalid_iceberg_row_mutation_activation("Iceberg COW identity lacks `_pos`")
        })?;
    let last_sequence_ordinal = contract
        .identity_fields()
        .iter()
        .find(|field| {
            field
                .field()
                .name()
                .eq_ignore_ascii_case(ICEBERG_LAST_UPDATED_SEQ_COL)
        })
        .map(|field| field.source_ordinal() as usize)
        .ok_or_else(|| {
            invalid_iceberg_row_mutation_activation(
                "Iceberg COW identity lacks `_last_updated_sequence_number`",
            )
        })?;
    let effect_ordinal = contract.effect_field().target_ordinal() as usize;
    let mut grouped = BTreeMap::<String, Vec<IcebergCowMatchedRow>>::new();
    let mut append_ordinals = Vec::new();
    let mut ordinal = 0_u64;
    for batch in selection.batches() {
        let effects = batch
            .column(effect_ordinal)
            .as_any()
            .downcast_ref::<Int8Array>()
            .ok_or_else(|| {
                invalid_iceberg_row_mutation_activation("Iceberg COW effect field is not Int8")
            })?;
        let files = batch
            .column(file_ordinal)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| {
                invalid_iceberg_row_mutation_activation("Iceberg COW `_file` identity is not UTF-8")
            })?;
        let row_ids = batch
            .column(row_id_ordinal)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| {
                invalid_iceberg_row_mutation_activation(
                    "Iceberg COW `_row_id` identity is not INT64",
                )
            })?;
        let positions = batch
            .column(position_ordinal)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| {
                invalid_iceberg_row_mutation_activation("Iceberg COW `_pos` identity is not INT64")
            })?;
        let last_sequences = batch
            .column(last_sequence_ordinal)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| {
                invalid_iceberg_row_mutation_activation(
                    "Iceberg COW `_last_updated_sequence_number` identity is not INT64",
                )
            })?;
        if effects.null_count() != 0 {
            return Err(invalid_iceberg_row_mutation_activation(
                "Iceberg COW effect field contains nulls",
            ));
        }
        for index in 0..batch.num_rows() {
            let selection_ordinal = ConnectorRowMutationSelectionOrdinal::new(ordinal);
            ordinal = ordinal.checked_add(1).ok_or_else(|| {
                ConnectorError::new(
                    ConnectorErrorKind::ResourceExhausted,
                    "Iceberg COW selection ordinal overflowed",
                )
            })?;
            let effect = match effects.value(index) {
                1 => ConnectorRowMutationEffect::Delete,
                2 => ConnectorRowMutationEffect::Replace,
                3 => ConnectorRowMutationEffect::Insert,
                _ => {
                    return Err(invalid_iceberg_row_mutation_activation(
                        "Iceberg COW selection contains an unknown logical effect",
                    ));
                }
            };
            if !preparation.intent().accepts(effect) {
                return Err(invalid_iceberg_row_mutation_activation(
                    "Iceberg COW selection effect is not admitted by the preparation",
                ));
            }
            if effect == ConnectorRowMutationEffect::Insert {
                append_ordinals.push(selection_ordinal);
                continue;
            }
            if files.is_null(index)
                || row_ids.is_null(index)
                || positions.is_null(index)
                || last_sequences.is_null(index)
            {
                return Err(invalid_iceberg_row_mutation_activation(
                    "Iceberg COW matched row has null physical identity or lineage",
                ));
            }
            grouped
                .entry(files.value(index).to_string())
                .or_default()
                .push(IcebergCowMatchedRow {
                    ordinal: selection_ordinal,
                    row_id: row_ids.value(index),
                    position: positions.value(index),
                    last_updated_sequence_number: last_sequences.value(index),
                });
        }
    }
    let mut globally_mapped_row_ids = BTreeSet::new();
    for rows in grouped.values_mut() {
        rows.sort_by_key(|row| row.ordinal);
        if rows
            .iter()
            .any(|row| !globally_mapped_row_ids.insert(row.row_id))
        {
            return Err(invalid_iceberg_row_mutation_activation(
                "Iceberg COW selection maps one row identity more than once",
            ));
        }
    }
    if grouped.is_empty() && append_ordinals.is_empty() {
        return Err(invalid_iceberg_row_mutation_activation(
            "Iceberg COW selection is known-empty and has no cohort to activate",
        ));
    }
    Ok((grouped, append_ordinals))
}

fn freeze_iceberg_cow_base(
    preparation: &ConnectorRowMutationPreparation,
    touched_files: &BTreeSet<String>,
    runtime: &IcebergControlRuntime,
) -> Result<IcebergFrozenCowBase, ConnectorError> {
    let table_payload: IcebergTablePayload =
        decode_payload(preparation.table().payload(), "admitted Iceberg COW table")?;
    if table_payload.metadata_table_type.is_some() || table_payload.row_mutation_frozen_source {
        return Err(invalid_iceberg_row_mutation_activation(
            "Iceberg COW activation requires an admitted base table",
        ));
    }
    let table_info = table_payload.table_info.as_ref().ok_or_else(|| {
        ConnectorError::new(
            ConnectorErrorKind::CorruptData,
            "admitted Iceberg COW table is missing its frozen descriptor",
        )
    })?;
    let serialized = table_info.serialized_metadata.as_deref().ok_or_else(|| {
        ConnectorError::new(
            ConnectorErrorKind::CorruptData,
            "admitted Iceberg COW table is missing serialized metadata",
        )
    })?;
    let metadata: TableMetadata = serde_json::from_str(serialized).map_err(|error| {
        ConnectorError::new(
            ConnectorErrorKind::CorruptData,
            format!("decode admitted Iceberg COW metadata: {error}"),
        )
    })?;
    let snapshot_id = preparation.base_version_ordinal().ok_or_else(|| {
        invalid_iceberg_row_mutation_activation("Iceberg COW activation requires a base snapshot")
    })?;
    let admitted_ref_snapshot = crate::ref_snapshot::resolve_branch_head_snapshot_id(
        &metadata,
        preparation.target_ref().as_str(),
    )
    .map_err(|error| ConnectorError::new(ConnectorErrorKind::CorruptData, error))?;
    let metadata_uuid = metadata.uuid().to_string();
    if admitted_ref_snapshot != Some(snapshot_id)
        || metadata.snapshot_by_id(snapshot_id).is_none()
        || table_info.table_uuid.as_deref() != Some(metadata_uuid.as_str())
        || table_info.location != metadata.location()
    {
        return Err(ConnectorError::new(
            ConnectorErrorKind::CorruptData,
            "Iceberg COW preparation does not match its admitted metadata base",
        ));
    }
    let identifier = crate::iceberg::TableIdent::from_strs([
        table_payload.namespace.as_str(),
        table_payload.table.as_str(),
    ])
    .map_err(|error| {
        invalid_iceberg_row_mutation_activation(format!(
            "build admitted Iceberg COW table identity: {error}"
        ))
    })?;
    let file_io = crate::fs_io::build_file_io_for_location(
        metadata.location(),
        runtime.resources().planning_binding().object_store_config(),
    );
    let table = crate::iceberg::table::Table::builder()
        .identifier(identifier)
        .file_io(file_io)
        .metadata(metadata.clone())
        .build()
        .map_err(|error| {
            ConnectorError::new(
                ConnectorErrorKind::CorruptData,
                format!("build admitted Iceberg COW table: {error}"),
            )
        })?;
    let files = runtime
        .resources()
        .catalog_runtime()
        .block_on(async move {
            crate::manifest::extract_data_files_with_stats_at(&table, snapshot_id).await
        })
        .map_err(unavailable_iceberg_row_mutation_activation)?
        .map_err(unavailable_iceberg_row_mutation_activation)?;
    let mut by_path = BTreeMap::new();
    for mut file in files {
        if !touched_files.contains(&file.path) {
            continue;
        }
        file.delete_files
            .sort_by(|left, right| left.path.cmp(&right.path));
        if file
            .delete_files
            .iter()
            .any(|delete| delete.path.is_empty())
        {
            return Err(ConnectorError::new(
                ConnectorErrorKind::CorruptData,
                format!(
                    "Iceberg COW data file `{}` has an empty applicable delete path",
                    file.path
                ),
            ));
        }
        if file
            .delete_files
            .windows(2)
            .any(|pair| pair[0].path == pair[1].path)
        {
            return Err(ConnectorError::new(
                ConnectorErrorKind::CorruptData,
                format!(
                    "Iceberg COW data file `{}` has duplicate applicable delete facts",
                    file.path
                ),
            ));
        }
        let path = file.path.clone();
        if by_path.insert(path.clone(), file).is_some() {
            return Err(ConnectorError::new(
                ConnectorErrorKind::CorruptData,
                format!("Iceberg COW admitted base contains duplicate data file `{path}`"),
            ));
        }
    }
    if by_path.len() != touched_files.len() {
        return Err(invalid_iceberg_row_mutation_activation(
            "Iceberg COW selection contains a file absent from the admitted base",
        ));
    }
    Ok(IcebergFrozenCowBase {
        table_payload,
        metadata,
        snapshot_id,
        files: by_path,
    })
}

fn validate_matched_rows_against_frozen_file(
    old_file: &str,
    rows: &[IcebergCowMatchedRow],
    data_file: &DataFileWithStats,
) -> Result<(), ConnectorError> {
    let first_row_id = data_file.first_row_id.ok_or_else(|| {
        invalid_iceberg_row_mutation_activation(format!(
            "Iceberg COW source `{old_file}` is missing first_row_id"
        ))
    })?;
    let record_count = data_file
        .record_count
        .filter(|count| *count >= 0)
        .ok_or_else(|| {
            invalid_iceberg_row_mutation_activation(format!(
                "Iceberg COW source `{old_file}` is missing a valid record count"
            ))
        })?;
    if data_file.data_sequence_number.is_none() {
        return Err(invalid_iceberg_row_mutation_activation(format!(
            "Iceberg COW source `{old_file}` is missing its data sequence"
        )));
    }
    let row_id_end = first_row_id.checked_add(record_count).ok_or_else(|| {
        ConnectorError::new(
            ConnectorErrorKind::CorruptData,
            format!("Iceberg COW source `{old_file}` row lineage overflowed"),
        )
    })?;
    for row in rows {
        let expected_position = row.row_id.checked_sub(first_row_id).ok_or_else(|| {
            invalid_iceberg_row_mutation_activation(format!(
                "Iceberg COW row {} precedes source `{old_file}` lineage",
                row.row_id
            ))
        })?;
        if row.row_id < first_row_id
            || row.row_id >= row_id_end
            || row.position != expected_position
            || row.last_updated_sequence_number < 0
        {
            return Err(invalid_iceberg_row_mutation_activation(format!(
                "Iceberg COW row {} does not belong to admitted source `{old_file}`",
                row.row_id
            )));
        }
    }
    Ok(())
}

fn freeze_iceberg_cow_source(
    preparation: &ConnectorRowMutationPreparation,
    frozen: &IcebergFrozenCowBase,
    data_file: DataFileWithStats,
    max_handle_payload_bytes: usize,
) -> Result<(ConnectorTableHandle, SchemaRef, [u8; 32]), ConnectorError> {
    let snapshot = frozen
        .metadata
        .snapshot_by_id(frozen.snapshot_id)
        .ok_or_else(|| {
            ConnectorError::new(
                ConnectorErrorKind::CorruptData,
                "Iceberg COW base snapshot disappeared from admitted metadata",
            )
        })?;
    let snapshot_schema = snapshot.schema(&frozen.metadata).map_err(|error| {
        ConnectorError::new(
            ConnectorErrorKind::CorruptData,
            format!("resolve Iceberg COW snapshot schema: {error}"),
        )
    })?;
    let mut fields = crate::iceberg::arrow::schema_to_arrow_schema(&snapshot_schema)
        .map_err(|error| {
            ConnectorError::new(
                ConnectorErrorKind::CorruptData,
                format!("convert Iceberg COW snapshot schema: {error}"),
            )
        })?
        .fields()
        .to_vec();
    let metadata_fields =
        crate::control_provider::metadata_arrow_fields(&frozen.table_payload.metadata_columns)?
            .into_iter()
            .map(|field| Arc::new(field.as_ref().clone().with_nullable(false)));
    fields.extend(metadata_fields);
    let scan_schema = Arc::new(Schema::new(fields));

    let mut source_payload = frozen.table_payload.clone();
    source_payload.prepared_files.clear();
    let explicit_file = data_file_with_stats_to_iceberg_data_file_info(data_file);
    crate::delete_file::validate_delete_apply_cost(&explicit_file)?;
    source_payload.explicit_files = Some(vec![explicit_file]);
    source_payload.row_mutation_frozen_source = true;
    let table_info = source_payload.table_info.as_mut().ok_or_else(|| {
        ConnectorError::new(
            ConnectorErrorKind::CorruptData,
            "Iceberg COW source lost its admitted table descriptor",
        )
    })?;
    table_info.current_snapshot_id = Some(frozen.snapshot_id);
    table_info.schema_id = snapshot_schema.schema_id();
    table_info.schema = crate::schema_facts::iceberg_schema_def(&snapshot_schema);
    let encoded = serde_json::to_vec(&source_payload).map_err(|error| {
        ConnectorError::new(
            ConnectorErrorKind::Internal,
            format!("encode Iceberg COW frozen source: {error}"),
        )
    })?;
    if encoded.len() > max_handle_payload_bytes {
        return Err(ConnectorError::new(
            ConnectorErrorKind::ResourceExhausted,
            "Iceberg COW frozen source exceeds the request handle budget",
        ));
    }
    let digest: [u8; 32] = Sha256::digest(&encoded).into();
    let source = ConnectorTableHandle::try_new(
        preparation.owner().instance_id.clone(),
        Bytes::from(encoded),
    )?;
    Ok((source, scan_schema, digest))
}

fn iceberg_cow_scan_bindings(
    preparation: &ConnectorRowMutationPreparation,
    input: &ConnectorWriteInputShape,
    scan_schema: &Schema,
) -> Result<
    (
        Vec<ConnectorRowMutationScanBinding>,
        Option<ConnectorWriteFieldToken>,
    ),
    ConnectorError,
> {
    let contract = preparation.match_contract();
    let mut bindings = BTreeMap::new();
    for (ordinal, field) in contract.after_fields().iter().enumerate() {
        let actual = scan_schema.field(ordinal);
        if actual.data_type() != field.field().data_type()
            || (actual.is_nullable() && !field.field().is_nullable())
        {
            return Err(ConnectorError::new(
                ConnectorErrorKind::CorruptData,
                "Iceberg COW snapshot schema differs from the signed after-image schema",
            ));
        }
        bindings.insert(field.token(), ordinal as u32);
    }
    for field in contract.identity_fields() {
        let ordinal = scan_schema
            .fields()
            .iter()
            .position(|candidate| candidate.name().eq_ignore_ascii_case(field.field().name()))
            .ok_or_else(|| {
                ConnectorError::new(
                    ConnectorErrorKind::CorruptData,
                    format!(
                        "Iceberg COW scan schema omits signed identity `{}`",
                        field.field().name()
                    ),
                )
            })?;
        bindings.insert(field.token(), ordinal as u32);
    }
    let route_tokens = input
        .fields()
        .into_iter()
        .map(|field| field.token())
        .collect::<BTreeSet<_>>();
    if route_tokens
        .iter()
        .any(|token| !bindings.contains_key(token))
        || contract
            .uniqueness_tokens()
            .iter()
            .any(|token| !bindings.contains_key(token))
    {
        return Err(ConnectorError::new(
            ConnectorErrorKind::CorruptData,
            "Iceberg COW scan bindings do not cover the signed route and match tokens",
        ));
    }
    let written_version_token = contract
        .identity_fields()
        .iter()
        .find(|field| {
            field
                .field()
                .name()
                .eq_ignore_ascii_case(ICEBERG_LAST_UPDATED_SEQ_COL)
        })
        .map(|field| field.token());
    if preparation.written_version_ordinal().is_some() && written_version_token.is_none() {
        return Err(ConnectorError::new(
            ConnectorErrorKind::CorruptData,
            "Iceberg COW preparation omits its written-version token",
        ));
    }
    Ok((
        bindings
            .into_iter()
            .map(|(token, ordinal)| ConnectorRowMutationScanBinding::new(token, ordinal))
            .collect(),
        written_version_token,
    ))
}

fn unavailable_iceberg_row_mutation_activation(message: impl Into<String>) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::Unavailable, message.into())
        .with_retryable_before_progress()
}

fn invalid_iceberg_row_mutation_activation(message: impl Into<String>) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::InvalidRequest, message.into())
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, HashMap};
    use std::sync::Arc;
    use std::time::Duration;

    use arrow::array::{ArrayRef, RecordBatch};
    use arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
    use novarocks_spi::connector::{
        ConnectorCancellation, ConnectorInstanceId, ConnectorInstanceIncarnation,
        ConnectorMutationEffectField, ConnectorMutationMatchContract, ConnectorMutationSourceField,
        ConnectorRequestContext, ConnectorRowMutationCohortRecipeBody, ConnectorRowMutationIntent,
        ConnectorTableHandle, ConnectorWriteAbortOutcome, ConnectorWriteAbortRequest,
        ConnectorWriteBaseVersion, ConnectorWriteCommitRequest, ConnectorWriteControl,
        ConnectorWriteLease, ConnectorWriteOperationId, ConnectorWritePlan,
        ConnectorWritePlanningRequest, ConnectorWriteReceipt, ConnectorWriteReconcileRequest,
        ConnectorWriteTargetRef, ExternalMutationOutcome,
    };

    use crate::control_provider::IcebergTablePayload;
    use crate::iceberg::spec::{
        FormatVersion, NestedField, Operation, PartitionSpec, PrimitiveType, Schema, Snapshot,
        SnapshotReference, SnapshotRetention, SortOrder, Summary, TableMetadataBuilder, Type,
    };
    use crate::row_mutation_payload::decode_cow_recipe;
    use crate::scan_model::IcebergTableInfo;

    use super::*;

    struct NeverCancelled;

    impl ConnectorCancellation for NeverCancelled {
        fn is_cancelled(&self) -> bool {
            false
        }
    }

    struct AlwaysCancelled;

    impl ConnectorCancellation for AlwaysCancelled {
        fn is_cancelled(&self) -> bool {
            true
        }
    }

    /// The activation path is the only method this control needs to answer;
    /// the rest of `ConnectorWriteControl` exists so the SPI lease — and its
    /// cohort/route validation — can be exercised without a catalog runtime.
    struct ActivationOnlyControl {
        key: ConnectorExecutionBindingKey,
        tamper: bool,
    }

    impl ConnectorWriteControl for ActivationOnlyControl {
        fn binding_key(&self) -> &ConnectorExecutionBindingKey {
            &self.key
        }

        fn activate_row_mutation(
            &self,
            request: ConnectorRowMutationActivationRequest,
        ) -> Result<ConnectorRowMutationExecutionPlan, ConnectorError> {
            let plan = activate_for_test(request, &self.key)?;
            if !self.tamper {
                return Ok(plan);
            }
            // Add one route whose cohort the sealed set does not carry. The
            // plan constructor accepts it (its recipes stay consistent), so
            // only the SPI lease can catch the unsealed cohort.
            let preparation = plan.preparation().clone();
            let (selection, sealed, recipes) = plan.copy_on_write().expect("copy-on-write plan");
            let mut routes = plan.routes().to_vec();
            let foreign_cohort = ConnectorWriteCohortId::derive(
                preparation.operation_id(),
                b"iceberg-cow-rewrite",
                [9_u8; 32],
            )?;
            routes.push(iceberg_row_mutation_route_with_preparation(
                &preparation,
                foreign_cohort,
                b"cow-rewrite",
                admitted_effects(&preparation, &[ConnectorRowMutationEffect::Replace]),
                routes[0].preparation().clone(),
                Vec::new(),
            )?);
            ConnectorRowMutationExecutionPlan::try_copy_on_write(
                preparation,
                selection.clone(),
                routes,
                sealed.clone(),
                recipes.to_vec(),
                &context(),
            )
        }

        fn plan_write(
            &self,
            _request: ConnectorWritePlanningRequest,
        ) -> Result<ConnectorWritePlan, ConnectorError> {
            Err(unsupported())
        }

        fn commit(
            &self,
            _request: ConnectorWriteCommitRequest,
        ) -> Result<ExternalMutationOutcome<ConnectorWriteReceipt>, ConnectorError> {
            Err(unsupported())
        }

        fn abort(
            &self,
            _request: ConnectorWriteAbortRequest,
        ) -> Result<ConnectorWriteAbortOutcome, ConnectorError> {
            Err(unsupported())
        }

        fn reconcile(
            &self,
            _request: ConnectorWriteReconcileRequest,
        ) -> Result<ExternalMutationOutcome<ConnectorWriteReceipt>, ConnectorError> {
            Err(unsupported())
        }
    }

    fn expect_error<T>(result: Result<T, ConnectorError>) -> ConnectorError {
        match result {
            Ok(_) => panic!("expected a connector error"),
            Err(error) => error,
        }
    }

    fn unsupported() -> ConnectorError {
        ConnectorError::new(
            ConnectorErrorKind::Unsupported,
            "activation-only test control",
        )
    }

    fn owner() -> ConnectorExecutionBindingKey {
        ConnectorExecutionBindingKey {
            instance_id: ConnectorInstanceId::parse("ice").expect("instance"),
            incarnation: ConnectorInstanceIncarnation::from_bytes([7; 16]),
        }
    }

    fn lease(owner: &ConnectorExecutionBindingKey, tamper: bool) -> ConnectorWriteLease {
        ConnectorWriteLease::new(
            owner.clone(),
            Arc::new(ActivationOnlyControl {
                key: owner.clone(),
                tamper,
            }),
            || {},
        )
        .expect("write lease")
    }

    fn context() -> ConnectorRequestContext {
        ConnectorRequestContext::try_new(
            Instant::now() + Duration::from_secs(30),
            Arc::new(NeverCancelled),
            1024 * 1024,
            4 * 1024 * 1024,
        )
        .expect("context")
    }

    fn frozen_file(path: &str, first_row_id: i64) -> DataFileWithStats {
        DataFileWithStats {
            path: path.to_string(),
            size: 128,
            record_count: Some(10),
            column_stats: None,
            partition_spec_id: Some(0),
            partition_key: None,
            partition_values: None,
            manifest_path: Some("file:///warehouse/db/t/metadata/manifest.avro".to_string()),
            partition_field_values: Vec::new(),
            first_row_id: Some(first_row_id),
            data_sequence_number: Some(1),
            delete_files: Vec::new(),
        }
    }

    fn frozen_base(preparation: &ConnectorRowMutationPreparation) -> IcebergFrozenCowBase {
        let table_payload: IcebergTablePayload =
            decode_payload(preparation.table().payload(), "test table").expect("payload");
        let metadata = table_metadata();
        IcebergFrozenCowBase {
            table_payload,
            metadata,
            snapshot_id: 1,
            files: [
                ("a.parquet".to_string(), frozen_file("a.parquet", 0)),
                ("b.parquet".to_string(), frozen_file("b.parquet", 10)),
            ]
            .into_iter()
            .collect(),
        }
    }

    fn activate_for_test(
        request: ConnectorRowMutationActivationRequest,
        owner: &ConnectorExecutionBindingKey,
    ) -> Result<ConnectorRowMutationExecutionPlan, ConnectorError> {
        request.validate(owner)?;
        if request.context().cancellation().is_cancelled() {
            return Err(ConnectorError::new(
                ConnectorErrorKind::Cancelled,
                "Iceberg row-mutation activation was cancelled before Provider planning",
            ));
        }
        if Instant::now() >= request.context().deadline() {
            return Err(ConnectorError::new(
                ConnectorErrorKind::DeadlineExceeded,
                "Iceberg row-mutation activation deadline elapsed before Provider planning",
            ));
        }
        match &request {
            ConnectorRowMutationActivationRequest::Direct { preparation, .. } => {
                activate_iceberg_direct_row_mutation(preparation)
            }
            ConnectorRowMutationActivationRequest::CopyOnWrite {
                preparation,
                selection,
                context,
            } => {
                let (rewrite_rows, append_ordinals) =
                    iceberg_cow_selection_groups(preparation, selection)?;
                activate_iceberg_cow_row_mutation_from_frozen(
                    preparation,
                    selection,
                    context,
                    rewrite_rows,
                    append_ordinals,
                    frozen_base(preparation),
                )
            }
        }
    }

    fn table_metadata() -> crate::iceberg::spec::TableMetadata {
        let schema = Schema::builder()
            .with_fields(vec![
                NestedField::optional(1, "value", Type::Primitive(PrimitiveType::Long)).into(),
            ])
            .build()
            .expect("schema");
        let snapshot = Snapshot::builder()
            .with_snapshot_id(1)
            .with_sequence_number(1)
            .with_timestamp_ms(1)
            .with_manifest_list("file:///warehouse/db/t/metadata/snap-1.avro".to_string())
            .with_summary(Summary {
                operation: Operation::Append,
                additional_properties: HashMap::new(),
            })
            .with_schema_id(schema.schema_id())
            .build();
        TableMetadataBuilder::new(
            schema,
            PartitionSpec::unpartition_spec().into_unbound(),
            SortOrder::unsorted_order(),
            "file:///warehouse/db/t".to_string(),
            FormatVersion::V2,
            HashMap::new(),
        )
        .expect("metadata builder")
        .add_snapshot(snapshot)
        .expect("snapshot")
        .set_ref(
            "main",
            SnapshotReference::new(
                1,
                SnapshotRetention::Branch {
                    min_snapshots_to_keep: None,
                    max_snapshot_age_ms: None,
                    max_ref_age_ms: None,
                },
            ),
        )
        .expect("main ref")
        .build()
        .expect("metadata")
        .metadata
    }

    fn table_handle(owner: &ConnectorExecutionBindingKey) -> ConnectorTableHandle {
        let metadata = table_metadata();
        let payload = IcebergTablePayload {
            namespace: "db".to_string(),
            table: "t".to_string(),
            table_info: Some(IcebergTableInfo {
                catalog: "ice".to_string(),
                namespace: "db".to_string(),
                table: "t".to_string(),
                table_uuid: Some(metadata.uuid().to_string()),
                current_snapshot_id: metadata.current_snapshot_id(),
                schema_id: metadata.current_schema_id(),
                location: metadata.location().to_string(),
                schema: crate::schema_facts::iceberg_schema_def(metadata.current_schema()),
                serialized_metadata: Some(serde_json::to_string(&metadata).expect("metadata JSON")),
                serialized_metadata_rows: None,
            }),
            metadata_columns: vec![
                "_file".to_string(),
                "_pos".to_string(),
                "_row_id".to_string(),
                "_last_updated_sequence_number".to_string(),
            ],
            metadata_table_type: None,
            prepared_files: Vec::new(),
            explicit_files: None,
            row_mutation_frozen_source: false,
            logical_type_columns: BTreeMap::new(),
            hidden_columns: Vec::new(),
        };
        ConnectorTableHandle::try_new(
            owner.instance_id.clone(),
            Bytes::from(serde_json::to_vec(&payload).expect("table payload")),
        )
        .expect("table handle")
    }

    fn token(marker: u8) -> ConnectorWriteFieldToken {
        ConnectorWriteFieldToken::from_bytes([marker; 32])
    }

    /// Mirrors the layout `prepare_row_mutation` signs: identity source
    /// ordinals first, then the before/after target block, then the effect
    /// field. The activation port depends on exactly these ordinals.
    fn preparation(
        owner: &ConnectorExecutionBindingKey,
        strategy: ConnectorRowMutationStrategy,
        intent: ConnectorRowMutationIntent,
    ) -> ConnectorRowMutationPreparation {
        let table = table_handle(owner);
        let base_version = ConnectorWriteBaseVersion::try_new(Bytes::from_static(
            b"iceberg/row-mutation-base/v1/uuid/main/1",
        ))
        .expect("base version");
        let identity_fields = vec![
            ConnectorMutationSourceField::new(
                token(1),
                Field::new("_file", DataType::Utf8, true),
                0,
            ),
            ConnectorMutationSourceField::new(
                token(2),
                Field::new("_pos", DataType::Int64, true),
                1,
            ),
            ConnectorMutationSourceField::new(
                token(3),
                Field::new("_row_id", DataType::Int64, true),
                2,
            ),
            ConnectorMutationSourceField::new(
                token(4),
                Field::new("_last_updated_sequence_number", DataType::Int64, true),
                3,
            ),
        ];
        let before_fields = vec![ConnectorMutationTargetField::new(
            token(5),
            Field::new("value", DataType::Int64, true),
            4,
        )];
        let after_fields = vec![ConnectorMutationTargetField::new(
            token(6),
            Field::new("value", DataType::Int64, true),
            5,
        )];
        let effect_field = ConnectorMutationEffectField::try_new(
            token(7),
            Field::new("__row_mutation_effect", DataType::Int8, false),
            6,
        )
        .expect("effect field");
        let contract = ConnectorMutationMatchContract::try_new(
            owner.clone(),
            table.clone(),
            base_version.clone(),
            identity_fields.clone(),
            before_fields,
            after_fields,
            identity_fields[..3]
                .iter()
                .map(ConnectorMutationSourceField::token)
                .collect(),
            effect_field,
        )
        .expect("match contract");
        ConnectorRowMutationPreparation::try_new(
            owner.clone(),
            ConnectorWriteOperationId::from_bytes([3; 16]),
            table.clone(),
            table,
            Arc::new(arrow::datatypes::Schema::new(vec![
                Field::new("value", DataType::Int64, true),
                Field::new("_file", DataType::Utf8, false),
                Field::new("_pos", DataType::Int64, false),
                Field::new("_row_id", DataType::Int64, false),
                Field::new("_last_updated_sequence_number", DataType::Int64, false),
            ])),
            ConnectorWriteTargetRef::main(),
            intent,
            base_version,
            contract,
            strategy,
            None,
            Some(1),
            Bytes::from_static(
                b"iceberg/row-mutation-preparation/v1/ice/uuid/main/none/PositionDelete",
            ),
        )
        .expect("preparation")
    }

    /// One selection batch laid out on the signed match ordinals:
    /// `_file`, `_pos`, `_row_id`, `_last_updated_sequence_number`, before
    /// `value`, after `value`, effect.
    fn selection_batch(
        files: Vec<Option<&str>>,
        row_ids: Vec<Option<i64>>,
        effects: Vec<i8>,
    ) -> RecordBatch {
        let rows = effects.len();
        let positions = files
            .iter()
            .zip(&row_ids)
            .map(|(file, row_id)| match (file, row_id) {
                (Some("a.parquet"), Some(row_id)) => Some(*row_id),
                (Some("b.parquet"), Some(row_id)) => Some(*row_id - 10),
                _ => None,
            })
            .collect::<Vec<_>>();
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("_file", DataType::Utf8, true),
            Field::new("_pos", DataType::Int64, true),
            Field::new("_row_id", DataType::Int64, true),
            Field::new("_last_updated_sequence_number", DataType::Int64, true),
            Field::new("value", DataType::Int64, true),
            Field::new("value", DataType::Int64, true),
            Field::new("__row_mutation_effect", DataType::Int8, false),
        ]));
        let columns: Vec<ArrayRef> = vec![
            Arc::new(StringArray::from(files)),
            Arc::new(Int64Array::from(positions)),
            Arc::new(Int64Array::from(row_ids)),
            Arc::new(Int64Array::from(vec![Some(1_i64); rows])),
            Arc::new(Int64Array::from(vec![Some(10_i64); rows])),
            Arc::new(Int64Array::from(vec![Some(20_i64); rows])),
            Arc::new(Int8Array::from(effects)),
        ];
        RecordBatch::try_new(schema, columns).expect("selection batch")
    }

    fn cow_selection() -> ConnectorRowMutationSelection {
        let batch = selection_batch(
            vec![
                Some("a.parquet"),
                Some("b.parquet"),
                Some("a.parquet"),
                None,
            ],
            vec![Some(7), Some(15), Some(3), None],
            vec![2, 1, 2, 3],
        );
        ConnectorRowMutationSelection::try_new(batch.schema(), vec![batch], 1024, 1024 * 1024)
            .expect("selection")
    }

    #[test]
    fn lowercase_hex_matches_the_canonical_encoding() {
        assert_eq!(lowercase_hex([0x00, 0x0f, 0xa5, 0xff]), "000fa5ff");
        assert_eq!(lowercase_hex([0u8; 32]).len(), 64);
    }

    #[test]
    fn direct_position_delete_activation_builds_the_primary_route() {
        let owner = owner();
        let preparation = preparation(
            &owner,
            ConnectorRowMutationStrategy::PositionDelete,
            ConnectorRowMutationIntent::Delete,
        );
        let plan = activate_for_test(
            ConnectorRowMutationActivationRequest::Direct {
                preparation: preparation.clone(),
                context: context(),
            },
            &owner,
        )
        .expect("direct activation");
        assert!(plan.copy_on_write().is_none());
        assert_eq!(plan.routes().len(), 1);
        let route = &plan.routes()[0];
        assert_eq!(
            route.cohort_id(),
            ConnectorWriteCohortId::primary(preparation.operation_id())
        );
        assert_eq!(
            route.accepted_effects(),
            &[ConnectorRowMutationEffect::Delete]
        );
        assert_eq!(
            route.route_id(),
            iceberg_row_mutation_route_id(
                &preparation,
                route.cohort_id(),
                b"direct-position-delete"
            )
        );
        assert_eq!(route.preparation().intent(), ConnectorWriteIntent::RowDelta);
        assert!(route.partition_fields().is_empty());
        match route.input() {
            ConnectorWriteInputShape::PositionDelete {
                identity_fields,
                partition_source_fields,
            } => {
                assert_eq!(identity_fields.len(), 2);
                assert!(
                    identity_fields
                        .iter()
                        .all(|field| !field.field().is_nullable())
                );
                assert_eq!(identity_fields[0].field().name(), "_file");
                assert_eq!(identity_fields[1].field().name(), "_pos");
                assert!(partition_source_fields.is_empty());
            }
            other => panic!("unexpected input shape: {other:?}"),
        }
        let ordinals = route
            .input_ordinals()
            .iter()
            .map(ConnectorMutationRouteInput::input_ordinal)
            .collect::<Vec<_>>();
        assert_eq!(ordinals, vec![0, 1]);
    }

    #[test]
    fn merge_on_read_activation_fans_out_three_sealed_cohorts() {
        let owner = owner();
        let preparation = preparation(
            &owner,
            ConnectorRowMutationStrategy::MergeOnRead,
            ConnectorRowMutationIntent::Merge {
                effects: vec![
                    ConnectorRowMutationEffect::Delete,
                    ConnectorRowMutationEffect::Replace,
                    ConnectorRowMutationEffect::Insert,
                ],
            },
        );
        let plan = activate_for_test(
            ConnectorRowMutationActivationRequest::Direct {
                preparation: preparation.clone(),
                context: context(),
            },
            &owner,
        )
        .expect("merge-on-read activation");
        assert_eq!(plan.routes().len(), 3);
        let actual = plan
            .routes()
            .iter()
            .map(ConnectorRowMutationRoute::cohort_id)
            .collect::<BTreeSet<_>>();
        let expected = [&b"mor-delete"[..], b"mor-replacement", b"mor-insert"]
            .into_iter()
            .map(|kind| iceberg_row_mutation_direct_cohort(&preparation, kind).expect("cohort"))
            .collect::<BTreeSet<_>>();
        assert_eq!(actual, expected);
        assert_eq!(
            plan.routes()
                .iter()
                .filter(|route| matches!(
                    route.input(),
                    ConnectorWriteInputShape::DeletionVector { .. }
                ))
                .count(),
            1
        );
        assert_eq!(
            plan.routes()
                .iter()
                .filter(|route| matches!(
                    route.input(),
                    ConnectorWriteInputShape::RowLineage { .. }
                ))
                .count(),
            1
        );
        assert_eq!(
            plan.routes()
                .iter()
                .filter(|route| matches!(route.input(), ConnectorWriteInputShape::Data { .. }))
                .count(),
            1
        );
    }

    #[test]
    fn copy_on_write_activation_seals_one_cohort_per_rewritten_file_and_the_append() {
        let owner = owner();
        let preparation = preparation(
            &owner,
            ConnectorRowMutationStrategy::CopyOnWrite,
            ConnectorRowMutationIntent::Merge {
                effects: vec![
                    ConnectorRowMutationEffect::Delete,
                    ConnectorRowMutationEffect::Replace,
                    ConnectorRowMutationEffect::Insert,
                ],
            },
        );
        let plan = activate_for_test(
            ConnectorRowMutationActivationRequest::CopyOnWrite {
                preparation: preparation.clone(),
                selection: cow_selection(),
                context: context(),
            },
            &owner,
        )
        .expect("copy-on-write activation");
        let (selection, sealed, recipes) = plan.copy_on_write().expect("copy-on-write body");
        assert_eq!(selection.row_count(), 4);
        assert_eq!(plan.routes().len(), 3);
        assert_eq!(sealed.cohorts().len(), 3);
        assert_eq!(recipes.len(), 3);
        assert_eq!(sealed.operation_id(), preparation.operation_id());
        // Every route's cohort must be sealed, and every recipe must name a
        // route that exists: this is exactly what the SPI re-checks.
        for route in plan.routes() {
            assert!(
                sealed
                    .cohorts()
                    .iter()
                    .any(|cohort| cohort.cohort_id() == route.cohort_id())
            );
        }
        let mut decoded = recipes
            .iter()
            .map(|recipe| {
                assert!(
                    plan.routes()
                        .iter()
                        .any(|route| route.route_id() == recipe.route_id())
                );
                let payload = decode_cow_recipe(recipe.payload()).expect("recipe");
                (payload.role, payload.old_file, payload.matched_row_ids)
            })
            .collect::<Vec<_>>();
        decoded.sort();
        assert_eq!(
            decoded,
            vec![
                ("append".to_string(), String::new(), Vec::new()),
                ("rewrite".to_string(), "a.parquet".to_string(), vec![7, 3]),
                ("rewrite".to_string(), "b.parquet".to_string(), vec![15]),
            ]
        );
        let mut ordinal_sets = recipes
            .iter()
            .map(|recipe| {
                recipe
                    .selection_ordinals()
                    .iter()
                    .map(|ordinal| ordinal.get())
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        ordinal_sets.sort();
        assert_eq!(ordinal_sets, vec![vec![0, 2], vec![1], vec![3]]);
        for recipe in recipes {
            let ConnectorRowMutationCohortRecipeBody::Rewrite {
                source,
                scan_schema,
                match_tokens,
                written_version_token,
                ..
            } = recipe.body()
            else {
                continue;
            };
            let source_payload: IcebergTablePayload =
                decode_payload(source.payload(), "frozen source").expect("source payload");
            assert!(source_payload.row_mutation_frozen_source);
            let explicit = source_payload.explicit_files.expect("explicit source file");
            assert_eq!(explicit.len(), 1);
            assert!(explicit[0].first_row_id.is_some());
            assert_eq!(explicit[0].data_sequence_number, Some(1));
            assert!(
                scan_schema.fields()[scan_schema.fields().len() - 4..]
                    .iter()
                    .all(|field| !field.is_nullable())
            );
            let written = written_version_token.expect("written version token");
            assert!(!match_tokens.contains(&written));
            assert_eq!(
                match_tokens,
                preparation.match_contract().uniqueness_tokens()
            );
        }
        let append_cohort = ConnectorWriteCohortId::derive(
            preparation.operation_id(),
            b"iceberg-cow-append",
            Sha256::digest(b"iceberg-cow-append").into(),
        )
        .expect("append cohort");
        assert!(
            sealed
                .cohorts()
                .iter()
                .any(|cohort| cohort.cohort_id() == append_cohort
                    && cohort.intent() == ConnectorWriteIntent::Append)
        );
    }

    #[test]
    fn direct_activation_rejects_a_copy_on_write_preparation() {
        let owner = owner();
        let preparation = preparation(
            &owner,
            ConnectorRowMutationStrategy::CopyOnWrite,
            ConnectorRowMutationIntent::Update,
        );
        // The SPI request check fires first for a Direct/COW mismatch; drive
        // the provider branch directly to cover its own fail-closed message.
        let error = expect_error(activate_iceberg_direct_row_mutation(&preparation));
        assert_eq!(error.kind(), ConnectorErrorKind::InvalidRequest);
        assert_eq!(
            error.to_string(),
            "InvalidRequest: Iceberg Copy-on-Write activation requires the bounded match selection"
        );
    }

    #[test]
    fn copy_on_write_rejects_duplicate_row_mapping_and_missing_lineage() {
        let owner = owner();
        let preparation = preparation(
            &owner,
            ConnectorRowMutationStrategy::CopyOnWrite,
            ConnectorRowMutationIntent::Update,
        );
        let batch = selection_batch(
            vec![Some("a.parquet"), Some("b.parquet")],
            vec![Some(7), Some(7)],
            vec![2, 2],
        );
        let selection =
            ConnectorRowMutationSelection::try_new(batch.schema(), vec![batch], 16, 64 * 1024)
                .expect("selection");
        let duplicate = expect_error(iceberg_cow_selection_groups(&preparation, &selection));
        assert_eq!(duplicate.kind(), ConnectorErrorKind::InvalidRequest);

        let mut file = frozen_file("a.parquet", 0);
        file.first_row_id = None;
        let missing = expect_error(validate_matched_rows_against_frozen_file(
            "a.parquet",
            &[IcebergCowMatchedRow {
                ordinal: ConnectorRowMutationSelectionOrdinal::new(0),
                row_id: 7,
                position: 7,
                last_updated_sequence_number: 1,
            }],
            &file,
        ));
        assert_eq!(missing.kind(), ConnectorErrorKind::InvalidRequest);
        assert!(missing.to_string().contains("missing first_row_id"));
    }

    #[test]
    fn activation_fails_closed_when_the_request_is_cancelled() {
        let owner = owner();
        let context = ConnectorRequestContext::try_new(
            Instant::now() + Duration::from_secs(30),
            Arc::new(AlwaysCancelled),
            1024 * 1024,
            4 * 1024 * 1024,
        )
        .expect("context");
        let error = expect_error(activate_for_test(
            ConnectorRowMutationActivationRequest::Direct {
                preparation: preparation(
                    &owner,
                    ConnectorRowMutationStrategy::PositionDelete,
                    ConnectorRowMutationIntent::Delete,
                ),
                context,
            },
            &owner,
        ));
        assert_eq!(error.kind(), ConnectorErrorKind::Cancelled);
        assert_eq!(
            error.to_string(),
            "Cancelled: Iceberg row-mutation activation was cancelled before Provider planning"
        );
    }

    #[test]
    fn activation_fails_closed_after_the_deadline() {
        let owner = owner();
        let context = ConnectorRequestContext::try_new(
            Instant::now() - Duration::from_secs(1),
            Arc::new(NeverCancelled),
            1024 * 1024,
            4 * 1024 * 1024,
        )
        .expect("context");
        let error = expect_error(activate_for_test(
            ConnectorRowMutationActivationRequest::Direct {
                preparation: preparation(
                    &owner,
                    ConnectorRowMutationStrategy::PositionDelete,
                    ConnectorRowMutationIntent::Delete,
                ),
                context,
            },
            &owner,
        ));
        assert_eq!(error.kind(), ConnectorErrorKind::DeadlineExceeded);
        assert_eq!(
            error.to_string(),
            "DeadlineExceeded: Iceberg row-mutation activation deadline elapsed before Provider planning"
        );
    }

    #[test]
    fn spi_lease_accepts_the_direct_and_copy_on_write_plans() {
        let owner = owner();
        let lease = lease(&owner, false);
        lease
            .activate_row_mutation(ConnectorRowMutationActivationRequest::Direct {
                preparation: preparation(
                    &owner,
                    ConnectorRowMutationStrategy::PositionDelete,
                    ConnectorRowMutationIntent::Delete,
                ),
                context: context(),
            })
            .expect("direct plan passes SPI validation");
        lease
            .activate_row_mutation(ConnectorRowMutationActivationRequest::CopyOnWrite {
                preparation: preparation(
                    &owner,
                    ConnectorRowMutationStrategy::CopyOnWrite,
                    ConnectorRowMutationIntent::Merge {
                        effects: vec![
                            ConnectorRowMutationEffect::Delete,
                            ConnectorRowMutationEffect::Replace,
                            ConnectorRowMutationEffect::Insert,
                        ],
                    },
                ),
                selection: cow_selection(),
                context: context(),
            })
            .expect("copy-on-write plan passes SPI validation");
    }

    #[test]
    fn copy_on_write_plan_rejects_a_route_whose_cohort_is_not_sealed() {
        let owner = owner();
        let error = expect_error(lease(&owner, true).activate_row_mutation(
            ConnectorRowMutationActivationRequest::CopyOnWrite {
                preparation: preparation(
                    &owner,
                    ConnectorRowMutationStrategy::CopyOnWrite,
                    ConnectorRowMutationIntent::Merge {
                        effects: vec![
                            ConnectorRowMutationEffect::Delete,
                            ConnectorRowMutationEffect::Replace,
                            ConnectorRowMutationEffect::Insert,
                        ],
                    },
                ),
                selection: cow_selection(),
                context: context(),
            },
        ));
        assert_eq!(error.kind(), ConnectorErrorKind::InvalidRequest);
        assert_eq!(
            error.to_string(),
            "InvalidRequest: copy-on-write plan has invalid budgets or non-exact route/cohort/recipe cardinality"
        );
    }
}
