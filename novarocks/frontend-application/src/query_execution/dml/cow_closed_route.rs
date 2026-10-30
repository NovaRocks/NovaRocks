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

// Preserve the original sealed route validation at the private COW boundary.
// Original signed types and exact routing remain mandatory caller inputs.
use super::cow_closed_ast as closed;
use closed::BuildError;
use novarocks_parser::ast::{BinaryOperator, Expr, Ident, Query};
use novarocks_sql::planning::query_execution::FrozenConnectorScanIdentity;

type Check<'a> = &'a dyn Fn() -> closed::Result<()>;

type Input = novarocks_spi::connector::ConnectorWriteInputShape;
type FieldBinding = novarocks_spi::connector::ConnectorWriteFieldBinding;

// Original enum iteration order, with no fields() Vec or deep field clone.
fn input_slices(input: &Input) -> (&[FieldBinding], &[FieldBinding]) {
    match input {
        Input::Data { fields } => (fields, &[]),
        Input::RowLineage {
            data_fields,
            row_identity_fields,
        } => (data_fields, row_identity_fields),
        Input::PositionDelete {
            identity_fields,
            partition_source_fields,
        }
        | Input::DeletionVector {
            identity_fields,
            partition_source_fields,
        } => (identity_fields, partition_source_fields),
        Input::EqualityDelete { equality_fields } => (equality_fields, &[]),
    }
}
fn input_fields(input: &Input) -> impl Iterator<Item = &FieldBinding> {
    let (first, second) = input_slices(input);
    first.iter().chain(second.iter())
}
fn check_input_order(
    input: &Input,
    route: &novarocks_spi::connector::write_stack::ConnectorWriteRouteFacts,
) -> closed::Result<usize> {
    let (first, second) = input_slices(input);
    let width = first
        .len()
        .checked_add(second.len())
        .ok_or(BuildError::ResourceExhausted)?;
    if route.input_ordinals().len() != width {
        return Err(BuildError::InvalidSource(
            "COW route input width differs from its target input",
        ));
    }
    for (ordinal, field) in route.input_ordinals().iter().zip(input_fields(input)) {
        if ordinal.token() != field.token() {
            return Err(BuildError::InvalidSource(
                "COW route token order differs from its target input",
            ));
        }
    }
    Ok(width)
}

fn selection_value_ast(
    selection: &novarocks_spi::connector::ConnectorRowMutationSelection,
    row: novarocks_spi::connector::ConnectorRowMutationSelectionOrdinal,
    field_ordinal: u32,
    declared: &novarocks_type_contract::FunctionValueType,
    check: Check<'_>,
) -> closed::Result<Expr> {
    let view = selection.locate(row).ok_or(BuildError::InvalidSource(
        "COW selection ordinal is out of bounds",
    ))?;
    let array =
        view.batch()
            .columns()
            .get(field_ordinal as usize)
            .ok_or(BuildError::InvalidSource(
                "COW selection field ordinal is out of bounds",
            ))?;
    let value = closed::checked_value(array.as_ref(), view.row_index(), &check)?;
    closed::cast(value, declared, &check)
}

fn selection_effect_ast(
    selection: &novarocks_spi::connector::ConnectorRowMutationSelection,
    row: novarocks_spi::connector::ConnectorRowMutationSelectionOrdinal,
    field_ordinal: u32,
    check: Check<'_>,
) -> closed::Result<Expr> {
    let view = selection.locate(row).ok_or(BuildError::InvalidSource(
        "COW selection ordinal is out of bounds",
    ))?;
    let array =
        view.batch()
            .columns()
            .get(field_ordinal as usize)
            .ok_or(BuildError::InvalidSource(
                "COW selection field ordinal is out of bounds",
            ))?;
    let value = closed::checked_value(array.as_ref(), view.row_index(), &check)?;
    closed::cast_effect(value, &check)
}
fn writer_type(
    binding: &FieldBinding,
) -> closed::Result<&novarocks_type_contract::FunctionValueType> {
    binding.execution_type().ok_or(BuildError::InvalidSource(
        "COW writer lacks its complete execution declaration",
    ))
}
fn signed_type<'a>(
    signed: novarocks_spi::connector::ConnectorMutationSelectionFieldRef<'a>,
) -> closed::Result<&'a novarocks_type_contract::FunctionValueType> {
    signed.execution_type().ok_or(BuildError::InvalidSource(
        "COW signed selection lacks its complete execution declaration",
    ))
}

pub(super) fn build_cow_append_query_materialized(
    selection: &novarocks_spi::connector::ConnectorRowMutationSelection,
    rows: &[novarocks_spi::connector::ConnectorRowMutationSelectionOrdinal],
    input: &novarocks_spi::connector::ConnectorWriteInputShape,
    route: &novarocks_spi::connector::write_stack::ConnectorWriteRouteFacts,
    contract: &novarocks_spi::connector::ConnectorMutationMatchContract,
    check: Check<'_>,
) -> closed::Result<Query> {
    build_cow_append_query_core(selection, rows, input, route, contract, check, true)
}

fn build_cow_append_query_core(
    selection: &novarocks_spi::connector::ConnectorRowMutationSelection,
    rows: &[novarocks_spi::connector::ConnectorRowMutationSelectionOrdinal],
    input: &novarocks_spi::connector::ConnectorWriteInputShape,
    route: &novarocks_spi::connector::write_stack::ConnectorWriteRouteFacts,
    contract: &novarocks_spi::connector::ConnectorMutationMatchContract,
    check: Check<'_>,
    materialized: bool,
) -> closed::Result<Query> {
    route
        .validate_selection_contract(input, contract)
        .map_err(|e| BuildError::OriginalSemantic(e.to_string()))?;
    let input_width = check_input_order(input, route)?;
    let mut value_rows = if materialized {
        Vec::new()
    } else {
        closed::exact_vec(rows.len())?
    };
    if !materialized {
        for row in rows {
            check()?;
            let mut values = closed::exact_vec(input_width)?;
            for (_writer, binding) in input_fields(input).zip(route.selection_bindings()) {
                let novarocks_spi::connector::write_stack::ConnectorWriteValueSource::Selection {
                token, ordinal, role: novarocks_spi::connector::write_stack::ConnectorWriteSelectionBindingRole::AfterImage,
            } = binding.source() else { return Err(BuildError::InvalidSource("COW append writer is not bound to an after-image")); };
                let signed = contract
                    .selection_field(token)
                    .ok_or(BuildError::InvalidSource(
                        "COW append carries a foreign selection token",
                    ))?;
                if signed.ordinal() != ordinal
                    || signed.role()
                        != novarocks_spi::connector::ConnectorMutationSelectionFieldRole::AfterImage
                {
                    return Err(BuildError::InvalidSource(
                        "COW append selection binding differs from its signed field",
                    ));
                }
                values.push(selection_value_ast(
                    selection,
                    *row,
                    signed.ordinal(),
                    signed_type(signed)?,
                    check,
                )?);
            }
            value_rows.push(values);
        }
    }
    let mut aliases: Vec<Ident> = closed::exact_vec(input_width)?;
    let mut projection = closed::exact_vec(input_width)?;
    for (ordinal, binding) in input_fields(input).enumerate() {
        let alias = closed::value_alias(ordinal)?;
        aliases.push(closed::ident(&alias, true)?);
        projection.push(closed::item(
            if materialized {
                closed::column("__nr_values", &alias)?
            } else {
                closed::cast(
                    closed::column("__nr_values", &alias)?,
                    writer_type(binding)?,
                    &check,
                )?
            },
            binding.field().name(),
        )?);
    }
    closed::select(
        projection,
        if materialized {
            closed::materialized_values("__nr_values", aliases)?
        } else {
            closed::values(value_rows, "__nr_values", aliases)?
        },
        None,
        None,
    )
}

#[cfg(test)]
pub(super) fn build_cow_rewrite_query_closed(
    selection: &novarocks_spi::connector::ConnectorRowMutationSelection,
    rows: &[novarocks_spi::connector::ConnectorRowMutationSelectionOrdinal],
    input: &novarocks_spi::connector::ConnectorWriteInputShape,
    route: &novarocks_spi::connector::write_stack::ConnectorWriteRouteFacts,
    source: &novarocks_spi::connector::write_stack::ConnectorWriteRewriteSource,
    contract: &novarocks_spi::connector::ConnectorMutationMatchContract,
    identity: &FrozenConnectorScanIdentity,
    check: Check<'_>,
) -> closed::Result<Query> {
    build_cow_rewrite_query_core(
        selection, rows, input, route, source, contract, identity, check, false,
    )
}

pub(super) fn build_cow_rewrite_query_materialized(
    selection: &novarocks_spi::connector::ConnectorRowMutationSelection,
    rows: &[novarocks_spi::connector::ConnectorRowMutationSelectionOrdinal],
    input: &novarocks_spi::connector::ConnectorWriteInputShape,
    route: &novarocks_spi::connector::write_stack::ConnectorWriteRouteFacts,
    source: &novarocks_spi::connector::write_stack::ConnectorWriteRewriteSource,
    contract: &novarocks_spi::connector::ConnectorMutationMatchContract,
    identity: &FrozenConnectorScanIdentity,
    check: Check<'_>,
) -> closed::Result<Query> {
    build_cow_rewrite_query_core(
        selection, rows, input, route, source, contract, identity, check, true,
    )
}

fn build_cow_rewrite_query_core(
    selection: &novarocks_spi::connector::ConnectorRowMutationSelection,
    rows: &[novarocks_spi::connector::ConnectorRowMutationSelectionOrdinal],
    input: &novarocks_spi::connector::ConnectorWriteInputShape,
    route: &novarocks_spi::connector::write_stack::ConnectorWriteRouteFacts,
    source: &novarocks_spi::connector::write_stack::ConnectorWriteRewriteSource,
    contract: &novarocks_spi::connector::ConnectorMutationMatchContract,
    identity: &FrozenConnectorScanIdentity,
    check: Check<'_>,
    materialized: bool,
) -> closed::Result<Query> {
    route
        .validate_selection_contract(input, contract)
        .map_err(|e| BuildError::OriginalSemantic(e.to_string()))?;
    let input_width = check_input_order(input, route)?;
    // Borrowed linear lookups preserve the signed token facts without allocating
    // copied HashMaps of the whole routing proof during AST construction.
    let selection_binding = |token| {
        route
            .selection_bindings()
            .iter()
            .find(|b| b.writer_token() == token)
            .ok_or(BuildError::InvalidSource(
                "COW rewrite token has no provider selection binding",
            ))
    };
    let mut inherited =
        route.selection_bindings().iter().filter(|b| {
            matches!(b.source(),
        novarocks_spi::connector::write_stack::ConnectorWriteValueSource::ProviderDerived(
            novarocks_spi::connector::write_stack::ConnectorWriteProviderDerivedValue::Inherit))
        });
    let written_version_matches = match (
        inherited.next(),
        inherited.next(),
        source.written_version_token(),
    ) {
        (None, None, None) => true,
        (Some(binding), None, Some(token)) => binding.writer_token() == token,
        _ => false,
    };
    if !written_version_matches {
        return Err(BuildError::InvalidSource(
            "COW rewrite written-version mapping differs from its frozen source",
        ));
    }
    let maximum_tokens = source
        .match_tokens()
        .len()
        .checked_add(input_width)
        .ok_or(BuildError::ResourceExhausted)?;
    // A scalar count pass makes the actual Vec capacity equal its final length.
    let is_new_after =
        |binding: &novarocks_spi::connector::ConnectorWriteFieldBinding| -> closed::Result<bool> {
            Ok(matches!(selection_binding(binding.token())?.source(), novarocks_spi::connector::write_stack::ConnectorWriteValueSource::Selection {
            role: novarocks_spi::connector::write_stack::ConnectorWriteSelectionBindingRole::AfterImage, ..
        }) && !source.match_tokens().contains(&binding.token()))
        };
    let mut token_count = source.match_tokens().len();
    for binding in input_fields(input) {
        if is_new_after(binding)? {
            token_count = token_count
                .checked_add(1)
                .ok_or(BuildError::ResourceExhausted)?;
        }
    }
    if token_count > maximum_tokens {
        return Err(BuildError::InvalidSource("COW route token cardinality"));
    }
    let mut values_tokens = closed::exact_vec(token_count)?;
    values_tokens.extend_from_slice(source.match_tokens());
    for binding in input_fields(input) {
        if is_new_after(binding)? {
            values_tokens.push(binding.token());
        }
    }
    let selection_field = |token| {
        let novarocks_spi::connector::write_stack::ConnectorWriteValueSource::Selection {
            token: selection_token,
            ordinal,
            ..
        } = selection_binding(token)?.source()
        else {
            return Err(BuildError::InvalidSource(
                "COW derived writer value has no selection field",
            ));
        };
        let signed = contract
            .selection_field(selection_token)
            .ok_or(BuildError::InvalidSource(
                "COW rewrite carries a foreign selection token",
            ))?;
        if signed.ordinal() != ordinal {
            return Err(BuildError::InvalidSource(
                "COW rewrite selection binding has a tampered ordinal",
            ));
        }
        Ok::<_, BuildError>((signed.ordinal(), signed_type(signed)?))
    };
    let width = values_tokens
        .len()
        .checked_add(2)
        .ok_or(BuildError::ResourceExhausted)?;
    let mut value_rows = if materialized {
        Vec::new()
    } else {
        closed::exact_vec(rows.len())?
    };
    if !materialized {
        for row in rows {
            check()?;
            let mut values = closed::exact_vec(width)?;
            for token in &values_tokens {
                let (ordinal, field) = selection_field(*token)?;
                values.push(selection_value_ast(selection, *row, ordinal, field, check)?);
            }
            values.push(closed::bool_value(true));
            values.push(selection_effect_ast(
                selection,
                *row,
                contract.effect_field().target_ordinal(),
                check,
            )?);
            value_rows.push(values);
        }
    }
    let mut aliases = closed::exact_vec(width)?;
    for ordinal in 0..values_tokens.len() {
        aliases.push(closed::ident(&closed::value_alias(ordinal)?, true)?);
    }
    aliases.push(closed::ident("__nr_matched", true)?);
    aliases.push(closed::ident("__nr_effect", true)?);
    let matched = || closed::column("__nr_match", "__nr_matched").map(|e| closed::is_null(e, true));
    let position = |token| {
        values_tokens
            .iter()
            .position(|t| *t == token)
            .ok_or(BuildError::InvalidSource(
                "COW after-image field has no VALUES binding",
            ))
    };
    let scan_column = |token| {
        let binding = source
            .scan_bindings()
            .iter()
            .find(|b| b.token() == token)
            .ok_or(BuildError::InvalidSource(
                "COW rewrite field has no scan binding",
            ))?;
        let field = source
            .scan_schema()
            .fields()
            .get(binding.scan_ordinal() as usize)
            .ok_or(BuildError::InvalidSource(
                "COW scan binding is outside the frozen scan schema",
            ))?;
        closed::column("__nr_scan", field.name())
    };
    let mut projection = closed::exact_vec(input_width)?;
    for binding in input_fields(input) {
        let scan = scan_column(binding.token())?;
        let expression = match selection_binding(binding.token())?.source() {
            novarocks_spi::connector::write_stack::ConnectorWriteValueSource::ProviderDerived(
                novarocks_spi::connector::write_stack::ConnectorWriteProviderDerivedValue::Inherit) => {
                if Some(binding.token()) != source.written_version_token() { return Err(BuildError::InvalidSource("COW inherited writer token differs from its frozen source")); }
                closed::case(matched()?, closed::null(), scan)?
            }
            novarocks_spi::connector::write_stack::ConnectorWriteValueSource::Selection {
                role: novarocks_spi::connector::write_stack::ConnectorWriteSelectionBindingRole::AfterImage, ..
            } => closed::case(matched()?, closed::column("__nr_match", &closed::value_alias(position(binding.token())?)?)?, scan)?,
            novarocks_spi::connector::write_stack::ConnectorWriteValueSource::Selection {
                role: novarocks_spi::connector::write_stack::ConnectorWriteSelectionBindingRole::Identity, ..
            } => scan,
        };
        projection.push(closed::item(
            if materialized {
                expression
            } else {
                closed::cast(expression, writer_type(binding)?, &check)?
            },
            binding.field().name(),
        )?);
    }
    let mut joins = None;
    for token in source.match_tokens() {
        let novarocks_spi::connector::write_stack::ConnectorWriteValueSource::Selection {
            token: selection_token,
            role:
                novarocks_spi::connector::write_stack::ConnectorWriteSelectionBindingRole::Identity,
            ..
        } = selection_binding(*token)?.source()
        else {
            return Err(BuildError::InvalidSource(
                "COW match token is not bound to a signed uniqueness identity",
            ));
        };
        if !contract.uniqueness_tokens().contains(&selection_token) {
            return Err(BuildError::InvalidSource(
                "COW match token is not bound to a signed uniqueness identity",
            ));
        }
        let equality = closed::binary(
            scan_column(*token)?,
            BinaryOperator::Equal,
            closed::column("__nr_match", &closed::value_alias(position(*token)?)?)?,
        );
        joins = Some(match joins {
            None => equality,
            Some(previous) => closed::binary(previous, BinaryOperator::And, equality),
        });
    }
    let joins = joins.ok_or(BuildError::InvalidSource(
        "COW rewrite branch carries no match key",
    ))?;
    let filter = closed::binary(
        closed::is_null(closed::column("__nr_match", "__nr_effect")?, false),
        BinaryOperator::Or,
        closed::binary(
            closed::column("__nr_match", "__nr_effect")?,
            BinaryOperator::NotEqual,
            closed::number(novarocks_spi::connector::ConnectorRowMutationEffect::Delete as i8)?,
        ),
    );
    closed::select(
        projection,
        closed::table(
            identity.catalog(),
            identity.namespace(),
            identity.table(),
            "__nr_scan",
        )?,
        Some((
            if materialized {
                closed::materialized_values("__nr_match", aliases)?
            } else {
                closed::values(value_rows, "__nr_match", aliases)?
            },
            joins,
        )),
        Some(filter),
    )
}

/// Borrow actual signed fields in exactly the relation's original column order.
/// The caller quotes the returned view Vec before this constructor.
pub(super) fn materialized_columns<'a>(
    input: &Input,
    route: &novarocks_spi::connector::write_stack::ConnectorWriteRouteFacts,
    contract: &'a novarocks_spi::connector::ConnectorMutationMatchContract,
    rewrite: Option<&novarocks_spi::connector::write_stack::ConnectorWriteRewriteSource>,
    check: Check<'_>,
) -> closed::Result<Vec<(u32, &'a novarocks_type_contract::FunctionValueType)>> {
    route
        .validate_selection_contract(input, contract)
        .map_err(|e| BuildError::OriginalSemantic(e.to_string()))?;
    check_input_order(input, route)?;
    let binding = |token| {
        route
            .selection_bindings()
            .iter()
            .find(|b| b.writer_token() == token)
            .ok_or(BuildError::InvalidSource(
                "COW compact token has no original route",
            ))
    };
    let signed = |writer_token| {
        let novarocks_spi::connector::write_stack::ConnectorWriteValueSource::Selection {
            token,
            ordinal,
            role,
        } = binding(writer_token)?.source()
        else {
            return Err(BuildError::InvalidSource(
                "COW inherited writer value cannot enter compact source",
            ));
        };
        let field = contract
            .selection_field(token)
            .ok_or(BuildError::InvalidSource(
                "COW compact source has a foreign token",
            ))?;
        if field.ordinal() != ordinal {
            return Err(BuildError::InvalidSource(
                "COW compact source ordinal differs from its signer",
            ));
        }
        if rewrite.is_none()
            && (role != novarocks_spi::connector::write_stack::ConnectorWriteSelectionBindingRole::AfterImage
                || field.role() != novarocks_spi::connector::ConnectorMutationSelectionFieldRole::AfterImage)
        {
            return Err(BuildError::InvalidSource("COW append selection differs from its signed field"));
        }
        Ok((ordinal, signed_type(field)?))
    };
    let is_after = |token| -> closed::Result<bool> {
        Ok(matches!(binding(token)?.source(), novarocks_spi::connector::write_stack::ConnectorWriteValueSource::Selection {role:novarocks_spi::connector::write_stack::ConnectorWriteSelectionBindingRole::AfterImage,..}))
    };
    let mut count = rewrite.map_or(0, |source| source.match_tokens().len());
    for field in input_fields(input) {
        check()?;
        writer_type(field)?;
        if rewrite.is_none()
            || (is_after(field.token())?
                && !rewrite
                    .expect("checked")
                    .match_tokens()
                    .contains(&field.token()))
        {
            count = count.checked_add(1).ok_or(BuildError::ResourceExhausted)?;
        }
    }
    let mut columns = closed::exact_vec(count)?;
    if let Some(source) = rewrite {
        for token in source.match_tokens() {
            check()?;
            let novarocks_spi::connector::write_stack::ConnectorWriteValueSource::Selection {
                token: selection_token,
                role:
                    novarocks_spi::connector::write_stack::ConnectorWriteSelectionBindingRole::Identity,
                ..
            } = binding(*token)?.source()
            else {
                return Err(BuildError::InvalidSource(
                    "COW match token is not bound to a signed uniqueness identity",
                ));
            };
            if !contract.uniqueness_tokens().contains(&selection_token) {
                return Err(BuildError::InvalidSource(
                    "COW match token is not bound to a signed uniqueness identity",
                ));
            }
            columns.push(signed(*token)?);
        }
    }
    for field in input_fields(input) {
        check()?;
        if rewrite.is_none()
            || (is_after(field.token())?
                && !rewrite
                    .expect("checked")
                    .match_tokens()
                    .contains(&field.token()))
        {
            columns.push(signed(field.token())?);
        }
    }
    if columns.len() != count {
        return Err(BuildError::InvalidSource(
            "COW compact source column count differs from its route",
        ));
    }
    Ok(columns)
}
