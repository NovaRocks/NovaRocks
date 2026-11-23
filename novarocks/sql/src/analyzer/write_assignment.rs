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

//! Store assignment consumes the fully analyzed source declaration.
//! The selected assignment authors V. Writer normalization consumes the
//! original provider's checked V-to-W projection at its own boundary.

use crate::analysis::TypedExpr;
use crate::analyze_error::AnalyzeError;
use crate::compiler::{SqlCompileControl, SqlCompileError, SqlWriteAssignmentSite};
use novarocks_parser::Span;
use novarocks_type_contract::{CheckedWriteInputProjection, CompilePhase, PureCompileControl};

impl super::AnalyzerContext<'_> {
    pub(super) fn bind_write_assignment_value(
        &self,
        source: TypedExpr,
        site: &SqlWriteAssignmentSite,
        span: Span,
    ) -> Result<TypedExpr, AnalyzeError> {
        bind_write_assignment_value(
            source,
            site,
            span,
            self.function_catalog,
            &self.sql_semantics,
            self.constant_policy,
            self.control,
        )
    }
}

pub(crate) fn bind_write_assignment_value(
    source: TypedExpr,
    site: &SqlWriteAssignmentSite,
    span: Span,
    catalog: &dyn crate::compiler::SqlFunctionCatalog,
    settings: &crate::sql_mode::SqlSemanticSettings,
    constant_policy: novarocks_functions::ConstantPolicy,
    control: &SqlCompileControl,
) -> Result<TypedExpr, AnalyzeError> {
    let related = |source: &TypedExpr| {
        CheckedWriteInputProjection::try_new_observed(
            source.value_type.typed_binding().borrowed(),
            site.input().typed_binding().borrowed(),
            site.storage().map(|ty| ty.typed_binding().borrowed()),
            || {
                control
                    .checkpoint(CompilePhase::Validate, 1)
                    .map_err(AnalyzeError::control)
            },
        )
        .map(|proof| proof.is_some())
    };
    // Preserve Fixed(m), actual NULL declarations and original constant
    // ownership whenever only the licensed write projection remains.
    if related(&source)? {
        return Ok(source);
    }
    // Exact writer declarations cannot smuggle a byte-width conversion
    // through ordinary safe CAST, which would turn bad widths into NULL.
    let mut fixed_target = false;
    novarocks_type_contract::visit_value_type_observed(site.input().value_type(), |event| {
        control
            .checkpoint(CompilePhase::Validate, 1)
            .map_err(AnalyzeError::control)?;
        if let novarocks_type_contract::ValueSemanticVisit::Node(value) = event {
            fixed_target |= matches!(
                value.kind(),
                novarocks_type_contract::ValueKind::Scalar(
                    arrow::datatypes::DataType::FixedSizeBinary(_)
                )
            );
        }
        Ok::<_, AnalyzeError>(())
    })?;
    if fixed_target {
        return Err(AnalyzeError::type_mismatch(
            "fixed write assignment requires its explicit signed byte projection",
            span,
        ));
    }
    if let Some(storage) = site.storage() {
        let projection = CheckedWriteInputProjection::try_new_observed(
            storage.typed_binding().borrowed(),
            site.input().typed_binding().borrowed(),
            Some(storage.typed_binding().borrowed()),
            || {
                control
                    .checkpoint(CompilePhase::Validate, 1)
                    .map_err(AnalyzeError::control)
            },
        )?
        .ok_or_else(|| AnalyzeError::internal("validated write declaration lost its projection"))?;
        if pending_fixed_projection(
            source.value_type.typed_binding().borrowed(),
            projection,
            control,
        )? {
            return Err(AnalyzeError::type_mismatch(
                "ordinary nested conversion cannot precede its licensed fixed byte projection",
                span,
            ));
        }
    }
    // Reuse the neutral declaration-widening author for a truthful SOURCE
    // conversion target. W itself retains its signed requiredness.
    // Normal literal constructors may choose tighter non-NULL promises.
    let target = super::declaration_widening::widen_declaration(site.input(), control)
        .map_err(original_analysis_error)?;
    use novarocks_functions::builtin::value_conversion::temporal_write_assignment_matches;
    let converted = if temporal_write_assignment_matches(&source.value_type, &target, control)
        .map_err(AnalyzeError::function_binding)?
    {
        bind_nested_temporal_assignment(
            source,
            target,
            catalog,
            settings,
            constant_policy,
            control,
        )?
    } else {
        let mut work =
            novarocks_type_contract::CompileCheckpoints::try_new(control, CompilePhase::Validate)
                .map_err(AnalyzeError::control)?;
        let compatible = novarocks_type_contract::trino_can_store_assign(
            source.value_type.value_type(),
            target.value_type(),
            &mut work,
        )
        .map_err(AnalyzeError::control)?;
        work.finish().map_err(AnalyzeError::control)?;
        if !compatible {
            return Err(AnalyzeError::type_mismatch(
                format!(
                    "Cannot assign {:?} to {:?}",
                    source.value_type.value_type().kind(),
                    target.value_type().kind(),
                ),
                span,
            ));
        }
        super::value_conversion::cast_with_trino_mode(
            source,
            target,
            novarocks_type_contract::TrinoCastMode::StoreAssignment,
            span,
            catalog,
            settings,
            constant_policy,
            control,
            false,
        )?
    };
    if !related(&converted)? {
        return Err(AnalyzeError::type_mismatch(
            "ordinary assignment conversion cannot satisfy the signed write projection",
            span,
        ));
    }
    Ok(converted)
}

fn bind_nested_temporal_assignment(
    source: TypedExpr,
    target: novarocks_type_contract::FunctionValueType,
    catalog: &dyn crate::compiler::SqlFunctionCatalog,
    settings: &crate::sql_mode::SqlSemanticSettings,
    constant_policy: novarocks_functions::ConstantPolicy,
    control: &SqlCompileControl,
) -> Result<TypedExpr, AnalyzeError> {
    use crate::analysis::{ExprKind, function_argument};
    use novarocks_functions::FunctionBindingRequest;
    use novarocks_functions::builtin::value_conversion as owner;
    use novarocks_type_contract::{FunctionId, FunctionOverloadId};

    let argument = function_argument(&source, constant_policy, control)
        .map_err(AnalyzeError::function_binding)?;
    let function = FunctionId::try_new(owner::VALUE_CONVERSION_FUNCTION_ID)
        .map_err(|error| AnalyzeError::internal(error.to_string()))?;
    let overload = FunctionOverloadId::try_new(owner::TEMPORAL_WRITE_ASSIGNMENT)
        .map_err(|error| AnalyzeError::internal(error.to_string()))?;
    let resolved = catalog
        .finish_scalar_binding(
            &function,
            &overload,
            FunctionBindingRequest {
                arguments: std::slice::from_ref(&argument),
                logical_argument_count: 1,
                expected_result_type: Some(&target),
            },
            control,
        )
        .map_err(AnalyzeError::function_binding)?;
    let volatility = resolved.semantics.volatility;
    let binding = crate::binding::SqlFunctionBinding::new_with_temporal_constraint(
        resolved,
        settings.sql_mode().decimal_overflow_policy(),
        target.clone(),
        target.nullable(),
    );
    Ok(TypedExpr {
        value_type: target,
        kind: ExprKind::FunctionCall {
            name: owner::VALUE_CONVERSION_NAME.into(),
            args: vec![source].into(),
            distinct: false,
            binding,
            volatility,
        },
    })
}

pub(crate) fn write_assignment_site_for_column(
    ordinal: usize,
    column: &novarocks_types::schema::ColumnDef,
    storage: Option<novarocks_type_contract::FunctionValueType>,
    control: &SqlCompileControl,
) -> Result<SqlWriteAssignmentSite, SqlCompileError> {
    let input = super::helpers::column_value_type(column, control)?;
    SqlWriteAssignmentSite::try_new(ordinal, input, storage, control)
}

impl super::AnalyzerContext<'_> {
    pub(super) fn bind_write_source_query(
        &self,
        query: crate::analysis::ResolvedQuery,
        assignments: &crate::compiler::SqlWriteAssignments,
        span: Span,
    ) -> Result<crate::analysis::ResolvedQuery, AnalyzeError> {
        use crate::analysis::{
            AnalyzedOutputField, AnalyzedProjectItem, ExprKind, QueryBody, Relation, ResolvedQuery,
            ResolvedSelect,
        };
        assignments
            .validate_output_width(query.output_columns.len(), self.control)
            .map_err(original_analysis_error)?;
        let mut original = Vec::new();
        original
            .try_reserve_exact(query.output_columns.len())
            .map_err(assignment_allocation)?;
        for field in &query.output_columns {
            self.check_control()?;
            original.push(field.clone());
        }
        let mut projection = Vec::new();
        let mut output_columns = Vec::new();
        projection
            .try_reserve_exact(original.len())
            .map_err(assignment_allocation)?;
        output_columns
            .try_reserve_exact(original.len())
            .map_err(assignment_allocation)?;
        let alias = "__nr_write_source";
        for (ordinal, field) in original.iter().enumerate() {
            self.check_control()?;
            let source = TypedExpr {
                kind: ExprKind::ColumnRef {
                    column_id: field.column_id,
                    qualifier: Some(alias.to_owned()),
                    column: field.name.clone(),
                },
                value_type: field.value_type.clone(),
            };
            let site = assignments
                .at_observed(ordinal, || {
                    self.control.checkpoint(CompilePhase::Validate, 1)
                })
                .map_err(AnalyzeError::control)?;
            let value = match site {
                Some(site) => self.bind_write_assignment_value(
                    source,
                    site,
                    site.source_span().unwrap_or(span),
                )?,
                None => source,
            };
            let column_id = if let ExprKind::ColumnRef { column_id, .. } = &value.kind {
                *column_id
            } else {
                let id = self.alloc_column_id(None, field.name.clone(), value.value_type.clone());
                let logical = self.logical_output_type(None, &value, &self.new_scope())?;
                self.factory.borrow_mut().set_logical_type(id, logical);
                id
            };
            output_columns.push(AnalyzedOutputField {
                column_id,
                name: field.name.clone(),
                field_name: field.field_name.clone(),
                value_type: value.value_type.clone(),
                is_internal: field.is_internal,
            });
            projection.push(AnalyzedProjectItem {
                expr: value,
                output_name: field.name.clone(),
                field_name: field.field_name.clone(),
                output_column_id: column_id,
            });
        }
        Ok(ResolvedQuery {
            body: QueryBody::Select(ResolvedSelect {
                from: Some(Relation::Subquery {
                    query: novarocks_parser::ast::AstBox::new(query),
                    alias: alias.into(),
                    output_columns: original,
                }),
                filter: None,
                group_by: Vec::new(),
                having: None,
                projection,
                has_aggregation: false,
                distinct: false,
                repeat: None,
                apply_specs: (Vec::new()).into(),
                predicate_apply_specs: (Vec::new()).into(),
            }),
            order_by: Vec::new(),
            limit: None,
            offset: None,
            output_columns,
            local_cte_ids: Vec::new(),
        })
    }
}

// Walk only children already authorized by the checked T/W proof. This is not
// another logical/layout matcher or a new nested cast implementation.
fn pending_fixed_projection(
    source: novarocks_type_contract::BorrowedTypedValueBinding<'_>,
    projection: CheckedWriteInputProjection<'_>,
    control: &crate::compiler::SqlCompileControl,
) -> Result<bool, AnalyzeError> {
    control
        .checkpoint(CompilePhase::Validate, 1)
        .map_err(AnalyzeError::control)?;
    if matches!(
        projection.leaf_projection(),
        novarocks_type_contract::WriteInputLeafProjection::FixedBytesToBinary { .. }
    ) && matches!(
        source.data_type(),
        arrow::datatypes::DataType::FixedSizeBinary(_)
    ) {
        return Ok(true);
    }
    let mut ordinal = 0;
    while let Some(child) = projection.child_at(ordinal) {
        if let Some(source) = source.child_at(ordinal)
            && pending_fixed_projection(source, child, control)?
        {
            return Ok(true);
        }
        ordinal += 1;
    }
    Ok(false)
}

fn assignment_allocation(cause: std::collections::TryReserveError) -> AnalyzeError {
    AnalyzeError::domain_source(
        novarocks_functions::allocation_failure::AllocationRefusal::new(
            "typed write assignment projection",
            cause,
        ),
    )
}

fn original_analysis_error(error: SqlCompileError) -> AnalyzeError {
    use novarocks_type_contract::CompileControlError;
    match error {
        SqlCompileError::Cancelled => AnalyzeError::control(CompileControlError::Cancelled),
        SqlCompileError::DeadlineExceeded => {
            AnalyzeError::control(CompileControlError::DeadlineExceeded)
        }
        SqlCompileError::ResourceExhausted => {
            AnalyzeError::control(CompileControlError::ResourceExhausted)
        }
        SqlCompileError::Analyze(error) => error,
        SqlCompileError::CompilationSource(source) => AnalyzeError::shared_domain_source(source),
        SqlCompileError::InvalidRequest(detail) | SqlCompileError::Compilation(detail) => {
            AnalyzeError::internal(detail)
        }
    }
}
