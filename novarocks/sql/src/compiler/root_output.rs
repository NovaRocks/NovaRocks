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

//! Exact root occurrence provenance carried through the owned compiler phases.

use novarocks_physical_plan::ResultValueDomain;
use novarocks_result_contract::{ScalarField, ScalarSchema};
use novarocks_types::schema::SqlType;

use crate::column_id::{ColumnId, ColumnRefFactory};
use crate::common::OutputColumn;

#[derive(Debug)]
pub(crate) struct RootOutputSemantics {
    occurrences: Box<[(ColumnId, ResultValueDomain)]>,
    scalar_field: Option<ScalarField>,
    private_persistence_domain: bool,
}

impl RootOutputSemantics {
    pub(crate) fn capture(
        columns: &[OutputColumn],
        factory: &ColumnRefFactory,
    ) -> Result<Self, String> {
        if columns.len() > novarocks_result_contract::RootProfileV1::MAX_COLUMNS {
            return Err("root output occurrence count exceeds its admitted profile".into());
        }
        // There can be only one scalar output. Borrow the full declared type
        // for preflight, then construct one bounded owned field; do not clone
        // a nested declaration once per repeated output occurrence.
        let scalar_field = match columns {
            [column]
                if column.value_type.logical_type
                    == novarocks_type_contract::ValueLogicalType::Uuid =>
            {
                None
            }
            [column] => super::root_scalar_type::scalar_field(
                &column.value_type.data_type,
                column.value_type.nullable,
                factory.borrowed_logical_type(column.column_id),
            )
            .ok(),
            _ => None,
        };
        let private_persistence_domain =
            columns
                .iter()
                .filter(|column| !column.is_internal)
                .any(|column| {
                    factory
                        .borrowed_logical_type(column.column_id)
                        .is_some_and(private_persistence_domain)
                });
        Ok(Self {
            scalar_field,
            private_persistence_domain,
            occurrences: columns
                .iter()
                .map(|column| {
                    let domain = match factory.borrowed_logical_type(column.column_id) {
                        Some(SqlType::Json) => ResultValueDomain::Json,
                        Some(SqlType::Variant) => ResultValueDomain::Variant,
                        Some(SqlType::Hll) => ResultValueDomain::Hll,
                        Some(SqlType::Bitmap) => ResultValueDomain::Bitmap,
                        Some(SqlType::Object) => ResultValueDomain::Object,
                        Some(SqlType::Percentile) => ResultValueDomain::Percentile,
                        _ => ResultValueDomain::Plain,
                    };
                    (column.column_id, domain)
                })
                .collect(),
        })
    }

    /// Retain a complete declared private-domain refusal independently of the
    /// scalar schema. Multi-column and unsupported scalar shapes still carry it.
    pub(crate) fn has_private_persistence_domain(&self) -> bool {
        self.private_persistence_domain
    }

    /// Optimizer aliases may repeat one value. They may not reorder or replace
    /// its semantic identity without an explicit compiler handoff.
    pub(crate) fn domains(
        &self,
        columns: &[OutputColumn],
    ) -> Result<Vec<ResultValueDomain>, String> {
        if self.occurrences.len() != columns.len() {
            return Err("root semantic occurrence count changed during optimization".into());
        }
        self.occurrences
            .iter()
            .zip(columns)
            .enumerate()
            .map(|(ordinal, ((id, domain), column))| {
                if *id != column.column_id || !domain.matches_storage(&column.value_type.data_type)
                {
                    return Err(format!(
                        "root semantic occurrence changed at ordinal {ordinal}"
                    ));
                }
                Ok(*domain)
            })
            .collect()
    }

    pub(crate) fn scalar_schema(
        self,
        types: &[novarocks_physical_plan::ValueType],
    ) -> Option<ScalarSchema> {
        let [ty] = types else {
            return None;
        };
        let mut field = self.scalar_field?;
        // Nullability is the final lowered value's fact. Move the one owned
        // type tree into the result port; no declaration or tree clone here.
        field.nullable = ty.nullable;
        if !novarocks_type_contract::result_scalar_type::scalar_field_matches_storage(
            &field,
            &ty.data_type,
            ty.nullable,
        ) || !super::root_scalar_type::nested_domains_match(&field, &ty.data_type)
        {
            return None;
        }
        ScalarSchema::try_new(field).ok()
    }
}

fn private_persistence_domain(logical: &SqlType) -> bool {
    match logical {
        SqlType::Object | SqlType::Percentile => true,
        SqlType::Array(item) => private_persistence_domain(item),
        SqlType::Map(key, value) => {
            private_persistence_domain(key) || private_persistence_domain(value)
        }
        SqlType::Struct(fields) => fields
            .iter()
            .any(|(_, field)| private_persistence_domain(field)),
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::compiler::completion::SqlCompletedPlan;
    use arrow::datatypes::{DataType as D, Field};
    use novarocks_physical_plan::ValueType;
    use novarocks_types::logical::{LogicalType, field_with_logical_type};
    use std::sync::Arc;

    fn declared_map(marked: bool) -> (ColumnRefFactory, OutputColumn) {
        let mut value = Field::new("value", D::Binary, true);
        if marked {
            value = field_with_logical_type(value, LogicalType::Hll);
        }
        let data_type = D::Map(
            Arc::new(Field::new(
                "entries",
                D::Struct(vec![Arc::new(Field::new("key", D::Utf8, true)), Arc::new(value)].into()),
                false,
            )),
            false,
        );
        let mut factory = ColumnRefFactory::new();
        let value_type = novarocks_type_contract::FunctionValueType::new(data_type.clone(), false);
        let id = factory.create(None, "m".into(), value_type.clone());
        factory.set_logical_type(
            id,
            Some(SqlType::Map(
                Box::new(SqlType::String),
                Box::new(SqlType::Hll),
            )),
        );
        (
            factory,
            OutputColumn {
                column_id: id,
                name: "m".into(),
                value_type,
                is_internal: false,
            },
        )
    }

    fn completed(column: OutputColumn, semantics: RootOutputSemantics) -> SqlCompletedPlan {
        use crate::planner::physical::{PhysicalPlanKind, PhysicalPlanNode};
        let physical = PhysicalPlanNode {
            kind: PhysicalPlanKind::Values(crate::planner::payload::PlanValuesNode {
                rows: vec![],
                columns: vec![column.clone()],
            }),
            children: vec![],
            output_columns: vec![column],
            stats: crate::planner::physical::PhysicalPlanStats {
                output_row_count: 0.0,
                row_count_confidence: crate::planner::physical::PlannerConfidence::Exact,
                column_statistics: Default::default(),
                cost_estimate: None,
                broadcast_decision: None,
            },
            probe_runtime_filters: vec![],
        };
        let builder =
            crate::planner::distributed::build::lower_final_physical_plan_with_root_semantics(
                &physical,
                novarocks_physical_plan::PlanVersionId::try_new([39; 16]).unwrap(),
                novarocks_physical_plan::PipelineDopDomain {
                    min: 1,
                    max: 1,
                    requires_power_of_two: false,
                },
                None,
                semantics,
                crate::functions::test_function_catalog_snapshot(),
                false,
                crate::constant::test_constant_policy(),
                crate::compiler::SqlPhysicalEmissionMode::OriginalNativeV1,
                &crate::compiler::SqlCompileControl::unbounded(),
            )
            .unwrap();
        let request = crate::compiler::completion::SqlCompileRequest::pending(
            crate::compiler::completion::CompilerStep::ready(
                novarocks_physical_plan::PlanVersionId::try_new([39; 16]).unwrap(),
                builder,
                crate::compiler::SqlDisplayIntent::Execute,
                [],
            ),
            crate::compiler::DEFAULT_COMPLETION_LIMITS,
        );
        let crate::compiler::SqlCompileProgress::Complete(completed) =
            crate::compiler::SqlCompiler::start(
                request,
                &crate::compiler::SqlCompileControl::unbounded(),
            )
            .unwrap()
        else {
            panic!("unexpected observation");
        };
        completed
    }

    #[test]
    fn complete_uuid_without_a_legacy_declaration_never_captures_largeint_proof() {
        use novarocks_type_contract::ValueLogicalType;
        for logical in [
            ValueLogicalType::Uuid,
            ValueLogicalType::LargeInt,
            ValueLogicalType::Physical,
        ] {
            let mut factory = ColumnRefFactory::new();
            let mut ty = ValueType::new(D::FixedSizeBinary(16), true);
            ty.logical_type = logical;
            let id = factory.create(None, "value".into(), ty.clone());
            assert!(factory.borrowed_logical_type(id).is_none());
            let column = OutputColumn {
                column_id: id,
                name: "value".into(),
                value_type: ty,
                is_internal: false,
            };
            let captured = RootOutputSemantics::capture(&[column], &factory).unwrap();
            if logical == ValueLogicalType::Uuid {
                assert!(captured.scalar_field.is_none());
            } else {
                assert!(matches!(
                    captured.scalar_field.unwrap().value_type,
                    novarocks_result_contract::ScalarValueType::LargeInt
                ));
            }
        }
    }

    #[test]
    fn m07_scalar_complete_nested_declaration_is_not_lost_as_plain_storage() {
        for marked in [false, true] {
            let (factory, column) = declared_map(marked);
            let semantics =
                RootOutputSemantics::capture(std::slice::from_ref(&column), &factory).unwrap();
            let completed = completed(column, semantics);
            assert_eq!(completed.scalar_schema().is_ok(), marked);
            if marked {
                let schema = completed.scalar_schema().unwrap();
                let novarocks_result_contract::ScalarValueType::Map { key, value } =
                    &schema.field().value_type
                else {
                    panic!("expected Map")
                };
                assert!(key.nullable);
                assert_eq!(
                    value.value_type,
                    novarocks_result_contract::ScalarValueType::Opaque(
                        novarocks_result_contract::ScalarOpaqueType::Hll
                    )
                );
            }
        }
    }

    #[test]
    fn m07_scalar_repeated_nested_occurrences_do_not_clone_a_declaration_tree() {
        let (factory, column) = declared_map(true);
        let repeated = vec![column.clone(); novarocks_result_contract::RootProfileV1::MAX_COLUMNS];
        let semantics = RootOutputSemantics::capture(&repeated, &factory).unwrap();
        assert!(semantics.scalar_field.is_none());
        assert_eq!(semantics.occurrences.len(), repeated.len());
        assert!(
            semantics
                .scalar_schema(&vec![
                    ValueType::new(
                        column.value_type.data_type.clone(),
                        true
                    );
                    2
                ])
                .is_none()
        );
        let excessive = vec![column; novarocks_result_contract::RootProfileV1::MAX_COLUMNS + 1];
        assert!(RootOutputSemantics::capture(&excessive, &factory).is_err());
    }

    #[test]
    fn m07_scalar_final_carrier_must_keep_the_captured_nested_domain() {
        let (factory, original) = declared_map(true);
        for replacement in [None, Some("bitmap"), Some("unknown"), Some(" HLL ")] {
            let semantics =
                RootOutputSemantics::capture(std::slice::from_ref(&original), &factory).unwrap();
            let mut value = Field::new("value", D::Binary, true);
            if let Some(marker) = replacement {
                value = value.with_metadata(
                    [(
                        novarocks_types::logical::NR_LOGICAL_TYPE_KEY.to_owned(),
                        marker.to_owned(),
                    )]
                    .into(),
                );
            }
            let final_type = D::Map(
                Arc::new(Field::new(
                    "entries",
                    D::Struct(
                        vec![Arc::new(Field::new("key", D::Utf8, true)), Arc::new(value)].into(),
                    ),
                    false,
                )),
                false,
            );
            assert_eq!(
                semantics
                    .scalar_schema(&[ValueType::new(final_type, false)])
                    .is_some(),
                replacement == Some(" HLL "),
                "{replacement:?}"
            );
        }
    }
}
