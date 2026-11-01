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

//! Generic terminal connector writer boundary.
//!
//! This module deliberately contains only Arrow and SQL planner facts.  The
//! application resolves a [`SqlWritePlanInput`] through its request-local
//! binding token and attaches a concrete connector writer after placement.

use std::sync::Arc;

use arrow::datatypes::{Schema, SchemaRef};
use novarocks_types::schema::ColumnDef;

use crate::planner::distributed::write::contract::SqlWritePlanInput;

/// The writer input schema, one field per input column.
///
/// Fields contain only the declared normalized value layout and NULL law.
/// Statistics borrow the same original input columns' complete declarations;
/// this representation never carries logical identity in annotations.
pub(crate) fn target_schema_from_sql_write_plan_input(
    sink: &SqlWritePlanInput,
    control: &dyn novarocks_type_contract::PureCompileControl,
) -> Result<SchemaRef, crate::compiler::SqlCompileError> {
    let fields = sink
        .contract
        .input_columns
        .iter()
        .map(|column| writer_input_field(column, control))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(Arc::new(Schema::new(fields)))
}

/// Materialize one writer input column as the Arrow field its declared value
/// type authors.
fn writer_input_field(
    column: &ColumnDef,
    control: &dyn novarocks_type_contract::PureCompileControl,
) -> Result<arrow::datatypes::Field, crate::compiler::SqlCompileError> {
    use super::auxiliary::WriterInputTypeError;
    control.checkpoint(novarocks_type_contract::CompilePhase::Validate, 0)?;
    let value = column
        .declared_value_type_observed::<WriterInputTypeError>(|| {
            control
                .checkpoint(novarocks_type_contract::CompilePhase::Validate, 1)
                .map_err(WriterInputTypeError::Control)
        })
        .map_err(|error| match error {
            WriterInputTypeError::Control(error) => crate::compiler::SqlCompileError::from(error),
            WriterInputTypeError::Declaration(error) => {
                crate::compiler::SqlCompileError::Compilation(format!(
                    "write input column `{}`: {error}",
                    column.name
                ))
            }
        })?;
    control.checkpoint(novarocks_type_contract::CompilePhase::Validate, 0)?;
    Ok(arrow::datatypes::Field::new(
        column.name.clone(),
        value.data_type().clone(),
        value.nullable(),
    ))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::datatypes::{DataType, Field, Schema};
    use novarocks_connector_iceberg_functions::{
        ICEBERG_THETA_AGGREGATE_NAME, iceberg_theta_registration,
    };
    use novarocks_functions::EngineFunctionCatalogBuilder;
    use novarocks_spi::connector::write_stack::WriteTargetOrdinal;
    use novarocks_spi::connector::{
        StatisticsArtifactIdentity, StatisticsRequiredAggregation, StatisticsScanColumn,
    };
    use novarocks_type_contract::{DecimalOverflowPolicy, FunctionValueType, ValueLogicalType};
    use novarocks_types::schema::{ColumnDef, SqlType};

    use super::target_schema_from_sql_write_plan_input;
    use crate::compiler::SqlFunctionCatalog;
    use crate::planner::distributed::write::auxiliary::{
        WriterStatisticsTargetInput, plan_writer_statistics,
    };
    use crate::planner::distributed::write::contract::ConnectorWriteInputBinding;
    use crate::planner::distributed::write::contract::test_support::simple_sql_write_plan_input;

    fn column(
        name: &str,
        data_type: DataType,
        nullable: bool,
        logical_type: Option<SqlType>,
    ) -> ColumnDef {
        ColumnDef {
            name: name.to_string(),
            data_type,
            nullable,
            write_default: None,
            logical_type,
        }
    }

    fn sink_with_input_columns(
        columns: Vec<ColumnDef>,
    ) -> crate::planner::distributed::write::contract::SqlWritePlanInput {
        let mut sink = simple_sql_write_plan_input(ConnectorWriteInputBinding::RootOutputByOrdinal);
        sink.contract.input_columns = columns;
        sink
    }

    fn theta_catalog() -> Arc<dyn SqlFunctionCatalog> {
        let registration = iceberg_theta_registration().expect("theta registration");
        let mut builder = EngineFunctionCatalogBuilder::new();
        builder
            .register(registration.definition().clone())
            .expect("register theta");
        Arc::new(builder.seal_bound().expect("seal catalog"))
    }

    fn largeint_pin(ordinal: usize, name: &str) -> StatisticsRequiredAggregation {
        StatisticsRequiredAggregation::try_new(
            StatisticsScanColumn::try_new(
                ordinal,
                name,
                FunctionValueType::try_with_logical_type(
                    DataType::FixedSizeBinary(16),
                    false,
                    ValueLogicalType::LargeInt,
                )
                .expect("LARGEINT value type"),
            )
            .expect("pinned input"),
            ICEBERG_THETA_AGGREGATE_NAME,
            StatisticsArtifactIdentity::try_new(vec![6], "test-largeint-pin-v1")
                .expect("artifact identity"),
        )
        .expect("requirement")
    }

    fn plan_statistics(
        schema: &Schema,
        columns: &[ColumnDef],
        requirements: &[StatisticsRequiredAggregation],
    ) -> Result<(), String> {
        plan_writer_statistics(
            &[WriterStatisticsTargetInput {
                target: WriteTargetOrdinal::try_new(0).expect("target"),
                input_schema: schema,
                input_columns: columns,
                requirements,
            }],
            theta_catalog().as_ref(),
            DecimalOverflowPolicy::OutputNull,
            crate::constant::test_constant_policy(),
            &crate::compiler::SqlCompileControl::unbounded(),
        )
        .map(|_| ())
        .map_err(|error| error.to_string())
    }

    /// Complete root domains remain on their original declarations while
    /// execution representation fields carry no annotations.
    #[test]
    fn writer_input_schema_keeps_root_logical_identities_and_plain_carriers() {
        let sink = sink_with_input_columns(vec![
            column("id", DataType::Int64, false, None),
            column(
                "big",
                DataType::FixedSizeBinary(16),
                true,
                Some(SqlType::LargeInt),
            ),
            column("h", DataType::Binary, true, Some(SqlType::Hll)),
            column("b", DataType::Binary, true, Some(SqlType::Bitmap)),
            column(
                "u",
                DataType::FixedSizeBinary(16),
                true,
                Some(SqlType::Uuid),
            ),
            column("v", DataType::LargeBinary, true, Some(SqlType::Variant)),
        ]);
        let schema = target_schema_from_sql_write_plan_input(
            &sink,
            &crate::compiler::SqlCompileControl::unbounded(),
        )
        .expect("writer input schema");
        assert_eq!(schema.field(0), &Field::new("id", DataType::Int64, false));
        for (ordinal, expected) in [
            (1, ValueLogicalType::LargeInt),
            (2, ValueLogicalType::Hll),
            (3, ValueLogicalType::Bitmap),
            (4, ValueLogicalType::Uuid),
            (5, ValueLogicalType::Variant),
        ] {
            let field = schema.field(ordinal);
            let value_type = sink.contract.input_columns[ordinal]
                .declared_value_type()
                .expect("complete declared value type");
            assert!(field.metadata().as_arc().is_none());
            assert_eq!(value_type.logical_type(), expected, "{}", field.name());
            assert_eq!(
                field.data_type(),
                &sink.contract.input_columns[ordinal].data_type
            );
        }
    }

    /// A LARGEINT column pinned by a collect-on-write statistics requirement
    /// plans against the writer input: the pin names the column's own value
    /// type, LargeInt included. A writer input that dropped the declaration
    /// is exactly what the pin check refuses.
    #[test]
    fn largeint_statistics_pin_plans_against_the_writer_input() {
        let sink = sink_with_input_columns(vec![
            column("id", DataType::Int64, false, None),
            column(
                "big",
                DataType::FixedSizeBinary(16),
                false,
                Some(SqlType::LargeInt),
            ),
        ]);
        let schema = target_schema_from_sql_write_plan_input(
            &sink,
            &crate::compiler::SqlCompileControl::unbounded(),
        )
        .expect("writer input schema");
        let requirements = vec![largeint_pin(1, "big")];
        plan_statistics(&schema, &sink.contract.input_columns, &requirements)
            .expect("LARGEINT pin plans");

        let undeclared = Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("big", DataType::FixedSizeBinary(16), false),
        ]);
        let error = plan_statistics(
            &undeclared,
            &[
                column("id", DataType::Int64, false, None),
                column("big", DataType::FixedSizeBinary(16), false, None),
            ],
            &requirements,
        )
        .expect_err("an undeclared carrier contradicts the pin");
        assert!(
            error.contains("does not match the pinned column"),
            "{error}"
        );
    }

    /// A declaration the carrier cannot hold is refused instead of being
    /// materialized as some other field.
    #[test]
    fn writer_input_schema_refuses_a_declaration_its_carrier_cannot_hold() {
        let sink = sink_with_input_columns(vec![column(
            "big",
            DataType::Int64,
            false,
            Some(SqlType::LargeInt),
        )]);
        let error = target_schema_from_sql_write_plan_input(
            &sink,
            &crate::compiler::SqlCompileControl::unbounded(),
        )
        .expect_err("LARGEINT over Int64 is refused");
        assert!(
            error.to_string().contains("write input column `big`"),
            "{error}"
        );
    }
}
