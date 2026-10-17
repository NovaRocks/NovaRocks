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

//! The catalog table author consumes the same validated formal source domains
//! as the read author. Format annotations contribute no execution semantics.
use crate::iceberg::spec::{Schema, TableMetadata, Type};
use crate::typed_read::IcebergMetadataColumn;
use arrow::datatypes::{Field, Schema as ArrowSchema};
use novarocks_connector_contract::{
    ConnectorError, ConnectorErrorKind, ConnectorReadExecutionSchema, PureProviderCompileError,
};
use novarocks_spi::connector::read_stack::ConnectorReadSchemaCreation;
type Failure = PureProviderCompileError<ConnectorError>;
fn bad() -> Failure {
    ConnectorError::new(
        ConnectorErrorKind::CorruptData,
        "Iceberg catalog metadata column has no explicit author",
    )
    .into()
}
pub(crate) fn table(
    metadata: &TableMetadata,
    declaration: &Schema,
    pseudo_columns: &[String],
    creation: &mut ConnectorReadSchemaCreation<'_>,
) -> Result<ConnectorReadExecutionSchema, Failure> {
    table_with_metadata_root_policy(
        metadata,
        declaration,
        pseudo_columns,
        MetadataRootPolicy::Ordinary,
        creation,
    )
}

/// The exact frozen COW source requires every metadata occurrence. Its loaded
/// data declaration remains unchanged; this is not a writer target widening.
pub(crate) fn frozen_row_mutation_table(
    metadata: &TableMetadata,
    declaration: &Schema,
    pseudo_columns: &[String],
    creation: &mut ConnectorReadSchemaCreation<'_>,
) -> Result<ConnectorReadExecutionSchema, Failure> {
    table_with_metadata_root_policy(
        metadata,
        declaration,
        pseudo_columns,
        MetadataRootPolicy::FrozenRowMutation,
        creation,
    )
}

#[derive(Clone, Copy)]
enum MetadataRootPolicy {
    Ordinary,
    FrozenRowMutation,
}

fn table_with_metadata_root_policy(
    metadata: &TableMetadata,
    declaration: &Schema,
    pseudo_columns: &[String],
    metadata_roots: MetadataRootPolicy,
    creation: &mut ConnectorReadSchemaCreation<'_>,
) -> Result<ConnectorReadExecutionSchema, Failure> {
    creation.work(1)?;
    let integers = crate::scalar_integer_domain::of_schema(
        declaration,
        &crate::scalar_integer_domain::metadata_declarations(metadata)?,
    )?;
    let nominals = super::nominal_declaration::from_metadata(metadata)?;
    creation.work(1)?;
    let count = declaration
        .as_struct()
        .fields()
        .len()
        .checked_add(pseudo_columns.len())
        .ok_or_else(bad)?;
    if count > novarocks_connector_contract::MAX_CONNECTOR_RECIPE_COLUMNS {
        return Err(bad());
    }
    let mut fields = creation.reserve(count)?;
    let mut bindings = creation.reserve(count)?;
    for field in declaration.as_struct().fields() {
        let value = super::typed_value::from_field(
            field,
            !field.required,
            &nominals,
            integers.get(&field.id).copied(),
            creation,
        )?
        .binding;
        let name = creation.text(&field.name)?;
        creation.work(1)?;
        fields.push(creation.arc(Field::new(
            name,
            value.data_type().clone(),
            value.nullable(),
        ))?);
        bindings.push(value);
    }
    for name in pseudo_columns {
        creation.work(name.len() + 1)?;
        let mut found = None;
        for column in crate::typed_read::ALWAYS_BOUND_METADATA_COLUMNS
            .into_iter()
            .chain(crate::typed_read::ROW_LINEAGE_METADATA_COLUMNS)
        {
            creation.work(1)?;
            if column.column_name() == name {
                found = Some(column);
                break;
            }
        }
        let column: IcebergMetadataColumn = found.ok_or_else(bad)?;
        let nullable = match metadata_roots {
            MetadataRootPolicy::Ordinary => column.nullable(),
            MetadataRootPolicy::FrozenRowMutation => false,
        };
        let value = metadata_column_with_nullability(column, nullable, creation)?;
        let name = creation.text(name)?;
        fields.push(creation.arc(Field::new(
            name,
            value.data_type().clone(),
            value.nullable(),
        ))?);
        bindings.push(value);
    }
    let fields = creation.arrow_fields(fields)?;
    let schema = creation.arc(ArrowSchema::new(fields))?;
    let storage = creation.storage();
    let (request, work) = creation.observers();
    ConnectorReadExecutionSchema::try_new_observed(
        schema,
        bindings.into_boxed_slice(),
        storage,
        |layout| request(layout).map_err(Failure::Control),
        |units| work(units).map_err(Failure::Control),
    )
}

/// Static format roles own their complete declaration independently of any
/// current or historical data schema and its field-ID scalar domains.
pub(crate) fn metadata_column(
    column: IcebergMetadataColumn,
    creation: &mut ConnectorReadSchemaCreation<'_>,
) -> Result<novarocks_type_contract::FunctionValueType, Failure> {
    metadata_column_with_nullability(column, column.nullable(), creation)
}

fn metadata_column_with_nullability(
    column: IcebergMetadataColumn,
    nullable: bool,
    creation: &mut ConnectorReadSchemaCreation<'_>,
) -> Result<novarocks_type_contract::FunctionValueType, Failure> {
    let ty = Type::Primitive(column.declared_type());
    Ok(super::typed_value::from_declaration(&ty, nullable, None, None, creation)?.binding)
}
