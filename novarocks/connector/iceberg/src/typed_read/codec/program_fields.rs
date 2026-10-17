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

//! Pure correspondence of private column facts and the frozen public source,
//! and the one author of the public fields that correspondence accepts.
//! Unknown public metadata remains owned by the original snapshot. This leaf
//! neither loads a file/schema nor decides file evolution/default/delete rules.

use crate::default_value::ICEBERG_INITIAL_DEFAULT_META_KEY;
use crate::iceberg::spec::{NestedField, Schema};
use crate::schema_mapping::sql_read_schema_from_iceberg;
use crate::typed_read::column_handle::parse_type;
use crate::typed_read::schema_binding::{
    IcebergMetadataColumn, annotated_read_schema, dereference_target_field, metadata_target_field,
};
use crate::typed_read::{
    ColumnIdentity, ICEBERG_CHANGE_OP_FIELD_ID, IcebergColumnHandle, IcebergRuntimeRelation,
    IcebergSystemTableType, IcebergTableExecuteProcedureHandle, IcebergTableHandle,
    REWRITE_POSITION_DELETE_OUTPUT_COLUMNS, change_op_column_handle,
};
use arrow::datatypes::{DataType, Field, FieldRef};
use novarocks_connector_contract::{ConnectorReadPublicFacts, PureProviderCompileError};
use novarocks_spi::connector::read_stack::ConnectorReadPublicSchema;
use novarocks_spi::connector::{
    ConnectorCodecError, ConnectorCodecErrorKind, ConnectorError, ConnectorErrorKind,
    ConnectorFieldPath,
};
#[cfg(test)]
use novarocks_type_contract::PureCompileControl;
use novarocks_type_contract::{
    CompileCheckpoints, CompileControlError, CompilePhase, FunctionValueType, ValueLogicalType,
};
use parquet::arrow::PARQUET_FIELD_ID_META_KEY;
use std::{collections::BTreeMap, sync::Arc};

type Failure = PureProviderCompileError<ConnectorCodecError>;
mod typed_compare;
mod typed_source;

/// Complete-input correspondence only. Public source properties/coverage and
/// unsupported relation kinds are also checked by the enclosing source owner.
/// Opaque SDK/serde/Arrow operations retain their original finite input bounds;
/// surrounding observations do not prove their internal cooperation or MEM.
pub(super) fn validate_program_columns(
    relation: &IcebergRuntimeRelation,
    columns: &[IcebergColumnHandle],
    public: &ConnectorReadPublicFacts,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), Failure> {
    work.flush()?;
    let result = (|| {
        let same_count = columns.len() == public.schema().fields().len();
        work.step()?;
        if !same_count {
            return Err(invalid("private/public read column count differs"));
        }
        if public.execution_schema().is_none() || public.source_schema_facts().is_none() {
            return Err(invalid(
                "Iceberg frozen read lacks complete execution/source declarations",
            ));
        }
        match relation {
            IcebergRuntimeRelation::SystemTable(reference) => {
                // System Arrow schemas have their own exact timestamp/map/JSON
                // source. Mirror them through the same original forward author,
                // rather than applying ordinary Iceberg table storage mapping.
                for (ordinal, column) in columns.iter().enumerate() {
                    let execution = public
                        .execution_schema()
                        .ok_or_else(|| invalid("missing system execution declaration"))?;
                    let source = public
                        .source_schema_facts()
                        .ok_or_else(|| invalid("missing system source facts"))?;
                    let binding = execution
                        .binding(ordinal)
                        .ok_or_else(|| invalid("missing system value binding"))?;
                    let shape = source
                        .shape()
                        .schema()
                        .fields()
                        .get(ordinal)
                        .ok_or_else(|| invalid("missing system source occurrence"))?;
                    let field = &public.schema().fields()[ordinal];
                    crate::typed_read::system_page_source::validate_system_column_value(
                        reference.system_table_type(),
                        field,
                        binding,
                        work,
                    )
                    .map_err(|cause| match cause {
                        PureProviderCompileError::Control(cause) => Failure::Control(cause),
                        PureProviderCompileError::Provider(cause) => source_error(cause),
                    })?;
                    if !equal_text(field.name(), shape.name(), work)?
                        || field.is_nullable() != shape.is_nullable()
                    {
                        return Err(invalid("system source occurrence name/NULL differs"));
                    }
                    typed_compare::shape(field.data_type(), shape.data_type(), work)?;
                    check_system_column(
                        reference.system_table_type(),
                        column,
                        &public.schema().fields()[ordinal],
                        public.logical_types()[ordinal],
                        work,
                    )?;
                    work.step()?;
                }
                for fact in public
                    .source_schema_facts()
                    .ok_or_else(|| invalid("missing system source facts"))?
                    .field_facts()
                {
                    work.step()?;
                    if fact.field_id().is_some() || fact.initial_default().is_some() {
                        return Err(invalid(
                            "system relation has no format-assigned source ID/default",
                        ));
                    }
                }
                Ok(())
            }
            _ => visit_source_fields(
                relation,
                columns,
                work,
                &mut |ordinal, field, nullable, declaration, input_work, work| {
                    compare_public(
                        relation,
                        &columns[ordinal],
                        field,
                        nullable,
                        ordinal,
                        public,
                        declaration,
                        input_work,
                        work,
                    )
                },
            ),
        }
    })();
    if matches!(&result, Err(Failure::Control(_))) {
        return result;
    }
    work.flush()?;
    result
}

/// The public schema of a frozen read of `columns` from an ordinary relation.
///
/// This is the single Iceberg author of public read fields: it runs the exact
/// per-column source derivation that `validate_program_columns` compares a
/// frozen public field against, and publishes that field with the column's
/// frozen NULL contract. A schema published here is therefore accepted by the
/// pure read compiler for the same relation and columns, field IDs, initial
/// defaults and nested domains included. A metadata relation's fields come from
/// its frozen system schema instead; see `system_public_read_schema`.
///
/// The relation carries its exact provider-authored field-ID declarations;
/// generic Arrow annotations cannot establish a missing value domain.
pub(crate) fn public_read_schema(
    relation: &IcebergRuntimeRelation,
    columns: &[&IcebergColumnHandle],
    creation: &mut novarocks_spi::connector::read_stack::ConnectorReadSchemaCreation<'_>,
) -> Result<ConnectorReadPublicSchema, PureProviderCompileError<ConnectorError>> {
    typed_source::publish(relation, columns, creation)
}

/// The public schema of a frozen read of `columns` from a metadata relation.
///
/// `system` is the relation's frozen output schema, authored by the original
/// system-table field author from the pinned table metadata the reference
/// verifies. Each column takes its field by name and is then checked through
/// the same mirror the pure read compiler applies to a frozen public field.
pub(crate) fn system_public_read_schema(
    kind: IcebergSystemTableType,
    columns: &[&IcebergColumnHandle],
    schema: &Schema,
    specs: &[&crate::iceberg::spec::PartitionSpec],
    creation: &mut novarocks_spi::connector::read_stack::ConnectorReadSchemaCreation<'_>,
) -> Result<ConnectorReadPublicSchema, PureProviderCompileError<ConnectorError>> {
    crate::typed_read::system_page_source::publish_read_schema(
        kind, columns, schema, specs, creation,
    )
}

/// Receives each column's frozen source field and NULL contract, in column
/// order, at the point the column has been proven against its frozen source.
type SourceFieldVisitor<'v> = dyn FnMut(
        usize,
        &Field,
        bool,
        Option<&NestedField>,
        usize,
        &mut CompileCheckpoints<'_>,
    ) -> Result<(), Failure>
    + 'v;

/// The per-column source derivation shared by the pure compiler and the public
/// field author, for every relation whose fields are derived from its own
/// frozen source. Metadata relations are mirrored from their system schema.
fn visit_source_fields(
    relation: &IcebergRuntimeRelation,
    columns: &[IcebergColumnHandle],
    work: &mut CompileCheckpoints<'_>,
    visit: &mut SourceFieldVisitor<'_>,
) -> Result<(), Failure> {
    match relation {
        IcebergRuntimeRelation::Table(table) => visit_table(table, columns, work, visit),
        IcebergRuntimeRelation::ChangeWindow(window) => {
            let schema = opaque_source(work, || window.parse_table_schema())?;
            let arrow = opaque_source(work, || annotated_read_schema(&schema))?;
            let index = source_index(&schema, &arrow, work)?;
            let window_columns = column_index(window.columns(), work)?;
            for (ordinal, column) in columns.iter().enumerate() {
                if column.base_field_id() == ICEBERG_CHANGE_OP_FIELD_ID {
                    let expected = opaque_source(work, change_op_column_handle)?;
                    compare_handle(column, &expected, work)?;
                    let field = projected_field(&expected, work)?;
                    visit(
                        ordinal,
                        &field,
                        column.nullable(),
                        None,
                        window.table_schema_json().len(),
                        work,
                    )?;
                } else {
                    // The frozen window owns the requested to-schema column
                    // handles, including its exact narrow-integer domains.
                    let path = observed_path(column.field_id_path(), work)?;
                    let expected = window_columns.get(&(column.base_field_id(), path)).copied();
                    work.step()?;
                    let expected = expected.ok_or_else(|| {
                        invalid("column is absent from frozen change-window columns")
                    })?;
                    compare_handle(column, expected, work)?;
                    let (field, nullable, declaration) =
                        source_column_field(column, &index, None, work)?;
                    visit(
                        ordinal,
                        &field,
                        nullable,
                        declaration,
                        window.table_schema_json().len(),
                        work,
                    )?;
                }
                work.step()?;
            }
            Ok(())
        }
        IcebergRuntimeRelation::SystemTable(_) => Err(unsupported(
            "Iceberg metadata relation fields are mirrored from their frozen system schema",
        )),
        IcebergRuntimeRelation::TableExecute(execute) => match execute.procedure_handle() {
            Some(IcebergTableExecuteProcedureHandle::Optimize(optimize)) => {
                visit_table(optimize.table_handle(), columns, work, visit)
            }
            Some(IcebergTableExecuteProcedureHandle::RewritePositionDeleteFiles(_)) => {
                for (ordinal, column) in columns.iter().enumerate() {
                    let mut expected = None;
                    for (name, metadata) in REWRITE_POSITION_DELETE_OUTPUT_COLUMNS {
                        if column.base_field_id() == metadata.field_id() {
                            expected = Some(opaque_source(work, || {
                                crate::typed_read::table_execute::rewrite_position_delete_pseudo_column(name, metadata)
                            })?);
                        }
                        work.step()?;
                    }
                    let expected = expected.ok_or_else(|| {
                        invalid("rewrite-position reader has no such output column")
                    })?;
                    compare_handle(column, &expected, work)?;
                    let field = projected_field(&expected, work)?;
                    visit(
                        ordinal,
                        &field,
                        expected.nullable(),
                        None,
                        column.type_json().len(),
                        work,
                    )?;
                    work.step()?;
                }
                Ok(())
            }
            None => Err(unsupported(
                "Iceberg table execute has no static read schema",
            )),
        },
        IcebergRuntimeRelation::TableFunction(_) | IcebergRuntimeRelation::MergeTable(_) => Err(
            unsupported("Iceberg relation does not publish final static read facts"),
        ),
    }
}

fn check_system_column(
    kind: IcebergSystemTableType,
    column: &IcebergColumnHandle,
    field: &Field,
    logical: ValueLogicalType,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), Failure> {
    let mirrored = crate::typed_read::system_page_source::system_column_field_for_compile(
        kind, column, field, logical, work,
    )
    .map_err(|failure| match failure {
        PureProviderCompileError::Control(cause) => Failure::Control(cause),
        PureProviderCompileError::Provider(error) => source_error(error),
    })?;
    let expected = opaque_source(work, || IcebergColumnHandle::base_column(&mirrored))?;
    compare_handle(column, &expected, work)
}

fn visit_table(
    table: &IcebergTableHandle,
    columns: &[IcebergColumnHandle],
    work: &mut CompileCheckpoints<'_>,
    visit: &mut SourceFieldVisitor<'_>,
) -> Result<(), Failure> {
    let schema = opaque_source(work, || table.parse_table_schema())?;
    let arrow = opaque_source(work, || annotated_read_schema(&schema))?;
    let index = source_index(&schema, &arrow, work)?;
    for (ordinal, column) in columns.iter().enumerate() {
        let (field, nullable, declaration) =
            source_column_field(column, &index, Some(table), work)?;
        visit(
            ordinal,
            &field,
            nullable,
            declaration,
            table.table_schema_json().len(),
            work,
        )?;
        work.step()?;
    }
    Ok(())
}

type SourceIndex<'a> = BTreeMap<i32, (&'a NestedField, &'a FieldRef)>;
fn source_index<'a>(
    schema: &'a Schema,
    arrow: &'a arrow::datatypes::SchemaRef,
    work: &mut CompileCheckpoints<'_>,
) -> Result<SourceIndex<'a>, Failure> {
    let mut index = BTreeMap::new();
    for (source, field) in schema.as_struct().fields().iter().zip(arrow.fields()) {
        index.insert(source.id, (source.as_ref(), field));
        work.step()?;
    }
    Ok(index)
}
/// One column's frozen source field and NULL contract, after the column is
/// proven to be exactly the handle its frozen source would author.
fn source_column_field<'a>(
    column: &IcebergColumnHandle,
    index: &SourceIndex<'a>,
    table: Option<&IcebergTableHandle>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(FieldRef, bool, Option<&'a NestedField>), Failure> {
    if let Some(metadata) = IcebergMetadataColumn::from_field_id(column.base_field_id()) {
        if let Some(source) = table.and_then(IcebergTableHandle::frozen_cow_source) {
            let expected = source
                .metadata_columns()
                .iter()
                .find(|expected| expected.base_field_id() == column.base_field_id());
            work.step()?;
            if expected != Some(column) {
                return Err(invalid(
                    "read metadata column differs from the original COW source",
                ));
            }
        }
        let field = opaque_source(work, || metadata_target_field(column, metadata))?;
        return Ok((field, column.nullable(), None));
    }
    let source = index.get(&column.base_field_id()).copied();
    work.step()?;
    let (source, base) =
        source.ok_or_else(|| invalid("read column ID is absent from frozen source schema"))?;
    let domain = table
        .and_then(|table| table.scalar_integer_domains().get(&source.id).copied())
        .or_else(|| {
            if table.is_none() {
                column.scalar_integer_domain()
            } else {
                None
            }
        });
    let expected = opaque_source(work, || IcebergColumnHandle::base_column(source))?;
    let expected = opaque_source(work, || expected.with_scalar_integer_domain(domain))?;
    let expected = opaque_source(work, || expected.dereference(column.field_id_path()))?;
    compare_handle(column, &expected, work)?;
    let mut field = opaque_source(work, || {
        dereference_target_field(base, column.field_id_path())
    })?;
    if let Some(domain) = expected.scalar_integer_domain() {
        field = opaque(work, || {
            Arc::new(field.as_ref().clone().with_data_type(domain.data_type()))
        })?;
    }
    let mut declaration = source;
    for id in column.field_id_path() {
        let crate::iceberg::spec::Type::Struct(fields) = declaration.field_type.as_ref() else {
            return Err(invalid("source projection does not traverse a struct"));
        };
        let mut child = None;
        for candidate in fields.fields() {
            work.step()?;
            if candidate.id == *id {
                child = Some(candidate.as_ref());
                break;
            }
        }
        declaration = child.ok_or_else(|| invalid("source projection field ID is absent"))?;
    }
    Ok((field, expected.nullable(), Some(declaration)))
}

fn observed_path(path: &[i32], work: &mut CompileCheckpoints<'_>) -> Result<Vec<i32>, Failure> {
    let mut output = Vec::with_capacity(path.len());
    for id in path {
        output.push(*id);
        work.step()?;
    }
    Ok(output)
}
fn column_index<'a>(
    columns: &'a [IcebergColumnHandle],
    work: &mut CompileCheckpoints<'_>,
) -> Result<BTreeMap<(i32, Vec<i32>), &'a IcebergColumnHandle>, Failure> {
    let mut index = BTreeMap::new();
    for column in columns {
        let path = observed_path(column.field_id_path(), work)?;
        // Preserve the original first-match behavior for repeated identities.
        index
            .entry((column.base_field_id(), path))
            .or_insert(column);
        work.step()?;
    }
    Ok(index)
}
fn projected_field(
    column: &IcebergColumnHandle,
    work: &mut CompileCheckpoints<'_>,
) -> Result<FieldRef, Failure> {
    let ty = opaque_source(work, || {
        parse_type(column.base_type_json(), "base_type_json")
    })?;
    // A system reference has no base schema. The private handle contains the
    // full base storage type and identity; its projected NULL fact is frozen
    // separately, so this temporary base Field does not author parent NULL.
    let source = NestedField::optional(
        column.base_field_id(),
        column.base_column_identity().name(),
        ty,
    );
    let identity = opaque_source(work, || ColumnIdentity::from_nested_field(&source))?;
    if !compare_identity(column.base_column_identity(), &identity, work)? {
        return Err(invalid(
            "column identity differs from its declared storage type",
        ));
    }
    let schema = opaque_source(work, || {
        Schema::builder()
            .with_fields(vec![Arc::new(source)])
            .build()
            .map_err(|error| crate::typed_read::column_handle::invalid(error.to_string()))
    })?;
    let arrow = opaque_source(work, || {
        sql_read_schema_from_iceberg(&schema).map_err(crate::typed_read::column_handle::invalid)
    })?;
    let mut field = opaque_source(work, || {
        dereference_target_field(&arrow.fields()[0], column.field_id_path())
    })?;
    if let Some(domain) = column.scalar_integer_domain() {
        field = opaque(work, || {
            Arc::new(field.as_ref().clone().with_data_type(domain.data_type()))
        })?;
    }
    work.step()?;
    Ok(field)
}

fn compare_handle(
    actual: &IcebergColumnHandle,
    expected: &IcebergColumnHandle,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), Failure> {
    if !compare_identity(
        actual.base_column_identity(),
        expected.base_column_identity(),
        work,
    )? {
        return Err(invalid("read column identity differs from frozen source"));
    }
    let same_path = actual.field_id_path().len() == expected.field_id_path().len();
    work.step()?;
    if !same_path {
        return Err(invalid("read column path differs from frozen source"));
    }
    for (a, b) in actual.field_id_path().iter().zip(expected.field_id_path()) {
        let same = a == b;
        work.step()?;
        if !same {
            return Err(invalid("read column path differs from frozen source"));
        }
    }
    for (a, b) in [
        (actual.base_type_json(), expected.base_type_json()),
        (actual.type_json(), expected.type_json()),
    ] {
        let a = opaque_source(work, || parse_type(a, "column type"))?;
        let b = opaque_source(work, || parse_type(b, "source type"))?;
        let same = opaque(work, || a == b)?;
        if !same {
            return Err(invalid("read column type differs from frozen source"));
        }
    }
    let same = actual.nullable() == expected.nullable()
        && actual.scalar_integer_domain() == expected.scalar_integer_domain();
    work.step()?;
    if !same {
        return Err(invalid(
            "read column NULL or narrow-integer domain differs from frozen source",
        ));
    }
    Ok(())
}
fn compare_identity(
    a: &ColumnIdentity,
    b: &ColumnIdentity,
    work: &mut CompileCheckpoints<'_>,
) -> Result<bool, Failure> {
    let mut pending = vec![(a, b)];
    while let Some((a, b)) = pending.pop() {
        let same = a.field_id() == b.field_id()
            && a.category() == b.category()
            && a.children().len() == b.children().len();
        work.step()?;
        if !same || !equal_text(a.name(), b.name(), work)? {
            return Ok(false);
        }
        for (a, b) in a.children().iter().zip(b.children()).rev() {
            pending.push((a, b));
            work.step()?;
        }
    }
    Ok(true)
}
fn equal_text(a: &str, b: &str, work: &mut CompileCheckpoints<'_>) -> Result<bool, Failure> {
    let same_length = a.len() == b.len();
    work.step()?;
    if !same_length {
        return Ok(false);
    }
    for (a, b) in a.as_bytes().chunks(1024).zip(b.as_bytes().chunks(1024)) {
        let same = a == b;
        work.step()?;
        if !same {
            return Ok(false);
        }
    }
    Ok(true)
}

fn compare_public(
    relation: &IcebergRuntimeRelation,
    column: &IcebergColumnHandle,
    expected: &Field,
    nullable: bool,
    ordinal: usize,
    public: &ConnectorReadPublicFacts,
    declaration: Option<&NestedField>,
    input_work: usize,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), Failure> {
    let execution = public
        .execution_schema()
        .ok_or_else(|| invalid("read has no complete execution schema"))?;
    let source = public
        .source_schema_facts()
        .ok_or_else(|| invalid("read has no separate source schema facts"))?;
    let actual = execution
        .binding(ordinal)
        .ok_or_else(|| invalid("missing public read value binding"))?;
    let shape = source
        .shape()
        .schema()
        .fields()
        .get(ordinal)
        .ok_or_else(|| invalid("missing public source occurrence"))?;
    if !equal_text(expected.name(), shape.name(), work)?
        || shape.is_nullable() != nullable
        || actual.nullable() != nullable
    {
        return Err(invalid(
            "public read source name or complete NULL contract differs",
        ));
    }
    let value_declaration = opaque_source(work, || parse_type(column.type_json(), "type_json"))?;
    let empty_nominals = Default::default();
    let nominals = typed_compare::nominal_declarations(relation).unwrap_or(&empty_nominals);
    let value_id = column
        .field_id_path()
        .last()
        .copied()
        .unwrap_or(column.base_field_id());
    typed_compare::value(
        &value_declaration,
        actual.value_type(),
        actual.data_type(),
        nominals.get(&value_id).map(|domain| domain.logical_type()),
        column.scalar_integer_domain(),
        nominals,
        work,
    )?;
    if let Some(declaration) = declaration {
        typed_compare::declared_shape(
            &declaration.field_type,
            shape.data_type(),
            column.scalar_integer_domain(),
            work,
        )?;
    } else {
        typed_compare::shape(expected.data_type(), shape.data_type(), work)?;
    }
    let path =
        [u32::try_from(ordinal).map_err(|_| invalid("source ordinal exceeds address space"))?];
    typed_compare::source_facts(column, declaration, input_work, source, &path, work)
}
fn opaque<T>(
    work: &mut CompileCheckpoints<'_>,
    operation: impl FnOnce() -> T,
) -> Result<T, Failure> {
    work.flush()?;
    let result = operation();
    work.flush()?;
    Ok(result)
}
fn opaque_source<T>(
    work: &mut CompileCheckpoints<'_>,
    operation: impl FnOnce() -> Result<T, ConnectorError>,
) -> Result<T, Failure> {
    work.flush()?;
    let result = operation().map_err(source_error);
    work.flush()?;
    result
}
fn source_error(error: ConnectorError) -> Failure {
    Failure::Provider(ConnectorCodecError::new(
        ConnectorFieldPath::root("read_columns"),
        match error.kind() {
            ConnectorErrorKind::Unsupported => ConnectorCodecErrorKind::Unsupported,
            ConnectorErrorKind::ResourceExhausted => ConnectorCodecErrorKind::Capacity,
            _ => ConnectorCodecErrorKind::InconsistentFields,
        },
        error.to_string(),
    ))
}
fn invalid(message: impl AsRef<str>) -> Failure {
    Failure::Provider(ConnectorCodecError::new(
        ConnectorFieldPath::root("read_columns"),
        ConnectorCodecErrorKind::InconsistentFields,
        message,
    ))
}
fn unsupported(message: &'static str) -> Failure {
    Failure::Provider(ConnectorCodecError::new(
        ConnectorFieldPath::root("read_columns"),
        ConnectorCodecErrorKind::Unsupported,
        message,
    ))
}

#[cfg(test)]
pub(crate) mod test_ingress;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::iceberg::spec::{PartitionSpec, PrimitiveType, StructType, Type};
    use crate::scalar_integer_domain::{ScalarIntegerDomain, ScalarIntegerDomains};
    use crate::typed_read::{
        IcebergChangeWindowHandle, IcebergChangeWindowHandleParams, IcebergColumnHandleParams,
        IcebergSystemTableReference, IcebergSystemTableReferenceParams, IcebergSystemTableType,
        IcebergTableHandleParams,
    };
    use novarocks_connector_contract::{
        ConnectorReadArtifactCoverage, ConnectorReadDistribution, ConnectorReadInputVersion,
        ConnectorReadProperties, ConnectorReadStaticFacts,
    };
    use novarocks_spi::connector::read_stack::{SchemaTableName, TupleDomain};
    use novarocks_type_contract::{CompilePhase, PureCompileControl};
    use std::sync::Mutex;

    #[derive(Default)]
    struct Control {
        trace: Mutex<Vec<u32>>,
        stop: Option<(usize, CompileControlError)>,
    }
    impl PureCompileControl for Control {
        fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
            assert_eq!(phase, CompilePhase::ProviderValidation);
            assert!(units <= 256);
            let mut trace = self.trace.lock().unwrap();
            let index = trace.len();
            trace.push(units);
            if let Some((stop, cause)) = self.stop
                && stop == index
            {
                return Err(cause);
            }
            Ok(())
        }
    }
    fn causes() -> [CompileControlError; 3] {
        [
            CompileControlError::Cancelled,
            CompileControlError::DeadlineExceeded,
            CompileControlError::ResourceExhausted,
        ]
    }
    fn schema(fields: Vec<NestedField>) -> Schema {
        Schema::builder()
            .with_fields(fields.into_iter().map(Arc::new).collect::<Vec<_>>())
            .build()
            .unwrap()
    }
    fn name() -> SchemaTableName {
        SchemaTableName::try_new("db", "events").unwrap()
    }
    fn table(schema: &Schema, domains: ScalarIntegerDomains) -> IcebergTableHandle {
        let spec = PartitionSpec::builder(schema.clone())
            .with_spec_id(0)
            .build()
            .unwrap();
        let domain = crate::delete_semantics::test_read_domain(schema, &[spec], 11);
        let table = IcebergTableHandle::try_new(IcebergTableHandleParams {
            schema_table_name: name(),
            snapshot_id: Some(11),
            read_domain: Some(domain.clone()),
            table_schema_json: domain.endpoint().schema_json().to_string(),
            spec_id: Some(0),
            partition_spec_jsons: domain
                .endpoint()
                .partition_spec_jsons()
                .iter()
                .map(|(id, json)| (*id, json.to_string()))
                .collect(),
            format_version: 2,
            unenforced_predicate: TupleDomain::all(),
            enforced_predicate: TupleDomain::all(),
            limit: None,
            // Projection membership is not the ordered output occurrence list.
            projected_columns: Default::default(),
            name_mapping_json: None,
            table_location: "s3://warehouse/db/events".into(),
            storage_properties: BTreeMap::new(),
            pinned_data_files: None,
        })
        .unwrap();
        table.with_scalar_integer_domains(domains).unwrap()
    }
    fn public(fields: Vec<FieldRef>, logical: Vec<ValueLogicalType>) -> ConnectorReadPublicFacts {
        let source = ConnectorReadStaticFacts::try_new(
            ConnectorReadInputVersion::try_new([7; 32]).unwrap(),
            [8; 32],
            ConnectorReadProperties::try_new(ConnectorReadDistribution::Unconstrained, vec![])
                .unwrap(),
            ConnectorReadArtifactCoverage::NoArtifactInputs,
            vec![],
        )
        .unwrap();
        typed_public(source, fields, logical, false)
    }
    fn typed_public(
        source: ConnectorReadStaticFacts<novarocks_connector_contract::ScanColumnId>,
        fields: Vec<FieldRef>,
        logical: Vec<ValueLogicalType>,
        system: bool,
    ) -> ConnectorReadPublicFacts {
        use novarocks_connector_contract::{
            ConnectorReadExecutionSchema, ConnectorSourceSchemaFacts, ConnectorSourceSchemaShape,
        };
        use novarocks_type_contract::{FunctionValueType, ValueTypeStorageLifetime};
        let storage = ValueTypeStorageLifetime::pure();
        let mut facts = Vec::new();
        let source_fields = fields
            .iter()
            .enumerate()
            .map(|(ordinal, field)| {
                if system {
                    test_ingress::system_source_field(field, &mut vec![ordinal as u32], &mut facts)
                } else {
                    test_ingress::source_field(field, &mut vec![ordinal as u32], &mut facts)
                }
            })
            .collect::<Vec<_>>();
        let source_schema = ConnectorSourceSchemaFacts::try_new(
            ConnectorSourceSchemaShape::try_new(
                Arc::new(arrow::datatypes::Schema::new(source_fields)),
                storage.clone(),
            )
            .unwrap(),
            std::collections::HashMap::new(),
            facts.into_boxed_slice(),
            storage.clone(),
        )
        .unwrap();
        let bindings = fields
            .iter()
            .zip(logical)
            .map(|(field, domain)| {
                FunctionValueType::try_with_logical_type(
                    field.data_type().clone(),
                    field.is_nullable(),
                    domain,
                )
                .unwrap()
            })
            .collect::<Vec<_>>();
        let fields = fields
            .iter()
            .zip(&bindings)
            .map(|(field, value)| {
                Arc::new(Field::new(
                    field.name(),
                    value.data_type().clone(),
                    value.nullable(),
                ))
            })
            .collect::<Vec<_>>();
        let execution = ConnectorReadExecutionSchema::try_new(
            Arc::new(arrow::datatypes::Schema::new(fields)),
            bindings.into_boxed_slice(),
            storage.clone(),
        )
        .unwrap();
        ConnectorReadPublicFacts::try_new_typed(source, None, execution, source_schema, storage)
            .unwrap()
    }
    fn physical_public(fields: Vec<FieldRef>) -> ConnectorReadPublicFacts {
        let logical = vec![ValueLogicalType::Physical; fields.len()];
        public(fields, logical)
    }
    fn system_public(
        kind: IcebergSystemTableType,
        fields: Vec<FieldRef>,
    ) -> ConnectorReadPublicFacts {
        let logical = fields
            .iter()
            .map(|field| {
                crate::typed_read::system_page_source::system_column_domain(kind, field.name())
            })
            .collect();
        let base = public(vec![], vec![]);
        typed_public(base.source().clone(), fields, logical, true)
    }
    fn run(
        relation: &IcebergRuntimeRelation,
        columns: &[IcebergColumnHandle],
        public: &ConnectorReadPublicFacts,
        control: &Control,
    ) -> Result<(), Failure> {
        let mut work = CompileCheckpoints::try_new(control, CompilePhase::ProviderValidation)?;
        validate_program_columns(relation, columns, public, &mut work)
    }
    fn rejected(
        relation: &IcebergRuntimeRelation,
        columns: &[IcebergColumnHandle],
        public: &ConnectorReadPublicFacts,
    ) {
        assert!(matches!(
            run(relation, columns, public, &Control::default()),
            Err(Failure::Provider(_))
        ));
    }
    #[test]
    fn formal_source_shape_keeps_its_anonymous_names_and_rejects_exact_null_or_offset_drift() {
        let declaration = Type::List(crate::iceberg::spec::ListType::new(Arc::new(
            NestedField::list_element(2, Type::Primitive(PrimitiveType::String), false),
        )));
        let Type::List(list) = &declaration else {
            unreachable!()
        };
        let formal_name = &list.element_field.name;
        for (actual, accepted) in [
            (
                DataType::List(Arc::new(Field::new(formal_name, DataType::Utf8, true))),
                true,
            ),
            (
                DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
                false,
            ),
            (
                DataType::List(Arc::new(Field::new(formal_name, DataType::Utf8, false))),
                false,
            ),
            (
                DataType::LargeList(Arc::new(Field::new(formal_name, DataType::Utf8, true))),
                false,
            ),
        ] {
            let control = Control::default();
            let mut work =
                CompileCheckpoints::try_new(&control, CompilePhase::ProviderValidation).unwrap();
            assert_eq!(
                typed_compare::declared_shape(&declaration, &actual, None, &mut work).is_ok(),
                accepted
            );
        }
    }

    fn replace_column(
        column: &IcebergColumnHandle,
        identity: ColumnIdentity,
        nullable: bool,
    ) -> IcebergColumnHandle {
        IcebergColumnHandle::try_new(IcebergColumnHandleParams {
            base_column_identity: identity,
            base_type_json: column.base_type_json().to_string(),
            type_json: column.type_json().to_string(),
            field_id_path: column.field_id_path().to_vec(),
            nullable,
            comment: None,
        })
        .unwrap()
    }
    fn system() -> IcebergRuntimeRelation {
        IcebergRuntimeRelation::SystemTable(
            IcebergSystemTableReference::try_new(IcebergSystemTableReferenceParams {
                schema_table_name: name(),
                system_table_type: IcebergSystemTableType::Files,
                metadata_file_location: "s3://warehouse/metadata/v1.json".into(),
                table_uuid: "00000000-0000-0000-0000-000000000007".into(),
                snapshot_id: Some(11),
            })
            .unwrap(),
        )
    }

    #[test]
    fn source_occurrences_preserve_reorder_duplicates_and_unknown_metadata() {
        let schema = schema(vec![
            NestedField::required(1, "id", Type::Primitive(PrimitiveType::Long)),
            NestedField::optional(2, "label", Type::Primitive(PrimitiveType::String)),
        ]);
        let fields = annotated_read_schema(&schema).unwrap();
        let column1 = IcebergColumnHandle::base_column_of(&schema, 1).unwrap();
        let column2 = IcebergColumnHandle::base_column_of(&schema, 2).unwrap();
        let columns = vec![column2, column1.clone(), column1];
        let mut field = fields.fields()[1].as_ref().clone();
        let mut metadata = field.metadata().clone();
        metadata.insert("provider.opaque".into(), "not reconstructed".into());
        field = field.with_metadata(metadata);
        let public = physical_public(vec![
            Arc::new(field),
            Arc::clone(&fields.fields()[0]),
            Arc::clone(&fields.fields()[0]),
        ]);
        let before = public.clone();
        run(
            &IcebergRuntimeRelation::Table(table(&schema, BTreeMap::new())),
            &columns,
            &public,
            &Control::default(),
        )
        .unwrap();
        assert_eq!(public, before);
        assert_eq!(
            public
                .source_schema_facts()
                .unwrap()
                .field_fact(&[0])
                .unwrap()
                .attributes()["provider.opaque"],
            "not reconstructed"
        );
    }

    #[test]
    fn source_ids_identity_type_path_and_nullable_are_independent_facts() {
        let schema = schema(vec![NestedField::optional(
            1,
            "parent",
            Type::Struct(StructType::new(vec![
                Arc::new(NestedField::required(
                    2,
                    "child",
                    Type::Primitive(PrimitiveType::Long),
                )),
                Arc::new(NestedField::required(
                    3,
                    "other",
                    Type::Primitive(PrimitiveType::String),
                )),
            ])),
        )]);
        let column = IcebergColumnHandle::base_column_of(&schema, 1)
            .unwrap()
            .dereference(&[2])
            .unwrap();
        assert!(column.nullable());
        let arrow = annotated_read_schema(&schema).unwrap();
        let field = dereference_target_field(&arrow.fields()[0], &[2]).unwrap();
        let public = physical_public(vec![Arc::new(field.as_ref().clone().with_nullable(true))]);
        let relation = IcebergRuntimeRelation::Table(table(&schema, BTreeMap::new()));
        run(
            &relation,
            std::slice::from_ref(&column),
            &public,
            &Control::default(),
        )
        .unwrap();
        let wrong_nullable = replace_column(&column, column.base_column_identity().clone(), false);
        rejected(&relation, &[wrong_nullable], &public);
        let wrong_path = IcebergColumnHandle::base_column_of(&schema, 1)
            .unwrap()
            .dereference(&[3])
            .unwrap();
        rejected(&relation, &[wrong_path], &public);
        let wrong_identity = ColumnIdentity::try_new(
            1,
            "stale-parent",
            column.base_column_identity().category(),
            column.base_column_identity().children().to_vec(),
        )
        .unwrap();
        rejected(
            &relation,
            &[replace_column(&column, wrong_identity, true)],
            &public,
        );
        let wrong_id = field.as_ref().clone().with_nullable(true).with_metadata(
            [(PARQUET_FIELD_ID_META_KEY.into(), "999".into())]
                .into_iter()
                .collect(),
        );
        rejected(
            &relation,
            std::slice::from_ref(&column),
            &physical_public(vec![Arc::new(wrong_id)]),
        );
        let wrong_type = Field::new("child", DataType::Int32, true);
        rejected(
            &relation,
            std::slice::from_ref(&column),
            &physical_public(vec![Arc::new(wrong_type)]),
        );
    }

    #[test]
    fn storage_type_json_whitespace_is_not_a_new_identity() {
        let schema = schema(vec![NestedField::required(
            1,
            "id",
            Type::Primitive(PrimitiveType::Long),
        )]);
        let column = IcebergColumnHandle::base_column_of(&schema, 1).unwrap();
        let spaced = IcebergColumnHandle::try_new(IcebergColumnHandleParams {
            base_column_identity: column.base_column_identity().clone(),
            base_type_json: " \n \"long\" \t".into(),
            type_json: " \"long\"\n".into(),
            field_id_path: vec![],
            nullable: false,
            comment: Some("comment is not a storage contract".into()),
        })
        .unwrap();
        let fields = annotated_read_schema(&schema).unwrap();
        run(
            &IcebergRuntimeRelation::Table(table(&schema, BTreeMap::new())),
            &[spaced],
            &physical_public(fields.fields().iter().cloned().collect()),
            &Control::default(),
        )
        .unwrap();
    }

    #[test]
    fn narrow_integer_source_requires_the_frozen_relation_domain() {
        let schema = schema(vec![NestedField::optional(
            17,
            "tiny",
            Type::Primitive(PrimitiveType::Int),
        )]);
        let column = IcebergColumnHandle::base_column_of(&schema, 17)
            .unwrap()
            .with_scalar_integer_domain(Some(ScalarIntegerDomain::Int8))
            .unwrap();
        let field = annotated_read_schema(&schema).unwrap().fields()[0]
            .as_ref()
            .clone()
            .with_data_type(DataType::Int8);
        let public = physical_public(vec![Arc::new(field)]);
        let relation = IcebergRuntimeRelation::Table(table(
            &schema,
            BTreeMap::from([(17, ScalarIntegerDomain::Int8)]),
        ));
        run(
            &relation,
            std::slice::from_ref(&column),
            &public,
            &Control::default(),
        )
        .unwrap();
        rejected(
            &IcebergRuntimeRelation::Table(table(&schema, BTreeMap::new())),
            std::slice::from_ref(&column),
            &public,
        );
        let widened = physical_public(vec![Arc::new(Field::new("tiny", DataType::Int32, true))]);
        rejected(&relation, &[column], &widened);
    }

    #[test]
    fn exact_uuid_variant_and_legacy_declared_nominal_storage_are_distinct() {
        let schema = schema(vec![
            NestedField::required(1, "u", Type::Primitive(PrimitiveType::Uuid)),
            NestedField::required(2, "v", Type::Variant(crate::iceberg::spec::VariantType)),
            NestedField::required(3, "raw", Type::Primitive(PrimitiveType::Fixed(16))),
            NestedField::required(4, "sketch", Type::Primitive(PrimitiveType::Binary)),
            NestedField::required(5, "text", Type::Primitive(PrimitiveType::String)),
        ]);
        let arrow = annotated_read_schema(&schema).unwrap();
        assert_eq!(
            arrow.fields()[1]
                .metadata()
                .get("ARROW:extension:name")
                .map(String::as_str),
            Some("arrow.parquet.variant")
        );
        assert!(
            !arrow.fields()[1]
                .metadata()
                .contains_key("ARROW:extension:metadata"),
            "the actual SDK field author preserves absent extension metadata"
        );
        let columns = (1..=5)
            .map(|id| IcebergColumnHandle::base_column_of(&schema, id).unwrap())
            .collect::<Vec<_>>();
        let relation = IcebergRuntimeRelation::Table(table(&schema, BTreeMap::new()));
        let original = public(
            arrow.fields().iter().cloned().collect(),
            vec![
                ValueLogicalType::Uuid,
                ValueLogicalType::Variant,
                ValueLogicalType::Physical,
                ValueLogicalType::Physical,
                ValueLogicalType::Physical,
            ],
        );
        assert_eq!(original.logical_types()[0], ValueLogicalType::Uuid);
        assert_eq!(original.logical_types()[1], ValueLogicalType::Variant);
        assert_eq!(original.logical_types()[2], ValueLogicalType::Physical);
        run(&relation, &columns, &original, &Control::default()).unwrap();
        // Same physical shape and complete value declaration cannot repair a
        // changed known source attribute. Empty is not the SDK's absent key.
        let mut variant_attrs = arrow.fields()[1].metadata().clone();
        variant_attrs.insert("ARROW:extension:metadata".into(), String::new());
        let mut wrong_source = arrow.fields().iter().cloned().collect::<Vec<_>>();
        wrong_source[1] = Arc::new(
            arrow.fields()[1]
                .as_ref()
                .clone()
                .with_metadata(variant_attrs),
        );
        rejected(
            &relation,
            &columns,
            &public(wrong_source, original.logical_types().to_vec()),
        );
        let mut logical = original.logical_types().to_vec();
        logical[2] = ValueLogicalType::LargeInt;
        logical[3] = ValueLogicalType::Hll;
        let authored = public(arrow.fields().iter().cloned().collect(), logical.clone());
        rejected(&relation, &columns, &authored);
        logical[3] = ValueLogicalType::Bitmap;
        rejected(
            &relation,
            &columns,
            &public(arrow.fields().iter().cloned().collect(), logical),
        );
        // Erasing the SDK's actual UUID label to Physical cannot pass merely
        // because both values occupy exactly sixteen bytes.
        let uuid_without_label = Arc::new(
            arrow.fields()[0].as_ref().clone().with_metadata(
                [(PARQUET_FIELD_ID_META_KEY.into(), "1".into())]
                    .into_iter()
                    .collect(),
            ),
        );
        rejected(
            &relation,
            &columns,
            &public(
                vec![
                    uuid_without_label,
                    Arc::clone(&arrow.fields()[1]),
                    Arc::clone(&arrow.fields()[2]),
                    Arc::clone(&arrow.fields()[3]),
                    Arc::clone(&arrow.fields()[4]),
                ],
                vec![
                    ValueLogicalType::Physical,
                    ValueLogicalType::Variant,
                    ValueLogicalType::Physical,
                    ValueLogicalType::Physical,
                    ValueLogicalType::Physical,
                ],
            ),
        );
        let mut logical = original.logical_types().to_vec();
        logical[4] = ValueLogicalType::Json;
        rejected(
            &relation,
            &columns,
            &public(arrow.fields().iter().cloned().collect(), logical),
        );
    }

    #[test]
    fn metadata_target_uses_the_real_pure_rules_without_runtime_lineage_defaults() {
        let schema = schema(vec![NestedField::required(
            1,
            "id",
            Type::Primitive(PrimitiveType::Long),
        )]);
        let relation = IcebergRuntimeRelation::Table(table(&schema, BTreeMap::new()));
        let column = crate::typed_read::table_execute::rewrite_position_delete_pseudo_column(
            "_file",
            IcebergMetadataColumn::Path,
        )
        .unwrap();
        let field = metadata_target_field(&column, IcebergMetadataColumn::Path).unwrap();
        run(
            &relation,
            std::slice::from_ref(&column),
            &physical_public(vec![field]),
            &Control::default(),
        )
        .unwrap();
        let wrong = replace_column(
            &column,
            ColumnIdentity::try_new(
                column.base_field_id(),
                "wrong",
                column.base_column_identity().category(),
                vec![],
            )
            .unwrap(),
            true,
        );
        rejected(
            &relation,
            &[wrong],
            &physical_public(vec![Arc::new(Field::new("_file", DataType::Utf8, true))]),
        );
        let deleted = IcebergColumnHandle::base_column(&NestedField::required(
            IcebergMetadataColumn::IsDeleted.field_id(),
            IcebergMetadataColumn::IsDeleted.column_name(),
            Type::Primitive(PrimitiveType::Boolean),
        ))
        .unwrap();
        let error = run(
            &relation,
            &[deleted],
            &physical_public(vec![Arc::new(Field::new(
                "_deleted",
                DataType::Boolean,
                false,
            ))]),
            &Control::default(),
        )
        .unwrap_err();
        assert!(
            matches!(error, Failure::Provider(error) if error.kind() == ConnectorCodecErrorKind::Unsupported)
        );
    }

    #[test]
    fn system_actual_author_keeps_utc_map_key_null_and_json_with_reordered_ids() {
        let schema = schema(vec![NestedField::required(
            1,
            "id",
            Type::Primitive(PrimitiveType::Long),
        )]);
        for kind in [
            crate::typed_read::IcebergSystemTableType::Snapshots,
            crate::typed_read::IcebergSystemTableType::Files,
        ] {
            let fields = crate::typed_read::system_relation_schema(kind, &schema, &[]).unwrap();
            let columns = crate::typed_read::system_relation_columns(kind, &schema, &[]).unwrap();
            let relation = IcebergRuntimeRelation::SystemTable(
                IcebergSystemTableReference::try_new(IcebergSystemTableReferenceParams {
                    schema_table_name: name(),
                    system_table_type: kind,
                    metadata_file_location: "s3://warehouse/metadata/v1.json".into(),
                    table_uuid: "00000000-0000-0000-0000-000000000007".into(),
                    snapshot_id: Some(11),
                })
                .unwrap(),
            );
            // Reversing output must not renumber the original synthetic IDs.
            let mut selected = columns.clone();
            selected.reverse();
            selected.push(columns[0].clone());
            let mut output = fields.fields().iter().rev().cloned().collect::<Vec<_>>();
            output.push(Arc::clone(&fields.fields()[0]));
            let mut first = output[0].as_ref().clone();
            let mut metadata = first.metadata().clone();
            metadata.insert("provider.dynamic".into(), "frozen".into());
            first = first.with_metadata(metadata);
            output[0] = Arc::new(first);
            let public = system_public(kind, output);
            let before = public.clone();
            run(&relation, &selected, &public, &Control::default()).unwrap();
            assert_eq!(public, before);
            if kind == crate::typed_read::IcebergSystemTableType::Snapshots {
                let mut bad = fields.fields().to_vec();
                bad[0] = Arc::new(bad[0].as_ref().clone().with_data_type(DataType::Timestamp(
                    arrow::datatypes::TimeUnit::Microsecond,
                    None,
                )));
                rejected(&relation, &columns, &system_public(kind, bad));
                let mut bad = fields.fields().to_vec();
                let DataType::Map(entries, sorted) = bad[5].data_type() else {
                    panic!("real summary map")
                };
                let DataType::Struct(children) = entries.data_type() else {
                    panic!("real entries")
                };
                let bad_children = vec![
                    Arc::new(children[0].as_ref().clone().with_nullable(true)),
                    Arc::clone(&children[1]),
                ];
                let bad_type = DataType::Map(
                    Arc::new(
                        entries
                            .as_ref()
                            .clone()
                            .with_data_type(DataType::Struct(bad_children.into())),
                    ),
                    *sorted,
                );
                assert!(
                    novarocks_type_contract::FunctionValueType::try_with_logical_type(
                        bad_type,
                        bad[5].is_nullable(),
                        ValueLogicalType::Physical
                    )
                    .is_err(),
                    "invalid Map key NULL is refused at the checked author boundary"
                );
            } else {
                let at = fields
                    .fields()
                    .iter()
                    .position(|field| field.name() == "readable_metrics")
                    .unwrap();
                let fields = fields.fields().to_vec();
                let mut logical = fields
                    .iter()
                    .map(|field| {
                        crate::typed_read::system_page_source::system_column_domain(
                            kind,
                            field.name(),
                        )
                    })
                    .collect::<Vec<_>>();
                assert_eq!(logical[at], ValueLogicalType::Json);
                logical[at] = ValueLogicalType::Physical;
                rejected(
                    &relation,
                    &columns,
                    &typed_public(public.source().clone(), fields, logical, true),
                );
            }
        }
    }

    #[test]
    fn actual_wide_system_mirror_keeps_original_postorder_control_and_tail() {
        let schema = schema(
            (1..=320)
                .map(|id| {
                    NestedField::required(
                        id,
                        format!("c{id}"),
                        Type::Primitive(PrimitiveType::Long),
                    )
                })
                .collect(),
        );
        let kind = crate::typed_read::IcebergSystemTableType::Files;
        let fields = crate::typed_read::system_relation_schema(kind, &schema, &[]).unwrap();
        let columns = crate::typed_read::system_relation_columns(kind, &schema, &[]).unwrap();
        let at = fields
            .fields()
            .iter()
            .position(|field| field.name() == "lower_bounds")
            .unwrap();
        let relation = IcebergRuntimeRelation::SystemTable(
            IcebergSystemTableReference::try_new(IcebergSystemTableReferenceParams {
                schema_table_name: name(),
                system_table_type: kind,
                metadata_file_location: "s3://warehouse/metadata/v1.json".into(),
                table_uuid: "00000000-0000-0000-0000-000000000007".into(),
                snapshot_id: Some(11),
            })
            .unwrap(),
        );
        let public = system_public(kind, vec![Arc::clone(&fields.fields()[at])]);
        let original = columns[at].clone();
        let columns = std::slice::from_ref(&columns[at]);
        // Mutating both private/public top NULL must still refuse the original
        // dynamic source author's nullable ROW declaration.
        let bad_column = replace_column(&original, original.base_column_identity().clone(), false);
        let bad_public = system_public(
            kind,
            vec![Arc::new(
                fields.fields()[at].as_ref().clone().with_nullable(false),
            )],
        );
        rejected(&relation, &[bad_column], &bad_public);
        let baseline = Control::default();
        run(&relation, columns, &public, &baseline).unwrap();
        let trace = baseline.trace.lock().unwrap().clone();
        let quantum = trace
            .iter()
            .position(|units| *units == 256)
            .expect("actual identity postorder exceeds one quantum");
        let tail = trace.len() - 1;
        assert!(trace[tail] > 0 && trace[tail] < 256);
        for cause in causes() {
            for at in [0, quantum, tail] {
                let control = Control {
                    trace: Mutex::default(),
                    stop: Some((at, cause)),
                };
                assert!(
                    matches!(run(&relation, columns, &public, &control), Err(Failure::Control(actual)) if actual == cause)
                );
                assert_eq!(*control.trace.lock().unwrap(), trace[..=at]);
            }
        }
    }

    #[test]
    fn system_nested_handle_is_not_a_runtime_projection_capability() {
        let schema = schema(vec![NestedField::optional(
            1,
            "partition",
            Type::Struct(StructType::new(vec![Arc::new(NestedField::required(
                2,
                "bucket",
                Type::Primitive(PrimitiveType::Int),
            ))])),
        )]);
        let column = IcebergColumnHandle::base_column_of(&schema, 1)
            .unwrap()
            .dereference(&[2])
            .unwrap();
        let public = physical_public(vec![Arc::new(Field::new("bucket", DataType::Int32, true))]);
        assert!(
            matches!(run(&system(), &[column], &public, &Control::default()),
            Err(Failure::Provider(error)) if error.kind() == ConnectorCodecErrorKind::Unsupported)
        );
    }

    #[test]
    fn change_window_uses_to_schema_and_frozen_column_domain_plus_change_sign() {
        let schema = schema(vec![NestedField::required(
            1,
            "id",
            Type::Primitive(PrimitiveType::Long),
        )]);
        let spec = PartitionSpec::builder(schema.clone())
            .with_spec_id(0)
            .build()
            .unwrap();
        let from =
            crate::delete_semantics::test_read_domain(&schema, std::slice::from_ref(&spec), 10);
        let to = crate::delete_semantics::test_read_domain(&schema, &[spec], 11);
        let column = IcebergColumnHandle::base_column_of(&schema, 1).unwrap();
        let relation = IcebergRuntimeRelation::ChangeWindow(
            IcebergChangeWindowHandle::try_new(IcebergChangeWindowHandleParams {
                schema_table_name: name(),
                table_schema_json: to.endpoint().schema_json().to_string(),
                columns: vec![column.clone()],
                name_mapping_json: None,
                from_snapshot_id_exclusive: 10,
                to_snapshot_id_inclusive: 11,
                from_read_domain: from,
                to_read_domain: to.clone(),
                partition_spec_jsons: to
                    .endpoint()
                    .partition_spec_jsons()
                    .iter()
                    .map(|(id, json)| (*id, json.to_string()))
                    .collect(),
            })
            .unwrap(),
        );
        let op = change_op_column_handle().unwrap();
        let public = physical_public(vec![
            Arc::clone(&annotated_read_schema(&schema).unwrap().fields()[0]),
            Arc::new(
                Field::new("__change_op", DataType::Int32, false).with_metadata(
                    [(
                        PARQUET_FIELD_ID_META_KEY.into(),
                        ICEBERG_CHANGE_OP_FIELD_ID.to_string(),
                    )]
                    .into_iter()
                    .collect(),
                ),
            ),
        ]);
        run(
            &relation,
            &[column.clone(), op.clone()],
            &public,
            &Control::default(),
        )
        .unwrap();
        let wrong = replace_column(&op, op.base_column_identity().clone(), true);
        rejected(&relation, &[column, wrong], &public);
    }

    #[test]
    fn known_initial_default_is_semantic_json_and_unknown_metadata_is_preserved() {
        use crate::iceberg::spec::Literal;
        let schema = schema(vec![
            NestedField::optional(1, "id", Type::Primitive(PrimitiveType::Long))
                .with_initial_default(Literal::long(7)),
        ]);
        let column = IcebergColumnHandle::base_column_of(&schema, 1).unwrap();
        let field = annotated_read_schema(&schema).unwrap().fields()[0]
            .as_ref()
            .clone();
        let mut metadata = field.metadata().clone();
        metadata.insert(ICEBERG_INITIAL_DEFAULT_META_KEY.into(), " 7 \n".into());
        let good = physical_public(vec![Arc::new(
            field.clone().with_metadata(metadata.clone()),
        )]);
        let relation = IcebergRuntimeRelation::Table(table(&schema, BTreeMap::new()));
        run(
            &relation,
            std::slice::from_ref(&column),
            &good,
            &Control::default(),
        )
        .unwrap();
        metadata.insert(ICEBERG_INITIAL_DEFAULT_META_KEY.into(), "8".into());
        rejected(
            &relation,
            &[column],
            &physical_public(vec![Arc::new(field.with_metadata(metadata))]),
        );
    }

    #[test]
    fn real_wide_source_has_original_entry_quantum_and_tail_typed_causes() {
        let schema = schema(
            (1..=320)
                .map(|id| {
                    NestedField::required(
                        id,
                        format!("c{id}"),
                        Type::Primitive(PrimitiveType::Long),
                    )
                })
                .collect(),
        );
        let columns = (1..=320)
            .map(|id| IcebergColumnHandle::base_column_of(&schema, id).unwrap())
            .collect::<Vec<_>>();
        let public = physical_public(
            annotated_read_schema(&schema)
                .unwrap()
                .fields()
                .iter()
                .cloned()
                .collect(),
        );
        let relation = IcebergRuntimeRelation::Table(table(&schema, BTreeMap::new()));
        let success = Control::default();
        run(&relation, &columns, &public, &success).unwrap();
        let trace = success.trace.lock().unwrap().clone();
        let quantum = trace
            .iter()
            .position(|units| *units == 256)
            .expect("source index has real 256 work");
        let tail = trace.len() - 1;
        assert!(trace[tail] > 0 && trace[tail] < 256);
        for cause in causes() {
            for at in [0, quantum, tail] {
                let control = Control {
                    trace: Mutex::default(),
                    stop: Some((at, cause)),
                };
                assert!(
                    matches!(run(&relation,&columns,&public,&control),Err(Failure::Control(actual)) if actual==cause)
                );
                assert_eq!(*control.trace.lock().unwrap(), trace[..=at]);
            }
        }
    }

    #[test]
    fn ordinary_shape_refusal_observes_tail_without_replacing_a_primary_control() {
        let schema = schema(vec![NestedField::required(
            1,
            "id",
            Type::Primitive(PrimitiveType::Long),
        )]);
        let relation = IcebergRuntimeRelation::Table(table(&schema, BTreeMap::new()));
        let public = physical_public(
            annotated_read_schema(&schema)
                .unwrap()
                .fields()
                .iter()
                .cloned()
                .collect(),
        );
        let baseline = Control::default();
        rejected(&relation, &[], &public);
        assert!(matches!(
            run(&relation, &[], &public, &baseline),
            Err(Failure::Provider(_))
        ));
        let trace = baseline.trace.lock().unwrap().clone();
        assert_eq!(trace.last(), Some(&1));
        for cause in causes() {
            let at = trace.len() - 1;
            let control = Control {
                trace: Mutex::default(),
                stop: Some((at, cause)),
            };
            assert!(
                matches!(run(&relation,&[],&public,&control),Err(Failure::Control(actual)) if actual==cause)
            );
            assert_eq!(*control.trace.lock().unwrap(), trace[..=at]);
        }
    }
}
