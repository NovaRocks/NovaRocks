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
use crate::schema_mapping::{field_id_for_arrow_field, sql_read_schema_from_iceberg};
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
use novarocks_type_contract::{
    CompileCheckpoints, CompileControlError, CompilePhase, FunctionValueType, PureCompileControl,
    ValueLogicalType, ValueTypeError,
};
use parquet::arrow::PARQUET_FIELD_ID_META_KEY;
use std::{collections::BTreeMap, sync::Arc};

type Failure = PureProviderCompileError<ConnectorCodecError>;

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
        match relation {
            IcebergRuntimeRelation::SystemTable(reference) => {
                // System Arrow schemas have their own exact timestamp/map/JSON
                // source. Mirror them through the same original forward author,
                // rather than applying ordinary Iceberg table storage mapping.
                for (ordinal, column) in columns.iter().enumerate() {
                    check_system_column(
                        reference.system_table_type(),
                        column,
                        &public.schema().fields()[ordinal],
                        public.logical_types()[ordinal],
                        work,
                    )?;
                    work.step()?;
                }
                Ok(())
            }
            _ => visit_source_fields(
                relation,
                columns,
                work,
                &mut |ordinal, field, nullable, work| {
                    compare_public(field, nullable, ordinal, public, work)
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
/// Nominal domains come from the provider's original selected-schema author
/// and remain frozen independently in the relation and column handles.
pub(crate) fn public_read_schema(
    relation: &IcebergRuntimeRelation,
    columns: &[IcebergColumnHandle],
) -> Result<ConnectorReadPublicSchema, ConnectorError> {
    let mut fields = Vec::with_capacity(columns.len());
    let mut logical_types = Vec::with_capacity(columns.len());
    author(|work| {
        visit_source_fields(relation, columns, work, &mut |_, field, nullable, _| {
            let field = field.clone().with_nullable(nullable);
            logical_types.push(source_logical_type(&field)?);
            fields.push(Arc::new(field));
            Ok(())
        })
    })?;
    ConnectorReadPublicSchema::try_new(
        Arc::new(arrow::datatypes::Schema::new(fields)),
        logical_types,
    )
}

/// The public schema of a frozen read of `columns` from a metadata relation.
///
/// `system` is the relation's frozen output schema, authored by the original
/// system-table field author from the pinned table metadata the reference
/// verifies. Each column takes its field by name and is then checked through
/// the same mirror the pure read compiler applies to a frozen public field.
pub(crate) fn system_public_read_schema(
    kind: IcebergSystemTableType,
    columns: &[IcebergColumnHandle],
    system: &arrow::datatypes::Schema,
) -> Result<ConnectorReadPublicSchema, ConnectorError> {
    let mut fields = Vec::with_capacity(columns.len());
    let mut logical_types = Vec::with_capacity(columns.len());
    author(|work| {
        for column in columns {
            let field = system
                .fields()
                .iter()
                .find(|field| field.name() == column.base_column_identity().name())
                .ok_or_else(|| unsupported("system relation has no such output column"))?;
            let logical = source_logical_type(field)?;
            check_system_column(kind, column, field, logical, work)?;
            fields.push(Arc::clone(field));
            logical_types.push(logical);
            work.step()?;
        }
        Ok(())
    })?;
    ConnectorReadPublicSchema::try_new(
        Arc::new(arrow::datatypes::Schema::new(fields)),
        logical_types,
    )
}

/// Runs a field author outside pure compilation. The author is the same code
/// the compiler runs; its checkpoints have no caller budget to observe here.
fn author(
    run: impl FnOnce(&mut CompileCheckpoints<'_>) -> Result<(), Failure>,
) -> Result<(), ConnectorError> {
    struct Unobserved;
    impl PureCompileControl for Unobserved {
        fn checkpoint(&self, _: CompilePhase, _: u32) -> Result<(), CompileControlError> {
            Ok(())
        }
    }
    let mut work = CompileCheckpoints::try_new(&Unobserved, CompilePhase::ProviderValidation)
        .map_err(|cause| author_error(Failure::Control(cause)))?;
    run(&mut work).map_err(author_error)?;
    work.finish()
        .map_err(|cause| author_error(Failure::Control(cause)))
}

fn author_error(failure: Failure) -> ConnectorError {
    match failure {
        Failure::Provider(error) => ConnectorError::new(
            match error.kind() {
                ConnectorCodecErrorKind::Unsupported => ConnectorErrorKind::Unsupported,
                ConnectorCodecErrorKind::Capacity => ConnectorErrorKind::ResourceExhausted,
                _ => ConnectorErrorKind::InvalidRequest,
            },
            format!("Iceberg public read schema: {}", error.detail()),
        ),
        Failure::Control(cause) => ConnectorError::new(
            ConnectorErrorKind::Internal,
            format!("Iceberg public read schema author was interrupted: {cause}"),
        ),
    }
}

fn source_logical_type(field: &Field) -> Result<ValueLogicalType, Failure> {
    novarocks_type_contract::field_logical_type(field)
        .map_err(|_| invalid("source field has invalid logical identity"))
}

/// Receives each column's frozen source field and NULL contract, in column
/// order, at the point the column has been proven against its frozen source.
type SourceFieldVisitor<'v> =
    dyn FnMut(usize, &Field, bool, &mut CompileCheckpoints<'_>) -> Result<(), Failure> + 'v;

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
                    visit(ordinal, &field, column.nullable(), work)?;
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
                    let (field, nullable) = source_column_field(column, &index, None, work)?;
                    visit(ordinal, &field, nullable, work)?;
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
                    visit(ordinal, &field, expected.nullable(), work)?;
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
        let (field, nullable) = source_column_field(column, &index, Some(table), work)?;
        visit(ordinal, &field, nullable, work)?;
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
fn source_column_field(
    column: &IcebergColumnHandle,
    index: &SourceIndex<'_>,
    table: Option<&IcebergTableHandle>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(FieldRef, bool), Failure> {
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
        return Ok((field, column.nullable()));
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
    let nominal = match table {
        Some(table) => table.nominal_read_domains().get(&source.id).copied(),
        None => column.nominal_read_domain(),
    };
    let expected = opaque_source(work, || expected.with_nominal_read_domain(nominal))?;
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
    if let Some(domain) = expected.nominal_read_domain() {
        field = opaque_source(work, || domain.annotate(&field).map(Arc::new))?;
    }
    Ok((field, expected.nullable()))
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
    if let Some(domain) = column.nominal_read_domain() {
        field = opaque_source(work, || domain.annotate(&field).map(Arc::new))?;
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
    let same_nominal = actual.nominal_read_domain() == expected.nominal_read_domain();
    work.step()?;
    if !same_nominal {
        return Err(invalid(
            "read column nominal domain differs from frozen source",
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

#[derive(Debug)]
enum CompareError {
    Control(CompileControlError),
    Value(ValueTypeError),
}
impl From<CompileControlError> for CompareError {
    fn from(e: CompileControlError) -> Self {
        Self::Control(e)
    }
}
impl From<ValueTypeError> for CompareError {
    fn from(e: ValueTypeError) -> Self {
        Self::Value(e)
    }
}
fn comparison_error(error: CompareError) -> Failure {
    match error {
        CompareError::Control(error) => Failure::Control(error),
        CompareError::Value(error) => invalid(format!(
            "read source has invalid value-type structure: {error}"
        )),
    }
}
fn compare_public(
    expected: &Field,
    nullable: bool,
    ordinal: usize,
    public: &ConnectorReadPublicFacts,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), Failure> {
    let actual = public
        .schema()
        .fields()
        .get(ordinal)
        .ok_or_else(|| invalid("missing public read column"))?;
    if !equal_text(expected.name(), actual.name(), work)? || actual.is_nullable() != nullable {
        return Err(invalid(
            "public read column name or NULL contract differs from source",
        ));
    }
    work.step()?;
    let source_logical = novarocks_type_contract::field_logical_type(expected)
        .map_err(|_| invalid("source field has invalid logical identity"))?;
    let public_logical = public.logical_types()[ordinal];
    let compatible_nominal = source_logical == public_logical;
    work.step()?;
    if !compatible_nominal {
        return Err(unsupported(
            "public read nominal domain is not compatible with this frozen storage source",
        ));
    }
    let a = opaque(work, || {
        FunctionValueType::new(expected.data_type().clone(), nullable)
    })?;
    let b = opaque(work, || {
        FunctionValueType::new(actual.data_type().clone(), actual.is_nullable())
    })?;
    let same = a
        .same_value_domain_observed::<CompareError>(&b, || {
            work.step().map_err(CompareError::Control)
        })
        .map_err(comparison_error)?;
    if !same {
        return Err(invalid(
            "public read carrier or nested domain differs from frozen source",
        ));
    }
    check_known_metadata(expected, actual, work)
}
fn check_known_metadata(
    expected: &Field,
    actual: &Field,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), Failure> {
    if actual.metadata().contains_key(PARQUET_FIELD_ID_META_KEY) {
        let a = opaque_source(work, || {
            field_id_for_arrow_field(actual).map_err(crate::typed_read::column_handle::invalid)
        })?;
        let b = opaque_source(work, || {
            field_id_for_arrow_field(expected).map_err(crate::typed_read::column_handle::invalid)
        })?;
        if a != b {
            return Err(invalid("public read field ID metadata differs from source"));
        }
    }
    work.step()?;
    if let Some(value) = actual.metadata().get(ICEBERG_INITIAL_DEFAULT_META_KEY) {
        let expected = expected
            .metadata()
            .get(ICEBERG_INITIAL_DEFAULT_META_KEY)
            .ok_or_else(|| invalid("public read initial default is absent from source"))?;
        // JSON spelling is not a semantic identity. Original serde conversion
        // remains opaque under the already admitted metadata byte bound.
        let same = opaque(work, || {
            let a: Result<serde_json::Value, _> = serde_json::from_str(value);
            let b: Result<serde_json::Value, _> = serde_json::from_str(expected);
            matches!((a,b), (Ok(a), Ok(b)) if a == b)
        })?;
        if !same {
            return Err(invalid("public read initial default differs from source"));
        }
    }
    work.step()?;
    match (expected.data_type(), actual.data_type()) {
        (DataType::Struct(a), DataType::Struct(b)) => {
            for (a, b) in a.iter().zip(b) {
                check_known_metadata(a, b, work)?;
            }
        }
        (DataType::List(a), DataType::List(b))
        | (DataType::LargeList(a), DataType::LargeList(b))
        | (DataType::FixedSizeList(a, _), DataType::FixedSizeList(b, _))
        | (DataType::Map(a, _), DataType::Map(b, _)) => check_known_metadata(a, b, work)?,
        _ => {}
    }
    Ok(())
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
        ConnectorReadPublicFacts::try_new(
            source,
            None,
            arrow::datatypes::Schema::new(fields),
            logical,
        )
        .unwrap()
    }
    fn physical_public(fields: Vec<FieldRef>) -> ConnectorReadPublicFacts {
        let logical = fields
            .iter()
            .map(|f| novarocks_type_contract::field_logical_type(f).unwrap())
            .collect();
        public(fields, logical)
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
            public.schema().fields()[0].metadata()["provider.opaque"],
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
            NestedField::required(2, "v", Type::Primitive(PrimitiveType::Variant)),
            NestedField::required(3, "raw", Type::Primitive(PrimitiveType::Fixed(16))),
            NestedField::required(4, "sketch", Type::Primitive(PrimitiveType::Binary)),
            NestedField::required(5, "text", Type::Primitive(PrimitiveType::String)),
        ]);
        let arrow = annotated_read_schema(&schema).unwrap();
        let columns = (1..=5)
            .map(|id| IcebergColumnHandle::base_column_of(&schema, id).unwrap())
            .collect::<Vec<_>>();
        let relation = IcebergRuntimeRelation::Table(table(&schema, BTreeMap::new()));
        let original = physical_public(arrow.fields().iter().cloned().collect());
        assert_eq!(original.logical_types()[0], ValueLogicalType::Uuid);
        assert_eq!(original.logical_types()[1], ValueLogicalType::Variant);
        assert_eq!(original.logical_types()[2], ValueLogicalType::Physical);
        run(&relation, &columns, &original, &Control::default()).unwrap();
        let mut logical = original.logical_types().to_vec();
        logical[2] = ValueLogicalType::LargeInt;
        logical[3] = ValueLogicalType::Hll;
        let authored = public(arrow.fields().iter().cloned().collect(), logical.clone());
        rejected(&relation, &columns, &authored);
        for sketch in [
            crate::nominal_read_domain::NominalReadDomain::Hll,
            crate::nominal_read_domain::NominalReadDomain::Bitmap,
        ] {
            let domains = BTreeMap::from([
                (3, crate::nominal_read_domain::NominalReadDomain::LargeInt),
                (4, sketch),
            ]);
            let declared = IcebergRuntimeRelation::Table(
                table(&schema, BTreeMap::new())
                    .with_nominal_read_domains(domains.clone())
                    .unwrap(),
            );
            let declared_columns = columns
                .iter()
                .map(|column| {
                    column
                        .clone()
                        .with_nominal_read_domain(domains.get(&column.base_field_id()).copied())
                        .unwrap()
                })
                .collect::<Vec<_>>();
            logical[3] = sketch.logical();
            let published = public(arrow.fields().iter().cloned().collect(), logical.clone());
            run(
                &declared,
                &declared_columns,
                &published,
                &Control::default(),
            )
            .unwrap();
            rejected(&declared, &columns, &published);
            rejected(&declared, &declared_columns, &original);
        }
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
            let public = physical_public(output);
            let before = public.clone();
            run(&relation, &selected, &public, &Control::default()).unwrap();
            assert_eq!(public, before);
            if kind == crate::typed_read::IcebergSystemTableType::Snapshots {
                let mut bad = fields.fields().to_vec();
                bad[0] = Arc::new(bad[0].as_ref().clone().with_data_type(DataType::Timestamp(
                    arrow::datatypes::TimeUnit::Microsecond,
                    None,
                )));
                rejected(&relation, &columns, &physical_public(bad));
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
                bad[5] = Arc::new(bad[5].as_ref().clone().with_data_type(bad_type));
                rejected(&relation, &columns, &physical_public(bad));
            } else {
                let at = fields
                    .fields()
                    .iter()
                    .position(|field| field.name() == "readable_metrics")
                    .unwrap();
                assert_eq!(
                    novarocks_type_contract::field_logical_type(&fields.fields()[at]).unwrap(),
                    ValueLogicalType::Json
                );
                let mut bad = fields.fields().to_vec();
                let mut metadata = bad[at].metadata().clone();
                metadata.remove(novarocks_type_contract::NR_LOGICAL_TYPE_KEY);
                bad[at] = Arc::new(bad[at].as_ref().clone().with_metadata(metadata));
                rejected(&relation, &columns, &physical_public(bad));
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
        let public = physical_public(vec![Arc::clone(&fields.fields()[at])]);
        let original = columns[at].clone();
        let columns = std::slice::from_ref(&columns[at]);
        // Mutating both private/public top NULL must still refuse the original
        // dynamic source author's nullable ROW declaration.
        let bad_column = replace_column(&original, original.base_column_identity().clone(), false);
        let bad_public = physical_public(vec![Arc::new(
            fields.fields()[at].as_ref().clone().with_nullable(false),
        )]);
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
            Arc::new(Field::new("__change_op", DataType::Int32, false)),
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

    #[test]
    fn public_nominal_source_preserves_reordered_duplicate_ids_and_storage_identities() {
        use crate::nominal_read_domain::NominalReadDomain;
        let schema = schema(vec![
            NestedField::optional(1, "n", Type::Primitive(PrimitiveType::Fixed(16))),
            NestedField::required(2, "h", Type::Primitive(PrimitiveType::Binary)),
            NestedField::optional(3, "b", Type::Primitive(PrimitiveType::Binary)),
            NestedField::optional(4, "u", Type::Primitive(PrimitiveType::Uuid)),
            NestedField::optional(5, "v", Type::Primitive(PrimitiveType::Variant)),
            NestedField::optional(6, "raw", Type::Primitive(PrimitiveType::Fixed(16))),
        ]);
        let domains = BTreeMap::from([
            (1, NominalReadDomain::LargeInt),
            (2, NominalReadDomain::Hll),
            (3, NominalReadDomain::Bitmap),
        ]);
        let relation = IcebergRuntimeRelation::Table(
            table(&schema, BTreeMap::new())
                .with_nominal_read_domains(domains.clone())
                .unwrap(),
        );
        let columns = [3, 1, 1, 4, 6, 2, 5].map(|id| {
            IcebergColumnHandle::base_column_of(&schema, id)
                .unwrap()
                .with_nominal_read_domain(domains.get(&id).copied())
                .unwrap()
        });
        let published = public_read_schema(&relation, &columns).unwrap();
        assert_eq!(
            published.logical_types(),
            &[
                ValueLogicalType::Bitmap,
                ValueLogicalType::LargeInt,
                ValueLogicalType::LargeInt,
                ValueLogicalType::Uuid,
                ValueLogicalType::Physical,
                ValueLogicalType::Hll,
                ValueLogicalType::Variant,
            ]
        );
        assert_eq!(
            published
                .schema()
                .fields()
                .iter()
                .map(|field| field.name().as_str())
                .collect::<Vec<_>>(),
            ["b", "n", "n", "u", "raw", "h", "v"]
        );
        assert_eq!(
            published
                .schema()
                .fields()
                .iter()
                .map(|field| field.metadata()[PARQUET_FIELD_ID_META_KEY].as_str())
                .collect::<Vec<_>>(),
            ["3", "1", "1", "4", "6", "2", "5"]
        );
        assert!(published.schema().field(0).is_nullable());
        assert!(!published.schema().field(5).is_nullable());
        assert_eq!(
            published.schema().field(1).metadata()[novarocks_type_contract::NR_LOGICAL_TYPE_KEY],
            "largeint"
        );
        let complete = public(
            published.schema().fields().iter().cloned().collect(),
            published.logical_types().to_vec(),
        );
        run(&relation, &columns, &complete, &Control::default()).unwrap();
        let declared_only = IcebergRuntimeRelation::Table(table(&schema, BTreeMap::new()));
        rejected(&declared_only, &columns, &complete);
    }
}
