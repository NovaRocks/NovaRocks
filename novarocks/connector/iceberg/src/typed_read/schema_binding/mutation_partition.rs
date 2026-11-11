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

//! File partition metadata for mutation inputs. Public metadata tables have a
//! separate projection policy.

use std::collections::{BTreeMap, btree_map::Entry};
use std::sync::Arc;

use arrow::array::{ArrayRef, StructArray};
use arrow::datatypes::{DataType, Field, Fields};
use novarocks_spi::connector::read_stack::ConnectorReadSchemaCreation;
use novarocks_spi::connector::{ConnectorError, ConnectorWriteFieldRequest};

use crate::iceberg::spec::{
    FormatVersion, Literal, NestedField, PartitionSpec, PrimitiveType, Schema, Struct, StructType,
    TableMetadata, Transform, Type,
};
use crate::manifest::partition_types::legal_partition_type_join;
use crate::schema_mapping::typed_value;

pub(crate) const SPEC_ID: i32 = i32::MAX - 4;
pub(crate) const PARTITION_ID: i32 = i32::MAX - 5;
pub(crate) const SPEC_NAME: &str = "_spec_id";
pub(crate) const PARTITION_NAME: &str = "_partition";

pub(crate) struct MutationMetadataSchema {
    spec: Arc<NestedField>,
    partition: Arc<NestedField>,
    carrier: DataType,
    projections: BTreeMap<i32, SpecProjection>,
}

struct SpecProjection {
    file_order: Vec<usize>,
    absent: Vec<usize>,
}

impl MutationMetadataSchema {
    pub(crate) fn try_new(
        metadata: &TableMetadata,
        query_schema: &Schema,
    ) -> Result<Self, ConnectorError> {
        let types =
            crate::read_snapshot::historical_mutation_partition_types(metadata, query_schema)
                .map_err(super::corrupt)?;
        let specs = metadata.partition_specs_iter().map(AsRef::as_ref);
        Self::from_types(metadata.format_version(), specs, |spec_id| {
            types.get(&spec_id).cloned().ok_or_else(|| {
                super::corrupt("Iceberg mutation spec has no historical partition type")
            })
        })
    }

    pub(crate) fn from_endpoint(
        endpoint: &crate::delete_semantics::PinnedEndpointFacts,
        version: FormatVersion,
        specs: impl IntoIterator<Item = PartitionSpec>,
    ) -> Result<Self, ConnectorError> {
        let specs = specs.into_iter().collect::<Vec<_>>();
        Self::from_types(version, &specs, |spec_id| {
            endpoint
                .partition_type(spec_id)
                .map_err(|error| super::corrupt(error.to_string()))
        })
    }

    fn from_types<'a>(
        version: FormatVersion,
        specs: impl IntoIterator<Item = &'a PartitionSpec>,
        mut partition_type: impl FnMut(i32) -> Result<StructType, ConnectorError>,
    ) -> Result<Self, ConnectorError> {
        let specs = specs.into_iter().collect::<Vec<_>>();
        let mut fields = BTreeMap::<i32, (i32, Transform, i32, Arc<NestedField>)>::new();
        for spec in &specs {
            let partition = partition_type(spec.spec_id())?;
            if spec.fields().len() != partition.fields().len() {
                return Err(super::corrupt(
                    "Iceberg mutation spec partition arity differs",
                ));
            }
            for (source, field) in spec.fields().iter().zip(partition.fields()) {
                if source.field_id != field.id {
                    return Err(super::corrupt(
                        "Iceberg mutation partition field ID differs",
                    ));
                }
                match fields.entry(field.id) {
                    Entry::Vacant(entry) => {
                        entry.insert((
                            source.source_id,
                            source.transform,
                            spec.spec_id(),
                            field.clone(),
                        ));
                    }
                    Entry::Occupied(mut entry) => {
                        let (source_id, transform, spec_id, current) = entry.get_mut();
                        if *source_id != source.source_id
                            || (*transform != source.transform
                                && *transform != Transform::Void
                                && source.transform != Transform::Void)
                        {
                            return Err(super::corrupt(format!(
                                "Iceberg mutation partition field {} changed identity",
                                field.id
                            )));
                        }
                        if source.transform == Transform::Void && *transform != Transform::Void {
                            continue;
                        }
                        let (Type::Primitive(old), Type::Primitive(new)) =
                            (current.field_type.as_ref(), field.field_type.as_ref())
                        else {
                            return Err(super::corrupt("Iceberg partition field is not primitive"));
                        };
                        let ty = if *transform == Transform::Void {
                            new.clone()
                        } else {
                            legal_partition_type_join(version, old, new).ok_or_else(|| {
                                super::corrupt(format!(
                                    "Iceberg mutation partition field {} has incompatible types",
                                    field.id
                                ))
                            })?
                        };
                        let name = if spec.spec_id() > *spec_id || *transform == Transform::Void {
                            *spec_id = spec.spec_id();
                            field.name.clone()
                        } else {
                            current.name.clone()
                        };
                        *transform = source.transform;
                        *current =
                            Arc::new(NestedField::optional(field.id, name, Type::Primitive(ty)));
                    }
                }
            }
        }
        let mut names = std::collections::HashSet::new();
        let fields = fields
            .into_values()
            .map(|(_, _, _, field)| {
                // A dropped field and a later field can reuse a name across
                // specs. Keep both IDs in the row with unique child names.
                if names.insert(field.name.to_lowercase()) {
                    field
                } else {
                    let mut name = format!("{}_{}", field.name, field.id);
                    while !names.insert(name.to_lowercase()) {
                        name.push('_');
                    }
                    Arc::new(NestedField::optional(
                        field.id,
                        name,
                        field.field_type.as_ref().clone(),
                    ))
                }
            })
            .collect();
        let mut schema = Self::from_type(StructType::new(fields))?;
        for spec in specs {
            let projection = SpecProjection::new(schema.partition_type(), spec)?;
            if schema
                .projections
                .insert(spec.spec_id(), projection)
                .is_some()
            {
                return Err(super::corrupt("Iceberg mutation schema repeats a spec ID"));
            }
        }
        Ok(schema)
    }

    /// The signed mutation shape may be wider than the exact scan's older
    /// projection, but it must cover every original field and preserve its
    /// identity. The bound split is decoded using its own per-spec type first.
    pub(crate) fn can_project_from(&self, scan: &Self, version: FormatVersion) -> bool {
        let target = self.partition_type().fields();
        let source = scan.partition_type().fields();
        target.len() == source.len()
            && source.iter().zip(target).all(|(source, target)| {
                let (Type::Primitive(from), Type::Primitive(to)) =
                    (source.field_type.as_ref(), target.field_type.as_ref())
                else {
                    return false;
                };
                source.id == target.id
                    && source.name == target.name
                    && legal_partition_type_join(version, from, to).as_ref() == Some(to)
            })
    }

    pub(crate) fn from_type(partition: StructType) -> Result<Self, ConnectorError> {
        let mut names = std::collections::HashSet::new();
        if partition.fields().iter().any(|field| {
            field.required
                || field.id <= 0
                || field.name.is_empty()
                || !names.insert(field.name.to_lowercase())
        }) || !partition
            .fields()
            .windows(2)
            .all(|fields| fields[0].id < fields[1].id)
        {
            return Err(super::corrupt(
                "Iceberg mutation partition fields must be nullable and ordered by ID",
            ));
        }
        let carrier = partition_carrier(&partition)?;
        Ok(Self {
            spec: Arc::new(NestedField::required(
                SPEC_ID,
                SPEC_NAME,
                Type::Primitive(PrimitiveType::Int),
            )),
            partition: Arc::new(NestedField::optional(
                PARTITION_ID,
                PARTITION_NAME,
                Type::Struct(partition),
            )),
            carrier,
            projections: BTreeMap::new(),
        })
    }

    pub(crate) fn spec_field(&self) -> &Arc<NestedField> {
        &self.spec
    }

    pub(crate) fn partition_field(&self) -> &Arc<NestedField> {
        &self.partition
    }

    pub(crate) fn partition_type(&self) -> &StructType {
        let Type::Struct(partition) = self.partition.field_type.as_ref() else {
            unreachable!("the mutation partition field is authored as a Struct")
        };
        partition
    }

    pub(crate) fn arrow_fields(&self) -> Result<[Arc<Field>; 2], ConnectorError> {
        let hidden = std::collections::HashMap::from([(
            novarocks_spi::connector::CONNECTOR_FIELD_HIDDEN_FROM_SQL.to_owned(),
            "true".to_owned(),
        )]);
        Ok([
            Arc::new(Field::new(SPEC_NAME, DataType::Int32, false).with_metadata(hidden.clone())),
            Arc::new(Field::new(PARTITION_NAME, self.carrier.clone(), true).with_metadata(hidden)),
        ])
    }

    /// Recover the original spec tuple from one valid row of the shared union.
    pub(crate) fn decode_partition(
        &self,
        spec: &PartitionSpec,
        historical_type: &StructType,
        array: &StructArray,
        row: usize,
    ) -> Result<Struct, ConnectorError> {
        use arrow::array::Array;
        if row >= array.len()
            || array.is_null(row)
            || array.data_type() != &self.carrier
            || spec.fields().len() != historical_type.fields().len()
        {
            return Err(super::corrupt(
                "Iceberg delete input requires a valid mutation partition Struct",
            ));
        }
        let projection = self.projections.get(&spec.spec_id()).ok_or_else(|| {
            super::corrupt("Iceberg mutation schema has no projection for the file spec")
        })?;
        if projection.file_order.len() != spec.fields().len() {
            return Err(super::corrupt(
                "Iceberg delete input spec field count differs",
            ));
        }
        let mut values = Vec::with_capacity(spec.fields().len());
        for ((part, historical), &ordinal) in spec
            .fields()
            .iter()
            .zip(historical_type.fields())
            .zip(&projection.file_order)
        {
            let field = &self.partition_type().fields()[ordinal];
            let column = array.column(ordinal);
            if historical.id != part.field_id || field.id != part.field_id {
                return Err(super::corrupt(
                    "Iceberg delete input historical field ID differs",
                ));
            }
            if part.transform == Transform::Void && !column.is_null(row) {
                return Err(super::corrupt("Iceberg void partition field is not NULL"));
            }
            let value = crate::iceberg::arrow::arrow_primitive_to_literal(
                &column.slice(row, 1),
                &field.field_type,
            )
            .map_err(|error| super::corrupt(error.to_string()))?
            .into_iter()
            .next()
            .ok_or_else(|| super::corrupt("Iceberg delete input lost its partition row"))?;
            values.push(restore(value, &field.field_type, &historical.field_type)?);
        }
        if projection
            .absent
            .iter()
            .any(|&ordinal| !array.column(ordinal).is_null(row))
        {
            return Err(super::corrupt(
                "Iceberg delete input has a value outside its file spec",
            ));
        }
        Ok(Struct::from_iter(values))
    }

    pub(crate) fn write_fields(
        &self,
        creation: &mut ConnectorReadSchemaCreation<'_>,
    ) -> Result<[ConnectorWriteFieldRequest; 2], ConnectorError> {
        let field = |field: &NestedField, creation: &mut ConnectorReadSchemaCreation<'_>| {
            let value =
                field_value(field, creation).map_err(crate::metadata::metadata_creation_error)?;
            ConnectorWriteFieldRequest::try_new_typed(
                Field::new(&field.name, value.data_type().clone(), value.nullable()),
                value,
            )
        };
        Ok([
            field(&self.spec, creation)?,
            field(&self.partition, creation)?,
        ])
    }
}

impl SpecProjection {
    fn new(partition: &StructType, spec: &PartitionSpec) -> Result<Self, ConnectorError> {
        let mut present = vec![false; partition.fields().len()];
        let mut file_order = Vec::with_capacity(spec.fields().len());
        for field in spec.fields() {
            let ordinal = partition
                .fields()
                .binary_search_by_key(&field.field_id, |field| field.id)
                .map_err(|_| super::corrupt("Iceberg mutation ROW omits a file partition field"))?;
            if std::mem::replace(&mut present[ordinal], true) {
                return Err(super::corrupt(
                    "Iceberg mutation file spec repeats a field ID",
                ));
            }
            file_order.push(ordinal);
        }
        let absent = present
            .into_iter()
            .enumerate()
            .filter_map(|(ordinal, present)| (!present).then_some(ordinal))
            .collect();
        Ok(Self { file_order, absent })
    }
}

pub(crate) fn is_file_metadata(field_id: i32) -> bool {
    matches!(field_id, SPEC_ID | PARTITION_ID)
}

pub(crate) fn field_value(
    field: &NestedField,
    creation: &mut ConnectorReadSchemaCreation<'_>,
) -> Result<
    novarocks_type_contract::FunctionValueType,
    novarocks_connector_contract::PureProviderCompileError<ConnectorError>,
> {
    Ok(
        typed_value::from_declaration(&field.field_type, !field.required, None, None, creation)?
            .binding,
    )
}

pub(crate) fn partition_carrier(partition: &StructType) -> Result<DataType, ConnectorError> {
    let fields = partition
        .fields()
        .iter()
        .map(|field| {
            let Type::Primitive(primitive) = field.field_type.as_ref() else {
                return Err(super::corrupt(
                    "Iceberg mutation partition child is not primitive",
                ));
            };
            let data_type = typed_value::primitive_layout(primitive, None)
                .map_err(crate::metadata::metadata_creation_error)?;
            Ok(Arc::new(Field::new(&field.name, data_type, true)))
        })
        .collect::<Result<Fields, ConnectorError>>()?;
    Ok(DataType::Struct(fields))
}

/// Bind the split's exact spec by field ID. Each slot refers to the spec's
/// original ordinal, and fields absent from this spec become NULL children.
pub(super) fn bind(
    handle: &super::IcebergColumnHandle,
    spec: &PartitionSpec,
    values: &Struct,
) -> Result<(Arc<Field>, super::IcebergColumnSource), ConnectorError> {
    use super::IcebergColumnSource;
    let declared = super::parse_type(handle.type_json(), "type_json")?;
    let name = handle.base_column_identity().name();
    let (data_type, source) = match (handle.base_field_id(), name, handle.nullable(), declared) {
        (SPEC_ID, SPEC_NAME, false, Type::Primitive(PrimitiveType::Int)) => (
            DataType::Int32,
            IcebergColumnSource::MutationSpecId(spec.spec_id()),
        ),
        (PARTITION_ID, PARTITION_NAME, true, Type::Struct(partition)) => {
            if spec.fields().len() != values.fields().len() {
                return Err(super::corrupt("Iceberg mutation partition arity differs"));
            }
            let projection = SpecProjection::new(&partition, spec)?;
            let mut projected = vec![None; partition.fields().len()];
            for ((field, value), ordinal) in spec
                .fields()
                .iter()
                .zip(values.fields())
                .zip(projection.file_order)
            {
                if field.transform == Transform::Void {
                    if value.is_some() {
                        return Err(super::corrupt("Iceberg void partition field is not NULL"));
                    }
                } else {
                    projected[ordinal] =
                        promote(value.as_ref(), &partition.fields()[ordinal].field_type)?;
                }
            }
            (
                partition_carrier(&partition)?,
                IcebergColumnSource::MutationPartition(projected),
            )
        }
        _ => {
            return Err(super::corrupt(
                "Iceberg mutation metadata declaration differs",
            ));
        }
    };
    if !handle.is_base_column() {
        return Err(super::corrupt(
            "Iceberg mutation metadata must be a base column",
        ));
    }
    Ok((
        Arc::new(Field::new(name, data_type, handle.nullable())),
        source,
    ))
}

fn promote(value: Option<&Literal>, target: &Type) -> Result<Option<Literal>, ConnectorError> {
    use crate::iceberg::spec::PrimitiveLiteral;
    let Some(Literal::Primitive(value)) = value else {
        if value.is_none() {
            return Ok(None);
        }
        return Err(super::corrupt(
            "Iceberg mutation partition value is not primitive",
        ));
    };
    let Type::Primitive(target) = target else {
        return Err(super::corrupt(
            "Iceberg mutation partition child is not primitive",
        ));
    };
    let promoted = match (value, target) {
        (PrimitiveLiteral::Int(days), PrimitiveType::Timestamp | PrimitiveType::TimestampNs) => {
            let units_per_day = if *target == PrimitiveType::Timestamp {
                86_400_000_000
            } else {
                86_400_000_000_000
            };
            PrimitiveLiteral::Long(i64::from(*days).checked_mul(units_per_day).ok_or_else(
                || super::corrupt("Iceberg mutation partition timestamp promotion overflowed"),
            )?)
        }
        _ => crate::manifest::partition_types::resolved_partition_primitive(value, target)
            .into_owned(),
    };
    Ok(Some(Literal::Primitive(promoted)))
}

fn restore(
    value: Option<Literal>,
    source: &Type,
    target: &Type,
) -> Result<Option<Literal>, ConnectorError> {
    use crate::iceberg::spec::PrimitiveLiteral;
    let Some(value) = value else {
        return Ok(None);
    };
    let wrong = || super::corrupt("Iceberg mutation partition cannot recover its historical type");
    if source == target {
        return Ok(Some(value));
    }
    let primitive = match (value, source, target) {
        (
            Literal::Primitive(PrimitiveLiteral::Long(value)),
            Type::Primitive(PrimitiveType::Long),
            Type::Primitive(PrimitiveType::Int),
        ) => PrimitiveLiteral::Int(i32::try_from(value).map_err(|_| wrong())?),
        (
            Literal::Primitive(PrimitiveLiteral::Double(value)),
            Type::Primitive(PrimitiveType::Double),
            Type::Primitive(PrimitiveType::Float),
        ) => {
            let narrowed = value.0 as f32;
            if !value.0.is_nan() && f64::from(narrowed) != value.0 {
                return Err(wrong());
            }
            PrimitiveLiteral::Float(narrowed.into())
        }
        (
            Literal::Primitive(PrimitiveLiteral::Long(value)),
            Type::Primitive(source @ (PrimitiveType::Timestamp | PrimitiveType::TimestampNs)),
            Type::Primitive(PrimitiveType::Date),
        ) => {
            let units_per_day = if *source == PrimitiveType::Timestamp {
                86_400_000_000
            } else {
                86_400_000_000_000
            };
            if value % units_per_day != 0 {
                return Err(wrong());
            }
            PrimitiveLiteral::Int(i32::try_from(value / units_per_day).map_err(|_| wrong())?)
        }
        (
            Literal::Primitive(value @ PrimitiveLiteral::Int128(_)),
            Type::Primitive(PrimitiveType::Decimal { scale: source, .. }),
            Type::Primitive(PrimitiveType::Decimal { scale: target, .. }),
        ) if source == target => value,
        _ => return Err(wrong()),
    };
    Ok(Some(Literal::Primitive(primitive)))
}

/// Preserve parent validity even with no children or all NULL children.
pub(super) fn materialize(
    values: &[Option<Literal>],
    children: &Fields,
    rows: usize,
) -> Result<ArrayRef, ConnectorError> {
    use crate::iceberg::spec::PrimitiveLiteral;
    use arrow::array::{FixedSizeBinaryBuilder, Time64MicrosecondArray};
    use arrow::datatypes::TimeUnit;

    let mut columns = Vec::with_capacity(children.len());
    for (value, child) in values.iter().zip(children) {
        let column: ArrayRef = match (value, child.data_type()) {
            (
                Some(Literal::Primitive(PrimitiveLiteral::Long(time))),
                DataType::Time64(TimeUnit::Microsecond),
            ) => Arc::new(Time64MicrosecondArray::from(vec![*time; rows])),
            (Some(Literal::Primitive(value)), DataType::FixedSizeBinary(width)) => {
                let uuid;
                let bytes = match value {
                    PrimitiveLiteral::Binary(bytes) => bytes.as_slice(),
                    PrimitiveLiteral::UInt128(value) => {
                        uuid = value.to_be_bytes();
                        &uuid
                    }
                    _ => {
                        return Err(super::corrupt(
                            "Iceberg mutation fixed partition value differs",
                        ));
                    }
                };
                let mut builder = FixedSizeBinaryBuilder::with_capacity(rows, *width);
                for _ in 0..rows {
                    builder
                        .append_value(bytes)
                        .map_err(|error| super::corrupt(error.to_string()))?;
                }
                Arc::new(builder.finish())
            }
            (value, _) => super::partition_constant(value.as_ref(), child, rows)?,
        };
        columns.push(column);
    }
    let row = StructArray::try_new_with_length(children.clone(), columns, None, rows)
        .map_err(|error| super::corrupt(format!("Iceberg mutation partition: {error}")))?;
    Ok(Arc::new(row))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::iceberg::spec::{PrimitiveLiteral, SortOrder, TableMetadataBuilder};
    use crate::typed_read::IcebergColumnHandle;
    use arrow::array::{Array, Int64Array, StringArray};

    fn spec(id: i32, fields: &[(i32, i32, &str)]) -> PartitionSpec {
        serde_json::from_value(serde_json::json!({
            "spec-id": id,
            "fields": fields.iter().map(|(field, source, name)| serde_json::json!({
                "field-id": field, "source-id": source, "name": name, "transform": "identity"
            })).collect::<Vec<_>>()
        }))
        .unwrap()
    }

    #[test]
    fn mutation_partition_preserves_dropped_source_and_original_spec_ordinals() {
        let old = Schema::builder()
            .with_fields([
                Arc::new(NestedField::optional(
                    1,
                    "region",
                    Type::Primitive(PrimitiveType::String),
                )),
                Arc::new(NestedField::optional(
                    2,
                    "bucket",
                    Type::Primitive(PrimitiveType::Int),
                )),
            ])
            .build()
            .unwrap();
        let initial_spec = spec(0, &[(1000, 1, "old_region"), (1001, 2, "bucket_part")]);
        let newer = Schema::builder()
            .with_fields([Arc::new(NestedField::optional(
                2,
                "bucket",
                Type::Primitive(PrimitiveType::Long),
            ))])
            .build()
            .unwrap();
        let metadata = TableMetadataBuilder::new(
            old,
            initial_spec.into_unbound(),
            SortOrder::unsorted_order(),
            "memory://warehouse/mutation".into(),
            FormatVersion::V3,
            Default::default(),
        )
        .unwrap()
        .add_partition_spec(PartitionSpec::unpartition_spec().into_unbound())
        .unwrap()
        .set_default_partition_spec(-1)
        .unwrap()
        .add_current_schema(newer)
        .unwrap()
        .build()
        .unwrap()
        .metadata;
        // Supply a source file with reverse physical ordering. The stable
        // signature comes from historical types, and the projection from its
        // actual spec ordinals.
        let old_spec = spec(0, &[(1001, 2, "bucket_part"), (1000, 1, "old_region")]);
        let schema = MutationMetadataSchema::try_new(&metadata, metadata.current_schema()).unwrap();
        assert_eq!(
            schema
                .partition_type()
                .fields()
                .iter()
                .map(|f| f.id)
                .collect::<Vec<_>>(),
            [1000, 1001]
        );
        assert_eq!(
            schema.spec_field().field_type.as_ref(),
            &Type::Primitive(PrimitiveType::Int)
        );
        assert!(schema.spec_field().required);
        assert!(!schema.partition_field().required);
        let handle = IcebergColumnHandle::base_column(schema.partition_field()).unwrap();
        let spec_handle = IcebergColumnHandle::base_column(schema.spec_field()).unwrap();
        let historical_values =
            Struct::from_iter([Some(Literal::int(7)), Some(Literal::string("past"))]);
        let (spec_field, spec_source) = bind(&spec_handle, &old_spec, &historical_values).unwrap();
        assert_eq!(spec_field.data_type(), &DataType::Int32);
        assert!(!spec_field.is_nullable());
        assert!(matches!(
            spec_source,
            super::super::IcebergColumnSource::MutationSpecId(0)
        ));
        let (field, super::super::IcebergColumnSource::MutationPartition(projected)) =
            bind(&handle, &old_spec, &historical_values).unwrap()
        else {
            panic!("expected bound partition constant")
        };
        let DataType::Struct(children) = field.data_type() else {
            panic!("expected Struct")
        };
        let batch = materialize(&projected, children, 2).unwrap();
        let array = batch.as_any().downcast_ref::<StructArray>().unwrap();
        assert_eq!(array.len(), 2);
        assert_eq!(array.null_count(), 0);
        assert_eq!(
            array
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(1),
            "past"
        );
        assert_eq!(
            array
                .column(1)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(1),
            7
        );
        let original_type = StructType::new(vec![
            Arc::new(NestedField::optional(
                1001,
                "bucket_part",
                Type::Primitive(PrimitiveType::Int),
            )),
            Arc::new(NestedField::optional(
                1000,
                "old_region",
                Type::Primitive(PrimitiveType::String),
            )),
        ]);
        let reverse_schema =
            MutationMetadataSchema::from_types(FormatVersion::V3, [&old_spec], |_| {
                Ok(StructType::new(vec![
                    Arc::new(NestedField::optional(
                        1001,
                        "bucket_part",
                        Type::Primitive(PrimitiveType::Long),
                    )),
                    Arc::new(NestedField::optional(
                        1000,
                        "old_region",
                        Type::Primitive(PrimitiveType::String),
                    )),
                ]))
            })
            .unwrap();
        assert_eq!(
            reverse_schema
                .decode_partition(&old_spec, &original_type, array, 0)
                .unwrap(),
            historical_values
        );
        assert_eq!(
            projected[1],
            Some(Literal::Primitive(PrimitiveLiteral::Long(7)))
        );
    }

    #[test]
    fn mutation_partition_empty_struct_is_present_and_null_parent_is_refused() {
        let spec = spec(5, &[]);
        let schema = MutationMetadataSchema::from_types(FormatVersion::V3, [&spec], |_| {
            Ok(StructType::new(vec![]))
        })
        .unwrap();
        let array = materialize(&[], &Fields::empty(), 3).unwrap();
        let array = array.as_any().downcast_ref::<StructArray>().unwrap();
        assert_eq!(array.len(), 3);
        assert_eq!(array.null_count(), 0);
        assert_eq!(
            schema
                .decode_partition(&spec, schema.partition_type(), array, 1)
                .unwrap(),
            Struct::empty()
        );
        let null = StructArray::new_empty_fields(3, Some(arrow::buffer::NullBuffer::new_null(3)));
        assert!(
            schema
                .decode_partition(&spec, schema.partition_type(), &null, 1)
                .is_err()
        );
    }
}
