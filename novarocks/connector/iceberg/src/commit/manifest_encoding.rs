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

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use crate::iceberg::spec::{
    DataFile, Datum, FormatVersion, Literal, ManifestMetadata, NestedFieldRef, PartitionSpec,
    PrimitiveLiteral, PrimitiveType, Schema, SchemaRef, Struct, StructType, TableMetadata,
};
use crate::iceberg::{Error, ErrorKind, Result};
use crate::manifest::partition_types::{legal_partition_type_join, resolved_partition_primitive};

use super::data_file::ManifestBounds;
use super::model::{EntryFacts, EntryIdentity, FrozenEntry, SeqField};
use super::staging::ManifestEntryWrite;

/// The actual storage schema and partition type of a manifest's entries.
/// Query projection types do not participate in manifest encoding.
pub(crate) struct ManifestWriteSource {
    schema: SchemaRef,
    spec: PartitionSpec,
    partition_type: StructType,
}

impl ManifestWriteSource {
    /// New position files have an independently signed historical tuple and
    /// no table-column bounds. The ordinary encoding chooser selects a real
    /// header whose partition type can represent that tuple.
    pub(crate) fn for_position_output(
        schema: SchemaRef,
        spec: PartitionSpec,
        partition_type: StructType,
    ) -> Self {
        Self {
            schema,
            spec,
            partition_type,
        }
    }

    pub(crate) fn from_header(header: &ManifestMetadata) -> Result<Self> {
        // The embedded schema carries its real ID. Java does not write the
        // redundant schema-id Avro metadata key.
        Self::new(header.schema.clone(), header.partition_spec.clone())
    }

    pub(crate) fn new(schema: SchemaRef, spec: PartitionSpec) -> Result<Self> {
        let partition_type = spec.partition_type(&schema)?;
        Ok(Self {
            schema,
            spec,
            partition_type,
        })
    }
}

pub(crate) struct SourcedManifestEntry {
    pub(crate) source: Arc<ManifestWriteSource>,
    pub(crate) entry: ManifestEntryWrite,
}

pub(super) struct ManifestWriteEncoding {
    pub(super) schema: SchemaRef,
    pub(super) spec: PartitionSpec,
    partition_type: StructType,
    version: FormatVersion,
}

pub(super) enum EncodedManifestEntry {
    Added {
        file: DataFile,
        data_sequence: SeqField,
    },
    Existing {
        file: DataFile,
        facts: EntryFacts,
    },
    Deleted {
        file: DataFile,
        facts: EntryFacts,
    },
}

impl ManifestWriteEncoding {
    pub(super) fn fixed(
        version: FormatVersion,
        schema: SchemaRef,
        spec: PartitionSpec,
    ) -> Result<Self> {
        let partition_type = spec.partition_type(&schema)?;
        Ok(Self {
            schema,
            spec,
            partition_type,
            version,
        })
    }

    pub(super) fn choose(
        metadata: &TableMetadata,
        spec: &PartitionSpec,
        entries: &[SourcedManifestEntry],
        mut check_active: impl FnMut() -> Result<()>,
    ) -> Result<Self> {
        let mut schemas = BTreeMap::new();
        for schema in metadata
            .schemas_iter()
            .chain(entries.iter().map(|entry| &entry.source.schema))
        {
            check_active()?;
            if let Some(previous) = schemas.insert(schema.schema_id(), schema)
                && previous != schema
            {
                return Err(invalid(format!(
                    "Conflicting definitions for manifest schema ID {}",
                    schema.schema_id()
                )));
            }
        }

        for entry in entries {
            check_active()?;
            if entry.source.spec != *spec {
                return Err(invalid(format!(
                    "Manifest source disagrees with partition spec {}",
                    spec.spec_id()
                )));
            }
            let file = validate_original_entry(&entry.entry, spec.spec_id())?;
            validate_partition(file.partition(), &entry.source.partition_type)?;
        }

        let version = metadata.format_version();
        let mut fallback = None;
        let mut encoding_error = None;
        'candidates: for schema in schemas.into_values() {
            check_active()?;
            let Ok(partition_type) = spec.partition_type(schema) else {
                continue;
            };
            let mut all_bounds_readable = true;
            for entry in entries {
                check_active()?;
                let file = entry.entry.file();
                if project_partition(
                    version,
                    file.partition(),
                    &entry.source.partition_type,
                    &partition_type,
                )
                .is_none()
                {
                    continue 'candidates;
                }
                match bounds_fit(
                    version,
                    schema,
                    metadata.current_schema(),
                    &entry.source.schema,
                    file,
                    &mut all_bounds_readable,
                ) {
                    Ok(true) => {}
                    Ok(false) => continue 'candidates,
                    Err(error) => {
                        encoding_error.get_or_insert(error);
                        continue 'candidates;
                    }
                }
            }
            let encoding = Self {
                schema: schema.clone(),
                spec: spec.clone(),
                partition_type,
                version,
            };
            if all_bounds_readable {
                return Ok(encoding);
            }
            fallback.get_or_insert(encoding);
        }
        fallback.ok_or_else(|| {
            encoding_error.unwrap_or_else(|| {
                invalid(format!(
                    "No compatible manifest schema for partition spec {}",
                    spec.spec_id()
                ))
            })
        })
    }

    pub(super) fn encode(
        &self,
        entry: ManifestEntryWrite,
        source: Option<&ManifestWriteSource>,
    ) -> Result<EncodedManifestEntry> {
        let original = validate_original_entry(&entry, self.spec.spec_id())?;
        let source_type = source
            .map(|source| &source.partition_type)
            .unwrap_or(&self.partition_type);
        let partition = project_partition(
            self.version,
            original.partition(),
            source_type,
            &self.partition_type,
        )
        .ok_or_else(|| invalid("Data file partition does not fit the selected manifest schema"))?;
        let bounds = normalize_bounds(self.version, &self.schema, original)?;
        let copy = |first_row_id| {
            if original.partition() == &partition
                && original.first_row_id() == first_row_id
                && bounds.is_none()
            {
                return Ok(original.clone());
            }
            super::data_file::clone_data_file_with_manifest_values(
                original,
                self.spec.spec_id(),
                partition,
                first_row_id,
                bounds,
            )
            .map_err(invalid)
        };
        Ok(match &entry {
            ManifestEntryWrite::Added(added) => EncodedManifestEntry::Added {
                file: copy(original.first_row_id())?,
                data_sequence: added.data_sequence(),
            },
            ManifestEntryWrite::Existing { frozen, .. } => EncodedManifestEntry::Existing {
                file: copy(frozen.facts().first_row_id)?,
                facts: frozen.facts().clone(),
            },
            ManifestEntryWrite::Deleted { frozen, .. } => EncodedManifestEntry::Deleted {
                file: copy(frozen.facts().first_row_id)?,
                facts: frozen.facts().clone(),
            },
        })
    }
}

fn validate_original_entry(entry: &ManifestEntryWrite, spec_id: i32) -> Result<&DataFile> {
    match entry {
        ManifestEntryWrite::Added(added) => {
            if added.partition_spec_id() != spec_id {
                return Err(invalid(
                    "Added content partition spec does not match its manifest",
                ));
            }
            Ok(added.file())
        }
        ManifestEntryWrite::Existing { file, frozen }
        | ManifestEntryWrite::Deleted { file, frozen } => {
            validate_original_file(file, frozen, spec_id)?;
            Ok(file)
        }
    }
}

fn validate_original_file(file: &DataFile, frozen: &FrozenEntry, spec_id: i32) -> Result<()> {
    let facts = frozen.facts();
    if EntryIdentity::try_from(file)? != *frozen.identity()
        || facts.partition_spec_id != spec_id
        || facts.partition != *file.partition()
        || facts.record_count != file.record_count()
    {
        return Err(invalid(
            "Carried data file does not match frozen entry facts",
        ));
    }
    Ok(())
}

fn validate_partition(partition: &Struct, storage_type: &StructType) -> Result<()> {
    if partition.fields().len() != storage_type.fields().len()
        || partition
            .fields()
            .iter()
            .zip(storage_type.fields())
            .any(|(value, field)| match value {
                None => false,
                Some(Literal::Primitive(value)) => !field
                    .field_type
                    .as_primitive_type()
                    .is_some_and(|ty| ty.compatible(value)),
                Some(_) => true,
            })
    {
        return Err(invalid(
            "Manifest entry partition does not match its source storage type",
        ));
    }
    Ok(())
}

fn project_partition(
    version: FormatVersion,
    partition: &Struct,
    source: &StructType,
    target: &StructType,
) -> Option<Struct> {
    if partition.fields().len() != source.fields().len()
        || source.fields().len() != target.fields().len()
    {
        return None;
    }
    partition
        .fields()
        .iter()
        .zip(source.fields().iter().zip(target.fields()))
        .map(|(value, (source, target))| {
            let Some(value) = value else {
                return Some(None);
            };
            let Literal::Primitive(value) = value else {
                return None;
            };
            let source = source.field_type.as_primitive_type()?;
            let target = target.field_type.as_primitive_type()?;
            if !source.compatible(value) || !can_promote(version, source, target) {
                return None;
            }
            let value = resolved_partition_primitive(value, target);
            target
                .compatible(&value)
                .then(|| Some(Literal::Primitive(value.into_owned())))
        })
        .collect::<Option<Struct>>()
}

fn can_promote(version: FormatVersion, source: &PrimitiveType, target: &PrimitiveType) -> bool {
    source == target || legal_partition_type_join(version, source, target).as_ref() == Some(target)
}

fn bounds_field(schema: &Schema, id: i32) -> Option<&NestedFieldRef> {
    schema
        .field_by_id(id)
        .or_else(|| crate::iceberg::metadata_columns::get_metadata_field(id).ok())
}

fn bounds_fit(
    version: FormatVersion,
    schema: &Schema,
    current_schema: &Schema,
    source_schema: &Schema,
    file: &DataFile,
    all_bounds_readable: &mut bool,
) -> Result<bool> {
    for (id, value) in file.lower_bounds().iter().chain(file.upper_bounds()) {
        let Some(field) = bounds_field(schema, *id) else {
            if bounds_field(current_schema, *id).is_some()
                || bounds_field(source_schema, *id).is_none()
            {
                return Ok(false);
            }
            // Keep the bytes even though the SDK will ignore this dropped
            // field when reading a manifest whose header lacks the field ID.
            *all_bounds_readable = false;
            continue;
        };
        let Some(target) = field.field_type.as_primitive_type() else {
            return Ok(false);
        };
        if !bounds_fit_type(version, value, target)? {
            return Ok(false);
        }
    }
    Ok(true)
}

fn bounds_fit_type(version: FormatVersion, value: &Datum, target: &PrimitiveType) -> Result<bool> {
    let source = value.data_type();
    if source == target {
        return Ok(true);
    }
    if !can_promote(version, source, target) {
        return Ok(false);
    }
    if normalize_date_bound(version, value, target)?.is_some() {
        return Ok(true);
    }
    // The other supported promotions retain their original bounds bytes.
    Ok(value
        .to_bytes()
        .is_ok_and(|bytes| Datum::try_from_bytes(&bytes, target.clone()).is_ok()))
}

fn normalize_date_bound(
    version: FormatVersion,
    value: &Datum,
    target: &PrimitiveType,
) -> Result<Option<Datum>> {
    let (ticks_per_day, timestamp): (i64, fn(i64) -> Datum) =
        match (version, value.data_type(), target) {
            (FormatVersion::V3, PrimitiveType::Date, PrimitiveType::Timestamp) => {
                (86_400_000_000, Datum::timestamp_micros)
            }
            (FormatVersion::V3, PrimitiveType::Date, PrimitiveType::TimestampNs) => {
                (86_400_000_000_000, Datum::timestamp_nanos)
            }
            _ => return Ok(None),
        };
    let PrimitiveLiteral::Int(days) = value.literal() else {
        return Err(invalid("DATE manifest bound must contain a day offset"));
    };
    let ticks = i64::from(*days).checked_mul(ticks_per_day).ok_or_else(|| {
        invalid(format!(
            "DATE manifest bound {days} days overflows {target}"
        ))
    })?;
    Ok(Some(timestamp(ticks)))
}

fn normalize_bounds(
    version: FormatVersion,
    schema: &Schema,
    file: &DataFile,
) -> Result<Option<ManifestBounds>> {
    let normalize_map = |bounds: &HashMap<i32, Datum>| {
        let mut normalized = None;
        for (id, value) in bounds {
            let target =
                bounds_field(schema, *id).and_then(|field| field.field_type.as_primitive_type());
            if let Some(target) = target
                && let Some(widened) = normalize_date_bound(version, value, target)?
            {
                normalized
                    .get_or_insert_with(|| bounds.clone())
                    .insert(*id, widened);
            }
        }
        Ok::<_, Error>(normalized)
    };
    let lower = normalize_map(file.lower_bounds())?;
    let upper = normalize_map(file.upper_bounds())?;
    if lower.is_none() && upper.is_none() {
        return Ok(None);
    }
    Ok(Some(ManifestBounds {
        lower: lower.unwrap_or_else(|| file.lower_bounds().clone()),
        upper: upper.unwrap_or_else(|| file.upper_bounds().clone()),
    }))
}

fn invalid(message: impl Into<String>) -> Error {
    Error::new(ErrorKind::DataInvalid, message)
}

#[cfg(test)]
mod tests {
    use apache_avro::{Reader, types::Value};

    use super::*;
    use crate::commit::staging::{TestWriter, write_sourced_manifest};
    use crate::iceberg::spec::{
        DataContentType, DataFileBuilder, DataFileFormat, Manifest, ManifestContentType,
        NestedField, SortOrder, TableMetadataBuilder, Transform,
    };

    fn schema(fields: &[(&str, PrimitiveType)]) -> Schema {
        Schema::builder()
            .with_fields(fields.iter().enumerate().map(|(i, (name, ty))| {
                Arc::new(NestedField::optional(
                    i as i32 + 1,
                    *name,
                    crate::iceberg::spec::Type::Primitive(ty.clone()),
                ))
            }))
            .build()
            .unwrap()
    }

    fn table(schema: Schema, spec: PartitionSpec) -> TableMetadata {
        TableMetadataBuilder::new(
            schema,
            spec,
            SortOrder::unsorted_order(),
            "s3://bucket/table".into(),
            FormatVersion::V3,
            HashMap::new(),
        )
        .unwrap()
        .build()
        .unwrap()
        .metadata
    }

    fn file(name: &str, partition: Struct, bounds: HashMap<i32, Datum>) -> DataFile {
        DataFileBuilder::default()
            .content(DataContentType::Data)
            .file_path(format!("s3://bucket/{name}.parquet"))
            .file_format(DataFileFormat::Parquet)
            .partition(partition)
            .lower_bounds(bounds)
            .record_count(1)
            .file_size_in_bytes(100)
            .build()
            .unwrap()
    }

    fn existing(source: &Arc<ManifestWriteSource>, file: &DataFile) -> SourcedManifestEntry {
        let frozen = FrozenEntry::new(
            EntryIdentity::try_from(file).unwrap(),
            EntryFacts {
                added_snapshot_id: Some(2),
                data_sequence: Some(1),
                file_sequence: Some(2),
                partition_spec_id: source.spec.spec_id(),
                partition: file.partition().clone(),
                record_count: file.record_count(),
                first_row_id: Some(25),
            },
        )
        .unwrap();
        SourcedManifestEntry {
            source: source.clone(),
            entry: ManifestEntryWrite::Existing {
                file: file.clone(),
                frozen,
            },
        }
    }

    async fn read_output(
        writer: &TestWriter,
        metadata: &TableMetadata,
        spec: &PartitionSpec,
        entries: Vec<SourcedManifestEntry>,
    ) -> (crate::iceberg::spec::ManifestFile, Vec<u8>) {
        use crate::commit::model::ArtifactWriter;
        let output = write_sourced_manifest(
            writer,
            metadata,
            3,
            spec,
            ManifestContentType::Data,
            entries,
        )
        .await
        .unwrap();
        let bytes = writer
            .file_io()
            .new_input(&output.manifest_path)
            .unwrap()
            .read()
            .await
            .unwrap();
        (output, bytes.to_vec())
    }

    fn field<'a>(value: &'a Value, name: &str) -> &'a Value {
        match value {
            Value::Union(_, value) => field(value, name),
            Value::Record(fields) => &fields.iter().find(|(key, _)| key == name).unwrap().1,
            _ => panic!("expected record"),
        }
    }

    fn lower_bound_widths(entry: Value) -> BTreeMap<i32, usize> {
        let mut bounds = field(field(&entry, "data_file"), "lower_bounds");
        if let Value::Union(_, value) = bounds {
            bounds = value;
        }
        let Value::Array(items) = bounds else {
            panic!("expected lower bound array");
        };
        items
            .iter()
            .map(|item| {
                let Value::Int(key) = field(item, "key") else {
                    panic!("expected int key");
                };
                let Value::Bytes(bytes) = field(item, "value") else {
                    panic!("expected bound bytes");
                };
                (*key, bytes.len())
            })
            .collect()
    }

    #[tokio::test]
    async fn same_spec_promotes_partition_literals_but_keeps_old_bound_width() {
        let original_schema = schema(&[("i", PrimitiveType::Int), ("f", PrimitiveType::Float)]);
        let spec = PartitionSpec::builder(Arc::new(original_schema.clone()))
            .add_partition_field("i", "partition_i", Transform::Identity)
            .unwrap()
            .add_partition_field("f", "partition_f", Transform::Identity)
            .unwrap()
            .build()
            .unwrap();
        let original = table(original_schema, spec);
        let spec = original.default_partition_spec().as_ref().clone();
        let old_source = Arc::new(
            ManifestWriteSource::new(original.current_schema().clone(), spec.clone()).unwrap(),
        );
        let promoted = original
            .into_builder(None)
            .add_schema(schema(&[
                ("i", PrimitiveType::Long),
                ("f", PrimitiveType::Double),
            ]))
            .unwrap()
            .set_current_schema(-1)
            .unwrap()
            .build()
            .unwrap()
            .metadata;
        let new_source = Arc::new(
            ManifestWriteSource::new(promoted.current_schema().clone(), spec.clone()).unwrap(),
        );
        let original_partition =
            Struct::from_iter([Some(Literal::int(4)), Some(Literal::float(0.5_f32))]);
        let old_file = file(
            "old",
            original_partition.clone(),
            HashMap::from([(1, Datum::int(4)), (2, Datum::float(0.5_f32))]),
        );
        let new_partition = Struct::from_iter([
            Some(Literal::long(3_000_000_000_i64)),
            Some(Literal::double(0.125_f64)),
        ]);
        let new_file = file(
            "new",
            new_partition.clone(),
            HashMap::from([
                (1, Datum::long(3_000_000_000_i64)),
                (2, Datum::double(0.125_f64)),
            ]),
        );
        let entries = vec![
            existing(&old_source, &old_file),
            existing(&new_source, &new_file),
        ];
        let writer = TestWriter::new();
        let (output, bytes) = read_output(&writer, &promoted, &spec, entries).await;
        let raw = Manifest::parse_avro(&bytes).unwrap();
        assert_eq!(
            raw.metadata().schema.schema_id(),
            promoted.current_schema_id()
        );
        assert_eq!(
            raw.entries()[0].data_file().partition(),
            &Struct::from_iter([Some(Literal::long(4)), Some(Literal::double(0.5_f64))])
        );
        assert_eq!(raw.entries()[1].data_file().partition(), &new_partition);
        assert_eq!(raw.entries()[0].data_file().first_row_id(), Some(25));
        assert_eq!(old_file.partition(), &original_partition);
        assert_eq!(old_file.first_row_id(), None);
        let mut avro = Reader::new(bytes.as_slice()).unwrap();
        assert_eq!(
            lower_bound_widths(avro.next().unwrap().unwrap()),
            BTreeMap::from([(1, 4), (2, 4)])
        );
        assert_eq!(
            lower_bound_widths(avro.next().unwrap().unwrap()),
            BTreeMap::from([(1, 8), (2, 8)])
        );
        let summaries = output.partitions.unwrap();
        assert_eq!(
            summaries[0].lower_bound.as_ref().unwrap().as_slice(),
            &4_i64.to_le_bytes()
        );
        assert_eq!(
            summaries[0].upper_bound.as_ref().unwrap().as_slice(),
            &3_000_000_000_i64.to_le_bytes()
        );
        assert_eq!(
            summaries[1].lower_bound.as_ref().unwrap().as_slice(),
            &0.125_f64.to_le_bytes()
        );
        assert_eq!(
            summaries[1].upper_bound.as_ref().unwrap().as_slice(),
            &0.5_f64.to_le_bytes()
        );
    }

    #[tokio::test]
    async fn old_bucket_uses_original_schema_even_when_query_type_widened() {
        let original_schema = schema(&[
            ("removed", PrimitiveType::Int),
            ("retained", PrimitiveType::Int),
        ]);
        let spec = PartitionSpec::builder(Arc::new(original_schema.clone()))
            .add_partition_field("removed", "bucket_removed", Transform::Bucket(4))
            .unwrap()
            .add_partition_field("retained", "partition_retained", Transform::Identity)
            .unwrap()
            .build()
            .unwrap();
        let original = table(original_schema, spec);
        let spec = original.default_partition_spec().as_ref().clone();
        let old_id = original.current_schema_id();
        let source = Arc::new(
            ManifestWriteSource::new(original.current_schema().clone(), spec.clone()).unwrap(),
        );
        let new_schema = Schema::builder()
            .with_fields([Arc::new(NestedField::optional(
                2,
                "retained",
                crate::iceberg::spec::Type::Primitive(PrimitiveType::Long),
            ))])
            .build()
            .unwrap();
        let evolved = original
            .into_builder(None)
            .add_partition_spec(PartitionSpec::unpartition_spec().into_unbound())
            .unwrap()
            .set_default_partition_spec(-1)
            .unwrap()
            .add_schema(new_schema)
            .unwrap()
            .set_current_schema(-1)
            .unwrap()
            .build()
            .unwrap()
            .metadata;
        assert!(evolved.current_schema().field_by_id(1).is_none());

        let partition = Struct::from_iter([Some(Literal::int(2)), Some(Literal::int(5))]);
        let file = file("historical", partition.clone(), HashMap::new());
        let writer = TestWriter::new();
        let (_, bytes) =
            read_output(&writer, &evolved, &spec, vec![existing(&source, &file)]).await;
        let output = Manifest::parse_avro(&bytes).unwrap();
        assert_eq!(output.metadata().schema.schema_id(), old_id);
        assert_eq!(output.metadata().partition_spec, spec);
        assert_eq!(output.entries()[0].data_file().partition(), &partition);
    }

    #[test]
    fn source_tuple_is_checked_before_output_projection() {
        let original_schema = schema(&[("i", PrimitiveType::Int)]);
        let spec = PartitionSpec::builder(Arc::new(original_schema.clone()))
            .add_partition_field("i", "partition_i", Transform::Identity)
            .unwrap()
            .build()
            .unwrap();
        let table = table(original_schema, spec);
        let spec = table.default_partition_spec().as_ref().clone();
        let source = Arc::new(
            ManifestWriteSource::new(table.current_schema().clone(), spec.clone()).unwrap(),
        );
        let partition = Struct::from_iter([Some(Literal::int(2))]);
        let file = file("old", partition, HashMap::new());
        let mut entry = existing(&source, &file);
        let ManifestEntryWrite::Existing { frozen, .. } = &mut entry.entry else {
            panic!()
        };
        let mut facts = frozen.facts().clone();
        facts.partition = Struct::from_iter([Some(Literal::long(2))]);
        *frozen = FrozenEntry::new(frozen.identity().clone(), facts).unwrap();
        let error = ManifestWriteEncoding::choose(&table, &spec, &[entry], || Ok(()))
            .err()
            .unwrap();
        assert!(
            error
                .to_string()
                .contains("does not match frozen entry facts")
        );
    }

    #[tokio::test]
    async fn prefer_a_schema_with_all_available_bounds_over_an_earlier_partial_one() {
        let original = table(
            schema(&[("active", PrimitiveType::Int)]),
            PartitionSpec::unpartition_spec(),
        );
        let active_id = original.current_schema_id();
        let historical = original
            .into_builder(None)
            .add_current_schema(schema(&[
                ("active", PrimitiveType::Int),
                ("dropped", PrimitiveType::Int),
            ]))
            .unwrap()
            .build()
            .unwrap()
            .metadata;
        let historical_schema = historical.current_schema().clone();
        let current = historical
            .into_builder(None)
            .set_current_schema(active_id)
            .unwrap()
            .build()
            .unwrap()
            .metadata;
        let spec = current.default_partition_spec().as_ref().clone();
        let old =
            Arc::new(ManifestWriteSource::new(historical_schema.clone(), spec.clone()).unwrap());
        let new = Arc::new(
            ManifestWriteSource::new(current.current_schema().clone(), spec.clone()).unwrap(),
        );
        let old_file = file("old", Struct::empty(), HashMap::from([(2, Datum::int(-7))]));
        let new_file = file("new", Struct::empty(), HashMap::from([(1, Datum::int(9))]));
        let (_, bytes) = read_output(
            &TestWriter::new(),
            &current,
            &spec,
            vec![existing(&old, &old_file), existing(&new, &new_file)],
        )
        .await;
        let manifest = Manifest::parse_avro(&bytes).unwrap();
        assert_eq!(
            manifest.metadata().schema.schema_id(),
            historical_schema.schema_id()
        );
        assert_eq!(
            manifest.entries()[0].data_file().lower_bounds(),
            &HashMap::from([(2, Datum::int(-7))])
        );
        assert_eq!(
            manifest.entries()[1].data_file().lower_bounds(),
            &HashMap::from([(1, Datum::int(9))])
        );
    }

    #[tokio::test]
    async fn incompatible_history_keeps_current_bound_fields_readable_and_writes_dropped_bytes() {
        let original = table(
            schema(&[("dropped", PrimitiveType::Int)]),
            PartitionSpec::unpartition_spec(),
        );
        let original_schema = original.current_schema().clone();
        let new_schema = Schema::builder()
            .with_fields([Arc::new(NestedField::optional(
                2,
                "active",
                crate::iceberg::spec::Type::Primitive(PrimitiveType::Long),
            ))])
            .build()
            .unwrap();
        let current = original
            .into_builder(None)
            .add_current_schema(new_schema)
            .unwrap()
            .build()
            .unwrap()
            .metadata;
        let spec = current.default_partition_spec().as_ref().clone();
        let old = Arc::new(ManifestWriteSource::new(original_schema, spec.clone()).unwrap());
        let new = Arc::new(
            ManifestWriteSource::new(current.current_schema().clone(), spec.clone()).unwrap(),
        );
        let old_file = file("old", Struct::empty(), HashMap::from([(1, Datum::int(-7))]));
        let new_file = file("new", Struct::empty(), HashMap::from([(2, Datum::long(9))]));
        let (_, bytes) = read_output(
            &TestWriter::new(),
            &current,
            &spec,
            vec![existing(&old, &old_file), existing(&new, &new_file)],
        )
        .await;
        let manifest = Manifest::parse_avro(&bytes).unwrap();
        assert_eq!(
            manifest.metadata().schema.schema_id(),
            current.current_schema_id()
        );
        assert!(manifest.entries()[0].data_file().lower_bounds().is_empty());
        assert_eq!(
            manifest.entries()[1].data_file().lower_bounds(),
            &HashMap::from([(2, Datum::long(9))])
        );
        let mut avro = Reader::new(bytes.as_slice()).unwrap();
        assert_eq!(
            lower_bound_widths(avro.next().unwrap().unwrap()),
            BTreeMap::from([(1, 4)])
        );
        assert_eq!(
            lower_bound_widths(avro.next().unwrap().unwrap()),
            BTreeMap::from([(2, 8)])
        );
    }
}
