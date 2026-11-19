// SPDX-License-Identifier: Apache-2.0

use novarocks_spi::connector::{
    ConnectorError, ConnectorErrorKind, ConnectorOperationControl, ConnectorRequestContext,
    DdlPropertyType, DdlPropertyValue,
};

use crate::iceberg::spec::{DataFileFormat, TableMetadata, TableProperties, Transform};

use super::projection::raw_key;

/// Project declared properties from the current, already selected metadata.
pub(crate) fn table_definition_properties(
    metadata: &TableMetadata,
    context: &ConnectorRequestContext,
) -> Result<Vec<(String, DdlPropertyValue)>, ConnectorError> {
    use DdlPropertyValue as Value;

    let declarations = super::table_property_declarations()
        .map_err(|error| ConnectorError::new(ConnectorErrorKind::Internal, error.to_string()))?;
    let raw = metadata.properties();
    let mut visible = Vec::new();
    let mut bytes = 0usize;
    for declaration in declarations
        .iter()
        .filter(|declaration| !declaration.hidden())
    {
        context.check_active()?;
        let name = declaration.name();
        let value = match name {
            "format" => {
                let format = raw
                    .get(TableProperties::PROPERTY_DEFAULT_FILE_FORMAT)
                    .map_or(
                        TableProperties::PROPERTY_DEFAULT_FILE_FORMAT_DEFAULT,
                        String::as_str,
                    )
                    .parse::<DataFileFormat>()
                    .map_err(|_| corrupt_property(name))?;
                if format == DataFileFormat::Puffin {
                    return Err(corrupt_property(name));
                }
                Some(Value::Varchar(format.to_string().to_ascii_uppercase()))
            }
            "format_version" => Some(Value::Integer(metadata.format_version() as i32)),
            "location" => (!metadata.location().is_empty())
                .then(|| Value::Varchar(metadata.location().to_owned())),
            "partitioning" => partitioning(metadata, context)?,
            _ => raw_key(name)
                .and_then(|key| raw.get(key))
                .map(|raw| raw_value(name, declaration.value_type(), raw))
                .transpose()?,
        };
        if let Some(value) = value {
            let payload = match &value {
                Value::Varchar(text) => text.len(),
                Value::ArrayVarchar(values) => values
                    .iter()
                    .flatten()
                    .fold(std::mem::size_of_val(values.as_slice()), |bytes, text| {
                        bytes.saturating_add(text.len())
                    }),
                _ => 0,
            };
            bytes = bytes
                .saturating_add(std::mem::size_of::<(String, Value)>())
                .saturating_add(name.len())
                .saturating_add(payload);
            if bytes > context.max_total_payload_bytes() {
                return Err(ConnectorError::new(
                    ConnectorErrorKind::ResourceExhausted,
                    "Iceberg definition properties exceed the request total payload budget",
                ));
            }
            visible.push((name.to_owned(), value));
        }
    }
    visible.sort_unstable_by(|left, right| left.0.cmp(&right.0));
    context.check_active()?;
    Ok(visible)
}

fn raw_value(
    name: &str,
    declared: DdlPropertyType,
    raw: &str,
) -> Result<DdlPropertyValue, ConnectorError> {
    use DdlPropertyValue as Value;

    match declared {
        DdlPropertyType::Boolean => raw
            .to_ascii_lowercase()
            .parse()
            .map(Value::Boolean)
            .map_err(|_| corrupt_property(name)),
        DdlPropertyType::Integer => raw
            .parse::<i32>()
            .ok()
            .filter(|value| *value >= 0)
            .map(Value::Integer)
            .ok_or_else(|| corrupt_property(name)),
        DdlPropertyType::Varchar
            if matches!(
                name,
                "parquet_writer_row_group_size" | "target_max_file_size"
            ) =>
        {
            let bytes = raw.parse::<u64>().map_err(|_| corrupt_property(name))?;
            Ok(Value::Varchar(format!("{bytes}B")))
        }
        DdlPropertyType::Varchar => Ok(Value::Varchar(raw.to_owned())),
        _ => Err(corrupt_property(name)),
    }
}

fn partitioning(
    metadata: &TableMetadata,
    context: &ConnectorRequestContext,
) -> Result<Option<DdlPropertyValue>, ConnectorError> {
    let spec = metadata.default_partition_spec();
    if spec.is_unpartitioned() {
        return Ok(None);
    }
    let schema = metadata.current_schema();
    let mut fields = Vec::with_capacity(spec.fields().len());
    for field in spec.fields() {
        context.check_active()?;
        let column = schema
            .name_by_field_id(field.source_id)
            .ok_or_else(|| corrupt_property("partitioning"))?;
        let mut bytes = column.bytes();
        let unquoted = bytes
            .next()
            .is_some_and(|first| first.is_ascii_lowercase() || first == b'_')
            && bytes.all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_');
        let column = if unquoted {
            column.to_owned()
        } else {
            format!("\"{}\"", column.replace('"', "\"\""))
        };
        let declaration = match field.transform {
            Transform::Identity => column,
            Transform::Bucket(width) => format!("bucket({column}, {width})"),
            Transform::Truncate(width) => format!("truncate({column}, {width})"),
            Transform::Year
            | Transform::Month
            | Transform::Day
            | Transform::Hour
            | Transform::Void => {
                format!("{}({column})", field.transform)
            }
            Transform::Unknown => {
                return Err(ConnectorError::new(
                    ConnectorErrorKind::Unsupported,
                    "Iceberg partition transform cannot be displayed",
                ));
            }
        };
        fields.push(Some(declaration));
    }
    Ok(Some(DdlPropertyValue::ArrayVarchar(fields)))
}

fn corrupt_property(name: &str) -> ConnectorError {
    ConnectorError::new(
        ConnectorErrorKind::CorruptData,
        format!("Iceberg metadata contains an invalid definition property '{name}'"),
    )
}
