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

use std::sync::Arc;

use arrow::datatypes::{DataType, FieldRef, Schema};
use parquet::arrow::ProjectionMask;
use parquet::arrow::arrow_reader::ArrowReaderMetadata;
use parquet::basic::{ConvertedType, Encoding, LogicalType, PageType};
use parquet::file::metadata::{ColumnChunkMetaData, ParquetMetaData};
use parquet::schema::types::ColumnDescriptor;

use crate::{FileError, FileErrorKind, FileResult, ParquetInterpretationPolicy, ReaderSizing};

#[cfg(test)]
mod tests;

// Design: ADR-0244 (docs/adr/ADR-0244-footer-derived-parquet-reader-representation.md)
pub(super) fn decoder_layout_and_rows(
    metadata: ArrowReaderMetadata,
    projected_roots: &[usize],
    row_groups: &[usize],
    representation_groups: &[usize],
    policy: ParquetInterpretationPolicy,
    sizing: ReaderSizing,
) -> FileResult<(ArrowReaderMetadata, usize)> {
    let parquet = metadata.parquet_schema();
    let projection = ProjectionMask::roots(parquet, projected_roots.iter().copied());
    let mut leaves = parquet
        .columns()
        .iter()
        .enumerate()
        .filter(|(index, _)| projection.leaf_included(*index))
        .map(|(index, column)| (index, column.as_ref()));
    let mut fields = metadata.schema().fields().to_vec();
    let mut widths = Vec::new();
    let mut all_leaves_matched = true;
    let derive = matches!(
        policy,
        ParquetInterpretationPolicy::DerivedByteRepresentation { .. }
    );
    let keep_dictionary = matches!(
        policy,
        ParquetInterpretationPolicy::DerivedByteRepresentation {
            preserve_top_level_dictionary: true,
        }
    );

    for &root in projected_roots {
        fields[root] = map_field_leaves(
            &fields[root],
            derive,
            true,
            &mut |field, ordinary, is_root| {
                let Some((index, column)) = leaves.next() else {
                    // An Arrow hint can describe a shape for which the physical
                    // leaf count gives no sizing estimate.
                    all_leaves_matched = false;
                    return Arc::clone(field);
                };
                all_leaves_matched &= parquet.get_column_root_idx(index) == root;
                let carrier = if ordinary {
                    derived_byte_carrier(
                        field.data_type(),
                        column,
                        keep_dictionary && is_root,
                        || {
                            representation_groups.iter().all(|&row_group| {
                                let group = metadata.metadata().row_group(row_group);
                                group.num_rows() == 0 || whole_chunk_dictionary(group.column(index))
                            })
                        },
                    )
                } else {
                    None
                };
                let width = ColumnWidth::for_carrier(carrier.as_ref().unwrap_or(field.data_type()));
                widths.push((index, width));
                match carrier {
                    Some(carrier) => Arc::new(field.as_ref().clone().with_data_type(carrier)),
                    None => Arc::clone(field),
                }
            },
        );
    }
    // Use the requested row limit when a hint prevents an exact estimate.
    // The representation option works from the standard schema and must
    // retain a bijection with the physical leaves.
    all_leaves_matched &= leaves.next().is_none();
    if !all_leaves_matched && derive {
        return Err(FileError::new(
            FileErrorKind::Internal,
            "derived Parquet schema does not match the projected physical leaf count",
        ));
    }
    let estimate = if all_leaves_matched {
        estimate_row_bits(metadata.metadata(), row_groups, &widths)
    } else {
        None
    };
    let batch_rows = decoder_batch_rows(sizing, estimate);
    if !derive || fields.as_slice() == &metadata.schema().fields()[..] {
        return Ok((metadata, batch_rows));
    }

    let schema = Schema::new_with_metadata(fields, metadata.schema().metadata().clone());
    let options = policy.arrow_options().with_schema(Arc::new(schema));
    let metadata = ArrowReaderMetadata::try_new(Arc::clone(metadata.metadata()), options).map_err(
        |error| {
            FileError::with_source(
                FileErrorKind::Internal,
                "verify Parquet decoder schema derived from the standard schema",
                error,
            )
        },
    )?;
    Ok((metadata, batch_rows))
}

fn decoder_batch_rows(sizing: ReaderSizing, row_bits: Option<u128>) -> usize {
    let max_rows = sizing.max_read_rows.get();
    match row_bits {
        Some(width) if width > 0 => {
            let bit_target = 8 * u128::from(sizing.max_read_bytes);
            let rows = (bit_target / width).max(1).min(max_rows as u128);
            usize::try_from(rows).expect("estimated rows are bounded by the requested limit")
        }
        _ => max_rows,
    }
}

/// Visit physical leaves in schema order. An extension owns the layout of its
/// entire storage subtree, including byte-array children.
fn map_field_leaves(
    field: &FieldRef,
    ordinary: bool,
    root: bool,
    leaf: &mut impl FnMut(&FieldRef, bool, bool) -> FieldRef,
) -> FieldRef {
    let ordinary = ordinary && field.extension_type_name().is_none();
    let data_type = match field.data_type() {
        DataType::Struct(fields) => DataType::Struct(
            fields
                .iter()
                .map(|field| map_field_leaves(field, ordinary, false, leaf))
                .collect::<Vec<_>>()
                .into(),
        ),
        DataType::List(item) => DataType::List(map_field_leaves(item, ordinary, false, leaf)),
        DataType::ListView(item) => {
            DataType::ListView(map_field_leaves(item, ordinary, false, leaf))
        }
        DataType::LargeList(item) => {
            DataType::LargeList(map_field_leaves(item, ordinary, false, leaf))
        }
        DataType::LargeListView(item) => {
            DataType::LargeListView(map_field_leaves(item, ordinary, false, leaf))
        }
        DataType::FixedSizeList(item, len) => {
            DataType::FixedSizeList(map_field_leaves(item, ordinary, false, leaf), *len)
        }
        DataType::Map(entries, sorted) => {
            DataType::Map(map_field_leaves(entries, ordinary, false, leaf), *sorted)
        }
        _ => return leaf(field, ordinary, root),
    };
    if &data_type == field.data_type() {
        Arc::clone(field)
    } else {
        Arc::new(field.as_ref().clone().with_data_type(data_type))
    }
}

fn derived_byte_carrier(
    data_type: &DataType,
    column: &ColumnDescriptor,
    keep_dictionary: bool,
    dictionary_encoded: impl FnOnce() -> bool,
) -> Option<DataType> {
    match (
        data_type,
        column.logical_type_ref(),
        column.converted_type(),
    ) {
        (DataType::Utf8, Some(LogicalType::String), _)
        | (DataType::Utf8, None, ConvertedType::UTF8) => {
            if keep_dictionary && dictionary_encoded() {
                Some(DataType::Dictionary(
                    Box::new(DataType::Int32),
                    Box::new(DataType::Utf8),
                ))
            } else {
                Some(DataType::Utf8View)
            }
        }
        (DataType::Binary, None, ConvertedType::NONE) => Some(DataType::BinaryView),
        _ => None,
    }
}

fn is_dictionary(encoding: Encoding) -> bool {
    matches!(
        encoding,
        Encoding::PLAIN_DICTIONARY | Encoding::RLE_DICTIONARY
    )
}

#[expect(deprecated, reason = "Parquet v1 can use BIT_PACKED for levels")]
fn whole_chunk_dictionary(column: &ColumnChunkMetaData) -> bool {
    if column.dictionary_page_offset().is_none() {
        return false;
    }
    if let Some(mask) = column.page_encoding_stats_mask() {
        return mask.as_i32() != 0 && mask.encodings().all(is_dictionary);
    }
    if let Some(stats) = column.page_encoding_stats() {
        let mut pages = stats
            .iter()
            .filter(|entry| {
                matches!(
                    entry.page_type,
                    PageType::DATA_PAGE | PageType::DATA_PAGE_V2
                ) && entry.count > 0
            })
            .peekable();
        return pages.peek().is_some() && pages.all(|entry| is_dictionary(entry.encoding));
    }
    // The legacy v1 footer can prove dictionary-only data without page stats.
    column.encodings_mask().is_set(Encoding::PLAIN_DICTIONARY)
        && column.encodings().all(|encoding| {
            matches!(
                encoding,
                Encoding::PLAIN_DICTIONARY | Encoding::RLE | Encoding::BIT_PACKED
            )
        })
}

#[derive(Clone, Copy)]
enum ColumnWidth {
    Bits(u64),
    BytesWithBacking {
        descriptor_bytes: u64,
        backing: BackingEstimate,
    },
    Unknown,
}

#[derive(Clone, Copy)]
enum BackingEstimate {
    Plain,
    View,
    Dictionary,
}

impl ColumnWidth {
    fn for_carrier(carrier: &DataType) -> Self {
        let (descriptor_bytes, backing) = match carrier {
            DataType::Null => return Self::Bits(0),
            DataType::Boolean => return Self::Bits(1),
            DataType::FixedSizeBinary(bytes) => {
                return u64::try_from(*bytes).map_or(Self::Unknown, |bytes| Self::Bits(8 * bytes));
            }
            DataType::Utf8 | DataType::Binary => (4, BackingEstimate::Plain),
            DataType::LargeUtf8 | DataType::LargeBinary => (8, BackingEstimate::Plain),
            DataType::Utf8View | DataType::BinaryView => (16, BackingEstimate::View),
            DataType::Dictionary(key, _) => {
                let Some(key_width) = key.primitive_width() else {
                    return Self::Unknown;
                };
                (key_width as u64, BackingEstimate::Dictionary)
            }
            value => {
                return value
                    .primitive_width()
                    .map_or(Self::Unknown, |width| Self::Bits(8 * width as u64));
            }
        };
        Self::BytesWithBacking {
            descriptor_bytes,
            backing,
        }
    }

    fn column_bits(self, column: &ColumnChunkMetaData, rows: u128) -> Option<u128> {
        let occurrences = if column.column_descr().max_rep_level() > 0 {
            u128::try_from(column.num_values()).ok()?
        } else {
            rows
        };
        match self {
            Self::Bits(bits) => occurrences.checked_mul(u128::from(bits)),
            Self::BytesWithBacking {
                descriptor_bytes,
                backing,
            } => {
                let descriptors = occurrences.checked_mul(u128::from(descriptor_bytes))?;
                // Encoded page size is a proxy for shared backing, with
                // the dictionary charged once per column chunk. For plain
                // byte arrays use the decoded payload statistic when present.
                let encoded = column.uncompressed_size();
                let estimate = match backing {
                    BackingEstimate::Dictionary => encoded,
                    BackingEstimate::View if whole_chunk_dictionary(column) => encoded,
                    BackingEstimate::View | BackingEstimate::Plain => {
                        column.unencoded_byte_array_data_bytes().unwrap_or(encoded)
                    }
                };
                descriptors
                    .checked_add(u128::try_from(estimate).ok()?)?
                    .checked_mul(8)
            }
            Self::Unknown => None,
        }
    }
}

fn estimate_row_bits(
    metadata: &ParquetMetaData,
    row_groups: &[usize],
    widths: &[(usize, ColumnWidth)],
) -> Option<u128> {
    let mut max_width = 0;
    for &row_group in row_groups {
        let group = metadata.row_group(row_group);
        let rows = u128::try_from(group.num_rows()).ok()?;
        if rows == 0 {
            continue;
        }
        let bits = widths.iter().try_fold(0u128, |total, &(column, width)| {
            total.checked_add(width.column_bits(group.column(column), rows)?)
        })?;
        max_width = max_width.max(bits.div_ceil(rows));
    }
    Some(max_width)
}
