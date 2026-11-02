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

//! Before-allocation bounds for signed COW layout normalization.
//! The shared value plan authors expansion and output geometry once. This
//! boundary adds its existing independent safe-copy coexistence and metadata
//! bounds; it never rescans dictionary keys or chooses another conversion.
#[cfg(test)]
use arrow::array::ArrayRef;
use arrow::{
    array::Array,
    datatypes::{DataType, Field, Schema, TimeUnit},
    record_batch::RecordBatch,
};
use novarocks_spi::connector::{
    ConnectorError, ConnectorErrorKind, ConnectorRowConversionFootprint,
};
const INPUT_BYTES: usize = 32 * 1024 * 1024;
const SELECTION_BYTES: usize = 64 * 1024 * 1024;
pub(super) const TRANSFORM_BYTES: usize = 256 * 1024 * 1024;
const METADATA_BYTES: usize = 16 * 1024 * 1024;
const MAX_NODES: usize = 4096;
const MAX_DEPTH: usize = 64;
const MAX_ROWS: usize = 1_048_576;
// ArrayData, builders, Arc headers, child vectors and bounded error formatting.
// This fixed allowance never stands in for value/offset/payload allocations.
const NODE_HEADERS: usize = 2048;
#[derive(Clone, Copy, Debug)]
pub(super) struct CowSignedLayoutFootprint {
    pub input_bytes: usize,
    pub output_bytes: usize,
    pub temporary_bytes: usize,
    pub copy_peak_bytes: usize,
    pub peak_bytes: usize,
}
impl CowSignedLayoutFootprint {
    #[cfg(test)]
    pub(super) fn for_batch(
        schema: &Schema,
        batch: &RecordBatch,
        retained: usize,
        scratch: usize,
        other: usize,
    ) -> Result<Self, ConnectorError> {
        Self::with_limit(schema, batch, retained, scratch, other, TRANSFORM_BYTES)
    }
    #[cfg(test)]
    fn with_limit(
        schema: &Schema,
        batch: &RecordBatch,
        retained: usize,
        scratch: usize,
        other: usize,
        limit: usize,
    ) -> Result<Self, ConnectorError> {
        preflight_inputs(schema, batch, retained, scratch)?;
        let mut context = novarocks_value_arrays::decode::ValueConversionContext::new(0);
        let plan = super::signed_layout::CowSignedLayoutPlan::plan(
            &std::sync::Arc::new(schema.clone()),
            batch,
            &super::signed_layout::fixture_declarations(schema),
            &mut context,
            TRANSFORM_BYTES,
            &mut |_| Ok(()),
        )?;
        Self::with_plans_limit(
            schema,
            batch,
            &plan,
            retained,
            scratch,
            other,
            limit,
            &mut |_| Ok(()),
        )
    }
    pub(super) fn for_plans(
        schema: &Schema,
        batch: &RecordBatch,
        plan: &super::signed_layout::CowSignedLayoutPlan,
        retained_selection_bytes: usize,
        source_scratch_bytes: usize,
        other_live_bytes: usize,
        work: &mut dyn FnMut(usize) -> Result<(), ConnectorError>,
    ) -> Result<Self, ConnectorError> {
        Self::with_plans_limit(
            schema,
            batch,
            plan,
            retained_selection_bytes,
            source_scratch_bytes,
            other_live_bytes,
            TRANSFORM_BYTES,
            work,
        )
    }
    fn with_plans_limit(
        schema: &Schema,
        batch: &RecordBatch,
        plan: &super::signed_layout::CowSignedLayoutPlan,
        retained_selection_bytes: usize,
        source_scratch_bytes: usize,
        other_live_bytes: usize,
        limit: usize,
        work: &mut dyn FnMut(usize) -> Result<(), ConnectorError>,
    ) -> Result<Self, ConnectorError> {
        if batch.num_rows() > MAX_ROWS || retained_selection_bytes > SELECTION_BYTES {
            return Err(exhausted());
        }
        let target = TypeWalk::schema(schema)?;
        let source = TypeWalk::schema(batch.schema_ref())?;
        let mut arrays = TypeWalk::default();
        for array in batch.columns() {
            arrays.data_type(array.data_type(), 0)?;
        }
        if batch.num_columns() != schema.fields().len() {
            return Err(invalid("COW output width differs from its signed contract"));
        }
        let input_bytes = add(
            ConnectorRowConversionFootprint::retained_batch_bytes(batch)?,
            source_scratch_bytes,
        )?;
        if input_bytes > INPUT_BYTES {
            return Err(exhausted());
        }
        let mut output_bytes = 0;
        let mut copy_peak_bytes = 0;
        for column in &plan.columns {
            column
                .visit_output_storage_observed(
                    &mut |storage| {
                        use novarocks_value_arrays::hydrate::PlannedOutputStorage;
                        match storage {
                            PlannedOutputStorage::Node {
                                rows,
                                source_offset,
                            } => {
                                if source_offset != 0 {
                                    return Err(invalid(
                                        "COW signed layout requires unsliced source arrays",
                                    ));
                                }
                                if rows > MAX_ROWS {
                                    return Err(exhausted());
                                }
                            }
                            PlannedOutputStorage::Buffer { bytes, newly_owned } => {
                                let bytes = round64(bytes)?;
                                if newly_owned {
                                    output_bytes = add(output_bytes, bytes)?;
                                }
                                copy_peak_bytes = add(copy_peak_bytes, bytes)?;
                            }
                        }
                        Ok(())
                    },
                    work,
                )
                .map_err(|failure| match failure {
                    novarocks_value_arrays::ValueFailure::Caller(error) => error,
                    novarocks_value_arrays::ValueFailure::Value(error) => {
                        super::signed_layout::value_error(error)
                    }
                })?;
        }
        let nodes = add(add(target.nodes, source.nodes)?, arrays.nodes)?;
        let metadata = add(add(target.metadata, source.metadata)?, arrays.metadata)?;
        let mut temporary_bytes = add(mul(nodes, NODE_HEADERS)?, mul(metadata, 36)?)?;
        temporary_bytes = add(temporary_bytes, plan.planning_requested_bytes)?;
        for column in &plan.columns {
            for layout in &column.requirements().execution_scratch {
                temporary_bytes = add(temporary_bytes, layout.size())?;
            }
        }
        let peak_bytes = add(
            add(
                add(input_bytes, retained_selection_bytes)?,
                other_live_bytes,
            )?,
            add(add(output_bytes, temporary_bytes)?, copy_peak_bytes)?,
        )?;
        if peak_bytes > limit {
            return Err(exhausted());
        }
        Ok(Self {
            input_bytes,
            output_bytes,
            temporary_bytes,
            copy_peak_bytes,
            peak_bytes,
        })
    }
}
pub(super) fn preflight_inputs(
    schema: &Schema,
    batch: &RecordBatch,
    retained: usize,
    scratch: usize,
) -> Result<(), ConnectorError> {
    if batch.num_rows() > MAX_ROWS
        || retained > SELECTION_BYTES
        || batch.num_columns() != schema.fields().len()
    {
        return Err(exhausted());
    }
    TypeWalk::schema(schema)?;
    TypeWalk::schema(batch.schema_ref())?;
    let mut arrays = TypeWalk::default();
    for array in batch.columns() {
        arrays.data_type(array.data_type(), 0)?;
    }
    if add(
        ConnectorRowConversionFootprint::retained_batch_bytes(batch)?,
        scratch,
    )? > INPUT_BYTES
    {
        return Err(exhausted());
    }
    Ok(())
}
pub(super) fn exhausted() -> ConnectorError {
    ConnectorError::new(
        ConnectorErrorKind::ResourceExhausted,
        "COW signed layout exceeds its fixed input, metadata or transform workspace",
    )
}
fn invalid(message: &'static str) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::InvalidRequest, message)
}
fn add(a: usize, b: usize) -> Result<usize, ConnectorError> {
    a.checked_add(b).ok_or_else(exhausted)
}
fn mul(a: usize, b: usize) -> Result<usize, ConnectorError> {
    a.checked_mul(b).ok_or_else(exhausted)
}
fn round64(bytes: usize) -> Result<usize, ConnectorError> {
    Ok(add(bytes, 63)? / 64 * 64)
}
#[derive(Default)]
struct TypeWalk {
    nodes: usize,
    metadata: usize,
}
impl TypeWalk {
    fn schema(schema: &Schema) -> Result<Self, ConnectorError> {
        if schema.fields().len() > MAX_NODES {
            return Err(exhausted());
        }
        let mut walk = Self::default();
        walk.metadata(schema.metadata())?;
        for field in schema.fields() {
            walk.field(field, 0)?;
        }
        Ok(walk)
    }
    fn bytes(&mut self, bytes: usize) -> Result<(), ConnectorError> {
        self.metadata = add(self.metadata, bytes)?;
        if self.metadata > METADATA_BYTES {
            return Err(exhausted());
        }
        Ok(())
    }
    fn metadata(&mut self, values: &arrow::datatypes::Metadata) -> Result<(), ConnectorError> {
        // The immutable BTree has entry traversal, without spare HashMap buckets.
        if values.len() > MAX_NODES {
            return Err(exhausted());
        }
        self.bytes(
            novarocks_type_contract::owned_resources::arrow_metadata::retained_backing_upper_bound(
                values.len(),
            )
            .map_err(metadata_resource_error)?,
        )?;
        for (key, value) in values {
            self.bytes(add(key.capacity(), value.capacity())?)?;
        }
        Ok(())
    }
    fn field(&mut self, field: &Field, depth: usize) -> Result<(), ConnectorError> {
        self.bytes(field.name().capacity())?;
        self.metadata(field.metadata())?;
        self.data_type(field.data_type(), depth)
    }
    fn data_type(&mut self, ty: &DataType, depth: usize) -> Result<(), ConnectorError> {
        if depth > MAX_DEPTH || self.nodes == MAX_NODES {
            return Err(exhausted());
        }
        self.nodes += 1;
        match ty {
            DataType::List(field)
            | DataType::LargeList(field)
            | DataType::ListView(field)
            | DataType::LargeListView(field)
            | DataType::FixedSizeList(field, _)
            | DataType::Map(field, _) => {
                if matches!(ty, DataType::FixedSizeList(_, n) if *n < 0) {
                    return Err(invalid("COW signed cast has a negative fixed list width"));
                }
                self.field(field, depth + 1)?;
            }
            DataType::Struct(fields) => {
                for field in fields {
                    self.field(field, depth + 1)?;
                }
            }
            DataType::Union(fields, _) => {
                for (_, field) in fields.iter() {
                    self.field(field, depth + 1)?;
                }
            }
            DataType::Dictionary(key, value) => {
                self.data_type(key, depth + 1)?;
                self.data_type(value, depth + 1)?;
            }
            DataType::RunEndEncoded(run, value) => {
                self.field(run, depth + 1)?;
                self.field(value, depth + 1)?;
            }
            DataType::Timestamp(_, Some(zone)) => self.bytes(zone.len())?,
            DataType::FixedSizeBinary(n) if *n < 0 => {
                return Err(invalid("COW signed cast has a negative fixed binary width"));
            }
            DataType::Decimal128(p, s) if *p == 0 || *p > 38 || !(-38..=38).contains(s) => {
                return Err(invalid("COW signed cast has invalid decimal128 metadata"));
            }
            DataType::Decimal256(p, s) if *p == 0 || *p > 76 || !(-76..=76).contains(s) => {
                return Err(invalid("COW signed cast has invalid decimal256 metadata"));
            }
            DataType::Decimal32(p, s) if *p == 0 || *p > 9 || !(-9..=9).contains(s) => {
                return Err(invalid("COW signed cast has invalid decimal32 metadata"));
            }
            DataType::Decimal64(p, s) if *p == 0 || *p > 18 || !(-18..=18).contains(s) => {
                return Err(invalid("COW signed cast has invalid decimal64 metadata"));
            }
            DataType::Time32(TimeUnit::Microsecond | TimeUnit::Nanosecond)
            | DataType::Time64(TimeUnit::Second | TimeUnit::Millisecond) => {
                return Err(invalid("COW signed cast has invalid time unit metadata"));
            }
            _ => {}
        }
        Ok(())
    }
}

fn metadata_resource_error(
    source: novarocks_type_contract::owned_resources::btree::BTreeResourceError,
) -> ConnectorError {
    use novarocks_type_contract::owned_resources::btree::BTreeResourceError;
    let kind = match &source {
        BTreeResourceError::Arithmetic(_) => ConnectorErrorKind::ResourceExhausted,
        BTreeResourceError::SourceModel(_) => ConnectorErrorKind::InvalidRequest,
    };
    ConnectorError::from_domain(kind, source)
}

#[cfg(test)]
mod tests;
