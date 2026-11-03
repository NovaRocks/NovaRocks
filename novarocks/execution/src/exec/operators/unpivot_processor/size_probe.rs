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

//! The same default IPC writer measures bytes through a checked Write sink.
//! The original creator and control observe the opaque writer before/after;
//! this sink makes no claim of cooperation inside Arrow encoding.

use std::io::{self, Write};

use arrow::error::ArrowError;
use arrow::ipc::writer::StreamWriter;
use novarocks_functions::arrow_ipc_construction::{
    IpcConstructionError, IpcGeometryError, observe_stream_logical_size_construction,
    observe_stream_writer_batch_construction, observe_stream_writer_schema_construction,
};
use novarocks_functions::{KernelEvaluationControl, KernelFailure};
use novarocks_type_contract::owned_resources::hashmap::HashMapResourceError;
use novarocks_type_contract::{ValueSchemaCreation, ValueTypeError};

use crate::exec::chunk::Chunk;
use crate::exec::pipeline::operator::input_construction::{
    ConstructionFailure, InputConstructionFactory,
};
use crate::runtime::task::ExecutionFailure;

#[derive(Debug)]
pub(super) enum UnpivotProbeFailure {
    Construction(ConstructionFailure),
    Kernel(KernelFailure),
    Geometry(IpcGeometryError),
    Shape(ValueTypeError),
    HashMap(HashMapResourceError),
    CountOverflow,
    RowCount { rows: usize },
    Encoding(ArrowError),
}
impl From<IpcConstructionError<ConstructionFailure>> for UnpivotProbeFailure {
    fn from(cause: IpcConstructionError<ConstructionFailure>) -> Self {
        match cause {
            IpcConstructionError::Creator(cause) => Self::Construction(cause),
            IpcConstructionError::Control(cause) => Self::Kernel(cause),
            IpcConstructionError::Geometry(cause) => Self::Geometry(cause),
            IpcConstructionError::Shape(cause) => Self::Shape(cause),
            IpcConstructionError::HashMap(cause) => Self::HashMap(cause),
        }
    }
}
impl From<ConstructionFailure> for UnpivotProbeFailure {
    fn from(cause: ConstructionFailure) -> Self {
        Self::Construction(cause)
    }
}
impl From<KernelFailure> for UnpivotProbeFailure {
    fn from(cause: KernelFailure) -> Self {
        Self::Kernel(cause)
    }
}
impl UnpivotProbeFailure {
    pub(super) fn into_execution(self) -> ExecutionFailure {
        match self {
            Self::Construction(cause) => cause.into(),
            Self::Kernel(cause) => cause.into(),
            Self::Geometry(cause) => ExecutionFailure::from_domain(cause),
            Self::Shape(cause) => ExecutionFailure::from_domain(cause),
            Self::HashMap(cause) => ExecutionFailure::from_domain(cause),
            source => ExecutionFailure::from_domain(source),
        }
    }
}
impl std::fmt::Display for UnpivotProbeFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Construction(source) => source.fmt(f),
            Self::Kernel(source) => source.fmt(f),
            Self::Geometry(source) => source.fmt(f),
            Self::Shape(source) => source.fmt(f),
            Self::HashMap(source) => source.fmt(f),
            Self::CountOverflow => f.write_str("unpivot IPC byte count overflow"),
            Self::RowCount { rows } => write!(
                f,
                "unpivot IPC batch has {rows} rows outside its signed wire carrier"
            ),
            Self::Encoding(source) => write!(f, "unpivot output size encoding failed: {source}"),
        }
    }
}
impl std::error::Error for UnpivotProbeFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Construction(source) => Some(source),
            Self::Kernel(source) => Some(source),
            Self::Geometry(source) => Some(source),
            Self::Shape(source) => Some(source),
            Self::HashMap(source) => Some(source),
            Self::Encoding(source) => Some(source),
            Self::CountOverflow | Self::RowCount { .. } => None,
        }
    }
}
impl novarocks_failure_contract::FailureProjection for UnpivotProbeFailure {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        use crate::runtime::task::ExecutionReason;
        use novarocks_failure_contract::{
            FailureClass, FailureFact, RepresentationBoundary, RepresentationFacts,
            RepresentationLayout, domain,
        };
        let layout = |code| RepresentationLayout {
            domain: domain::EXECUTION,
            code: std::num::NonZeroU16::new(code).unwrap(),
        };
        match self {
            Self::Construction(source) => source.failure_fact(),
            Self::Kernel(source) => source.failure_fact(),
            Self::Geometry(source) => source.failure_fact(),
            Self::Shape(source) => source.failure_fact(),
            Self::HashMap(source) => source.failure_fact(),
            Self::CountOverflow => FailureFact::new(
                FailureClass::Representation,
                ExecutionReason::UnpivotIpcBytes.reason(),
            )
            .with_representation(RepresentationFacts {
                boundary: RepresentationBoundary::SingleOutput,
                layout: layout(8),
                required: None,
                extent_limit: u64::try_from(usize::MAX).ok(),
                representable_prefix: None,
            }),
            Self::RowCount { rows } => FailureFact::new(
                FailureClass::Representation,
                ExecutionReason::UnpivotIpcRows.reason(),
            )
            .with_representation(RepresentationFacts {
                boundary: RepresentationBoundary::SingleOutput,
                layout: layout(9),
                required: u64::try_from(*rows).ok(),
                extent_limit: Some(i64::MAX as u64),
                representable_prefix: None,
            }),
            Self::Encoding(ArrowError::MemoryError(_)) => FailureFact::new(
                FailureClass::Resource,
                ExecutionReason::UnpivotIpcMemory.reason(),
            ),
            Self::Encoding(ArrowError::OffsetOverflowError(required)) => FailureFact::new(
                FailureClass::Representation,
                ExecutionReason::UnpivotIpcOffset.reason(),
            )
            .with_representation(RepresentationFacts {
                boundary: RepresentationBoundary::SingleOutput,
                layout: layout(10),
                required: u64::try_from(*required).ok(),
                extent_limit: None,
                representable_prefix: None,
            }),
            Self::Encoding(ArrowError::IoError(..) | ArrowError::ExternalError(_)) => {
                FailureFact::new(
                    FailureClass::Unexpected,
                    ExecutionReason::UnpivotIpcUnexpected.reason(),
                )
            }
            Self::Encoding(_) => FailureFact::new(
                FailureClass::Invariant,
                ExecutionReason::UnpivotIpcShape.reason(),
            ),
        }
    }
}

#[derive(Debug)]
struct CountOverflow;
impl std::fmt::Display for CountOverflow {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("unpivot IPC byte count overflow")
    }
}
impl std::error::Error for CountOverflow {}

#[derive(Default)]
struct CountingWrite {
    bytes: usize,
}
impl Write for CountingWrite {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        let next = self
            .bytes
            .checked_add(bytes.len())
            .ok_or_else(|| io::Error::other(CountOverflow))?;
        self.bytes = next;
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
fn writer_failure(cause: ArrowError) -> UnpivotProbeFailure {
    match cause {
        ArrowError::IoError(description, source)
            if source
                .get_ref()
                .is_some_and(|error| error.is::<CountOverflow>()) =>
        {
            // get_ref/into_inner share std's actual Custom-error branch.
            // The nominal sink payload, rather than its text, identifies
            // this single local arithmetic failure.
            let kind = source.kind();
            match source.into_inner() {
                Some(payload) => match payload.downcast::<CountOverflow>() {
                    Ok(_) => UnpivotProbeFailure::CountOverflow,
                    Err(payload) => UnpivotProbeFailure::Encoding(ArrowError::IoError(
                        description,
                        io::Error::new(kind, payload),
                    )),
                },
                None => UnpivotProbeFailure::Geometry(IpcGeometryError::SourceModel),
            }
        }
        cause => UnpivotProbeFailure::Encoding(cause),
    }
}

pub(super) fn output_size_with_construction(
    chunk: &Chunk,
    factory: &dyn InputConstructionFactory,
    control: &dyn KernelEvaluationControl,
) -> Result<usize, UnpivotProbeFailure> {
    let batch = chunk.record_batch();
    // The actual wire length is signed i64. This is representability, not a
    // new row quota; a Null batch can have huge legal rows and tiny payload.
    i64::try_from(batch.num_rows()).map_err(|_| UnpivotProbeFailure::RowCount {
        rows: batch.num_rows(),
    })?;
    let (encoded_bytes, batch_facts) = {
        let owner = factory.begin_input_construction(chunk)?;
        let mut request = |layout| owner.request(layout);
        let mut opaque = |bytes| owner.observe_opaque_upper_bound(bytes);
        let mut creation =
            ValueSchemaCreation::new(owner.storage_lifetime(), &mut request, &mut opaque);
        let schema = batch.schema_ref();
        let schema_facts =
            observe_stream_writer_schema_construction(schema.as_ref(), &mut creation, control)?;
        let mut encoded = CountingWrite::default();
        let batch_facts;
        {
            control.checkpoint(0)?;
            let result = StreamWriter::try_new(&mut encoded, schema);
            let mut writer = match result {
                Ok(writer) => writer,
                Err(cause) => {
                    control.checkpoint(0)?;
                    return Err(writer_failure(cause));
                }
            };
            control.checkpoint(0)?;
            batch_facts = observe_stream_writer_batch_construction(
                batch,
                &schema_facts,
                &mut creation,
                control,
            )?;
            control.checkpoint(0)?;
            let result = writer.write(batch);
            control.checkpoint(0)?;
            result.map_err(writer_failure)?;
            let result = writer.finish();
            control.checkpoint(0)?;
            result.map_err(writer_failure)?;
        }
        // Writer, encoded scratch and their request responsibility exit here.
        // Only a scalar count and borrowed numeric facts survive this scope.
        (encoded.bytes, batch_facts)
    };
    let (retained, logical) = {
        let owner = factory.begin_input_construction(chunk)?;
        let mut request = |layout| owner.request(layout);
        let mut opaque = |bytes| owner.observe_opaque_upper_bound(bytes);
        let mut creation =
            ValueSchemaCreation::new(owner.storage_lifetime(), &mut request, &mut opaque);
        observe_stream_logical_size_construction(batch, &batch_facts, &mut creation, control)?;
        control.checkpoint(0)?;
        let retained = chunk.estimated_bytes();
        let logical = chunk.logical_bytes();
        control.checkpoint(0)?;
        (retained, logical)
    };
    // This second to_data/HashSet scope also exits before returning a count.
    // Neither scope retains the source, query, operator or returned payload.
    Ok(retained.max(logical).max(encoded_bytes))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{
        Array, ArrayRef, DictionaryArray, Int32Array, ListBuilder, StringArray, StringBuilder,
    };
    use arrow::datatypes::{Field, Int32Type, Schema};
    use arrow::record_batch::RecordBatch;
    use std::sync::Arc;

    fn write_batch<W: Write>(target: W, batch: &RecordBatch) {
        let mut writer = StreamWriter::try_new(target, batch.schema_ref()).unwrap();
        writer.write(batch).unwrap();
        writer.finish().unwrap();
    }

    #[test]
    fn checked_count_sink_matches_the_same_default_vec_writer() {
        let mut nested = ListBuilder::new(StringBuilder::new());
        nested.values().append_value("visible");
        nested.values().append_null();
        nested.append(true);
        nested.append(false);
        let sources: Vec<ArrayRef> = vec![
            Arc::new(StringArray::from(vec![Some("a"), None, Some("snow-雪")])),
            Arc::new(StringArray::from(Vec::<Option<&str>>::new())),
            Arc::new(
                DictionaryArray::<Int32Type>::try_new(
                    Int32Array::from(vec![Some(0), Some(1), None, Some(0)]),
                    Arc::new(StringArray::from(vec![Some("value"), None])),
                )
                .unwrap(),
            ),
            Arc::new(nested.finish()),
        ];
        for source in sources {
            let field = Field::new("source", source.data_type().clone(), true);
            let batch =
                RecordBatch::try_new(Arc::new(Schema::new(vec![field])), vec![source]).unwrap();
            let mut bytes = Vec::new();
            let mut counter = CountingWrite::default();
            // One independent ordinary Vec sink is the actual byte-length
            // oracle. Both use the SAME writer/options/framing, not another
            // serializer or a second numerical schema implementation.
            write_batch(&mut bytes, &batch);
            write_batch(&mut counter, &batch);
            assert_eq!(counter.bytes, bytes.len());
        }
    }

    #[test]
    fn checked_count_overflow_preserves_its_nominal_io_payload_and_foreign_io() {
        let mut counter = CountingWrite { bytes: usize::MAX };
        let cause = counter.write(&[1]).unwrap_err();
        assert_eq!(counter.bytes, usize::MAX);
        assert!(matches!(
            writer_failure(ArrowError::from(cause)),
            UnpivotProbeFailure::CountOverflow
        ));
        use novarocks_failure_contract::{FailureClass, FailureProjection, RepresentationBoundary};
        let mut counter = CountingWrite { bytes: usize::MAX };
        let cause =
            writer_failure(ArrowError::from(counter.write(&[1]).unwrap_err())).into_execution();
        let fact = cause.failure_fact();
        assert_eq!(fact.class(), FailureClass::Representation);
        let rep = fact.representation().unwrap();
        assert_eq!(rep.boundary, RepresentationBoundary::SingleOutput);
        assert_eq!(rep.required, None);
        assert_eq!(rep.extent_limit, u64::try_from(usize::MAX).ok());
        let cause = io::Error::from(io::ErrorKind::BrokenPipe);
        match writer_failure(ArrowError::from(cause)) {
            UnpivotProbeFailure::Encoding(ArrowError::IoError(_, cause)) => {
                assert_eq!(cause.kind(), io::ErrorKind::BrokenPipe);
            }
            cause => panic!("foreign IO classification changed: {cause:?}"),
        }
    }
}
