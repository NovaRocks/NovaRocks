// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
//! Observe the actual constructor inputs and lifetime, never a memory grant.
use super::stages::{AnalyticChunkConstruction, LegacyWork};
use super::*;
use arrow::array::{RecordBatch, RecordBatchOptions, StructArray, UInt32Array};
use arrow::buffer::NullBuffer;
use arrow::datatypes::SchemaRef;

struct Birth {
    stage: AnalyticInputStage,
    schema: ChunkSchemaRef,
    batch_schema: Option<SchemaRef>,
    columns: Vec<ArrayRef>,
    rows: usize,
    callbacks: usize,
}
struct Constructors<'a> {
    original: Work<'a>,
    control: &'a Control,
    births: Vec<Birth>,
    bodies: usize,
    refusal: Option<(usize, KernelFailure)>,
    taken: Vec<ArrayRef>,
    slices: Vec<ArrayRef>,
}
impl<'a> Constructors<'a> {
    fn new(control: &'a Control) -> Self {
        Self {
            original: Work::new(control, Arc::new(AtomicUsize::new(0))),
            control,
            births: Vec::new(),
            bodies: 0,
            refusal: None,
            taken: Vec::new(),
            slices: Vec::new(),
        }
    }
    fn birth(
        &mut self,
        scope: &LifetimeWitness,
        schema: &ChunkSchemaRef,
        batch_schema: Option<SchemaRef>,
        columns: &[ArrayRef],
        rows: usize,
    ) -> Result<(), KernelFailure> {
        assert!(Arc::ptr_eq(&scope.0, &self.original.drops));
        let ordinal = self.births.len();
        self.births.push(Birth {
            stage: *self
                .original
                .admitted
                .last()
                .expect("actual stage admission"),
            schema: Arc::clone(schema),
            batch_schema,
            columns: columns.to_vec(),
            rows,
            callbacks: self.control.trace().len(),
        });
        if let Some((stop, cause)) = &self.refusal {
            assert!(ordinal <= *stop, "constructor after first refusal");
            if ordinal == *stop {
                return Err(cause.clone());
            }
        }
        self.bodies += 1;
        Ok(())
    }
}
fn project(
    result: AnalyticChunkConstruction<std::convert::Infallible>,
) -> AnalyticChunkConstruction<KernelFailure> {
    match result {
        Ok(chunk) => Ok(chunk),
        Err(Ok(message)) => Err(Ok(message)),
        Err(Err(never)) => match never {},
    }
}
impl AnalyticInputWork for Constructors<'_> {
    type Failure = KernelFailure;
    type Scope = LifetimeWitness;
    fn admit(&mut self, source: AnalyticInputSource<'_>) -> Result<Self::Scope, Self::Failure> {
        self.original.admit(source)
    }
    fn step(&mut self) -> Result<(), Self::Failure> {
        self.original.step()
    }
    fn boundary(&mut self) -> Result<(), Self::Failure> {
        self.original.boundary()
    }
    fn take_column(
        &mut self,
        _: &mut Self::Scope,
        source: &Chunk,
        ordinal: usize,
        indices: &Arc<UInt32Array>,
    ) -> Result<ArrayRef, stages::AnalyticCopyFailure<Self::Failure>> {
        match LegacyWork.take_column(&mut (), source, ordinal, indices) {
            Ok(column) => {
                self.taken.push(Arc::clone(&column));
                Ok(column)
            }
            Err(stages::AnalyticCopyFailure::OriginalArrow(error)) => {
                Err(stages::AnalyticCopyFailure::OriginalArrow(error))
            }
            Err(stages::AnalyticCopyFailure::OriginalText(message)) => {
                Err(stages::AnalyticCopyFailure::OriginalText(message))
            }
            Err(stages::AnalyticCopyFailure::Control(never)) => match never {},
        }
    }
    fn slice_column(
        &mut self,
        _: &mut Self::Scope,
        source: &ArrayRef,
        offset: usize,
        len: usize,
    ) -> Result<ArrayRef, Self::Failure> {
        let column = match LegacyWork.slice_column(&mut (), source, offset, len) {
            Ok(column) => column,
            Err(never) => match never {},
        };
        self.slices.push(Arc::clone(&column));
        Ok(column)
    }
    fn construct_batch_chunk(
        &mut self,
        scope: &mut Self::Scope,
        batch: RecordBatch,
        schema: ChunkSchemaRef,
    ) -> AnalyticChunkConstruction<Self::Failure> {
        self.birth(
            scope,
            &schema,
            Some(batch.schema()),
            batch.columns(),
            batch.num_rows(),
        )
        .map_err(Err)?;
        project(LegacyWork.construct_batch_chunk(&mut (), batch, schema))
    }
    fn construct_columns_chunk(
        &mut self,
        scope: &mut Self::Scope,
        schema: ChunkSchemaRef,
        columns: Vec<ArrayRef>,
    ) -> AnalyticChunkConstruction<Self::Failure> {
        self.birth(
            scope,
            &schema,
            None,
            &columns,
            columns.first().map_or(0, |c| c.len()),
        )
        .map_err(Err)?;
        project(LegacyWork.construct_columns_chunk(&mut (), schema, columns))
    }
}

#[test]
fn c02_p2_constructor_gather_original_batch_schema_and_zero_column_rows() {
    let inputs = [ints(vec![Some(7), None]), ints(vec![Some(4), Some(3)])];
    let control = Control::new(None);
    let mut work = Constructors::new(&control);
    let result = stages::gather(&inputs, &mut work).unwrap();
    let original_batch = arrow::compute::concat_batches(
        &inputs[0].schema(),
        inputs.iter().map(|input| &input.batch),
    )
    .unwrap();
    assert_eq!(result.value.batch, original_batch);
    assert_eq!(work.births.len(), 1);
    assert_eq!(work.bodies, 1);
    let birth = &work.births[0];
    assert_eq!(birth.stage, AnalyticInputStage::Gather);
    assert_eq!(birth.rows, 4);
    assert!(Arc::ptr_eq(&birth.schema, &inputs[0].chunk_schema_ref()));
    assert!(Arc::ptr_eq(
        birth.batch_schema.as_ref().unwrap(),
        &inputs[0].schema()
    ));
    assert!(Arc::ptr_eq(&result.value.chunk_schema_ref(), &birth.schema));
    assert!(Arc::ptr_eq(&result.value.columns()[0], &birth.columns[0]));

    let schema = Arc::new(ChunkSchema::empty());
    let source = |rows| {
        Chunk::try_new_with_chunk_schema(
            RecordBatch::try_new_with_options(
                schema.arrow_schema_ref(),
                vec![],
                &RecordBatchOptions::new().with_row_count(Some(rows)),
            )
            .unwrap(),
            Arc::clone(&schema),
        )
        .unwrap()
    };
    let result = stages::gather(&[source(2), source(3)], &mut work).unwrap();
    assert_eq!(result.value.len(), 5);
    assert_eq!(work.births[1].rows, 5);
    assert!(work.births[1].columns.is_empty());
    assert!(Arc::ptr_eq(&work.births[1].schema, &schema));
    assert_eq!(work.bodies, 2);
}

#[test]
fn c02_p2_constructor_fast_paths_have_no_birth() {
    let control = Control::new(None);
    let mut work = Constructors::new(&control);
    let input = ints(vec![Some(1), Some(2)]);
    let single = stages::gather(std::slice::from_ref(&input), &mut work).unwrap();
    assert!(Arc::ptr_eq(&single.value.columns()[0], &input.columns()[0]));
    for source in [ints(vec![]), ints(vec![Some(1)]), input.clone()] {
        let result = stages::regroup(&source, &[], &mut work).unwrap();
        assert!(Arc::ptr_eq(
            &result.value.columns()[0],
            &source.columns()[0]
        ));
    }
    for source in [ints(vec![]), ints(vec![Some(1)])] {
        let trace_start = control.trace().len();
        let key = Arc::clone(&source.columns()[0]);
        let result = stages::regroup(&source, &[key], &mut work).unwrap();
        assert_eq!(&control.trace()[trace_start..], &[0, 0]);
        assert!(Arc::ptr_eq(
            &result.value.columns()[0],
            &source.columns()[0]
        ));
        assert!(work.births.is_empty());
        assert_eq!(work.bodies, 0);
    }
    let key = Arc::clone(&input.columns()[0]);
    let identity = stages::regroup(&input, &[key], &mut work).unwrap();
    assert!(Arc::ptr_eq(
        &identity.value.columns()[0],
        &input.columns()[0]
    ));
    let empty = ints(vec![]);
    let split = stages::split(
        empty.chunk_schema_ref(),
        empty.columns(),
        std::slice::from_ref(&empty),
        &mut work,
    )
    .unwrap();
    assert!(split.value.is_empty());
    assert!(work.births.is_empty());
    assert_eq!(work.bodies, 0);
}

#[test]
fn c02_p2_constructor_regroup_consumes_actual_taken_columns_and_stable_ties() {
    let source = ints(vec![Some(10), None, Some(20), Some(30)]);
    let keys: ArrayRef = Arc::new(Int32Array::from(vec![Some(2), Some(1), Some(1), None]));
    let control = Control::new(None);
    let mut work = Constructors::new(&control);
    let result = stages::regroup(&source, &[keys], &mut work).unwrap();
    assert_eq!(work.births.len(), 1);
    assert_eq!(work.bodies, 1);
    let birth = &work.births[0];
    assert_eq!(birth.stage, AnalyticInputStage::Regroup);
    assert!(birth.batch_schema.is_none());
    assert!(Arc::ptr_eq(&birth.schema, &source.chunk_schema_ref()));
    assert!(Arc::ptr_eq(&birth.columns[0], &work.taken[0]));
    assert_eq!(
        result.value.columns()[0]
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .iter()
            .collect::<Vec<_>>(),
        vec![Some(30), None, Some(20), Some(10)]
    );
    let original =
        Chunk::try_new_with_columns(source.chunk_schema_ref(), work.taken.clone()).unwrap();
    assert_eq!(result.value.batch, original.batch);
    assert_eq!(result.value.chunk_schema_ref(), original.chunk_schema_ref());
    assert!(!Arc::ptr_eq(&result.value.schema(), &source.schema()));
}

#[test]
fn c02_p2_constructor_split_actual_slices_reconcile_each_null_and_nested_metadata() {
    let mut metadata = std::collections::HashMap::new();
    metadata.insert("nested-original".into(), "exact-value".into());
    let field = Arc::new(Field::new("nested", DataType::Int32, true).with_metadata(metadata));
    let dtype = DataType::Struct(vec![Arc::clone(&field)].into());
    let output = Arc::new(
        ChunkSchema::try_new(vec![ChunkSlotSchema::new_with_field(
            SlotId::new(7),
            Field::new("root", dtype, false),
            None,
            None,
        )])
        .unwrap(),
    );
    let foreign = Arc::new(Field::new("nested", DataType::Int32, true));
    let column: ArrayRef = Arc::new(StructArray::new(
        vec![foreign].into(),
        vec![Arc::new(Int32Array::from(vec![
            Some(1),
            Some(2),
            None,
            Some(4),
        ]))],
        Some(NullBuffer::from(vec![true, true, false, true])),
    ));
    let arrivals = [
        ints(vec![Some(1), Some(2)]),
        ints(vec![]),
        ints(vec![Some(3), Some(4)]),
    ];
    let control = Control::new(None);
    let mut work = Constructors::new(&control);
    let result = stages::split(
        Arc::clone(&output),
        std::slice::from_ref(&column),
        &arrivals,
        &mut work,
    )
    .unwrap();
    assert_eq!(result.value.len(), 2);
    assert_eq!(work.births.len(), 2);
    assert_eq!(work.bodies, 2);
    for (ordinal, actual) in result.value.iter().enumerate() {
        let birth = &work.births[ordinal];
        assert_eq!(birth.stage, AnalyticInputStage::OutputSplit);
        assert_eq!(birth.rows, 2);
        assert!(Arc::ptr_eq(&birth.schema, &output));
        assert!(Arc::ptr_eq(&birth.columns[0], &work.slices[ordinal]));
        assert_eq!(
            birth.columns[0].to_data(),
            column.slice(ordinal * 2, 2).to_data()
        );
        let original =
            Chunk::try_new_with_columns(Arc::clone(&output), vec![column.slice(ordinal * 2, 2)])
                .unwrap();
        assert_eq!(actual.batch, original.batch);
        assert_eq!(actual.chunk_schema_ref(), original.chunk_schema_ref());
        assert_eq!(actual.schema().field(0).is_nullable(), ordinal == 1);
        let DataType::Struct(fields) = actual.columns()[0].data_type() else {
            panic!("original struct carrier")
        };
        assert_eq!(fields[0].metadata(), field.metadata());
    }
}

fn evaluate<W: AnalyticInputWork>(
    stage: AnalyticInputStage,
    work: &mut W,
) -> Result<(), AnalyticStageFailure<W::Scope, W::Failure>> {
    let inputs = [ints(vec![Some(2), None]), ints(vec![Some(1), Some(0)])];
    match stage {
        AnalyticInputStage::Gather => drop(stages::gather(&inputs, work)?),
        AnalyticInputStage::Regroup => {
            let source = ints(vec![Some(2), None, Some(1), Some(0)]);
            let key: ArrayRef = Arc::new(Int32Array::from(vec![2, 1, 1, 0]));
            drop(stages::regroup(&source, &[key], work)?);
        }
        AnalyticInputStage::OutputSplit => {
            let column: ArrayRef =
                Arc::new(Int32Array::from(vec![Some(2), None, Some(1), Some(0)]));
            drop(stages::split(
                schema(DataType::Int32),
                &[column],
                &inputs,
                work,
            )?);
        }
    }
    Ok(())
}
#[test]
fn c02_p2_constructor_defaults_keep_original_control_trace_and_scope() {
    for stage in [
        AnalyticInputStage::Gather,
        AnalyticInputStage::Regroup,
        AnalyticInputStage::OutputSplit,
    ] {
        let original_control = Control::new(None);
        let mut original = Work::new(&original_control, Arc::new(AtomicUsize::new(0)));
        assert!(evaluate(stage, &mut original).is_ok());
        let control = Control::new(None);
        let mut work = Constructors::new(&control);
        assert!(evaluate(stage, &mut work).is_ok());
        assert_eq!(control.trace(), original_control.trace());
        assert_eq!(work.original.drops.load(Ordering::SeqCst), 1);
        assert_eq!(
            work.bodies,
            if stage == AnalyticInputStage::OutputSplit {
                2
            } else {
                1
            }
        );
    }
}

#[test]
fn c02_p2_constructor_original_split_data_full_text_scope_and_no_footer() {
    let source = ints(vec![Some(4), None]);
    let output = schema(DataType::Int64);
    let expected = format!(
        "build analytic output batch: {}",
        Chunk::try_new_with_columns(Arc::clone(&output), source.columns().to_vec()).unwrap_err()
    );
    let control = Control::new(None);
    let mut work = Constructors::new(&control);
    let error = stages::split(
        Arc::clone(&output),
        source.columns(),
        std::slice::from_ref(&source),
        &mut work,
    )
    .unwrap_err();
    match &error {
        AnalyticStageFailure::OriginalData { stage, message, .. } => {
            assert_eq!(*stage, AnalyticInputStage::OutputSplit);
            assert_eq!(message, &expected);
        }
        other => panic!("wrong constructor channel {other:?}"),
    }
    assert_eq!(work.bodies, 1);
    assert_eq!(work.births.len(), 1);
    assert_eq!(control.trace().len(), work.births[0].callbacks);
    assert_eq!(work.original.drops.load(Ordering::SeqCst), 0);
    drop(error);
    assert_eq!(work.original.drops.load(Ordering::SeqCst), 1);
    let control = Control::new(Some((control.trace().len(), KernelFailure::Cancelled)));
    let mut work = Constructors::new(&control);
    assert!(
        matches!(stages::split(output, source.columns(), std::slice::from_ref(&source), &mut work),
        Err(AnalyticStageFailure::OriginalData { message, .. }) if message == expected)
    );
    assert_eq!(control.trace().len(), work.births[0].callbacks);
}

#[test]
fn c02_p2_constructor_seven_typed_refusals_stop_before_body_and_successor() {
    for stage in [
        AnalyticInputStage::Gather,
        AnalyticInputStage::Regroup,
        AnalyticInputStage::OutputSplit,
    ] {
        let control = Control::new(None);
        let mut success = Constructors::new(&control);
        assert!(evaluate(stage, &mut success).is_ok());
        for ordinal in 0..success.births.len() {
            for cause in causes() {
                let control = Control::new(None);
                let mut work = Constructors::new(&control);
                work.refusal = Some((ordinal, cause.clone()));
                assert!(matches!(evaluate(stage, &mut work),
                    Err(AnalyticStageFailure::Control { stage: actual, cause: actual_cause })
                    if actual == stage && actual_cause == cause));
                assert_eq!(work.births.len(), ordinal + 1);
                assert_eq!(work.bodies, ordinal);
                assert_eq!(control.trace().len(), success.births[ordinal].callbacks);
                assert_eq!(work.original.drops.load(Ordering::SeqCst), 1);
            }
        }
    }
}
