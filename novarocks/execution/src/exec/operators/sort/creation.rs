// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file distributed
// with this work for additional information regarding copyright ownership.
// The ASF licenses this file to you under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with it.
// You may obtain a copy at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Borrowed construction access for the original sort bodies. The installed
//! application owns admission; independently emitted wrappers retain only
//! finite creation and shared type storage, never this access or an operator.

use crate::exec::chunk::Chunk;
use crate::exec::pipeline::operator::{
    ConstructionFailure, InputConstructionFactory, InputConstructionOwner,
};
use crate::runtime::fragment::ExecutionResult;
use arrow::record_batch::RecordBatch;

enum ComparisonFailure {
    Creator(ConstructionFailure),
    Value(novarocks_type_contract::ValueTypeError),
}
impl From<novarocks_type_contract::ValueTypeError> for ComparisonFailure {
    fn from(value: novarocks_type_contract::ValueTypeError) -> Self {
        Self::Value(value)
    }
}
impl ComparisonFailure {
    fn into_execution(self) -> crate::runtime::fragment::ExecutionFailure {
        match self {
            Self::Creator(cause) => cause.into(),
            Self::Value(cause) => ConstructionFailure::InvalidSource(format!(
                "sort complete declaration is invalid: {cause}"
            ))
            .into(),
        }
    }
}

pub(crate) struct CreatedSortColumns {
    columns: Vec<arrow::array::ArrayRef>,
    type_storage: novarocks_type_contract::ValueTypeStorageLifetime,
    _array_storage: novarocks_type_contract::ValueTypeStorageLifetime,
}
impl CreatedSortColumns {
    pub(crate) fn new(
        columns: Vec<arrow::array::ArrayRef>,
        type_storage: novarocks_type_contract::ValueTypeStorageLifetime,
        array_storage: novarocks_type_contract::ValueTypeStorageLifetime,
    ) -> Self {
        Self {
            columns,
            type_storage,
            _array_storage: array_storage,
        }
    }
    pub(crate) fn storage_lifetime(&self) -> novarocks_type_contract::ValueTypeStorageLifetime {
        self.type_storage.clone()
    }
    pub(crate) fn array_storage_lifetime(
        &self,
    ) -> novarocks_type_contract::ValueTypeStorageLifetime {
        self._array_storage.clone()
    }
    pub(crate) fn columns(&self) -> &[arrow::array::ArrayRef] {
        &self.columns
    }
}
impl std::ops::Deref for CreatedSortColumns {
    type Target = [arrow::array::ArrayRef];
    fn deref(&self) -> &Self::Target {
        self.columns()
    }
}

pub(crate) struct CreatedSortArray {
    array: arrow::array::ArrayRef,
    _storage: novarocks_type_contract::ValueTypeStorageLifetime,
}
impl CreatedSortArray {
    pub(crate) fn new(
        array: arrow::array::ArrayRef,
        storage: novarocks_type_contract::ValueTypeStorageLifetime,
    ) -> Self {
        Self {
            array,
            _storage: storage,
        }
    }
    pub(crate) fn array(&self) -> &arrow::array::ArrayRef {
        &self.array
    }
}

/// The original converter owns its codec/type structures. The finite marker
/// also loans only the source type backing shared by its SortField clones.
/// Data drops before its responsibility; input arrays and factory are absent.
pub(crate) struct CreatedSortConverter {
    converter: arrow::row::RowConverter,
    _storage: novarocks_type_contract::ValueTypeStorageLifetime,
}
impl CreatedSortConverter {
    pub(crate) fn converter(&self) -> &arrow::row::RowConverter {
        &self.converter
    }
    pub(crate) fn storage_lifetime(&self) -> novarocks_type_contract::ValueTypeStorageLifetime {
        self._storage.clone()
    }
}
impl std::ops::Deref for CreatedSortConverter {
    type Target = arrow::row::RowConverter;
    fn deref(&self) -> &Self::Target {
        &self.converter
    }
}
impl std::ops::DerefMut for CreatedSortConverter {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.converter
    }
}

pub(crate) struct SortCreation<'a> {
    factory: &'a dyn InputConstructionFactory,
    output: &'a crate::exec::chunk::ChunkSchemaRef,
    evaluation_control: Option<&'a dyn crate::exec::expr::ExprEvaluationControl>,
}
impl<'a> SortCreation<'a> {
    pub(crate) fn new(
        factory: &'a dyn InputConstructionFactory,
        output: &'a crate::exec::chunk::ChunkSchemaRef,
    ) -> Self {
        Self {
            factory,
            output,
            evaluation_control: None,
        }
    }

    pub(crate) fn with_evaluation_control(
        mut self,
        control: &'a dyn crate::exec::expr::ExprEvaluationControl,
    ) -> Self {
        self.evaluation_control = Some(control);
        self
    }

    pub(crate) fn expression_context<'b>(
        &'b self,
        arena: &'b crate::exec::expr::ExprArena,
        owner: &'b dyn InputConstructionOwner,
    ) -> ExecutionResult<crate::exec::expr::ExprEvaluationContext<'b>> {
        Ok(crate::exec::expr::ExprEvaluationContext::new(
            arena,
            owner,
            self.evaluation_control
                .ok_or(ConstructionFailure::MissingOwner)?,
        ))
    }

    pub(crate) fn row_converter<I>(
        &self,
        key_columns: &[arrow::array::ArrayRef],
        type_storage: novarocks_type_contract::ValueTypeStorageLifetime,
        options: I,
    ) -> ExecutionResult<CreatedSortConverter>
    where
        I: Clone + ExactSizeIterator<Item = arrow::compute::SortOptions>,
    {
        if key_columns.len() != options.len() {
            return Err(ConstructionFailure::InvalidSource(
                "sort converter key and option counts differ".into(),
            )
            .into());
        }
        let temporary = self.factory.begin_typed_construction(self.output)?;
        let retained = self.factory.begin_typed_construction(self.output)?;
        let mut temporary_request = |layout| temporary.request(layout);
        let mut temporary_opaque = |bytes| temporary.observe_opaque_upper_bound(bytes);
        let mut retained_request = |layout| retained.request(layout);
        let mut retained_opaque = |bytes| retained.observe_opaque_upper_bound(bytes);
        let mut temporary_creation = novarocks_type_contract::ValueSchemaCreation::new(
            temporary.storage_lifetime(),
            &mut temporary_request,
            &mut temporary_opaque,
        );
        let mut retained_creation = novarocks_type_contract::ValueSchemaCreation::new(
            retained.storage_lifetime(),
            &mut retained_request,
            &mut retained_opaque,
        );
        novarocks_functions::selected_copy::preflight_row_converter_structure(
            key_columns.iter().map(|column| column.data_type()),
            &mut temporary_creation,
            &mut retained_creation,
            &mut |_boundary| self.factory.observe_input_work(1),
        )
        .map_err(|failure| match failure {
            novarocks_functions::selected_copy::RowConversionStructureFailure::Construction(
                cause,
            ) => copy_failure(cause),
            novarocks_functions::selected_copy::RowConversionStructureFailure::ValueType(cause) => {
                ConstructionFailure::InvalidSource(format!(
                    "sort row converter source declaration: {cause}"
                ))
                .into()
            }
            novarocks_functions::selected_copy::RowConversionStructureFailure::Unsupported(
                cause,
            ) => ConstructionFailure::InvalidSource(cause.into()).into(),
        })?;
        // Any composite holder is observed before it is created; only finite
        // source TYPE storage is retained, not key arrays or their payload hold.
        let storage = retained
            .storage_lifetime()
            .retaining_observed::<ConstructionFailure>(&type_storage, |layout| {
                retained.request(layout)
            })?;
        // The sole preflight includes this exact-size SortField Vec and its
        // actual type clones. Keep temporary alive through ONE pinned new.
        let fields = key_columns
            .iter()
            .zip(options)
            .map(|(column, options)| {
                arrow::row::SortField::new_with_options(column.data_type().clone(), options)
            })
            .collect::<Vec<_>>();
        let converter = arrow::row::RowConverter::new(fields).map_err(|cause| cause.to_string())?;
        Ok(CreatedSortConverter {
            converter,
            _storage: storage,
        })
    }

    pub(crate) fn verify_chunks(&self, chunks: &[Chunk]) -> ExecutionResult<()> {
        let first = chunks.first().ok_or_else(|| {
            ConstructionFailure::InvalidSource("sort requires an explicit input declaration".into())
        })?;
        if !first.chunk_schema().is_typed() {
            return Err(ConstructionFailure::InvalidSource(
                "sort input lacks its complete declaration".into(),
            )
            .into());
        }
        for chunk in chunks {
            self.factory.observe_input_work(1)?;
            let expected = self.output.slots();
            let actual = chunk.chunk_schema().slots();
            if !chunk.chunk_schema().is_typed() || expected.len() != actual.len() {
                return Err(ConstructionFailure::InvalidSource(
                    "sort input changes its declared channel count".into(),
                )
                .into());
            }
            for (expected, actual) in expected.iter().zip(actual) {
                self.factory.observe_input_work(
                    expected
                        .name()
                        .len()
                        .max(actual.name().len())
                        .saturating_add(1),
                )?;
                if expected.slot_id() != actual.slot_id() || expected.name() != actual.name() {
                    return Err(ConstructionFailure::InvalidSource(
                        "sort input changes its declared channel occurrence".into(),
                    )
                    .into());
                }
                let actual = novarocks_type_contract::FunctionValueType::try_from_typed_binding(
                    actual.typed_binding().clone(),
                )
                .map_err(|cause| ConstructionFailure::InvalidSource(cause.to_string()))?;
                let expected = novarocks_type_contract::FunctionValueType::try_from_typed_binding(
                    expected.typed_binding().clone(),
                )
                .map_err(|cause| ConstructionFailure::InvalidSource(cause.to_string()))?;
                let same = actual
                    .fits_physical_binding_observed::<ComparisonFailure>(&expected, || {
                        self.factory
                            .observe_input_work(1)
                            .map_err(ComparisonFailure::Creator)
                    })
                    .map_err(ComparisonFailure::into_execution)?;
                if !same {
                    return Err(ConstructionFailure::InvalidSource(
                        "sort input changes its complete value semantics or layout".into(),
                    )
                    .into());
                }
            }
        }
        Ok(())
    }

    pub(crate) fn slice(
        &self,
        source: &Chunk,
        offset: usize,
        length: usize,
    ) -> ExecutionResult<Chunk> {
        let owner = self.factory.begin_input_construction(source)?;
        Ok(source.slice_typed_observed::<ConstructionFailure, _>(
            offset,
            length,
            owner.storage_lifetime(),
            |step| owner.observe(0, &[], step),
        )?)
    }

    pub(crate) fn vector<T>(
        &self,
        count: usize,
        owner: &dyn InputConstructionOwner,
    ) -> ExecutionResult<Vec<T>> {
        owner.request(std::alloc::Layout::array::<T>(count).map_err(|_| {
            ConstructionFailure::Resource(
                novarocks_type_contract::owned_resources::layout::LayoutResourceError::Arithmetic,
            )
        })?)?;
        Ok(Vec::with_capacity(count))
    }

    pub(crate) fn buffer_adoption(
        &self,
        owner: &dyn InputConstructionOwner,
    ) -> Result<(), ConstructionFailure> {
        let mut request = |layout| owner.request(layout);
        let mut opaque = |bytes| owner.observe_opaque_upper_bound(bytes);
        let mut creation = novarocks_type_contract::ValueSchemaCreation::new(
            owner.storage_lifetime(),
            &mut request,
            &mut opaque,
        );
        novarocks_functions::preflight_buffer_adoption_structure(&mut creation, &mut |_boundary| {
            self.factory.observe_input_work(0)
        })
        .map_err(|error| match error {
            novarocks_functions::selected_copy::CopyConstructionError::Creator(cause) => cause,
            novarocks_functions::selected_copy::CopyConstructionError::Resource(cause) => {
                ConstructionFailure::Resource(cause)
            }
            other => ConstructionFailure::Caller(Box::new(copy_failure(other))),
        })
    }

    pub(crate) fn lexsort(
        &self,
        columns: &[arrow::compute::SortColumn],
        limit: Option<usize>,
        source: &Chunk,
    ) -> ExecutionResult<CreatedSortArray> {
        let temporary = self.begin(source)?;
        let retained = self.begin(source)?;
        let mut temporary_request = |layout| temporary.request(layout);
        let mut temporary_opaque = |bytes| temporary.observe_opaque_upper_bound(bytes);
        let mut retained_request = |layout| retained.request(layout);
        let mut retained_opaque = |bytes| retained.observe_opaque_upper_bound(bytes);
        let mut temporary_creation = novarocks_type_contract::ValueSchemaCreation::new(
            temporary.storage_lifetime(),
            &mut temporary_request,
            &mut temporary_opaque,
        );
        let mut retained_creation = novarocks_type_contract::ValueSchemaCreation::new(
            retained.storage_lifetime(),
            &mut retained_request,
            &mut retained_opaque,
        );
        novarocks_functions::arrow_sort_construction::preflight_lexsort_structure_construction(
            columns,
            limit,
            &mut temporary_creation,
            &mut retained_creation,
            &mut |_boundary| self.factory.observe_input_work(1),
        )
        .map_err(copy_failure)?;
        let indices = arrow::compute::lexsort_to_indices(columns, limit)
            .map_err(|cause| cause.to_string())?;
        // The shared result header bound includes this ONE public Array Arc.
        let array = std::sync::Arc::new(indices) as arrow::array::ArrayRef;
        Ok(CreatedSortArray::new(array, retained.storage_lifetime()))
    }

    pub(crate) fn stable_index(
        &self,
        columns: &mut Vec<arrow::compute::SortColumn>,
        rows: usize,
        owner: &dyn InputConstructionOwner,
    ) -> ExecutionResult<()> {
        super::append_stable_row_index_sort_column_observed(
            columns,
            rows,
            &mut |event| match event {
                super::StableIndexConstruction::ArrayHeader => owner.request(
                    novarocks_type_contract::owned_resources::layout::arc_layout(
                        std::alloc::Layout::new::<arrow::array::UInt64Array>(),
                    )?,
                ),
                super::StableIndexConstruction::ColumnGrowth(layout) => owner.request(layout),
                super::StableIndexConstruction::BufferAdoption => self.buffer_adoption(owner),
            },
        )?;
        Ok(())
    }

    pub(crate) fn take_batch(
        &self,
        batch: &RecordBatch,
        indices: &dyn arrow::array::Array,
        source: &Chunk,
        retained: &dyn InputConstructionOwner,
    ) -> ExecutionResult<RecordBatch> {
        use novarocks_functions::selected_copy::{
            CopyIndexView, preflight_take_structure_construction,
        };
        let selection = if let Some(indices) =
            indices.as_any().downcast_ref::<arrow::array::UInt32Array>()
        {
            CopyIndexView::UInt32(indices)
        } else if let Some(indices) = indices.as_any().downcast_ref::<arrow::array::UInt64Array>() {
            CopyIndexView::UInt64(indices)
        } else {
            return Err(ConstructionFailure::InvalidSource(
                "sort original selection has an unsupported index carrier".into(),
            )
            .into());
        };
        let temporary = self.begin(source)?;
        retained.request(std::alloc::Layout::array::<arrow::array::ArrayRef>(batch.num_columns()).map_err(|_| ConstructionFailure::Resource(novarocks_type_contract::owned_resources::layout::LayoutResourceError::Arithmetic))?)?;
        let mut columns = Vec::with_capacity(batch.num_columns());
        for column in batch.columns() {
            let mut temporary_request = |layout| temporary.request(layout);
            let mut temporary_opaque = |bytes| temporary.observe_opaque_upper_bound(bytes);
            let mut retained_request = |layout| retained.request(layout);
            let mut retained_opaque = |bytes| retained.observe_opaque_upper_bound(bytes);
            let mut temporary_creation = novarocks_type_contract::ValueSchemaCreation::new(
                temporary.storage_lifetime(),
                &mut temporary_request,
                &mut temporary_opaque,
            );
            let mut retained_creation = novarocks_type_contract::ValueSchemaCreation::new(
                retained.storage_lifetime(),
                &mut retained_request,
                &mut retained_opaque,
            );
            preflight_take_structure_construction(
                column.as_ref(),
                selection,
                &mut temporary_creation,
                &mut retained_creation,
                &mut |_boundary| self.factory.observe_input_work(1),
            )
            .map_err(copy_failure)?;
            columns.push(
                arrow::compute::take(column.as_ref(), indices, None)
                    .map_err(|cause| cause.to_string())?,
            );
        }
        Ok(RecordBatch::try_new(batch.schema(), columns).map_err(|cause| cause.to_string())?)
    }

    pub(crate) fn normalize_key(
        &self,
        values: &arrow::array::ArrayRef,
        declared: &novarocks_type_contract::ValueTypeRef,
        owner: &dyn InputConstructionOwner,
        types: &dyn InputConstructionOwner,
    ) -> ExecutionResult<arrow::array::ArrayRef> {
        use super::SortKeyConstruction;
        use arrow::array::{
            Decimal128Array, FixedSizeListArray, Int8Array, LargeListArray, ListArray, MapArray,
            StructArray,
        };
        use arrow::datatypes::DataType;
        use novarocks_type_contract::owned_resources::layout::{LayoutResourceError, arc_layout};
        let result = super::normalize_sort_key_array_observed::<ConstructionFailure>(
            values,
            Some(declared),
            &mut |event| match event {
                SortKeyConstruction::Field { source, target } => types.observe(
                    0,
                    &[],
                    crate::exec::chunk::ArrayNormalizationStep::RebuildField {
                        source,
                        target,
                        name: source.name(),
                    },
                ),
                SortKeyConstruction::ArrayTable(count) => owner.request(
                    std::alloc::Layout::array::<arrow::array::ArrayRef>(count).map_err(|_| {
                        ConstructionFailure::Resource(LayoutResourceError::Arithmetic)
                    })?,
                ),
                SortKeyConstruction::FieldTable(count) => types.request(
                    std::alloc::Layout::array::<arrow::datatypes::FieldRef>(count).map_err(
                        |_| ConstructionFailure::Resource(LayoutResourceError::Arithmetic),
                    )?,
                ),
                SortKeyConstruction::SharedFields(count) => types.request(arc_layout(
                    std::alloc::Layout::array::<arrow::datatypes::FieldRef>(count).map_err(
                        |_| ConstructionFailure::Resource(LayoutResourceError::Arithmetic),
                    )?,
                )?),
                SortKeyConstruction::BufferAdoption => self.buffer_adoption(owner),
                SortKeyConstruction::Type(ty) => types.observe(
                    0,
                    &[],
                    crate::exec::chunk::ArrayNormalizationStep::RebuildType { source: ty },
                ),
                SortKeyConstruction::Work(units) => self.factory.observe_input_work(units),
                SortKeyConstruction::ArrayHeader(ty) => {
                    let layout = match ty {
                        DataType::Int8 => std::alloc::Layout::new::<Int8Array>(),
                        DataType::Decimal128(_, _) => std::alloc::Layout::new::<Decimal128Array>(),
                        DataType::List(_) => std::alloc::Layout::new::<ListArray>(),
                        DataType::LargeList(_) => std::alloc::Layout::new::<LargeListArray>(),
                        DataType::FixedSizeList(_, _) => {
                            std::alloc::Layout::new::<FixedSizeListArray>()
                        }
                        DataType::Struct(_) => std::alloc::Layout::new::<StructArray>(),
                        DataType::Map(_, _) => std::alloc::Layout::new::<MapArray>(),
                        _ => {
                            return Err(ConstructionFailure::InvalidSource(
                                "sort key header constructor has an unsupported source".into(),
                            ));
                        }
                    };
                    owner.request(arc_layout(layout)?)
                }
            },
        )?;
        Ok(result)
    }

    /// The original array concat entry uses the same neutral constructor body
    /// as payload concat. Its temporary source/index structures exit with this
    /// call; only the supplied result scope is retained by the caller.
    pub(crate) fn concat_array(
        &self,
        parts: &[&arrow::array::ArrayRef],
        source: &Chunk,
        retained: &dyn InputConstructionOwner,
    ) -> ExecutionResult<arrow::array::ArrayRef> {
        let temporary = self.begin(source)?;
        let mut temporary_request = |layout| temporary.request(layout);
        let mut temporary_opaque = |bytes| temporary.observe_opaque_upper_bound(bytes);
        let mut retained_request = |layout| retained.request(layout);
        let mut retained_opaque = |bytes| retained.observe_opaque_upper_bound(bytes);
        let mut temporary_creation = novarocks_type_contract::ValueSchemaCreation::new(
            temporary.storage_lifetime(),
            &mut temporary_request,
            &mut temporary_opaque,
        );
        let mut retained_creation = novarocks_type_contract::ValueSchemaCreation::new(
            retained.storage_lifetime(),
            &mut retained_request,
            &mut retained_opaque,
        );
        novarocks_functions::selected_copy::preflight_concat_structure_construction(
            parts,
            &mut temporary_creation,
            &mut retained_creation,
            &mut |boundary| self.factory.observe_input_work(usize::from(!boundary)),
        )
        .map_err(concat_failure)?;
        let mut sources =
            self.vector::<&dyn arrow::array::Array>(parts.len(), temporary.as_ref())?;
        sources.extend(parts.iter().map(|array| array.as_ref()));
        self.factory.observe_input_work(0)?;
        let result = arrow::compute::concat(&sources).map_err(|cause| cause.to_string())?;
        self.factory.observe_input_work(0)?;
        Ok(result)
    }

    /// Raw internal sort keys keep their already declared value semantics;
    /// this operation only describes and executes the original Arrow take.
    pub(crate) fn take_array(
        &self,
        array: &arrow::array::ArrayRef,
        indices: &dyn arrow::array::Array,
        source: &Chunk,
        retained: &dyn InputConstructionOwner,
    ) -> ExecutionResult<arrow::array::ArrayRef> {
        use novarocks_functions::selected_copy::{
            CopyIndexView, preflight_take_structure_construction,
        };
        let selection = if let Some(indices) =
            indices.as_any().downcast_ref::<arrow::array::UInt32Array>()
        {
            CopyIndexView::UInt32(indices)
        } else if let Some(indices) = indices.as_any().downcast_ref::<arrow::array::UInt64Array>() {
            CopyIndexView::UInt64(indices)
        } else {
            return Err(ConstructionFailure::InvalidSource(
                "sort original selection has an unsupported index carrier".into(),
            )
            .into());
        };
        let temporary = self.begin(source)?;
        let mut temporary_request = |layout| temporary.request(layout);
        let mut temporary_opaque = |bytes| temporary.observe_opaque_upper_bound(bytes);
        let mut retained_request = |layout| retained.request(layout);
        let mut retained_opaque = |bytes| retained.observe_opaque_upper_bound(bytes);
        let mut temporary_creation = novarocks_type_contract::ValueSchemaCreation::new(
            temporary.storage_lifetime(),
            &mut temporary_request,
            &mut temporary_opaque,
        );
        let mut retained_creation = novarocks_type_contract::ValueSchemaCreation::new(
            retained.storage_lifetime(),
            &mut retained_request,
            &mut retained_opaque,
        );
        preflight_take_structure_construction(
            array.as_ref(),
            selection,
            &mut temporary_creation,
            &mut retained_creation,
            &mut |boundary| self.factory.observe_input_work(usize::from(!boundary)),
        )
        .map_err(copy_failure)?;
        self.factory.observe_input_work(0)?;
        let result = arrow::compute::take(array.as_ref(), indices, None)
            .map_err(|cause| cause.to_string())?;
        self.factory.observe_input_work(0)?;
        Ok(result)
    }

    pub(crate) fn concat(
        &self,
        chunks: &[Chunk],
        retained: &dyn InputConstructionOwner,
    ) -> ExecutionResult<RecordBatch> {
        self.verify_chunks(chunks)?;
        let first = &chunks[0];
        let temporary = self.begin(first)?;
        let schema = self.output.arrow_schema_ref();
        // These are the original concat_batches outer constructors. The
        // per-array neutral model owns only that kernel's internal structure.
        if !schema.fields().is_empty() {
            temporary.request(std::alloc::Layout::array::<&RecordBatch>(chunks.len())
                .map_err(|_| ConstructionFailure::Resource(novarocks_type_contract::owned_resources::layout::LayoutResourceError::Arithmetic))?)?;
            retained.request(std::alloc::Layout::array::<arrow::array::ArrayRef>(schema.fields().len())
                .map_err(|_| ConstructionFailure::Resource(novarocks_type_contract::owned_resources::layout::LayoutResourceError::Arithmetic))?)?;
        }
        for column in 0..schema.fields().len() {
            temporary.request(std::alloc::Layout::array::<&dyn arrow::array::Array>(chunks.len())
                .map_err(|_| ConstructionFailure::Resource(novarocks_type_contract::owned_resources::layout::LayoutResourceError::Arithmetic))?)?;
            let mut sources =
                self.vector::<&arrow::array::ArrayRef>(chunks.len(), temporary.as_ref())?;
            sources.extend(
                chunks
                    .iter()
                    .map(|chunk| chunk.record_batch().column(column)),
            );
            let mut temporary_request = |layout| temporary.request(layout);
            let mut temporary_opaque = |bytes| temporary.observe_opaque_upper_bound(bytes);
            let mut result_request = |layout| retained.request(layout);
            let mut result_opaque = |bytes| retained.observe_opaque_upper_bound(bytes);
            let mut temporary_creation = novarocks_type_contract::ValueSchemaCreation::new(
                temporary.storage_lifetime(),
                &mut temporary_request,
                &mut temporary_opaque,
            );
            let mut result_creation = novarocks_type_contract::ValueSchemaCreation::new(
                retained.storage_lifetime(),
                &mut result_request,
                &mut result_opaque,
            );
            novarocks_functions::selected_copy::preflight_concat_structure_construction(
                &sources,
                &mut temporary_creation,
                &mut result_creation,
                &mut |_boundary| self.factory.observe_input_work(1),
            )
            .map_err(concat_failure)?;
        }
        // Original payload algorithm and SchemeS are unchanged. Complete
        // declarations are already exact; there is no NULL/name inference.
        Ok(
            arrow::compute::concat_batches(&schema, chunks.iter().map(Chunk::record_batch))
                .map_err(|cause| cause.to_string())?,
        )
    }

    pub(crate) fn source_type_storage(
        &self,
        source: &Chunk,
        types: &dyn InputConstructionOwner,
    ) -> ExecutionResult<novarocks_type_contract::ValueTypeStorageLifetime> {
        let mut storage = types.storage_lifetime();
        for slot in source.chunk_schema().slots() {
            self.factory.observe_input_work(1)?;
            storage = storage.retaining_observed::<ConstructionFailure>(
                &slot
                    .typed_binding()
                    .layout()
                    .arrow_backing_storage_lifetime(),
                |layout| types.request(layout),
            )?;
        }
        Ok(storage)
    }

    pub(crate) fn begin(&self, source: &Chunk) -> ExecutionResult<Box<dyn InputConstructionOwner>> {
        Ok(self.factory.begin_input_construction(source)?)
    }

    // The original Arrow operation has already authored the payload. This
    // method governs only the actual Chunk columns and RecordBatch wrapper;
    // the operation's own array/header construction is a separate preflight.
    pub(crate) fn publish(
        &self,
        batch: RecordBatch,
        source: &Chunk,
        owner: &dyn InputConstructionOwner,
    ) -> ExecutionResult<Chunk> {
        let (_, columns, rows) = batch.into_parts();
        let storage = owner
            .storage_lifetime()
            .retaining_observed(&source.array_type_storage_lifetime(), |layout| {
                owner.request(layout)
            })?;
        Ok(Chunk::try_new_typed_with_columns_owned_observed::<
            ConstructionFailure,
            _,
        >(
            self.output.clone(),
            columns,
            rows,
            storage,
            |step| owner.observe(0, &[], step),
        )?)
    }
}

fn copy_failure(
    error: novarocks_functions::selected_copy::CopyConstructionError<ConstructionFailure>,
) -> crate::runtime::fragment::ExecutionFailure {
    use novarocks_functions::selected_copy::{CopyConstructionError, CopyError};
    match error {
        CopyConstructionError::Creator(cause) => cause.into(),
        CopyConstructionError::Resource(cause) => ConstructionFailure::Resource(cause).into(),
        CopyConstructionError::Copy(CopyError::Control(cause)) => cause.into(),
        CopyConstructionError::Copy(CopyError::Extent) => {
            novarocks_functions::KernelFailure::ResourceExhausted.into()
        }
        CopyConstructionError::Copy(CopyError::Invalid(message)) => {
            ConstructionFailure::InvalidSource(message.into()).into()
        }
        CopyConstructionError::Copy(CopyError::Unsupported(ty)) => {
            ConstructionFailure::InvalidSource(format!(
                "sort original copy does not support {ty:?}"
            ))
            .into()
        }
    }
}

fn concat_failure(
    error: novarocks_functions::selected_copy::ConcatStructureFailure<ConstructionFailure>,
) -> crate::runtime::fragment::ExecutionFailure {
    match error {
        novarocks_functions::selected_copy::ConcatStructureFailure::Construction(cause) => {
            copy_failure(cause)
        }
        novarocks_functions::selected_copy::ConcatStructureFailure::ValueType(cause) => {
            ConstructionFailure::InvalidSource(format!("sort concat source declaration: {cause}"))
                .into()
        }
        novarocks_functions::selected_copy::ConcatStructureFailure::Source(cause) => {
            ConstructionFailure::InvalidSource(cause).into()
        }
    }
}
