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

//! Driver-local ordered RF state for the original compiled aggregate.
//! Group identity and expression evaluation remain with that processor. The
//! helper only copies a newly inserted group's already evaluated key
//! onto its creating host and uses the existing neutral producer lifecycle.

use crate::exec::hash_table::key_index::{KeyId, KeyReadColumn, OwnedKeyReadBatch};
use novarocks_functions::{KernelDiagnostic, KernelEvaluationControl, KernelFailure};
use novarocks_local_program::{AggregateTopNFilter, LocalProgram, ProgramNodeId, ProgramNodeKind};

use super::aggregate::AGGREGATE_TOPN_PUBLISH_ROWS;
use super::aggregate::native_runtime_filter::{
    AggregateTopNProducerSession, AggregateTopNProducerSessionFactory,
};
use super::aggregate::topn_boundary::{
    AggregateTopNBoundaryError, OwnedAggregateTopNBoundaryState, visit_key_read_page,
};
use super::compiled_expression::RuntimeKernelControl;
use crate::exec::hash_table::allocation::AggregateVec;
use crate::runtime::fragment::{ExecutionFailure, ExecutionResult};
use crate::runtime_filter::{
    OrderedTopNAllocationHost, RuntimeFilterProducerFailure, RuntimeFilterSessionRef,
};

fn invalid(message: &'static str) -> ExecutionFailure {
    KernelFailure::InvalidProgram(KernelDiagnostic::new(message)).into()
}

fn specifications(
    program: &LocalProgram,
    node: ProgramNodeId,
) -> ExecutionResult<&[AggregateTopNFilter]> {
    let Some(node) = program.graph().nodes().get(node.index()) else {
        return Err(invalid("compiled aggregate TopN node is absent"));
    };
    let ProgramNodeKind::Aggregate { topn_filters, .. } = node.kind() else {
        return Err(invalid("compiled aggregate TopN node is not an Aggregate"));
    };
    if topn_filters.is_empty() {
        return Err(invalid(
            "compiled aggregate TopN has no frozen producer sites",
        ));
    }
    Ok(topn_filters.as_slice())
}

pub(crate) struct CompiledAggregateTopNFactory {
    node: ProgramNodeId,
    session: AggregateTopNProducerSessionFactory,
    host: OrderedTopNAllocationHost,
    control: RuntimeKernelControl,
}

impl CompiledAggregateTopNFactory {
    pub(crate) fn try_new(
        program: &LocalProgram,
        node: ProgramNodeId,
        session: RuntimeFilterSessionRef,
        actual_dop: i32,
        host: &OrderedTopNAllocationHost,
        control: &RuntimeKernelControl,
    ) -> ExecutionResult<Self> {
        let specs = specifications(program, node)?;
        let session = AggregateTopNProducerSessionFactory::try_from_compiled(
            specs, session, actual_dop, host, control,
        )?;
        Ok(Self {
            node,
            session,
            host: host.clone(),
            control: control.clone(),
        })
    }

    pub(crate) fn create_for_driver(
        &self,
        program: &LocalProgram,
        dop: i32,
        driver_id: i32,
    ) -> ExecutionResult<CompiledAggregateTopN> {
        let specs = specifications(program, self.node)?;
        self.control.checkpoint(0)?;
        let mut boundaries = AggregateVec::new_in(self.host.vector_allocator());
        boundaries
            .try_reserve_exact(specs.len())
            .map_err(|_| KernelFailure::ResourceExhausted)?;
        for (index, spec) in specs.iter().enumerate() {
            self.control.checkpoint(1)?;
            let order = self.session.order_contract(index).ok_or_else(|| {
                invalid("compiled aggregate TopN producer and boundary sites differ")
            })?;
            boundaries.push(OwnedAggregateTopNBoundaryState::try_new_owned(
                spec.limit,
                order.clone(),
                &self.host,
            )?);
        }
        if self.session.order_contract(specs.len()).is_some() {
            return Err(invalid(
                "compiled aggregate TopN producer and boundary sites differ",
            ));
        }
        self.control.checkpoint(0)?;
        // Complete every boundary before constructing a session. No producer
        // is opened until the processor reaches its original runtime bind.
        let session = self
            .session
            .create_for_driver_owned(dop, driver_id, &self.control)?;
        Ok(CompiledAggregateTopN {
            node: self.node,
            host: self.host.clone(),
            control: self.control.clone(),
            session,
            boundaries,
            rows_since_publish: 0,
        })
    }
}

pub(crate) struct CompiledAggregateTopN {
    node: ProgramNodeId,
    host: OrderedTopNAllocationHost,
    control: RuntimeKernelControl,
    session: AggregateTopNProducerSession,
    boundaries: AggregateVec<OwnedAggregateTopNBoundaryState>,
    rows_since_publish: usize,
}

impl CompiledAggregateTopN {
    pub(crate) fn bind(&mut self) -> ExecutionResult<()> {
        self.session.bind_owned(&self.control)
    }

    /// The caller passes only newly published complete K classes. A loan can
    /// group physical pages in a different order, so restore requested ordinal
    /// order using metadata while keeping copied scalar payloads incremental.
    pub(crate) fn observe_new_groups(
        &mut self,
        program: &LocalProgram,
        read: &OwnedKeyReadBatch<'_>,
        ids: &[KeyId],
    ) -> ExecutionResult<()> {
        let specs = specifications(program, self.node)?;
        if specs.len() != self.boundaries.len() || read.len() != ids.len() {
            return Err(invalid(
                "compiled aggregate TopN loan and producer sites differ",
            ));
        }
        for (spec, boundary) in specs.iter().zip(self.boundaries.iter_mut()) {
            self.control.checkpoint(1)?;
            let column = read.columns().get(spec.group_key_ordinal).ok_or(
                AggregateTopNBoundaryError::MissingGroupKey {
                    ordinal: spec.group_key_ordinal,
                    actual: read.columns().len(),
                },
            )?;
            observe_key_column(column, ids, boundary, &self.host, &self.control)?;
        }
        Ok(())
    }

    /// Publication follows successful evaluation/update of the complete batch.
    pub(crate) fn after_successful_input(&mut self, rows: usize) -> ExecutionResult<()> {
        self.rows_since_publish = self
            .rows_since_publish
            .checked_add(rows)
            .ok_or(KernelFailure::ResourceExhausted)?;
        if self.rows_since_publish >= AGGREGATE_TOPN_PUBLISH_ROWS {
            self.session
                .submit_pending_owned(&mut self.boundaries, &self.control)?;
            self.rows_since_publish = 0;
        }
        Ok(())
    }

    pub(crate) fn finish(&mut self) -> ExecutionResult<()> {
        self.session
            .finish_owned(&mut self.boundaries, &self.control)
    }

    pub(crate) fn fail(&mut self, reason: RuntimeFilterProducerFailure) -> ExecutionResult<()> {
        self.session.fail_owned(reason)
    }
}

fn observe_key_column(
    column: &KeyReadColumn<'_>,
    ids: &[KeyId],
    boundary: &mut OwnedAggregateTopNBoundaryState,
    host: &OrderedTopNAllocationHost,
    control: &dyn KernelEvaluationControl,
) -> ExecutionResult<()> {
    control.checkpoint(0)?;
    let mut positions = AggregateVec::new_in(host.vector_allocator());
    positions.try_reserve_exact(ids.len()).map_err(|source| {
        host.vector_allocator()
            .allocation_error("reserve aggregate TopN loan positions", source)
    })?;
    for _ in ids {
        control.checkpoint(1)?;
        positions.push(None);
    }
    for (page_index, page) in column.pages().iter().enumerate() {
        control.checkpoint(1)?;
        for (row_index, row) in page.rows().iter().enumerate() {
            control.checkpoint(1)?;
            let target = positions
                .get_mut(row.ordinal)
                .ok_or_else(|| invalid("aggregate TopN loan ordinal is outside new classes"))?;
            if target.replace((page_index, row_index)).is_some() {
                return Err(invalid("aggregate TopN loan ordinal is repeated"));
            }
        }
    }
    let contract = boundary.contract().clone();
    let mut begin = 0;
    while begin < ids.len() {
        control.checkpoint(1)?;
        let (page_index, row_index) =
            positions[begin].ok_or_else(|| invalid("aggregate TopN loan ordinal is missing"))?;
        let mut end = begin + 1;
        while end < ids.len() && positions[end] == Some((page_index, row_index + end - begin)) {
            control.checkpoint(1)?;
            end += 1;
        }
        let page = &column.pages()[page_index];
        visit_key_read_page(
            &contract,
            page.values(),
            &page.rows()[row_index..row_index + end - begin],
            control,
            &mut |ordinal, value| {
                let key = host.copy_scalar(value, control)?;
                boundary.observe_owned_group(ids[ordinal].index(), key, control)?;
                Ok(())
            },
        )?;
        begin = end;
    }
    Ok(())
}

#[cfg(test)]
mod key_index_tests {
    use super::*;
    use crate::exec::hash_table::{
        allocation::AggregateAllocator,
        key_index::{
            IndexPurpose, KeyIndex, KeyInput, KeyMatch, KeySpec, NullPolicy, PreparedKeyBatch,
            hash_batch,
        },
    };
    use crate::exec::pipeline::operator::InputConstructionOwner;
    use crate::runtime::{mem_tracker::MemTracker, runtime_state::RuntimeErrorState};
    use crate::runtime_filter::contribution::{
        OrderedScalarRef, OrderedTupleRef, RuntimeOrderContract, RuntimeOrderKey,
        RuntimeOrderNullOrder, RuntimeOrderSortDirection,
    };
    use arrow::{
        array::{ArrayRef, DictionaryArray, Int32Array, Int64Array, StringArray},
        datatypes::{DataType, Int32Type, Schema},
    };
    use novarocks_failure_contract::FailureProjection;
    use novarocks_functions::FunctionValueType;
    use novarocks_value_arrays::{
        batch::hash::HashScratch, decode::ValueConversionContext, reachability::ReachableRows,
    };
    use std::{num::NonZeroU32, sync::Arc};
    struct Pass;
    impl KernelEvaluationControl for Pass {
        fn checkpoint(&self, _: u32) -> Result<(), KernelFailure> {
            Ok(())
        }
        fn wait(&self, _: std::time::Duration) -> Result<(), KernelFailure> {
            panic!("TopN key copy never waits")
        }
    }
    struct Stop;
    impl KernelEvaluationControl for Stop {
        fn checkpoint(&self, _: u32) -> Result<(), KernelFailure> {
            Err(KernelFailure::DeadlineExceeded)
        }
        fn wait(&self, _: std::time::Duration) -> Result<(), KernelFailure> {
            panic!("TopN key copy never waits")
        }
    }
    fn owner(tracker: Arc<MemTracker>) -> Box<dyn InputConstructionOwner> {
        let factory=crate::exec::pipeline::operator::input_construction::try_runtime_input_construction_factory(
            crate::exec::pipeline::pipeline::representation_construction_profile().unwrap(),tracker,Arc::new(RuntimeErrorState::default()),
        ).unwrap();
        let layout = crate::exec::chunk::typed_fixture::layout(Arc::new(Schema::empty()), &[], &[]);
        let schema = factory.create_compiled_chunk_schema(&layout).unwrap();
        factory.begin_typed_construction(&schema).unwrap()
    }
    fn index(
        types: &[FunctionValueType],
        arrays: &[ArrayRef],
        construction: &dyn InputConstructionOwner,
        tracker: &Arc<MemTracker>,
    ) -> (KeyIndex, Vec<KeyId>) {
        let spec = KeySpec::new(
            types,
            &vec![NullPolicy::Equal; types.len()],
            construction,
            &|| Ok(()),
        )
        .unwrap();
        let mut table =
            KeyIndex::new(spec, IndexPurpose::KeyIds, tracker.clone(), construction).unwrap();
        let rows = (0..arrays[0].len()).collect::<Vec<_>>();
        let readable = ReachableRows::all(rows.len())
            .materialize_observed(
                &mut |l| construction.request(l).map_err(ExecutionFailure::from),
                &mut |_| Ok(()),
            )
            .unwrap();
        let input = KeyInput::new_for_spec(
            table.spec(),
            arrays,
            types,
            &readable,
            &rows,
            &mut ValueConversionContext::default(),
            construction,
            &|| Ok(()),
        )
        .unwrap();
        let prepared = PreparedKeyBatch::from_captured(
            table.spec(),
            &input,
            AggregateAllocator::new(tracker.clone()),
            &|| Ok(()),
        )
        .unwrap();
        let hashes = hash_batch(
            &prepared,
            table.policy(),
            &mut HashScratch::default(),
            construction,
            &|| Ok(()),
        )
        .unwrap();
        let intern = table
            .intern_batch(&prepared, &hashes, construction, &Pass)
            .unwrap();
        assert_eq!(
            intern
                .matches()
                .iter()
                .filter(|v| matches!(v, KeyMatch::Found(_)))
                .count(),
            rows.len()
        );
        let ids = intern.new_keys().iter().map(|new| new.id).collect();
        (table, ids)
    }
    fn order(
        ty: FunctionValueType,
        nulls: RuntimeOrderNullOrder,
    ) -> crate::runtime_filter::RuntimeOrderContractRef {
        Arc::new(RuntimeOrderContract::from_frozen(
            [RuntimeOrderKey::with_value_type(
                ty,
                RuntimeOrderSortDirection::Ascending,
                nulls,
            )],
            [2; 32],
            [3; 32],
        ))
        .into()
    }
    #[test]
    fn production_typed_page_leaf_counts_complete_groups_across_dictionary_nulls_and_keeps_original_owned_utf8()
     {
        for nulls in [RuntimeOrderNullOrder::First, RuntimeOrderNullOrder::Last] {
            let tracker = MemTracker::new_root("K AggregateTopN source");
            let construction = owner(tracker.clone());
            let projection = FunctionValueType::new(DataType::Utf8, true);
            let dictionary: ArrayRef = Arc::new(
                DictionaryArray::<Int32Type>::try_new(
                    Int32Array::from(vec![0, 0, 0, 1]),
                    Arc::new(StringArray::from(vec![
                        Some("same long ordered projection"),
                        None,
                    ])),
                )
                .unwrap(),
            );
            let weak = Arc::downgrade(&dictionary);
            let arrays: [ArrayRef; 2] = [dictionary, Arc::new(Int64Array::from(vec![0, 1, 0, 2]))];
            let (table, ids) = index(
                &[
                    projection.clone(),
                    FunctionValueType::new(DataType::Int64, false),
                ],
                &arrays,
                construction.as_ref(),
                &tracker,
            );
            assert_eq!(
                ids.iter().map(|id| id.index()).collect::<Vec<_>>(),
                vec![0, 1, 2]
            );
            let read = table.read_keys(&ids, construction.as_ref(), &Pass).unwrap();
            let rf_tracker = MemTracker::new_root("K AggregateTopN original ordered copy");
            let host = OrderedTopNAllocationHost::new(rf_tracker.clone());
            // Ordered endpoints share the value domain while retaining
            // their own distinct root NULL declarations.
            let contract = order(projection.with_nullable(false).unwrap(), nulls);
            let mut boundary = OwnedAggregateTopNBoundaryState::try_new_owned(
                NonZeroU32::new(2).unwrap(),
                contract,
                &host,
            )
            .unwrap();
            observe_key_column(&read.columns()[0], &ids, &mut boundary, &host, &Pass).unwrap();
            let key = boundary.take_pending_controlled(&Pass).unwrap().unwrap();
            drop(read);
            drop(table);
            drop(arrays);
            drop(construction);
            assert!(weak.upgrade().is_none());
            // Three complete groups project [text,text,NULL]. The second best
            // is text for both NULL orders; the duplicate complete key adds no
            // fourth group. No K hash participates in this ordered expectation.
            assert!(matches!(
                key.borrowed(),
                OrderedTupleRef::Single(Some(OrderedScalarRef::Utf8(
                    "same long ordered projection"
                )))
            ));
            assert!(rf_tracker.current() > 0);
        }
    }
    #[test]
    fn production_fixed_page_leaf_preserves_projected_multiplicity_of_real_complete_k_groups() {
        let tracker = MemTracker::new_root("K TopN fixed complete groups");
        let construction = owner(tracker.clone());
        let projection = FunctionValueType::new(DataType::Int64, false);
        let arrays: [ArrayRef; 2] = [
            Arc::new(Int64Array::from(vec![7, 7, 7])),
            Arc::new(StringArray::from(vec!["a", "b", "a"])),
        ];
        let (table, ids) = index(
            &[
                projection.clone(),
                FunctionValueType::new(DataType::Utf8, false),
            ],
            &arrays,
            construction.as_ref(),
            &tracker,
        );
        assert_eq!(
            ids.iter().map(|id| id.index()).collect::<Vec<_>>(),
            vec![0, 1]
        );
        let read = table.read_keys(&ids, construction.as_ref(), &Pass).unwrap();
        let host = OrderedTopNAllocationHost::new(MemTracker::new_root("K TopN fixed RF owner"));
        for limit in [2, 3] {
            let mut boundary = OwnedAggregateTopNBoundaryState::try_new_owned(
                NonZeroU32::new(limit).unwrap(),
                order(
                    projection.with_nullable(true).unwrap(),
                    RuntimeOrderNullOrder::Last,
                ),
                &host,
            )
            .unwrap();
            observe_key_column(&read.columns()[0], &ids, &mut boundary, &host, &Pass).unwrap();
            let pending = boundary.take_pending_controlled(&Pass).unwrap();
            if limit == 2 {
                assert!(matches!(
                    pending.unwrap().borrowed(),
                    OrderedTupleRef::Single(Some(OrderedScalarRef::Int64(7)))
                ));
            } else {
                assert!(
                    pending.is_none(),
                    "a repeated complete key cannot become a third group"
                );
            }
        }
    }

    #[test]
    fn production_typed_page_leaf_preserves_full_nominal_contract_and_original_control_failure() {
        let tracker = MemTracker::new_root("K TopN nominal source");
        let construction = owner(tracker.clone());
        let json = FunctionValueType::try_with_logical_type(
            DataType::Utf8,
            true,
            novarocks_type_contract::ValueLogicalType::Json,
        )
        .unwrap();
        let arrays: [ArrayRef; 1] = [Arc::new(StringArray::from(vec![Some("[1]"), None]))];
        let (table, ids) = index(&[json], &arrays, construction.as_ref(), &tracker);
        let read = table.read_keys(&ids, construction.as_ref(), &Pass).unwrap();
        let host =
            OrderedTopNAllocationHost::new(MemTracker::new_root("K TopN nominal ordered owner"));
        let contract = order(
            FunctionValueType::new(DataType::Utf8, true),
            RuntimeOrderNullOrder::Last,
        );
        let mut boundary = OwnedAggregateTopNBoundaryState::try_new_owned(
            NonZeroU32::new(1).unwrap(),
            contract,
            &host,
        )
        .unwrap();
        assert!(observe_key_column(&read.columns()[0], &ids, &mut boundary, &host, &Stop).is_err());
        let cause =
            observe_key_column(&read.columns()[0], &ids, &mut boundary, &host, &Pass).unwrap_err();
        assert_eq!(
            cause.failure_fact(),
            AggregateTopNBoundaryError::CandidateContractMismatch(
                crate::runtime_filter::contribution::OrderedTupleError::TypeMismatch
            )
            .failure_fact()
        );
        assert!(boundary.take_pending_controlled(&Pass).unwrap().is_none());
    }
}
