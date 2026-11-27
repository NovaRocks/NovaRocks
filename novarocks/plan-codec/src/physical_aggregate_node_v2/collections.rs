// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

//! Original Aggregate collection authors shared with complete TopN projection.
//! No scope, header, grouping law or runtime capability is created here.

use super::*;

pub(crate) fn node_gate(
    model: &Model,
    source: usize,
    values: usize,
    limits: Option<NodeProjectionLimits>,
    admit: &mut Option<&mut NodeAdmit<'_>>,
) -> Result<AggregateNodeProjectionFacts, Error> {
    match admit {
        Some(parent) => model.admit_in(source, values, limits, *parent),
        None => model.numerical_facts(source, values, limits),
    }
}
pub(crate) fn node_facts(
    model: &Model,
    source: usize,
    values: usize,
    limits: Option<NodeProjectionLimits>,
    admit: &mut Option<&mut NodeAdmit<'_>>,
    w: &mut CompileCheckpoints<'_>,
) -> Result<AggregateNodeProjectionFacts, Error> {
    match admit {
        Some(parent) => model.facts_in(source, values, limits, *parent, w),
        None => model.facts(source, values, limits, w),
    }
}

/// A mutable loan of the containing node's original numerical model. The
/// caller includes the actual outer collection capacities in `known`, charges
/// the group/call roots and their lookup work before invoking these ports, and
/// validates the original namespace loans. Existing non-collection work stays
/// in the same model; source coexistence is never invoiced a second time.
pub(crate) struct CollectionProjection<'model> {
    pub(crate) model: &'model mut Model,
    pub(crate) known: usize,
    pub(crate) source: usize,
    pub(crate) values: usize,
    pub(crate) limits: Option<NodeProjectionLimits>,
}

pub(crate) fn encode_collection_lookup_work(
    groups: &[(novarocks_expression_contract::RootOrdinal, p::ValueId)],
    calls: &[p::AggregateCall],
    e: &EncodedExpressions<'_, '_, '_>,
) -> Result<usize, Error> {
    add(
        mul(mul(2, calls.len())?, e.aggregates().source_counts())?,
        mul(groups.len(), e.lookup_work_upper_bound()?)?,
    )
}
pub(crate) fn decode_collection_lookup_work(
    groups: &[wire::ExpressionOutput],
    calls: &[wire::AggregateCall],
    e: &DecodedExpressions<'_, '_, '_>,
    a: &MaterializedAggregateBindings<'_, '_, '_>,
) -> Result<usize, Error> {
    add(
        mul(mul(3, calls.len())?, add(a.definitions().len(), 2)?)?,
        mul(groups.len(), e.lookup_work_upper_bound()?)?,
    )
}

impl CollectionProjection<'_> {
    pub(crate) fn count_encode(
        self,
        calls: &[p::AggregateCall],
        e: &EncodedExpressions<'_, '_, '_>,
        w: &mut CompileCheckpoints<'_>,
    ) -> Result<(), Error> {
        self.count_encode_core(calls, e, None, w)
    }
    pub(crate) fn count_encode_in(
        self,
        calls: &[p::AggregateCall],
        e: &EncodedExpressions<'_, '_, '_>,
        admit: &mut NodeAdmit<'_>,
        w: &mut CompileCheckpoints<'_>,
    ) -> Result<(), Error> {
        self.count_encode_core(calls, e, Some(admit), w)
    }
    fn count_encode_core(
        self,
        calls: &[p::AggregateCall],
        e: &EncodedExpressions<'_, '_, '_>,
        mut admit: Option<&mut NodeAdmit<'_>>,
        w: &mut CompileCheckpoints<'_>,
    ) -> Result<(), Error> {
        let Self {
            model,
            mut known,
            source,
            values: value_count,
            limits,
        } = self;
        let initial_work = model.delegated_work;
        let expression_lookup = e.lookup_work_upper_bound()?;
        let mut nested_exprs = 0;
        for call in calls {
            let nested = add(
                add(call.arguments.len(), call.order_by.len())?,
                usize::from(call.filter_root.is_some()),
            )?;
            model.items = add(model.items, nested)?;
            known = add(
                known,
                add(
                    bytes::<novarocks_expression_contract::RootOrdinal>(call.arguments.len())?,
                    bytes::<p::SortExpr>(call.order_by.len())?,
                )?,
            )?;
            nested_exprs = add(nested_exprs, nested)?;
            model.request::<u32>(call.arguments.len(), 1)?;
            model.request::<wire::SortExpression>(call.order_by.len(), 1)?;
            model.delegated_work = add(initial_work, mul(nested_exprs, expression_lookup)?)?;
            node_gate(model, source, value_count, limits, &mut admit)?;
            count_prefix(model.inputs, model.items, source, known, limits, w)?;
            w.step()?;
        }
        Ok(())
    }
    pub(crate) fn count_decode(
        self,
        calls: &[wire::AggregateCall],
        e: &DecodedExpressions<'_, '_, '_>,
        w: &mut CompileCheckpoints<'_>,
    ) -> Result<(), Error> {
        self.count_decode_core(calls, e, None, w)
    }
    pub(crate) fn count_decode_in(
        self,
        calls: &[wire::AggregateCall],
        e: &DecodedExpressions<'_, '_, '_>,
        admit: &mut NodeAdmit<'_>,
        w: &mut CompileCheckpoints<'_>,
    ) -> Result<(), Error> {
        self.count_decode_core(calls, e, Some(admit), w)
    }
    fn count_decode_core(
        self,
        calls: &[wire::AggregateCall],
        e: &DecodedExpressions<'_, '_, '_>,
        mut admit: Option<&mut NodeAdmit<'_>>,
        w: &mut CompileCheckpoints<'_>,
    ) -> Result<(), Error> {
        let Self {
            model,
            mut known,
            source,
            values: value_count,
            limits,
        } = self;
        let initial_work = model.delegated_work;
        let expression_lookup = e.lookup_work_upper_bound()?;
        let mut nested_exprs = 0;
        for call in calls {
            let nested = add(
                add(call.argument_roots.len(), call.order_by.len())?,
                usize::from(call.filter_root.is_some()),
            )?;
            model.items = add(model.items, nested)?;
            known = add(
                known,
                add(
                    bytes::<u32>(call.argument_roots.capacity())?,
                    bytes::<wire::SortExpression>(call.order_by.capacity())?,
                )?,
            )?;
            nested_exprs = add(nested_exprs, nested)?;
            model.request::<novarocks_expression_contract::RootOrdinal>(
                call.argument_roots.len(),
                2,
            )?;
            model.request::<p::SortExpr>(call.order_by.len(), 2)?;
            model.delegated_work = add(initial_work, mul(nested_exprs, expression_lookup)?)?;
            node_gate(model, source, value_count, limits, &mut admit)?;
            count_prefix(model.inputs, model.items, source, known, limits, w)?;
            w.step()?;
        }
        Ok(())
    }
}

pub(crate) fn preflight_collection_binding_copies(
    calls: &[wire::AggregateCall],
    e: &DecodedExpressions<'_, '_, '_>,
    a: &MaterializedAggregateBindings<'_, '_, '_>,
    parent: CollectionProjection<'_>,
    dependency: usize,
    limits: Option<BindingProjectionLimits>,
    w: &mut CompileCheckpoints<'_>,
) -> Result<AggregateNodeProjectionFacts, Error> {
    preflight_collection_binding_copies_core(calls, e, a, parent, dependency, limits, None, w)
}
pub(crate) fn preflight_collection_binding_copies_in<'parent>(
    calls: &[wire::AggregateCall],
    e: &DecodedExpressions<'_, '_, '_>,
    a: &MaterializedAggregateBindings<'_, '_, '_>,
    parent: CollectionProjection<'_>,
    dependency: usize,
    limits: Option<BindingProjectionLimits>,
    admit: &'parent mut NodeAdmit<'parent>,
    w: &mut CompileCheckpoints<'_>,
) -> Result<AggregateNodeProjectionFacts, Error> {
    preflight_collection_binding_copies_core(
        calls,
        e,
        a,
        parent,
        dependency,
        limits,
        Some(admit),
        w,
    )
}
pub(crate) fn preflight_collection_binding_copies_core<'parent>(
    calls: &[wire::AggregateCall],
    e: &DecodedExpressions<'_, '_, '_>,
    a: &MaterializedAggregateBindings<'_, '_, '_>,
    parent: CollectionProjection<'_>,
    dependency: usize,
    limits: Option<BindingProjectionLimits>,
    admit: Option<&'parent mut NodeAdmit<'parent>>,
    w: &mut CompileCheckpoints<'_>,
) -> Result<AggregateNodeProjectionFacts, Error> {
    let mut child = MaterializationModel::for_composition(
        calls.len(),
        add(e.types().value_types().len(), 1)?,
        parent.source,
        dependency,
    );
    let caller_owned = admit.is_some();
    match admit {
        Some(admit) => {
            child.compose_in_node_in(*parent.model, parent.values, parent.limits, limits, admit)?
        }
        None => child.compose_in_node(*parent.model, parent.values, parent.limits, limits)?,
    }
    // Every delegated count/type check gates the same containing node before
    // its next observer. All signatures are counted before any full type walk.
    for call in calls {
        let id = required(call.aggregate_binding_id, w)?;
        if caller_owned {
            let found = a.definition_captured(
                id,
                &mut |source, w| {
                    preflight_aggregate_binding_copy_counts_in(
                        source,
                        &mut child,
                        limits,
                        &mut |_| Ok(()),
                        w,
                    )
                },
                w,
            )?;
            completed(
                found
                    .ok_or_else(|| invalid("Aggregate binding is not in original owned namespace")),
                w,
            )?;
        } else {
            let source_binding = binding(id, a, w)?;
            preflight_aggregate_binding_copy_counts(source_binding, &mut child, limits, w)?;
        }
        w.step()?;
    }
    child.node_facts(0, w)?;
    for call in calls {
        let source_binding = binding(required(call.aggregate_binding_id, w)?, a, w)?;
        if caller_owned {
            preflight_aggregate_binding_copy_types_in(
                source_binding,
                &mut child,
                limits,
                &mut |_| Ok(()),
                w,
            )?;
        } else {
            preflight_aggregate_binding_copy_types(source_binding, &mut child, limits, w)?;
        }
        w.step()?;
    }
    let copy_work = mul(2, child.facts.cumulative_work_upper_bound)?;
    Ok(child.node_facts(copy_work, w)?)
}

pub(crate) fn encode_collection_references(
    groups: &[(novarocks_expression_contract::RootOrdinal, p::ValueId)],
    calls: &[p::AggregateCall],
    values: &EncodedValues<'_, '_, '_>,
    e: &EncodedExpressions<'_, '_, '_>,
    w: &mut CompileCheckpoints<'_>,
) -> Result<(), Error> {
    for (expr, value) in groups {
        encoded_expr(expr.get(), e, w)?;
        reference(value.get(), values, w)?;
        w.step()?;
    }
    for call in calls {
        e.aggregates().source_id_observed(&call.binding, w)?;
        for arg in &call.arguments {
            encoded_expr(arg.get(), e, w)?;
            w.step()?;
        }
        for sort in &call.order_by {
            encoded_expr(sort.expr.get(), e, w)?;
            w.step()?;
        }
        if let Some(filter) = call.filter_root {
            encoded_expr(filter.get(), e, w)?;
            w.step()?;
        }
        reference(call.output.get(), values, w)?;
        w.step()?;
    }
    Ok(())
}
pub(crate) fn decode_collection_references(
    groups: &[wire::ExpressionOutput],
    calls: &[wire::AggregateCall],
    e: &DecodedExpressions<'_, '_, '_>,
    w: &mut CompileCheckpoints<'_>,
) -> Result<(), Error> {
    for group in groups {
        decoded_expr(required(group.root_ordinal, w)?, e, w)?;
        reference(required(group.value_id, w)?, e.values(), w)?;
        w.step()?;
    }
    for call in calls {
        for arg in &call.argument_roots {
            decoded_expr(*arg, e, w)?;
            w.step()?;
        }
        for sort in &call.order_by {
            decoded_expr(required(sort.root_ordinal, w)?, e, w)?;
            completed(
                properties::decode_direction(sort.direction).map_err(Error::from),
                w,
            )?;
            completed(
                properties::decode_nulls(sort.null_ordering).map_err(Error::from),
                w,
            )?;
            w.step()?;
        }
        if let Some(filter) = call.filter_root {
            decoded_expr(filter, e, w)?;
            w.step()?;
        }
        reference(required(call.output_value_id, w)?, e.values(), w)?;
        w.step()?;
    }
    Ok(())
}

type WireCollections = (Vec<wire::ExpressionOutput>, Vec<wire::AggregateCall>);
pub(crate) fn encode_collections(
    groups: &[(novarocks_expression_contract::RootOrdinal, p::ValueId)],
    calls: &[p::AggregateCall],
    e: &EncodedExpressions<'_, '_, '_>,
    w: &mut CompileCheckpoints<'_>,
) -> Result<WireCollections, Error> {
    let mut group_by = reserve(groups.len(), w)?;
    for (expr, value) in groups {
        group_by.push(wire::ExpressionOutput {
            root_ordinal: Some(expr.get()),
            value_id: Some(value.get()),
        });
        w.step()?;
    }
    let mut emitted = reserve(calls.len(), w)?;
    for call in calls {
        let id = e.aggregates().source_id_observed(&call.binding, w)?;
        let mut args = reserve(call.arguments.len(), w)?;
        for arg in &call.arguments {
            args.push(arg.get());
            w.step()?;
        }
        emitted.push(wire::AggregateCall {
            id: call.id.get(),
            aggregate_binding_id: Some(id),
            argument_roots: args,
            filter_root: call.filter_root.map(|root| root.get()),
            distinct: call.distinct,
            order_by: encode_sorts(&call.order_by, w)?,
            output_value_id: Some(call.output.get()),
        });
        w.step()?;
    }
    Ok((group_by, emitted))
}

type OwnedCollections = (
    Vec<(novarocks_expression_contract::RootOrdinal, p::ValueId)>,
    Vec<p::AggregateCall>,
);
/// Return the original Vecs. Boxing remains at the containing node's original
/// construction position, after both collection loops have completed.
pub(crate) fn decode_collections(
    groups: &[wire::ExpressionOutput],
    raw_calls: &[wire::AggregateCall],
    a: &MaterializedAggregateBindings<'_, '_, '_>,
    w: &mut CompileCheckpoints<'_>,
) -> Result<OwnedCollections, Error> {
    let mut group_by = reserve(groups.len(), w)?;
    for group in groups {
        group_by.push((
            novarocks_expression_contract::RootOrdinal::new(required(group.root_ordinal, w)?),
            p::ValueId::new(required(group.value_id, w)?),
        ));
        w.step()?;
    }
    let mut calls = reserve(raw_calls.len(), w)?;
    for call in raw_calls {
        let binding = copy_aggregate_binding_observed(
            binding(required(call.aggregate_binding_id, w)?, a, w)?,
            w,
        )?;
        let mut args = reserve(call.argument_roots.len(), w)?;
        for id in &call.argument_roots {
            args.push(novarocks_expression_contract::RootOrdinal::new(*id));
            w.step()?;
        }
        calls.push(p::AggregateCall {
            id: p::AggregateCallId::new(call.id),
            binding,
            arguments: boxed(args, w)?,
            filter_root: call
                .filter_root
                .map(novarocks_expression_contract::RootOrdinal::new),
            distinct: call.distinct,
            order_by: decode_sorts(&call.order_by, w)?,
            output: p::ValueId::new(required(call.output_value_id, w)?),
        });
        w.step()?;
    }
    Ok((group_by, calls))
}
