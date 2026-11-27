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

use super::*;
use crate::{
    allocation_exit_v2::reserve_exit,
    physical_node_v2 as resources,
    physical_type_v2::{
        DecodedTypeTable, clone_value_type_observed, preflight_value_type_clone,
        preflight_value_type_clone_admitted, value_type_clone_preflight_work_upper_bound,
    },
};
use novarocks_type_contract::{
    FunctionArgumentType, FunctionId, FunctionKind, FunctionOverloadId, FunctionValueType,
};
use std::{alloc::Layout, mem::size_of};

type Error = BindingCodecError;
fn shape(message: &'static str) -> Error {
    Error::InvalidShape(message)
}
fn numeric(error: resources::NodeCodecError) -> Error {
    match error {
        resources::NodeCodecError::Control(cause) => Error::Control(cause),
        _ => Error::Control(CompileControlError::ResourceExhausted),
    }
}
pub(crate) fn add(a: usize, b: usize) -> Result<usize, Error> {
    resources::add(a, b).map_err(numeric)
}
pub(crate) fn mul(a: usize, b: usize) -> Result<usize, Error> {
    resources::mul(a, b).map_err(numeric)
}
pub(crate) fn bytes<T>(n: usize) -> Result<usize, Error> {
    resources::bytes::<T>(n).map_err(numeric)
}
pub(crate) fn cap(n: usize, maximum: usize) -> Result<(), Error> {
    if n > maximum {
        Err(CompileControlError::ResourceExhausted.into())
    } else {
        Ok(())
    }
}
pub(crate) fn finish<T>(w: CompileCheckpoints<'_>, result: Result<T, Error>) -> Result<T, Error> {
    if matches!(&result, Err(Error::Control(_))) {
        return result;
    }
    w.finish()?;
    result
}
pub(crate) fn completed<T>(
    result: Result<T, Error>,
    w: &mut CompileCheckpoints<'_>,
) -> Result<T, Error> {
    if matches!(&result, Err(Error::Control(_))) {
        return result;
    }
    w.step()?;
    result
}

/// One existing Physical signature carrier; this is not a second signature,
/// installed capability, occurrence effect or original-request authority.
#[derive(Debug)]
pub enum MaterializedFunctionBinding {
    Scalar(BoundFunction),
    Table(BoundTableFunction),
}
impl MaterializedFunctionBinding {
    pub fn as_source(&self) -> BindingSource<'_> {
        match self {
            Self::Scalar(value) => BindingSource::Scalar(value),
            Self::Table(value) => BindingSource::Table(value),
        }
    }
}
/// Owned signatures remain associated with their actual receiving namespace.
/// Into-definitions deliberately ends these source loans and grants no proof.
pub struct MaterializedFunctionBindings<'loan, 'source> {
    definitions: Box<[(u32, MaterializedFunctionBinding)]>,
    headers: &'loan PreparedFunctionBindingHeaders<'source>,
    source_invoice: usize,
    retained_bytes: usize,
    facts: BindingProjectionFacts,
    _storage: novarocks_type_contract::ValueTypeStorageLifetime,
    construction_request_upper: Option<usize>,
}
impl<'loan, 'source> MaterializedFunctionBindings<'loan, 'source> {
    pub fn definitions(&self) -> &[(u32, MaterializedFunctionBinding)] {
        &self.definitions
    }
    /// Completed original-author construction upper, including temporary
    /// requests. Pure authors have no supplied construction provenance.
    /// This is not precise retained bytes, a capacity grant or a MEM ledger.
    pub fn creation_request_bytes_upper_bound(&self) -> Option<usize> {
        self.construction_request_upper
    }
    pub fn facts(&self) -> &BindingProjectionFacts {
        &self.facts
    }
    /// Count-sized source lookup, never a dense allocation based on maximum ID.
    /// The consuming caller admits repeated count work and owns its footer.
    pub fn definition_observed(
        &self,
        id: u32,
        w: &mut CompileCheckpoints<'_>,
    ) -> Result<Option<&MaterializedFunctionBinding>, Error> {
        self.definition_captured(id, &mut |_, _| Ok(()), w)
    }
    /// Admit an actually captured source before the lookup's completed step.
    pub(crate) fn definition_captured<'a>(
        &'a self,
        id: u32,
        capture: &mut impl FnMut(
            &'a MaterializedFunctionBinding,
            &mut CompileCheckpoints<'_>,
        ) -> Result<(), Error>,
        w: &mut CompileCheckpoints<'_>,
    ) -> Result<Option<&'a MaterializedFunctionBinding>, Error> {
        let same = std::ptr::addr_eq(w.control(), self.headers.original_control());
        w.step()?;
        if !same {
            return Err(shape(
                "materialized binding lookup has a different original control",
            ));
        }
        for (candidate, definition) in &self.definitions {
            let matches = *candidate == id;
            if matches {
                capture(definition, w)?;
            }
            w.step()?;
            if matches {
                return Ok(Some(definition));
            }
        }
        Ok(None)
    }
    /// Same owned namespace lookup, prefunded in the consuming parent.
    pub fn definition_in(
        &self,
        id: u32,
        admit: &mut impl FnMut(&BindingProjectionFacts) -> Result<(), CompileControlError>,
        w: &mut CompileCheckpoints<'_>,
    ) -> Result<Option<&MaterializedFunctionBinding>, Error> {
        admit(&super::owner_admission::lookup_facts(
            self.definitions.len(),
            self.definitions.len(),
        )?)?;
        self.definition_observed(id, w)
    }
    pub fn into_definitions(self) -> Box<[(u32, MaterializedFunctionBinding)]> {
        self.definitions
    }
    /// The exact original namespace loan, not an equal reconstructed table.
    pub(crate) fn headers(&self) -> &'loan PreparedFunctionBindingHeaders<'source> {
        self.headers
    }
    /// Only this owned output; excludes the already invoiced source namespace.
    pub(crate) fn retained_output_floor(&self) -> Result<usize, Error> {
        add(size_of::<Self>(), self.retained_bytes)
    }
    /// A necessary retained composition floor, not full backing or a MEM grant.
    pub fn retained_invoice_floor(&self) -> Result<usize, Error> {
        add(self.source_invoice, self.retained_output_floor()?)
    }
}
/// Preparation allocates no output and keeps all original header/type/control
/// loans through the only consuming materialization operation.
pub struct PreparedFunctionBindingsMaterialization<'loan, 'source> {
    headers: &'loan PreparedFunctionBindingHeaders<'source>,
    source_invoice: usize,
    retained_bytes: usize,
    facts: BindingProjectionFacts,
}
impl PreparedFunctionBindingsMaterialization<'_, '_> {
    pub fn facts(&self) -> &BindingProjectionFacts {
        &self.facts
    }
}
pub(crate) struct Model<'parent> {
    pub(crate) facts: BindingProjectionFacts,
    pub(crate) items: usize,
    lookup: usize,
    source: usize,
    known: usize,
    pub(crate) retained: usize,
    extra_work: usize,
    node_admission: Option<NodeAdmission<'parent>>,
}
/// A containing node's original numerical facts, not a child budget reset.
/// Only the sole Node model authors the composed axes and work formula.
struct NodeAdmission<'parent> {
    base: resources::Model,
    limits: Option<resources::NodeProjectionLimits>,
    values: usize,
    peak: usize,
    admit: Option<&'parent mut resources::NodeAdmit<'parent>>,
}
impl NodeAdmission<'_> {
    fn accepts_update(
        &self,
        base: resources::Model,
        values: usize,
        limits: Option<resources::NodeProjectionLimits>,
    ) -> bool {
        let counts = |model: resources::Model| {
            [
                model.inputs,
                model.refs,
                model.items,
                model.requests,
                model.requested,
                model.delegated_work,
            ]
        };
        self.values == values
            && self.limits == limits
            && counts(base)
                .into_iter()
                .zip(counts(self.base))
                .all(|(new, old)| new >= old)
    }
    fn composed(
        &self,
        items: usize,
        child: BindingProjectionFacts,
    ) -> Result<resources::Model, Error> {
        Ok(resources::Model {
            inputs: self.base.inputs,
            refs: self.base.refs,
            items: add(self.base.items, items)?,
            requests: add(self.base.requests, child.allocation_requests_upper_bound)?,
            requested: add(self.base.requested, child.request_bytes_upper_bound)?,
            delegated_work: add(self.base.delegated_work, self.peak)?,
        })
    }
    fn check(
        &mut self,
        items: usize,
        child: BindingProjectionFacts,
        source: usize,
    ) -> Result<(), Error> {
        self.peak = self.peak.max(child.cumulative_work_upper_bound);
        let facts = self
            .composed(items, child)?
            .numerical_facts(source, self.values, self.limits)
            .map_err(numeric)?;
        if let Some(admit) = &mut self.admit {
            admit(&facts)?;
        }
        Ok(())
    }
}
impl<'parent> Model<'parent> {
    fn new(headers: &PreparedFunctionBindingHeaders<'_>, source: usize) -> Result<Self, Error> {
        Ok(Self::for_composition(
            headers.as_wire().len(),
            add(headers.type_table().value_types().len(), 1)?,
            source,
            headers.retained_invoice_floor()?,
        ))
    }
    /// One cumulative model for an owning composition. The caller supplies the
    /// real original namespace lookup ceiling and necessary source floor.
    pub(crate) fn for_composition(
        definitions: usize,
        lookup_work: usize,
        source: usize,
        known: usize,
    ) -> Self {
        Self {
            facts: BindingProjectionFacts {
                definition_count: definitions,
                type_reference_count: 0,
                allocation_requests_upper_bound: 0,
                request_bytes_upper_bound: 0,
                coexisting_source_and_request_bytes_upper_bound: 0,
                cumulative_work_upper_bound: 0,
            },
            items: 0,
            lookup: lookup_work,
            source,
            known,
            retained: 0,
            extra_work: 0,
            node_admission: None,
        }
    }
    /// Bind or update the containing node before a delegated walk. An update
    /// keeps the already admitted peak; callbacks still belong to the caller.
    pub(crate) fn compose_in_node(
        &mut self,
        base: resources::Model,
        values: usize,
        node_limits: impl Into<Option<resources::NodeProjectionLimits>>,
        binding_limits: impl Into<Option<BindingProjectionLimits>> + Copy,
    ) -> Result<(), Error> {
        let node_limits = node_limits.into();
        if self
            .node_admission
            .as_ref()
            .is_some_and(|parent| !parent.accepts_update(base, values, node_limits))
        {
            return Err(shape(
                "containing node envelope changed or cumulative facts decreased",
            ));
        }
        if let Some(parent) = &mut self.node_admission {
            // The source, envelope and existing peak stay with the same owner.
            // An updated base never drops the containing package's loan.
            parent.base = base;
        } else {
            self.node_admission = Some(NodeAdmission {
                base,
                limits: node_limits,
                values,
                peak: 0,
                admit: None,
            });
        }
        self.check(binding_limits)
    }
    /// Borrow the containing package's node author for all delegated prefixes.
    /// Later base updates retain this loan and the original work peak.
    pub(crate) fn compose_in_node_in(
        &mut self,
        base: resources::Model,
        values: usize,
        node_limits: impl Into<Option<resources::NodeProjectionLimits>>,
        binding_limits: impl Into<Option<BindingProjectionLimits>> + Copy,
        admit: &'parent mut resources::NodeAdmit<'parent>,
    ) -> Result<(), Error> {
        if self
            .node_admission
            .as_ref()
            .is_some_and(|parent| parent.admit.is_some())
        {
            return Err(shape("containing node admission loan is already installed"));
        }
        self.compose_in_node(base, values, node_limits, binding_limits)?;
        let parent = self
            .node_admission
            .as_mut()
            .ok_or_else(|| shape("binding model has no containing node admission"))?;
        parent.admit = Some(admit);
        self.check(binding_limits)
    }
    pub(crate) fn node_work_peak(&self) -> usize {
        self.node_admission.as_ref().map_or(0, |parent| parent.peak)
    }
    /// Complete the same composition, retaining any earlier coarse work gate.
    pub(crate) fn node_facts(
        &mut self,
        work_ceiling: usize,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<resources::NodeProjectionFacts, Error> {
        let (model, values, limits) = self.node_projection_model(work_ceiling)?;
        match self
            .node_admission
            .as_mut()
            .and_then(|parent| parent.admit.as_mut())
        {
            Some(admit) => model
                .facts_in(self.source, values, limits, *admit, work)
                .map_err(numeric),
            None => model
                .facts(self.source, values, limits, work)
                .map_err(numeric),
        }
    }
    /// Compose the same current child snapshot and original work peak before
    /// a caller's synchronous parent admission. This adds no observation.
    pub(crate) fn node_numerical_facts(
        &mut self,
        work_ceiling: usize,
    ) -> Result<resources::NodeProjectionFacts, Error> {
        let (model, values, limits) = self.node_projection_model(work_ceiling)?;
        model
            .numerical_facts(self.source, values, limits)
            .map_err(numeric)
    }
    fn node_projection_model(
        &mut self,
        work_ceiling: usize,
    ) -> Result<
        (
            resources::Model,
            usize,
            Option<resources::NodeProjectionLimits>,
        ),
        Error,
    > {
        let parent = self
            .node_admission
            .as_mut()
            .ok_or_else(|| shape("binding model has no containing node admission"))?;
        parent.peak = parent.peak.max(work_ceiling);
        Ok((
            parent.composed(self.items, self.facts)?,
            parent.values,
            parent.limits,
        ))
    }
    pub(crate) fn request<T>(&mut self, count: usize, times: usize) -> Result<(), Error> {
        let size = bytes::<T>(count)?;
        self.charge_requests(usize::from(size != 0) * times, mul(size, times)?, size)
    }
    /// Charge actual layout requests without treating their conservative
    /// upper bound as a measured necessary floor of retained private storage.
    pub(crate) fn request_layouts(&mut self, layout: Layout, requests: usize) -> Result<(), Error> {
        self.charge_requests(
            usize::from(layout.size() != 0) * requests,
            mul(layout.size(), requests)?,
            0,
        )
    }
    fn charge_requests(
        &mut self,
        requests: usize,
        bytes: usize,
        retained: usize,
    ) -> Result<(), Error> {
        self.retained = add(self.retained, retained)?;
        self.facts.request_bytes_upper_bound = add(self.facts.request_bytes_upper_bound, bytes)?;
        self.facts.allocation_requests_upper_bound =
            add(self.facts.allocation_requests_upper_bound, requests)?;
        Ok(())
    }
    /// Delegated work is admitted here; this numerical ceiling does not
    /// synthesize observer callbacks or reset the caller's resource budget.
    pub(crate) fn add_work(&mut self, work: usize) -> Result<(), Error> {
        self.extra_work = add(self.extra_work, work)?;
        Ok(())
    }
    pub(crate) fn check_admitted(
        &mut self,
        limits: impl Into<Option<BindingProjectionLimits>>,
        admit: &mut super::owner_admission::Admit<'_>,
    ) -> Result<(), Error> {
        self.check(limits)?;
        admit(&self.facts)?;
        Ok(())
    }
    pub(crate) fn check(
        &mut self,
        limits: impl Into<Option<BindingProjectionLimits>>,
    ) -> Result<(), Error> {
        let limits = limits.into();
        if let Some(limits) = limits {
            cap(self.facts.definition_count, limits.max_definitions)?;
            cap(self.facts.type_reference_count, limits.max_type_references)?;
            cap(
                self.facts.request_bytes_upper_bound,
                limits.max_request_bytes,
            )?;
            cap(
                self.facts.allocation_requests_upper_bound,
                limits.max_allocation_requests,
            )?;
        }
        self.facts.coexisting_source_and_request_bytes_upper_bound =
            add(self.source, self.facts.request_bytes_upper_bound)?;
        if let Some(limits) = limits {
            cap(
                self.facts.coexisting_source_and_request_bytes_upper_bound,
                limits.max_coexisting_source_and_request_bytes,
            )?;
        }
        // The sole clone grammar's maximum admits its preflight before that
        // bounded traversal. Keep the conservative ceiling in final facts so
        // exact-envelope replay cannot rely on an unreported early ceiling.
        let clone_work = value_type_clone_preflight_work_upper_bound();
        self.facts.cumulative_work_upper_bound = add(
            add(128, mul(32, add(self.items, self.facts.definition_count)?)?)?,
            add(
                mul(
                    self.facts.type_reference_count,
                    add(self.lookup, clone_work)?,
                )?,
                add(
                    mul(self.facts.request_bytes_upper_bound, 4)?,
                    add(self.facts.allocation_requests_upper_bound, self.extra_work)?,
                )?,
            )?,
        )?;
        if let Some(limits) = limits {
            cap(self.facts.cumulative_work_upper_bound, limits.max_work)?;
        }
        if let Some(parent) = &mut self.node_admission {
            parent.check(self.items, self.facts, self.source)?;
        }
        if self.source < self.known {
            return Err(shape(
                "binding materialization source invoice omits original namespace",
            ));
        }
        Ok(())
    }
}
fn value<'a>(
    types: &'a DecodedTypeTable,
    id: u32,
    w: &mut CompileCheckpoints<'_>,
) -> Result<&'a FunctionValueType, Error> {
    value_captured(types, id, &mut |_, _| Ok(()), w)
}
/// The captured loan is admitted before observing the completed lookup.
/// Plain callers use the same body with an empty capture hook.
pub(super) fn value_captured<'a>(
    types: &'a DecodedTypeTable,
    id: u32,
    capture: &mut impl FnMut(&'a FunctionValueType, &mut CompileCheckpoints<'_>) -> Result<(), Error>,
    w: &mut CompileCheckpoints<'_>,
) -> Result<&'a FunctionValueType, Error> {
    w.flush()?;
    let found = types.value_type(id);
    if let Some(source) = found {
        capture(source, w)?;
    }
    w.step()?;
    w.flush()?;
    found.ok_or_else(|| shape("materialized binding value type is absent"))
}
fn count_clone(
    types: &DecodedTypeTable,
    id: u32,
    model: &mut Model,
    limits: Option<BindingProjectionLimits>,
    observed: bool,
    admit: &mut super::owner_admission::Admit<'_>,
    w: &mut CompileCheckpoints<'_>,
) -> Result<(), Error> {
    // Coarse bounded-type work was included before entering the sole grammar.
    if observed {
        model.check_admitted(limits, admit)?;
    } else {
        model.check(limits)?;
    }
    if observed {
        value_captured(
            types,
            id,
            &mut |source, work| model.count_owned_type_clone_in(source, limits, admit, work),
            w,
        )?;
        Ok(())
    } else {
        let source = value(types, id, w)?;
        model.count_owned_type_clone(source, limits, w)
    }
}
impl Model<'_> {
    /// Caller admits this occurrence's reference count before visiting the sole
    /// clone grammar. Repeated owned roots are charged once per actual copy.
    pub(crate) fn count_owned_type_clone(
        &mut self,
        source: &FunctionValueType,
        limits: impl Into<Option<BindingProjectionLimits>>,
        w: &mut CompileCheckpoints<'_>,
    ) -> Result<(), Error> {
        let limits = limits.into();
        self.check(limits)?;
        w.flush()?;
        let clone = preflight_value_type_clone(source, w)?;
        self.merge_clone_requests(
            clone.allocation_requests_upper_bound(),
            clone.allocation_request_bytes_upper_bound(),
        )?;
        // The original facts must fit the original shared maximum; no second
        // datatype grammar or recursive measurement is constructed here.
        cap(
            clone.work_upper_bound(),
            value_type_clone_preflight_work_upper_bound(),
        )?;
        // The clone counts are now known. Their originating refusal must win
        // before a post-delegate quantum or flush can observe later control.
        self.check(limits)?;
        w.step()?;
        w.flush()?;
        Ok(())
    }
    /// Admit the exact containing-node snapshot before observation and each
    /// growing request prefix in the original clone author.
    pub(crate) fn count_owned_type_clone_admitted(
        &mut self,
        source: &FunctionValueType,
        limits: impl Into<Option<BindingProjectionLimits>>,
        admit: &mut impl FnMut(&resources::NodeProjectionFacts) -> Result<(), CompileControlError>,
        w: &mut CompileCheckpoints<'_>,
    ) -> Result<(), Error> {
        self.count_owned_type_clone_core(
            source,
            limits.into(),
            &mut |model| {
                admit(&model.node_numerical_facts(model.node_work_peak())?)?;
                Ok(())
            },
            w,
        )
    }
    /// Synchronous binding-parent admission for the sole clone-prefix author.
    pub(crate) fn count_owned_type_clone_in(
        &mut self,
        source: &FunctionValueType,
        limits: impl Into<Option<BindingProjectionLimits>>,
        admit: &mut super::owner_admission::Admit<'_>,
        w: &mut CompileCheckpoints<'_>,
    ) -> Result<(), Error> {
        self.count_owned_type_clone_core(
            source,
            limits.into(),
            &mut |model| {
                admit(&model.facts)?;
                Ok(())
            },
            w,
        )
    }
    fn count_owned_type_clone_core(
        &mut self,
        source: &FunctionValueType,
        limits: Option<BindingProjectionLimits>,
        admit: &mut impl FnMut(&mut Self) -> Result<(), Error>,
        w: &mut CompileCheckpoints<'_>,
    ) -> Result<(), Error> {
        self.check(limits)?;
        admit(self)?;
        let mut first_prefix = true;
        let mut previous_requests = 0;
        let mut previous_bytes = 0;
        preflight_value_type_clone_admitted(
            source,
            &mut |clone, work| {
                let requests = clone.allocation_requests_upper_bound();
                let bytes = clone.allocation_request_bytes_upper_bound();
                self.merge_clone_requests(requests - previous_requests, bytes - previous_bytes)?;
                previous_requests = requests;
                previous_bytes = bytes;
                cap(
                    clone.work_upper_bound(),
                    value_type_clone_preflight_work_upper_bound(),
                )?;
                self.check(limits)?;
                admit(self)?;
                if first_prefix {
                    first_prefix = false;
                    work.flush()?;
                }
                Ok::<_, Error>(())
            },
            w,
        )?;
        self.check(limits)?;
        admit(self)?;
        w.step()?;
        w.flush()?;
        Ok(())
    }
    fn merge_clone_requests(&mut self, requests: usize, bytes: usize) -> Result<(), Error> {
        self.facts.allocation_requests_upper_bound =
            add(self.facts.allocation_requests_upper_bound, requests)?;
        self.facts.request_bytes_upper_bound = add(self.facts.request_bytes_upper_bound, bytes)?;
        self.retained = add(self.retained, bytes)?;
        Ok(())
    }
}

fn preflight(
    headers: &PreparedFunctionBindingHeaders<'_>,
    mut model: Model,
    limits: Option<BindingProjectionLimits>,
    observed: bool,
    admit: &mut super::owner_admission::Admit<'_>,
    w: &mut CompileCheckpoints<'_>,
) -> Result<(BindingProjectionFacts, usize), Error> {
    if observed {
        model.check_admitted(limits, admit)?;
    } else {
        model.check(limits)?;
    }
    for definition in headers.as_wire() {
        model.items = add(model.items, definition.arguments.len())?;
        model.request::<FunctionArgumentType>(definition.arguments.len(), 2)?;
        if !definition.arguments.is_empty() {
            model.request_layouts(
                novarocks_type_contract::FunctionArgumentTypes::storage_allocation_layout()
                    .map_err(Error::Resource)?,
                1,
            )?;
        }
        model.request::<u8>(definition.function_id.len(), 1)?;
        model.request::<u8>(definition.overload_id.len(), 1)?;
        model.request_layouts(
            FunctionId::storage_allocation_layout()
                .map_err(|_| CompileControlError::ResourceExhausted)?,
            1,
        )?;
        model.request_layouts(
            FunctionOverloadId::storage_allocation_layout()
                .map_err(|_| CompileControlError::ResourceExhausted)?,
            1,
        )?;
        if observed {
            model.check_admitted(limits, admit)?;
        } else {
            model.check(limits)?;
        }
        for argument in &definition.arguments {
            match &argument.kind {
                Some(
                    wire::function_argument_type::Kind::ValueTypeId(_)
                    | wire::function_argument_type::Kind::CandidateGroupElementValueTypeId(_),
                ) => model.facts.type_reference_count = add(model.facts.type_reference_count, 1)?,
                Some(wire::function_argument_type::Kind::Lambda(lambda)) => {
                    model.items = add(model.items, lambda.parameter_value_type_ids.len())?;
                    model.facts.type_reference_count = add(
                        model.facts.type_reference_count,
                        add(lambda.parameter_value_type_ids.len(), 1)?,
                    )?;
                    model.request::<FunctionValueType>(lambda.parameter_value_type_ids.len(), 2)?;
                    if !lambda.parameter_value_type_ids.is_empty() {
                        model.request_layouts(novarocks_type_contract::FunctionParameterTypes::storage_allocation_layout().map_err(Error::Resource)?,1)?;
                    }
                }
                None => {
                    w.step()?;
                    return Err(shape("materialized binding argument is absent"));
                }
            }
            if observed {
                model.check_admitted(limits, admit)?;
            } else {
                model.check(limits)?;
            }
            w.step()?;
        }
        match &definition.result {
            Some(wire::function_binding_definition::Result::ScalarValueTypeId(_)) => {
                model.facts.type_reference_count = add(model.facts.type_reference_count, 1)?
            }
            Some(wire::function_binding_definition::Result::Relation(relation)) => {
                model.items = add(model.items, relation.value_type_ids.len())?;
                model.facts.type_reference_count = add(
                    model.facts.type_reference_count,
                    relation.value_type_ids.len(),
                )?;
                model.request::<FunctionValueType>(relation.value_type_ids.len(), 2)?;
                if !relation.value_type_ids.is_empty() {
                    model.request_layouts(
                        novarocks_type_contract::FunctionParameterTypes::storage_allocation_layout(
                        )
                        .map_err(Error::Resource)?,
                        1,
                    )?;
                }
            }
            None => {
                w.step()?;
                return Err(shape("materialized binding result is absent"));
            }
        }
        if observed {
            model.check_admitted(limits, admit)?;
        } else {
            model.check(limits)?;
        }
        w.step()?;
    }
    // Full cumulative own/delegate/output requests precede every clone walk
    // and the first output allocation; all IDs retain original source order.
    for definition in headers.as_wire() {
        for argument in &definition.arguments {
            match &argument.kind {
                Some(
                    wire::function_argument_type::Kind::ValueTypeId(id)
                    | wire::function_argument_type::Kind::CandidateGroupElementValueTypeId(id),
                ) => count_clone(
                    headers.type_table(),
                    *id,
                    &mut model,
                    limits,
                    observed,
                    admit,
                    w,
                )?,
                Some(wire::function_argument_type::Kind::Lambda(lambda)) => {
                    for id in &lambda.parameter_value_type_ids {
                        count_clone(
                            headers.type_table(),
                            *id,
                            &mut model,
                            limits,
                            observed,
                            admit,
                            w,
                        )?;
                        w.step()?;
                    }
                    count_clone(
                        headers.type_table(),
                        lambda
                            .result_value_type_id
                            .ok_or_else(|| shape("materialized Lambda result is absent"))?,
                        &mut model,
                        limits,
                        observed,
                        admit,
                        w,
                    )?;
                }
                None => return Err(shape("materialized binding argument changed")),
            }
            w.step()?;
        }
        match &definition.result {
            Some(wire::function_binding_definition::Result::ScalarValueTypeId(id)) => count_clone(
                headers.type_table(),
                *id,
                &mut model,
                limits,
                observed,
                admit,
                w,
            )?,
            Some(wire::function_binding_definition::Result::Relation(relation)) => {
                for id in &relation.value_type_ids {
                    count_clone(
                        headers.type_table(),
                        *id,
                        &mut model,
                        limits,
                        observed,
                        admit,
                        w,
                    )?;
                    w.step()?;
                }
            }
            None => return Err(shape("materialized binding result changed")),
        }
        w.step()?;
    }
    Ok((model.facts, model.retained))
}
pub fn prepare_function_bindings_materialization<'loan, 'source>(
    headers: &'loan PreparedFunctionBindingHeaders<'source>,
    source_retained_bytes: usize,
    limits: BindingProjectionLimits,
) -> Result<PreparedFunctionBindingsMaterialization<'loan, 'source>, Error> {
    let mut w = CompileCheckpoints::try_new(headers.original_control(), CompilePhase::Decode)?;
    let result = (|| {
        let mut model = Model::new(headers, source_retained_bytes)?;
        model.request::<(u32, MaterializedFunctionBinding)>(headers.as_wire().len(), 2)?;
        preflight(headers, model, Some(limits), false, &mut |_| Ok(()), &mut w)
    })()
    .map(
        |(facts, retained_bytes)| PreparedFunctionBindingsMaterialization {
            headers,
            source_invoice: source_retained_bytes,
            retained_bytes,
            facts,
        },
    );
    finish(w, result)
}
pub(crate) fn reserve<T>(count: usize, w: &mut CompileCheckpoints<'_>) -> Result<Vec<T>, Error> {
    bytes::<T>(count)?;
    w.flush()?;
    let mut values = Vec::new();
    reserve_exit::<Error>(values.try_reserve_exact(count), w)?;
    w.step()?;
    w.flush()?;
    Ok(values)
}
pub(crate) fn boxed<T>(values: Vec<T>, w: &mut CompileCheckpoints<'_>) -> Result<Box<[T]>, Error> {
    w.flush()?;
    let values = values.into_boxed_slice();
    w.step()?;
    w.flush()?;
    Ok(values)
}
fn clone_type(
    types: &DecodedTypeTable,
    id: u32,
    w: &mut CompileCheckpoints<'_>,
) -> Result<FunctionValueType, Error> {
    let source = value(types, id, w)?;
    w.flush()?;
    let value = clone_value_type_observed(source, w)?;
    w.step()?;
    w.flush()?;
    Ok(value)
}
pub fn materialize_function_bindings<'loan, 'source>(
    token: PreparedFunctionBindingsMaterialization<'loan, 'source>,
) -> Result<MaterializedFunctionBindings<'loan, 'source>, Error> {
    let mut w =
        CompileCheckpoints::try_new(token.headers.original_control(), CompilePhase::Decode)?;
    let result = materialize_in(token, &mut w);
    finish(w, result)
}
fn materialize_in<'loan, 'source>(
    token: PreparedFunctionBindingsMaterialization<'loan, 'source>,
    w: &mut CompileCheckpoints<'_>,
) -> Result<MaterializedFunctionBindings<'loan, 'source>, Error> {
    materialize_core::<std::convert::Infallible>(token, None, w)
        .map_err(crate::host_projection_v2::ProjectionFailure::without_host)
}
fn materialize_core<'loan, 'source, H>(
    token: PreparedFunctionBindingsMaterialization<'loan, 'source>,
    creator: Option<&mut dyn novarocks_type_contract::ValueSchemaCreationScope<H>>,
    w: &mut CompileCheckpoints<'_>,
) -> Result<MaterializedFunctionBindings<'loan, 'source>, super::creation::Failure<H>> {
    let mut construction = super::creation::Construction { creator };
    let storage = construction.storage();
    let construction_request_upper = construction
        .creator
        .as_ref()
        .map(|_| token.facts.request_bytes_upper_bound);
    w.step()?;
    (|| {
        let mut definitions = construction.reserve(token.headers.as_wire().len(), w)?;
        for raw in token.headers.as_wire() {
            let binding = materialize_definition_in(raw, &mut construction, w, &mut |id, w| {
                clone_type(token.headers.type_table(), id, w)
                    .map_err(crate::host_projection_v2::ProjectionFailure::Codec)
            })?;
            definitions.push((raw.id, binding));
            w.step()?;
        }
        Ok(MaterializedFunctionBindings {
            definitions: construction.boxed(definitions, w)?,
            headers: token.headers,
            source_invoice: token.source_invoice,
            retained_bytes: token.retained_bytes,
            facts: token.facts,
            _storage: storage,
            construction_request_upper,
        })
    })()
}

fn materialize_definition_in<H>(
    raw: &wire::FunctionBindingDefinition,
    construction: &mut super::creation::Construction<'_, H>,
    w: &mut CompileCheckpoints<'_>,
    clone_type: &mut impl FnMut(
        u32,
        &mut CompileCheckpoints<'_>,
    ) -> Result<FunctionValueType, super::creation::Failure<H>>,
) -> Result<MaterializedFunctionBinding, super::creation::Failure<H>> {
    let function = construction.function(&raw.function_id, w)?;
    let overload = construction.overload(&raw.overload_id, w)?;
    let mut arguments = construction.reserve(raw.arguments.len(), w)?;
    for argument in &raw.arguments {
        let value = match &argument.kind {
            Some(wire::function_argument_type::Kind::ValueTypeId(id)) => {
                FunctionArgumentType::Value(clone_type(*id, w)?)
            }
            Some(wire::function_argument_type::Kind::CandidateGroupElementValueTypeId(id)) => {
                FunctionArgumentType::CandidateGroup {
                    element_type: clone_type(*id, w)?,
                }
            }
            Some(wire::function_argument_type::Kind::Lambda(lambda)) => {
                let mut parameters =
                    construction.reserve(lambda.parameter_value_type_ids.len(), w)?;
                for id in &lambda.parameter_value_type_ids {
                    parameters.push(clone_type(*id, w)?);
                    w.step()?;
                }
                FunctionArgumentType::Lambda {
                    parameter_types: {
                        let boxed = construction.boxed(parameters, w)?;
                        construction.collection(boxed, w)?
                    },
                    result_type: clone_type(
                        lambda
                            .result_value_type_id
                            .ok_or_else(|| shape("materialized Lambda result is absent"))?,
                        w,
                    )?,
                }
            }
            None => return Err(shape("materialized binding argument changed").into()),
        };
        arguments.push(value);
        w.step()?;
    }
    let boxed_arguments = construction.boxed(arguments, w)?;
    let arguments = construction.collection(boxed_arguments, w)?;
    let binding = match (&raw.result, wire::FunctionKind::try_from(raw.kind)) {
        (Some(wire::function_binding_definition::Result::ScalarValueTypeId(id)), Ok(kind)) => {
            let kind = match kind {
                wire::FunctionKind::Scalar => FunctionKind::Scalar,
                wire::FunctionKind::Aggregate => FunctionKind::Aggregate,
                wire::FunctionKind::Window => FunctionKind::Window,
                _ => {
                    return Err(shape("materialized scalar result has a different kind").into());
                }
            };
            MaterializedFunctionBinding::Scalar(BoundFunction::from_exact_signature(
                function,
                overload,
                kind,
                arguments,
                clone_type(*id, w)?,
            ))
        }
        (
            Some(wire::function_binding_definition::Result::Relation(relation)),
            Ok(wire::FunctionKind::Table),
        ) => {
            let mut result = construction.reserve(relation.value_type_ids.len(), w)?;
            for id in &relation.value_type_ids {
                result.push(clone_type(*id, w)?);
                w.step()?;
            }
            MaterializedFunctionBinding::Table(BoundTableFunction::from_exact_signature(
                function,
                overload,
                arguments,
                {
                    let boxed = construction.boxed(result, w)?;
                    construction.collection(boxed, w)?
                },
            ))
        }
        _ => return Err(shape("materialized binding kind and result differ").into()),
    };
    Ok(binding)
}

/// Original preparation with growing binding facts and no scope entry/footer.
/// Passing None derives the checked prefix amounts from the original headers
/// and exact type namespace, with admission supplied by the caller.
pub fn prepare_function_bindings_materialization_in<'loan, 'source>(
    headers: &'loan PreparedFunctionBindingHeaders<'source>,
    source_retained_bytes: usize,
    limits: impl Into<Option<BindingProjectionLimits>>,
    admit: &mut impl FnMut(&BindingProjectionFacts) -> Result<(), CompileControlError>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<PreparedFunctionBindingsMaterialization<'loan, 'source>, Error> {
    let limits = limits.into();
    // Pure initial geometry wins before the first identity observation.
    let mut model = Model::new(headers, source_retained_bytes)?;
    model.request::<(u32, MaterializedFunctionBinding)>(headers.as_wire().len(), 2)?;
    model.check_admitted(limits, admit)?;
    let same = std::ptr::addr_eq(work.control(), headers.original_control());
    work.step()?;
    if !same {
        return Err(shape(
            "binding materialization has a different original control",
        ));
    }
    let (facts, retained_bytes) = preflight(headers, model, limits, true, admit, work)?;
    Ok(PreparedFunctionBindingsMaterialization {
        headers,
        source_invoice: source_retained_bytes,
        retained_bytes,
        facts,
    })
}
/// Consume the original prepared namespace using its already admitted complete
/// inventory. The callback replaces this operation's previous snapshot.
pub fn materialize_function_bindings_in<'loan, 'source>(
    token: PreparedFunctionBindingsMaterialization<'loan, 'source>,
    admit: &mut impl FnMut(&BindingProjectionFacts) -> Result<(), CompileControlError>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<MaterializedFunctionBindings<'loan, 'source>, Error> {
    admit(&token.facts)?;
    let same = std::ptr::addr_eq(work.control(), token.headers.original_control());
    work.step()?;
    if !same {
        return Err(shape(
            "binding materialization has a different original control",
        ));
    }
    materialize_in(token, work)
}

#[cfg(test)]
#[path = "materialize_tests.rs"]
mod tests;

/// Strong consumer of the original token. Numeric facts remain structural
/// observations; every actual constructor request goes to the supplied host.
pub fn materialize_function_bindings_with_creator_in<'loan, 'source, H>(
    token: PreparedFunctionBindingsMaterialization<'loan, 'source>,
    creation: &mut novarocks_type_contract::ValueSchemaCreation<'_, H>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<MaterializedFunctionBindings<'loan, 'source>, super::creation::Failure<H>> {
    let same = std::ptr::addr_eq(work.control(), token.headers.original_control());
    work.step()?;
    if !same {
        return Err(shape("binding materialization has a different original control").into());
    }
    materialize_core(token, Some(creation), work)
}
