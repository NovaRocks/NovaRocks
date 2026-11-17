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

//! Move original SQL request data into the neutral fragment namespace.
//! This port neither reconstructs physical arguments nor grants capability.

use std::{alloc::Layout, collections::BTreeMap};

use novarocks_constant_contract::{ConstantPolicy, ConstantValue};
use novarocks_functions::{FunctionArgument, FunctionBindingRequest, MAX_CALL_EFFECT_ARGUMENTS};
use novarocks_type_contract::{CompileCheckpoints, CompileControlError};
use {
    novarocks_expression_contract::PhysicalCallRequest,
    novarocks_physical_plan::{
        ConstantReference, FragmentId, MAX_FRAGMENT_DYNAMIC_BYTES, MAX_FRAGMENT_DYNAMIC_ITEMS,
        MAX_PLAN_DYNAMIC_BYTES, MAX_PLAN_DYNAMIC_ITEMS, StaticFunctionArgument, ValueType,
    },
};

use super::{
    AggregateRuntimeDemand, ContractLoweringError, LoweredAggregateSourceEntry, ScalarSourceSite,
    SqlLogicalSourceJournal,
};

/// Construction-only routing. Scalar requests move straight into their actual
/// node unit; only relation entries enter the fragment's request table.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum RequestTarget {
    Unit {
        node: novarocks_physical_plan::NodeId,
        definition: novarocks_expression_contract::ExprId,
    },
    Relation(novarocks_physical_plan::PhysicalCallSite),
}
type Entries = Vec<(RequestTarget, PhysicalCallRequest)>;
pub(super) type Transferred = BTreeMap<FragmentId, Entries>;

/// The caller owns the original entry/footer and backing registry. Completed
/// type/Box/BTree operations are opaque caller-admitted work, not a new grant.
/// No source CV is admitted again; the callback only registers its real pool.
pub(super) fn transfer_neutral_requests_observed(
    journal: &SqlLogicalSourceJournal,
    work: &mut CompileCheckpoints<'_>,
    mut register: impl FnMut(
        &ConstantValue,
        &mut CompileCheckpoints<'_>,
    ) -> Result<ConstantReference, ContractLoweringError>,
) -> Result<Transferred, ContractLoweringError> {
    let total = journal
        .expression_entries
        .len()
        .checked_add(journal.window_entries.len())
        .and_then(|count| count.checked_add(journal.table_entries.len()))
        .and_then(|count| count.checked_add(journal.entries.len()))
        .ok_or(CompileControlError::ResourceExhausted)?;
    let layout = Layout::array::<(RequestTarget, PhysicalCallRequest)>(total)
        .map_err(|_| CompileControlError::ResourceExhausted)?;
    if total > MAX_PLAN_DYNAMIC_ITEMS || layout.size() > MAX_PLAN_DYNAMIC_BYTES {
        return Err(CompileControlError::ResourceExhausted.into());
    }
    work.step()?;

    // Count every definition before reserving any output Vec. Map nodes are
    // original caller-admitted opaque scratch; total source cardinality was
    // checked before the first insertion.
    let mut counts = BTreeMap::<FragmentId, usize>::new();
    for fragment in journal
        .expression_entries
        .keys()
        .map(|address| {
            let ScalarSourceSite::Node { fragment, .. } = address.site;
            fragment
        })
        .chain(journal.window_entries.keys().map(|(fragment, _)| *fragment))
        .chain(journal.table_entries.keys().map(|(fragment, _)| *fragment))
    {
        count_fragment(&mut counts, fragment, work)?;
    }
    for (fragment, _) in journal.entries.keys() {
        count_fragment(&mut counts, *fragment, work)?;
    }
    let mut output = BTreeMap::new();
    for (fragment, count) in counts {
        let mut entries = Vec::new();
        Layout::array::<(RequestTarget, PhysicalCallRequest)>(count)
            .map_err(|_| CompileControlError::ResourceExhausted)?;
        work.step()?;
        work.flush()?;
        entries
            .try_reserve_exact(count)
            .map_err(|_| CompileControlError::ResourceExhausted)?;
        work.step()?;
        work.flush()?;
        output.insert(fragment, entries);
        work.step()?;
    }
    for (&address, entry) in &journal.expression_entries {
        work.step()?;
        let ScalarSourceSite::Node { fragment, node } = address.site;
        let expression = address.definition;
        let captured = entry.captured.captured();
        let canonical = entry.canonical_operational.as_ref();
        work.step()?;
        let canonical = canonical.ok_or_else(|| {
            invalid("neutral expression request has no canonical original source")
        })?;
        let belongs = canonical.belongs_to(captured);
        work.step()?;
        if !belongs {
            return Err(invalid(
                "neutral expression request has another original source",
            ));
        }
        let record = transfer_request(
            canonical.request(),
            captured.constant_policy(),
            work,
            &mut register,
        )?;
        push(
            &mut output,
            fragment,
            RequestTarget::Unit {
                node,
                definition: expression,
            },
            record,
            work,
        )?;
    }
    for (&(fragment, site), entry) in &journal.window_entries {
        work.step()?;
        let canonical = &entry.canonical_operational;
        let belongs = canonical.belongs_to(&entry.captured);
        work.step()?;
        if !belongs {
            return Err(invalid(
                "neutral Window request has another original source",
            ));
        }
        let record = transfer_request(
            canonical.request(),
            entry.captured.constant_policy(),
            work,
            &mut register,
        )?;
        push(
            &mut output,
            fragment,
            RequestTarget::Relation(site),
            record,
            work,
        )?;
    }
    for (&(fragment, node), entry) in &journal.table_entries {
        work.step()?;
        let canonical = &entry.canonical_operational;
        let belongs = canonical.belongs_to(&entry.captured);
        work.step()?;
        if !belongs {
            return Err(invalid("neutral table request has another original source"));
        }
        let record = transfer_request(
            canonical.request(),
            entry.captured.constant_policy(),
            work,
            &mut register,
        )?;
        push(
            &mut output,
            fragment,
            RequestTarget::Relation(novarocks_physical_plan::PhysicalCallSite::Table { node }),
            record,
            work,
        )?;
    }
    for (&(fragment, site), entry) in &journal.entries {
        work.step()?;
        let (request, policy) = aggregate_request(entry, work)?;
        let record = transfer_request(request, policy, work, &mut register)?;
        push(
            &mut output,
            fragment,
            RequestTarget::Relation(site),
            record,
            work,
        )?;
    }
    Ok(output)
}

fn invalid(detail: &'static str) -> ContractLoweringError {
    ContractLoweringError::InvalidFunctionBinding {
        detail: detail.into(),
    }
}

fn count_fragment(
    counts: &mut BTreeMap<FragmentId, usize>,
    fragment: FragmentId,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), ContractLoweringError> {
    work.flush()?;
    let count = counts.entry(fragment).or_default();
    work.step()?;
    let next = count
        .checked_add(1)
        .ok_or(CompileControlError::ResourceExhausted)?;
    if next > MAX_FRAGMENT_DYNAMIC_ITEMS {
        return Err(CompileControlError::ResourceExhausted.into());
    }
    *count = next;
    work.step()?;
    Ok(())
}

fn aggregate_request<'a>(
    entry: &'a LoweredAggregateSourceEntry,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(FunctionBindingRequest<'a>, ConstantPolicy), ContractLoweringError> {
    let captured = entry.logical.captured();
    work.step()?;
    let captured = captured.ok_or(ContractLoweringError::InvalidAggregate {
        detail: "neutral aggregate request has no authenticated logical source",
    })?;
    let update = entry.phase.consumes_logical_arguments();
    let same_runtime = update == matches!(entry.runtime, AggregateRuntimeDemand::Update);
    work.step()?;
    if !same_runtime {
        return Err(ContractLoweringError::InvalidAggregate {
            detail: "neutral aggregate request has another runtime lifecycle",
        });
    }
    let request = if update {
        let canonical = entry.canonical.as_ref();
        work.step()?;
        let canonical = canonical.ok_or(ContractLoweringError::InvalidAggregate {
            detail: "neutral aggregate update has no canonical original source",
        })?;
        let belongs = canonical.belongs_to(captured);
        work.step()?;
        if !belongs {
            return Err(ContractLoweringError::InvalidAggregate {
                detail: "neutral aggregate update has another original source",
            });
        }
        canonical.request()
    } else if let Some(canonical) = entry.canonical.as_ref() {
        // The same construction transaction authored this merge's current
        // request from its actual contributors. Publication borrows that ONE
        // receipt, retaining the consumer's own captured source and identity.
        work.step()?;
        let belongs = canonical.belongs_to(captured);
        work.step()?;
        if !belongs {
            return Err(ContractLoweringError::InvalidAggregate {
                detail: "neutral aggregate merge has another original source",
            });
        }
        canonical.request()
    } else {
        // Original lowering has no current merge receipt. Preserve its exact
        // captured request; absence does not synthesize computed metadata.
        captured.request()
    };
    Ok((request, captured.constant_policy()))
}

fn transfer_request(
    request: FunctionBindingRequest<'_>,
    policy: ConstantPolicy,
    work: &mut CompileCheckpoints<'_>,
    register: &mut impl FnMut(
        &ConstantValue,
        &mut CompileCheckpoints<'_>,
    ) -> Result<ConstantReference, ContractLoweringError>,
) -> Result<PhysicalCallRequest, ContractLoweringError> {
    let count = request.arguments.len();
    if count > MAX_CALL_EFFECT_ARGUMENTS {
        return Err(CompileControlError::ResourceExhausted.into());
    }
    let logical_count_fits = request.logical_argument_count <= count;
    work.step()?;
    if !logical_count_fits {
        return Err(invalid(
            "neutral request logical prefix exceeds its actual argument extent",
        ));
    }
    Layout::array::<StaticFunctionArgument<ConstantReference>>(count)
        .map_err(|_| CompileControlError::ResourceExhausted)?;
    work.step()?;
    // Known inline request storage is a necessary lower bound under the same
    // structural ceilings. Full nested type/backing accounting stays with the
    // original mandatory attachment/resource author and caller admission.
    let mut items = count;
    let mut bytes = count
        .checked_mul(std::mem::size_of::<StaticFunctionArgument<ConstantReference>>())
        .ok_or(CompileControlError::ResourceExhausted)?;
    for argument in request.arguments {
        work.step()?;
        if let FunctionArgument::Lambda {
            parameter_types, ..
        } = argument
        {
            items = items
                .checked_add(parameter_types.len())
                .ok_or(CompileControlError::ResourceExhausted)?;
            let layout = Layout::array::<ValueType>(parameter_types.len())
                .map_err(|_| CompileControlError::ResourceExhausted)?;
            bytes = bytes
                .checked_add(layout.size())
                .ok_or(CompileControlError::ResourceExhausted)?;
            work.step()?;
            if items > MAX_FRAGMENT_DYNAMIC_ITEMS || bytes > MAX_FRAGMENT_DYNAMIC_BYTES {
                return Err(CompileControlError::ResourceExhausted.into());
            }
        }
    }
    if items > MAX_FRAGMENT_DYNAMIC_ITEMS || bytes > MAX_FRAGMENT_DYNAMIC_BYTES {
        return Err(CompileControlError::ResourceExhausted.into());
    }
    work.flush()?;
    let mut arguments = Vec::new();
    arguments
        .try_reserve_exact(count)
        .map_err(|_| CompileControlError::ResourceExhausted)?;
    work.step()?;
    work.flush()?;
    for argument in request.arguments {
        work.step()?;
        let argument = match argument {
            FunctionArgument::Value {
                value_type,
                constant,
            } => {
                let constant = match constant {
                    Some(value) => Some(register(value, work)?),
                    None => None,
                };
                StaticFunctionArgument::Value {
                    value_type: clone_type(value_type, work)?,
                    constant,
                }
            }
            FunctionArgument::Lambda {
                parameter_types,
                result_type,
            } => {
                let parameters = parameter_types.clone_observed(|| work.step())?;
                work.flush()?;
                StaticFunctionArgument::Lambda {
                    parameter_types: parameters,
                    result_type: clone_type(result_type, work)?,
                }
            }
        };
        arguments.push(argument);
        work.step()?;
    }
    let expected_result_type = request
        .expected_result_type
        .map(|ty| clone_type(ty, work))
        .transpose()?;
    work.flush()?;
    let arguments = arguments.into_boxed_slice();
    work.step()?;
    work.flush()?;
    Ok(PhysicalCallRequest {
        arguments,
        logical_argument_count: request.logical_argument_count,
        expected_result_type,
        constant_policy: policy,
    })
}

fn clone_type(
    ty: &ValueType,
    work: &mut CompileCheckpoints<'_>,
) -> Result<ValueType, ContractLoweringError> {
    work.flush()?;
    let cloned = ty.clone();
    work.step()?;
    work.flush()?;
    Ok(cloned)
}

fn push(
    output: &mut Transferred,
    fragment: FragmentId,
    definition: RequestTarget,
    request: PhysicalCallRequest,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), ContractLoweringError> {
    let entries = output.get_mut(&fragment);
    work.step()?;
    let entries = entries.ok_or_else(|| invalid("neutral request fragment count is absent"))?;
    let room = entries.len() < entries.capacity();
    work.step()?;
    if !room {
        return Err(invalid(
            "neutral request exceeds its admitted definition extent",
        ));
    }
    entries.push((definition, request));
    work.step()?;
    Ok(())
}

/// Feed a direct-component fixture through the original request transfer leaf.
/// This does not construct a SQL journal, source loan, entry or completion scope.
#[cfg(test)]
pub(in crate::planner::distributed::build) fn transfer_component_request_for_test(
    request: FunctionBindingRequest<'_>,
    policy: ConstantPolicy,
    work: &mut CompileCheckpoints<'_>,
    register: &mut impl FnMut(
        &ConstantValue,
        &mut CompileCheckpoints<'_>,
    ) -> Result<ConstantReference, ContractLoweringError>,
) -> Result<PhysicalCallRequest, ContractLoweringError> {
    transfer_request(request, policy, work, register)
}

#[cfg(test)]
#[path = "neutral_requests/tests.rs"]
mod tests;
