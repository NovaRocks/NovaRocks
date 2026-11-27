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

//! Exact content roles and the independent deletion summary of ContentApply.

use super::{
    FragmentValidationIndexes, ValidationContext, ValidationError,
    value_bindings_match_ignoring_root_nullability,
};
use crate::{
    ContentApplySpec, Distribution, Fragment, FragmentSink, PhysicalNode, PhysicalProperties,
    RowMultiplicity, SummaryAuthor, SummaryKind, ValueId, ValueOrigin,
};
use arrow_schema::DataType;
use novarocks_type_contract::PartitionHashAlgorithm;
use std::collections::BTreeSet;

pub(super) fn validate_content_apply(
    fragment: &Fragment,
    node: &PhysicalNode,
    spec: &ContentApplySpec,
    indexes: &FragmentValidationIndexes,
    path: &str,
    errors: &mut ValidationContext,
) {
    let [weighted, target] = node.inputs.as_ref() else {
        return;
    };
    let (Some(weighted), Some(target)) = (indexes.output(*weighted), indexes.output(*target))
    else {
        return;
    };
    let width = spec.weighted_content.len();
    let data = node.data_output_columns();
    let binding = |id: ValueId| fragment.values().get(&id).map(|definition| &definition.ty);
    if width == 0
        || width != spec.target_content.len()
        || data.len() != width.saturating_add(5)
        || !spec
            .weighted_content
            .iter()
            .zip(spec.target_content.iter())
            .zip(data)
            .all(|((&weighted_key, &target_key), &output)| {
                weighted.contains(&weighted_key)
                    && target.contains(&target_key)
                    && matches!(
                        (binding(weighted_key), binding(target_key), binding(output)),
                        (Some(left), Some(right), Some(out)) if left == right && left == out
                    )
            })
    {
        errors.push(ValidationError::new(
            path,
            "content application requires corresponding input and output content declarations",
        ));
    }
    let is_weight =
        |id| binding(id).is_some_and(|ty| ty == &crate::ValueType::new(DataType::Int8, false));
    if !weighted.contains(&spec.weight)
        || !is_weight(spec.weight)
        || !data.last().copied().is_some_and(is_weight)
    {
        errors.push(ValidationError::new(
            path,
            "content application requires non-null Int8 input weight and output effect",
        ));
    }
    let identities = spec.target.columns();
    let valid_identity = identities.iter().enumerate().all(|(index, &value)| {
        target.contains(&value)
            && binding(value).is_some_and(|ty| {
                !ty.nullable()
                    && matches!(
                        (index, ty.data_type()),
                        (0, DataType::Utf8)
                            | (1, DataType::Int64)
                            | (2, DataType::Int32)
                            | (3, DataType::Struct(_))
                    )
                    && data
                        .get(width.saturating_add(index))
                        .and_then(|&output| binding(output))
                        .is_some_and(|output| {
                            output.nullable()
                                && value_bindings_match_ignoring_root_nullability(ty, output)
                        })
            })
    });
    if !valid_identity
        || data.iter().enumerate().any(|(ordinal, value)| {
            !fragment.values().get(value).is_some_and(|definition| {
                matches!(
                    definition.origin,
                    ValueOrigin::NodeOutput { node: owner, output_ordinal }
                    if owner == node.id && output_ordinal as usize == ordinal
                )
            })
        })
    {
        errors.push(ValidationError::new(
            path,
            "content application requires exact live positions and its own ordered data outputs",
        ));
    }
    let route_type = PartitionHashAlgorithm::NativeExchangeV1;
    if spec.route_keys.is_empty()
        || spec.route_keys.iter().collect::<BTreeSet<_>>().len() != spec.route_keys.len()
        || !spec.route_keys.iter().all(|&ordinal| {
            spec.weighted_content
                .get(ordinal as usize)
                .and_then(|&value| binding(value))
                .is_some_and(|ty| route_type.supports_partition_value_type(ty))
        })
    {
        errors.push(ValidationError::new(
            path,
            "content application requires distinct native exchange hash content ordinals",
        ));
    }
    let summary = fragment.summaries().iter().find(|summary| {
        summary.kind() == SummaryKind::DeleteVerification
            && summary.author == SummaryAuthor::Node(node.id)
    });
    if fragment.root() == node.id
        || !matches!(fragment.sink(), FragmentSink::Router { .. })
        || !fragment.summaries().iter().any(|summary| {
            summary.kind() == SummaryKind::WriteCounts
                && summary.author == SummaryAuthor::RouterSink
        })
        || !summary.is_some_and(|summary| {
            node.output.columns.get(data.len()..) == Some(summary.columns.values())
        })
    {
        errors.push(ValidationError::new(
            path,
            "content application requires independent ordered deletion verification and router write counts",
        ));
    }
}

/// Check the two original child placements, including their shared partition
/// count parameter. Shape alone cannot identify a common owner.
pub(super) fn routes_agree(
    spec: &ContentApplySpec,
    weighted: &PhysicalProperties,
    target: &PhysicalProperties,
) -> bool {
    let (
        Distribution::Hash {
            keys: weighted_keys,
            scheme: weighted_scheme,
        },
        Distribution::Hash {
            keys: target_keys,
            scheme: target_scheme,
        },
    ) = (&weighted.distribution, &target.distribution)
    else {
        return false;
    };
    weighted.row_multiplicity == RowMultiplicity::SingleCopy
        && target.row_multiplicity == RowMultiplicity::SingleCopy
        && weighted_scheme == target_scheme
        && weighted_scheme.definition.algorithm == PartitionHashAlgorithm::NativeExchangeV1
        && weighted_keys.len() == spec.route_keys.len()
        && target_keys.len() == spec.route_keys.len()
        && spec
            .route_keys
            .iter()
            .zip(weighted_keys.iter().zip(target_keys.iter()))
            .all(|(&ordinal, (weighted, target))| {
                spec.weighted_content.get(ordinal as usize) == Some(weighted)
                    && spec.target_content.get(ordinal as usize) == Some(target)
            })
}
