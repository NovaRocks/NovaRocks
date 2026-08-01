//! Runtime-filter lifecycle contract codec.
//!
//! These DTO values describe query-lifecycle installation, not a fragment
//! program. Keep this small codec in Core so lifecycle handling does not retain
//! a reachability edge to the backend-owned fragment decoder.

use std::num::NonZeroU32;

use crate::exec::node::runtime_filter::{
    ArtifactCapability, ArtifactMembershipSchema, ComparatorDigest, CompletionFenceKind,
    CompletionRequirement, ConsumerActivation, ContributionKind, LateApplyGranularity, NullOrder,
    OrderContract, OrderKeyContract, ReductionRequirement, RuntimeFilterLogicalDomain,
    RuntimeOrderContract, RuntimeOrderKey, RuntimeTopKSummaryContract, SortDirection,
    TopKSummaryRequirement,
};
use crate::proto::plan;
use crate::protocol::{FieldPath, ProtocolError, ProtocolErrorKind, ProtocolFamily};

type CodecResult<T> = Result<T, ProtocolError>;

fn error(path: FieldPath, kind: ProtocolErrorKind, detail: impl Into<String>) -> ProtocolError {
    ProtocolError::new(ProtocolFamily::Native, path, kind, detail)
}

fn missing(path: FieldPath, detail: impl Into<String>) -> ProtocolError {
    error(path, ProtocolErrorKind::MissingField, detail)
}

fn invalid(path: FieldPath, detail: impl Into<String>) -> ProtocolError {
    error(path, ProtocolErrorKind::InvalidValue, detail)
}

fn inconsistent(path: FieldPath, detail: impl Into<String>) -> ProtocolError {
    error(path, ProtocolErrorKind::InconsistentFields, detail)
}

fn digest32(binding_id: u32, field: &str, bytes: &[u8]) -> Result<[u8; 32], String> {
    bytes.try_into().map_err(|_| format!(
        "native runtime-filter binding_id={binding_id} {field} must be exactly 32 bytes, got {}",
        bytes.len()
    ))
}

enum DecodedContract {
    Membership {
        canonical_schema: Vec<u8>,
    },
    Ordered {
        keys: Vec<RuntimeOrderKey>,
        comparator_digest: [u8; 32],
    },
}

enum DecodedReduction {
    SetUnion,
    TightenOrderedBound,
    MergeTopKSummary { k: NonZeroU32 },
}

pub(in crate::protocol::native) fn decode_runtime_filter_logical_domain_and_reduction(
    wire_type: Option<&crate::proto::common::TypeDesc>,
    wire_contract: Option<&plan::RuntimeFilterContract>,
    wire_reduction: Option<&plan::RuntimeFilterReductionContract>,
    path: FieldPath,
) -> CodecResult<(RuntimeFilterLogicalDomain, ReductionRequirement)> {
    let type_path = path.clone().field("value_type");
    let wire_type = wire_type.ok_or_else(|| {
        missing(
            type_path.clone(),
            "runtime filter deployment logical domain is missing value type",
        )
    })?;
    let value_type =
        super::type_mapping::decode_type(wire_type).map_err(|detail| invalid(type_path, detail))?;
    let contract = decode_contract(
        0,
        &value_type,
        wire_contract,
        path.clone().field("contract"),
    )?;
    let reduction = decode_reduction(0, &contract, wire_reduction, path.field("reduction"))?;
    let domain = match &contract {
        DecodedContract::Membership { canonical_schema } => {
            let schema = ArtifactMembershipSchema::view(canonical_schema).map_err(|reason| {
                invalid(
                    FieldPath::root("runtime_filter_install")
                        .field("logical_domain")
                        .field("contract"),
                    format!("invalid membership schema: {reason:?}"),
                )
            })?;
            RuntimeFilterLogicalDomain::Membership {
                value_type,
                null_semantics: schema.null_semantics(),
            }
        }
        DecodedContract::Ordered {
            keys,
            comparator_digest,
        } => RuntimeFilterLogicalDomain::OrderedBound(OrderContract {
            keys: keys
                .iter()
                .map(|key| OrderKeyContract {
                    data_type: key.data_type().clone(),
                    direction: key.direction(),
                    null_order: key.null_order(),
                })
                .collect(),
            inclusive: true,
            comparator_digest: ComparatorDigest::new(*comparator_digest),
        }),
    };
    let reduction = match reduction {
        DecodedReduction::SetUnion => ReductionRequirement::SetUnion,
        DecodedReduction::TightenOrderedBound => ReductionRequirement::TightenOrderedBound,
        DecodedReduction::MergeTopKSummary { k } => ReductionRequirement::MergeTopKSummary(
            TopKSummaryRequirement::try_new(k.get()).expect("decoded TopK K is nonzero"),
        ),
    };
    Ok((domain, reduction))
}

pub(in crate::protocol::native) fn decode_runtime_filter_contribution_kind(
    raw: i32,
    path: FieldPath,
) -> CodecResult<ContributionKind> {
    match plan::RuntimeFilterContributionKind::try_from(raw) {
        Ok(plan::RuntimeFilterContributionKind::ValueDomainDelta) => {
            Ok(ContributionKind::ValueDomainDelta)
        }
        Ok(plan::RuntimeFilterContributionKind::FinalDomainShard) => {
            Ok(ContributionKind::FinalDomainShard)
        }
        Ok(plan::RuntimeFilterContributionKind::OrderedBoundUpdate) => {
            Ok(ContributionKind::OrderedBoundUpdate)
        }
        Ok(plan::RuntimeFilterContributionKind::TopkSummary) => Ok(ContributionKind::TopKSummary),
        Ok(plan::RuntimeFilterContributionKind::ProducerClosed) => {
            Ok(ContributionKind::ProducerClosed)
        }
        Ok(plan::RuntimeFilterContributionKind::Unspecified) | Err(_) => Err(error(
            path,
            ProtocolErrorKind::InvalidEnum,
            format!("invalid runtime filter contribution kind={raw}"),
        )),
    }
}

pub(in crate::protocol::native) fn decode_runtime_filter_completion(
    raw: i32,
    path: FieldPath,
) -> CodecResult<CompletionRequirement> {
    match plan::RuntimeFilterCompletionRequirement::try_from(raw) {
        Ok(plan::RuntimeFilterCompletionRequirement::ProducerClosed) => {
            Ok(CompletionRequirement::ProducerClosed)
        }
        Ok(plan::RuntimeFilterCompletionRequirement::FencedCommittedDomainFrozen) => Ok(
            CompletionRequirement::FencedFinalDomain(CompletionFenceKind::CommittedDomainFrozen),
        ),
        Ok(plan::RuntimeFilterCompletionRequirement::Unspecified) | Err(_) => Err(error(
            path,
            ProtocolErrorKind::InvalidEnum,
            format!("invalid runtime filter completion requirement={raw}"),
        )),
    }
}

pub(in crate::protocol::native) fn decode_runtime_filter_capability(
    raw: i32,
    path: FieldPath,
) -> CodecResult<ArtifactCapability> {
    match plan::RuntimeFilterArtifactCapability::try_from(raw) {
        Ok(plan::RuntimeFilterArtifactCapability::Membership) => Ok(ArtifactCapability::Membership),
        Ok(plan::RuntimeFilterArtifactCapability::OrderedRange) => {
            Ok(ArtifactCapability::OrderedRange)
        }
        Ok(plan::RuntimeFilterArtifactCapability::EmptyDomain) => {
            Ok(ArtifactCapability::EmptyDomain)
        }
        Ok(plan::RuntimeFilterArtifactCapability::Unspecified) | Err(_) => Err(error(
            path,
            ProtocolErrorKind::InvalidEnum,
            format!("invalid runtime filter artifact capability={raw}"),
        )),
    }
}

pub(in crate::protocol::native) fn decode_runtime_filter_activation(
    wire: Option<&plan::RuntimeFilterConsumerActivation>,
    path: FieldPath,
) -> CodecResult<ConsumerActivation> {
    let wire =
        wire.ok_or_else(|| missing(path.clone(), "missing runtime filter consumer activation"))?;
    match wire.kind.as_ref().ok_or_else(|| {
        missing(
            path.clone().field("kind"),
            "missing runtime filter consumer activation kind",
        )
    })? {
        plan::runtime_filter_consumer_activation::Kind::BlockingSnapshot(true) => {
            Ok(ConsumerActivation::BlockingSnapshot)
        }
        plan::runtime_filter_consumer_activation::Kind::BlockingSnapshot(false) => Err(invalid(
            path.field("kind").field("blocking_snapshot"),
            "runtime filter blocking activation marker must be true",
        )),
        plan::runtime_filter_consumer_activation::Kind::NonBlockingLive(raw) => {
            let late_apply = match plan::RuntimeFilterLateApplyGranularity::try_from(*raw) {
                Ok(plan::RuntimeFilterLateApplyGranularity::Row) => LateApplyGranularity::Row,
                Ok(plan::RuntimeFilterLateApplyGranularity::Batch) => LateApplyGranularity::Batch,
                Ok(plan::RuntimeFilterLateApplyGranularity::RowGroup) => {
                    LateApplyGranularity::RowGroup
                }
                Ok(plan::RuntimeFilterLateApplyGranularity::Split) => LateApplyGranularity::Split,
                Ok(plan::RuntimeFilterLateApplyGranularity::File) => LateApplyGranularity::File,
                Ok(plan::RuntimeFilterLateApplyGranularity::Unspecified) | Err(_) => {
                    return Err(error(
                        path.field("kind").field("non_blocking_live"),
                        ProtocolErrorKind::InvalidEnum,
                        format!("invalid runtime filter late-apply granularity={raw}"),
                    ));
                }
            };
            Ok(ConsumerActivation::NonBlockingLive { late_apply })
        }
    }
}

fn decode_contract(
    binding_id: u32,
    expression_type: &arrow::datatypes::DataType,
    wire: Option<&plan::RuntimeFilterContract>,
    path: FieldPath,
) -> CodecResult<DecodedContract> {
    let wire = wire.ok_or_else(|| {
        missing(
            path.clone(),
            format!("native runtime-filter binding_id={binding_id} missing contract"),
        )
    })?;
    let kind = wire.kind.as_ref().ok_or_else(|| {
        missing(
            path.clone().field("kind"),
            format!("native runtime-filter binding_id={binding_id} missing contract kind"),
        )
    })?;
    match kind {
        plan::runtime_filter_contract::Kind::Membership(membership) => {
            let path = path.field("membership");
            if membership.canonical_schema.is_empty() {
                return Err(invalid(
                    path.clone().field("canonical_schema"),
                    format!(
                        "native runtime-filter binding_id={binding_id} membership schema is empty"
                    ),
                ));
            }
            let view = ArtifactMembershipSchema::view(&membership.canonical_schema).map_err(|reason| invalid(
                path.clone().field("canonical_schema"),
                format!("native runtime-filter binding_id={binding_id} membership schema is noncanonical: {reason:?}"),
            ))?;
            let digest = digest32(
                binding_id,
                "membership schema_digest",
                &membership.schema_digest,
            )
            .map_err(|detail| invalid(path.clone().field("schema_digest"), detail))?;
            if view.digest().bytes() != digest {
                return Err(inconsistent(
                    path.clone().field("schema_digest"),
                    format!(
                        "native runtime-filter binding_id={binding_id} membership schema digest mismatch"
                    ),
                ));
            }
            let expected = ArtifactMembershipSchema::new(expression_type, view.null_semantics()).map_err(|reason| invalid(
                path.clone().field("canonical_schema"),
                format!("native runtime-filter binding_id={binding_id} expression type cannot form membership schema: {reason:?}"),
            ))?;
            if expected.canonical_bytes() != membership.canonical_schema {
                return Err(inconsistent(
                    path.field("canonical_schema"),
                    format!(
                        "native runtime-filter binding_id={binding_id} membership schema does not match expression type"
                    ),
                ));
            }
            Ok(DecodedContract::Membership {
                canonical_schema: membership.canonical_schema.clone(),
            })
        }
        plan::runtime_filter_contract::Kind::Ordered(ordered) => {
            let path = path.field("ordered");
            if ordered.keys.len() != 1 {
                return Err(invalid(
                    path.clone().field("keys"),
                    format!(
                        "native runtime-filter binding_id={binding_id} ordered contract must contain exactly one key, got {}",
                        ordered.keys.len()
                    ),
                ));
            }
            let mut keys = Vec::with_capacity(ordered.keys.len());
            for (index, key) in ordered.keys.iter().enumerate() {
                let key_path = path.clone().field("keys").index(index);
                let wire_type = key.r#type.as_ref().ok_or_else(|| {
                    missing(
                        key_path.clone().field("type"),
                        format!(
                            "native runtime-filter binding_id={binding_id} ordered key type missing"
                        ),
                    )
                })?;
                let data_type = super::type_mapping::decode_type(wire_type)
                    .map_err(|detail| invalid(key_path.clone().field("type"), detail))?;
                let direction = match plan::RuntimeFilterSortDirection::try_from(key.direction) {
                    Ok(plan::RuntimeFilterSortDirection::Ascending) => SortDirection::Ascending,
                    Ok(plan::RuntimeFilterSortDirection::Descending) => SortDirection::Descending,
                    Ok(plan::RuntimeFilterSortDirection::Unspecified) | Err(_) => {
                        return Err(error(
                            key_path.clone().field("direction"),
                            ProtocolErrorKind::InvalidEnum,
                            format!(
                                "native runtime-filter binding_id={binding_id} invalid sort direction={}",
                                key.direction
                            ),
                        ));
                    }
                };
                let null_order = match plan::RuntimeFilterNullOrder::try_from(key.null_order) {
                    Ok(plan::RuntimeFilterNullOrder::First) => NullOrder::First,
                    Ok(plan::RuntimeFilterNullOrder::Last) => NullOrder::Last,
                    Ok(plan::RuntimeFilterNullOrder::Unspecified) | Err(_) => {
                        return Err(error(
                            key_path.field("null_order"),
                            ProtocolErrorKind::InvalidEnum,
                            format!(
                                "native runtime-filter binding_id={binding_id} invalid null order={}",
                                key.null_order
                            ),
                        ));
                    }
                };
                keys.push(RuntimeOrderKey::new(data_type, direction, null_order));
            }
            if keys[0].data_type() != expression_type {
                return Err(inconsistent(
                    path.clone().field("keys").index(0).field("type"),
                    format!(
                        "native runtime-filter binding_id={binding_id} ordered key type {:?} does not match expression type {:?}",
                        keys[0].data_type(),
                        expression_type
                    ),
                ));
            }
            let comparator = digest32(binding_id, "comparator_digest", &ordered.comparator_digest)
                .map_err(|detail| invalid(path.clone().field("comparator_digest"), detail))?;
            let order_digest = digest32(
                binding_id,
                "order_contract_digest",
                &ordered.order_contract_digest,
            )
            .map_err(|detail| invalid(path.clone().field("order_contract_digest"), detail))?;
            let order = OrderContract {
                keys: keys
                    .iter()
                    .map(|key| OrderKeyContract {
                        data_type: key.data_type().clone(),
                        direction: key.direction(),
                        null_order: key.null_order(),
                    })
                    .collect(),
                inclusive: true,
                comparator_digest: ComparatorDigest::new(comparator),
            };
            let canonical = RuntimeOrderContract::try_from_plan(&order).map_err(|reason| invalid(path.clone(), format!("native runtime-filter binding_id={binding_id} ordered contract is noncanonical: {reason:?}")))?;
            if canonical.digest().bytes() != order_digest {
                return Err(inconsistent(
                    path.field("order_contract_digest"),
                    format!(
                        "native runtime-filter binding_id={binding_id} order contract digest mismatch"
                    ),
                ));
            }
            Ok(DecodedContract::Ordered {
                keys,
                comparator_digest: comparator,
            })
        }
    }
}

fn decode_reduction(
    binding_id: u32,
    contract: &DecodedContract,
    wire: Option<&plan::RuntimeFilterReductionContract>,
    path: FieldPath,
) -> CodecResult<DecodedReduction> {
    let wire = wire.ok_or_else(|| {
        missing(
            path.clone(),
            format!("native runtime-filter binding_id={binding_id} missing reduction contract"),
        )
    })?;
    let kind = wire.kind.as_ref().ok_or_else(|| {
        missing(
            path.clone().field("kind"),
            format!("native runtime-filter binding_id={binding_id} missing reduction kind"),
        )
    })?;
    match kind {
        plan::runtime_filter_reduction_contract::Kind::SetUnion(true) => {
            Ok(DecodedReduction::SetUnion)
        }
        plan::runtime_filter_reduction_contract::Kind::TightenOrderedBound(true) => {
            Ok(DecodedReduction::TightenOrderedBound)
        }
        plan::runtime_filter_reduction_contract::Kind::SetUnion(false)
        | plan::runtime_filter_reduction_contract::Kind::TightenOrderedBound(false) => {
            Err(invalid(
                path.field("kind"),
                format!(
                    "native runtime-filter binding_id={binding_id} reduction marker must be true"
                ),
            ))
        }
        plan::runtime_filter_reduction_contract::Kind::MergeTopkSummary(topk) => {
            let topk_path = path.field("kind").field("merge_topk_summary");
            let k = NonZeroU32::new(topk.k).ok_or_else(|| {
                invalid(
                    topk_path.clone().field("k"),
                    format!("native runtime-filter binding_id={binding_id} TopK K must be nonzero"),
                )
            })?;
            let digest = digest32(binding_id, "TopK contract_digest", &topk.contract_digest)
                .map_err(|detail| invalid(topk_path.clone().field("contract_digest"), detail))?;
            let DecodedContract::Ordered {
                keys,
                comparator_digest,
            } = contract
            else {
                return Err(inconsistent(
                    topk_path.clone(),
                    format!(
                        "native runtime-filter binding_id={binding_id} TopK reduction requires ordered contract"
                    ),
                ));
            };
            let order = OrderContract {
                keys: keys
                    .iter()
                    .map(|key| OrderKeyContract {
                        data_type: key.data_type().clone(),
                        direction: key.direction(),
                        null_order: key.null_order(),
                    })
                    .collect(),
                inclusive: true,
                comparator_digest: ComparatorDigest::new(*comparator_digest),
            };
            let expected = RuntimeTopKSummaryContract::try_from_plan(&order, TopKSummaryRequirement::try_new(k.get()).expect("nonzero"))
                .map_err(|reason| invalid(topk_path.clone(), format!("native runtime-filter binding_id={binding_id} TopK contract is noncanonical: {reason:?}")))?;
            if expected.digest().bytes() != digest {
                return Err(inconsistent(
                    topk_path.field("contract_digest"),
                    format!(
                        "native runtime-filter binding_id={binding_id} TopK contract digest mismatch"
                    ),
                ));
            }
            Ok(DecodedReduction::MergeTopKSummary { k })
        }
    }
}
