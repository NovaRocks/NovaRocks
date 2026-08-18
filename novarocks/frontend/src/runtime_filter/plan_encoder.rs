//! Frontend-owned semantic mapping from sealed RF facts to native plan DTOs.

use crate::query_execution::artifact::{
    RuntimeFilterBindingAttachment, RuntimeFilterBindingEncodingView,
};
use crate::query_execution::contract::DistributedQueryError;
use crate::query_execution::{
    RuntimeFilterApplyPoint, RuntimeFilterArtifactCapability, RuntimeFilterBindingFacts,
    RuntimeFilterBindingFragmentFactsView, RuntimeFilterBindingRoleFacts,
    RuntimeFilterCompletionRequirement, RuntimeFilterConsumerActivation,
    RuntimeFilterConsumerTarget, RuntimeFilterContributionKind, RuntimeFilterLateApplyGranularity,
    RuntimeFilterProducerTarget,
};
use novarocks_protocol::plan;

use super::semantic_encoder;

fn encoding_error(message: impl Into<String>) -> DistributedQueryError {
    crate::query_execution::contract::DistributedQueryError::new(
        crate::query_execution::contract::DistributedQueryErrorKind::ContractViolation,
        message,
    )
}

/// Encode the complete, stable-ordered binding table for every sealed native
/// fragment and seal it into a consuming Core attachment.
pub fn encode_binding_attachment(
    view: RuntimeFilterBindingEncodingView<'_>,
) -> Result<RuntimeFilterBindingAttachment, DistributedQueryError> {
    let tables = view
        .facts()
        .fragments()
        .map(encode_binding_table)
        .collect::<Result<Vec<_>, _>>()?;
    view.seal(tables)
}

fn encode_binding_table(
    fragment: RuntimeFilterBindingFragmentFactsView<'_>,
) -> Result<plan::RuntimeFilterBindingTable, DistributedQueryError> {
    encode_binding_table_from_facts(fragment.fragment_id(), fragment.bindings())
}

fn encode_binding_table_from_facts<'a>(
    fragment_id: u32,
    bindings: impl IntoIterator<Item = RuntimeFilterBindingFacts<'a>>,
) -> Result<plan::RuntimeFilterBindingTable, DistributedQueryError> {
    let mut previous = None;
    let bindings = bindings
        .into_iter()
        .map(|binding| {
            validate_binding_order(&mut previous, binding.binding_id())?;
            encode_binding(binding)
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(plan::RuntimeFilterBindingTable {
        fragment_id,
        bindings,
    })
}

fn validate_binding_order(
    previous: &mut Option<u32>,
    binding_id: u32,
) -> Result<(), DistributedQueryError> {
    if previous.is_some_and(|prior| prior >= binding_id) {
        return Err(encoding_error(format!(
            "runtime filter binding facts are not strictly ordered: previous={previous:?} current={binding_id}"
        )));
    }
    *previous = Some(binding_id);
    Ok(())
}

fn encode_binding(
    binding: crate::query_execution::RuntimeFilterBindingFacts<'_>,
) -> Result<plan::RuntimeFilterBinding, DistributedQueryError> {
    let logical_domain = semantic_encoder::encode_logical_domain(binding.logical_domain())?;
    Ok(plan::RuntimeFilterBinding {
        binding_id: binding.binding_id(),
        channel_id: binding.channel_id(),
        node_id: binding.node_id(),
        apply_point: encode_apply_point(binding.apply_point()),
        expression: Some(
            crate::native::fragment_encoder::expr::encode_expr(binding.expression())
                .map_err(encoding_error)?,
        ),
        contract: Some(logical_domain.contract()),
        reduction: Some(logical_domain.encode_reduction(binding.reduction())?),
        role: Some(encode_role(binding.role())?),
    })
}

fn encode_apply_point(apply_point: RuntimeFilterApplyPoint) -> i32 {
    match apply_point {
        RuntimeFilterApplyPoint::NodeInput => i32::from(plan::RuntimeFilterApplyPoint::NodeInput),
        RuntimeFilterApplyPoint::NodeOutput => i32::from(plan::RuntimeFilterApplyPoint::NodeOutput),
    }
}

fn encode_role(
    role: RuntimeFilterBindingRoleFacts,
) -> Result<plan::runtime_filter_binding::Role, DistributedQueryError> {
    Ok(match role {
        RuntimeFilterBindingRoleFacts::Producer {
            contribution_kinds,
            completion_requirement,
            target,
        } => plan::runtime_filter_binding::Role::Producer(plan::RuntimeFilterProducerRole {
            contribution_kinds: contribution_kinds
                .into_iter()
                .map(|kind| match kind {
                    RuntimeFilterContributionKind::ValueDomainDelta => {
                        i32::from(plan::RuntimeFilterContributionKind::ValueDomainDelta)
                    }
                    RuntimeFilterContributionKind::FinalDomainShard => {
                        i32::from(plan::RuntimeFilterContributionKind::FinalDomainShard)
                    }
                    RuntimeFilterContributionKind::OrderedBoundUpdate => {
                        i32::from(plan::RuntimeFilterContributionKind::OrderedBoundUpdate)
                    }
                    RuntimeFilterContributionKind::TopKSummary => {
                        i32::from(plan::RuntimeFilterContributionKind::TopkSummary)
                    }
                    RuntimeFilterContributionKind::ProducerClosed => {
                        i32::from(plan::RuntimeFilterContributionKind::ProducerClosed)
                    }
                })
                .collect(),
            completion_requirement: match completion_requirement {
                RuntimeFilterCompletionRequirement::ProducerClosed => {
                    i32::from(plan::RuntimeFilterCompletionRequirement::ProducerClosed)
                }
                RuntimeFilterCompletionRequirement::FencedCommittedDomainFrozen => {
                    i32::from(plan::RuntimeFilterCompletionRequirement::FencedCommittedDomainFrozen)
                }
            },
            target: Some(match target {
                RuntimeFilterProducerTarget::JoinBuildKey { ordinal } => {
                    plan::runtime_filter_producer_role::Target::JoinBuildKey(
                        plan::RuntimeFilterJoinBuildKey { ordinal },
                    )
                }
                RuntimeFilterProducerTarget::AggregateTopNKey {
                    group_key_ordinal,
                    limit,
                } => plan::runtime_filter_producer_role::Target::AggregateTopnKey(
                    plan::RuntimeFilterAggregateTopNKey {
                        group_key_ordinal,
                        limit,
                    },
                ),
            }),
        }),
        RuntimeFilterBindingRoleFacts::Consumer {
            capabilities,
            activation,
            target,
        } => plan::runtime_filter_binding::Role::Consumer(plan::RuntimeFilterConsumerRole {
            capabilities: capabilities
                .into_iter()
                .map(|capability| match capability {
                    RuntimeFilterArtifactCapability::Membership => {
                        i32::from(plan::RuntimeFilterArtifactCapability::Membership)
                    }
                    RuntimeFilterArtifactCapability::OrderedRange => {
                        i32::from(plan::RuntimeFilterArtifactCapability::OrderedRange)
                    }
                    RuntimeFilterArtifactCapability::EmptyDomain => {
                        i32::from(plan::RuntimeFilterArtifactCapability::EmptyDomain)
                    }
                })
                .collect(),
            activation: Some(plan::RuntimeFilterConsumerActivation {
                kind: Some(match activation {
                    RuntimeFilterConsumerActivation::BlockingSnapshot => {
                        plan::runtime_filter_consumer_activation::Kind::BlockingSnapshot(true)
                    }
                    RuntimeFilterConsumerActivation::NonBlockingLive(granularity) => {
                        plan::runtime_filter_consumer_activation::Kind::NonBlockingLive(
                            match granularity {
                                RuntimeFilterLateApplyGranularity::Row => {
                                    i32::from(plan::RuntimeFilterLateApplyGranularity::Row)
                                }
                                RuntimeFilterLateApplyGranularity::Batch => {
                                    i32::from(plan::RuntimeFilterLateApplyGranularity::Batch)
                                }
                                RuntimeFilterLateApplyGranularity::RowGroup => {
                                    i32::from(plan::RuntimeFilterLateApplyGranularity::RowGroup)
                                }
                                RuntimeFilterLateApplyGranularity::Split => {
                                    i32::from(plan::RuntimeFilterLateApplyGranularity::Split)
                                }
                                RuntimeFilterLateApplyGranularity::File => {
                                    i32::from(plan::RuntimeFilterLateApplyGranularity::File)
                                }
                            },
                        )
                    }
                }),
            }),
            target: Some(match target {
                RuntimeFilterConsumerTarget::DirectInputOrdinal(ordinal) => {
                    plan::runtime_filter_consumer_role::Target::DirectInputOrdinal(ordinal)
                }
                RuntimeFilterConsumerTarget::SourceBoundary { scan_domain_target } => {
                    let scan_domain_target = scan_domain_target
                        .map(|target| {
                            Ok(plan::RuntimeFilterScanDomainTarget {
                                field_ordinal: target.field_ordinal,
                                r#type: Some(semantic_encoder::encode_type(&target.data_type)?),
                                nullable: target.nullable,
                            })
                        })
                        .transpose()?;
                    plan::runtime_filter_consumer_role::Target::SourceBoundaryTarget(
                        plan::RuntimeFilterSourceBoundaryTarget { scan_domain_target },
                    )
                }
            }),
        }),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query_execution::{
        RuntimeFilterLogicalDomainFacts, RuntimeFilterNullOrder, RuntimeFilterNullSemantics,
        RuntimeFilterOrderKeyFacts, RuntimeFilterReductionFacts, RuntimeFilterScanDomainTarget,
        RuntimeFilterSortDirection,
    };
    use arrow::datatypes::DataType;
    use plan::runtime_filter_binding::Role;
    use plan::runtime_filter_consumer_activation::Kind as ActivationKind;
    use plan::runtime_filter_reduction_contract::Kind as ReductionKind;
    use sha2::Digest;

    #[test]
    fn empty_fragment_encodes_an_explicit_empty_binding_table() {
        let table = encode_binding_table_from_facts(
            17,
            std::iter::empty::<RuntimeFilterBindingFacts<'static>>(),
        )
        .expect("empty sealed fragment must encode");

        assert_eq!(table.fragment_id, 17);
        assert!(table.bindings.is_empty());
    }

    #[test]
    fn binding_order_rejects_duplicate_and_reversed_ids() {
        let mut previous = None;
        validate_binding_order(&mut previous, 7).expect("first binding");
        let duplicate =
            validate_binding_order(&mut previous, 7).expect_err("duplicate binding id must fail");
        assert!(duplicate.to_string().contains("previous=Some(7) current=7"));

        let mut previous = Some(9);
        let reversed =
            validate_binding_order(&mut previous, 8).expect_err("reversed binding id must fail");
        assert!(reversed.to_string().contains("previous=Some(9) current=8"));
    }

    #[test]
    fn apply_points_map_without_defaults() {
        assert_eq!(
            encode_apply_point(RuntimeFilterApplyPoint::NodeInput),
            i32::from(plan::RuntimeFilterApplyPoint::NodeInput)
        );
        assert_eq!(
            encode_apply_point(RuntimeFilterApplyPoint::NodeOutput),
            i32::from(plan::RuntimeFilterApplyPoint::NodeOutput)
        );
    }

    #[test]
    fn frontend_semantic_encoder_preserves_membership_and_ordered_contracts() {
        let membership =
            semantic_encoder::encode_logical_domain(RuntimeFilterLogicalDomainFacts::Membership {
                value_type: DataType::Int32,
                null_semantics: RuntimeFilterNullSemantics::NeverMatches,
            })
            .expect("membership contract")
            .contract();
        let Some(plan::runtime_filter_contract::Kind::Membership(membership)) = membership.kind
        else {
            panic!("membership kind");
        };
        assert!(!membership.canonical_schema.is_empty());
        assert_eq!(membership.schema_digest.len(), 32);

        let ordered =
            semantic_encoder::encode_logical_domain(RuntimeFilterLogicalDomainFacts::Ordered {
                keys: vec![
                    RuntimeFilterOrderKeyFacts {
                        data_type: DataType::Int32,
                        direction: RuntimeFilterSortDirection::Ascending,
                        null_order: RuntimeFilterNullOrder::First,
                    },
                    RuntimeFilterOrderKeyFacts {
                        data_type: DataType::Int64,
                        direction: RuntimeFilterSortDirection::Descending,
                        null_order: RuntimeFilterNullOrder::Last,
                    },
                ],
                inclusive: true,
                comparator_digest: {
                    let mut canonical = Vec::new();
                    canonical.extend_from_slice(&2u32.to_be_bytes());
                    canonical.extend_from_slice(&[4, 1, 1, 5, 2, 2]);
                    let mut digest = sha2::Sha256::new();
                    digest.update(b"novarocks.runtime-filter.comparator");
                    digest.update(1u16.to_be_bytes());
                    digest.update(canonical);
                    sha2::Digest::finalize(digest).into()
                },
            })
            .expect("ordered contract")
            .contract();
        let Some(plan::runtime_filter_contract::Kind::Ordered(ordered)) = ordered.kind else {
            panic!("ordered kind");
        };
        assert_eq!(ordered.keys.len(), 2);
        assert_eq!(
            ordered.keys[0].direction,
            i32::from(plan::RuntimeFilterSortDirection::Ascending)
        );
        assert_eq!(
            ordered.keys[1].null_order,
            i32::from(plan::RuntimeFilterNullOrder::Last)
        );
        assert_eq!(ordered.comparator_digest.len(), 32);
        assert_eq!(ordered.order_contract_digest.len(), 32);
    }

    #[test]
    fn reductions_are_encoded_by_the_same_semantic_domain() {
        let membership =
            semantic_encoder::encode_logical_domain(RuntimeFilterLogicalDomainFacts::Membership {
                value_type: DataType::Int32,
                null_semantics: RuntimeFilterNullSemantics::NeverMatches,
            })
            .expect("membership domain");
        assert_eq!(
            membership
                .encode_reduction(RuntimeFilterReductionFacts::SetUnion)
                .expect("membership reduction")
                .kind,
            Some(ReductionKind::SetUnion(true))
        );
    }

    #[test]
    fn producer_role_preserves_contributions_completion_and_targets() {
        let role = encode_role(RuntimeFilterBindingRoleFacts::Producer {
            contribution_kinds: vec![
                RuntimeFilterContributionKind::ValueDomainDelta,
                RuntimeFilterContributionKind::FinalDomainShard,
                RuntimeFilterContributionKind::OrderedBoundUpdate,
                RuntimeFilterContributionKind::TopKSummary,
                RuntimeFilterContributionKind::ProducerClosed,
            ],
            completion_requirement: RuntimeFilterCompletionRequirement::FencedCommittedDomainFrozen,
            target: RuntimeFilterProducerTarget::JoinBuildKey { ordinal: 3 },
        })
        .expect("producer role");
        let Role::Producer(producer) = role else {
            panic!("producer role");
        };
        assert_eq!(
            producer.contribution_kinds,
            vec![
                i32::from(plan::RuntimeFilterContributionKind::ValueDomainDelta),
                i32::from(plan::RuntimeFilterContributionKind::FinalDomainShard),
                i32::from(plan::RuntimeFilterContributionKind::OrderedBoundUpdate),
                i32::from(plan::RuntimeFilterContributionKind::TopkSummary),
                i32::from(plan::RuntimeFilterContributionKind::ProducerClosed),
            ]
        );
        assert_eq!(
            producer.completion_requirement,
            i32::from(plan::RuntimeFilterCompletionRequirement::FencedCommittedDomainFrozen)
        );
        assert_eq!(
            producer.target,
            Some(plan::runtime_filter_producer_role::Target::JoinBuildKey(
                plan::RuntimeFilterJoinBuildKey { ordinal: 3 }
            ))
        );

        let Role::Producer(aggregate) = encode_role(RuntimeFilterBindingRoleFacts::Producer {
            contribution_kinds: vec![RuntimeFilterContributionKind::ProducerClosed],
            completion_requirement: RuntimeFilterCompletionRequirement::ProducerClosed,
            target: RuntimeFilterProducerTarget::AggregateTopNKey {
                group_key_ordinal: 4,
                limit: 9,
            },
        })
        .expect("aggregate producer role") else {
            panic!("aggregate producer role");
        };
        assert_eq!(
            aggregate.target,
            Some(
                plan::runtime_filter_producer_role::Target::AggregateTopnKey(
                    plan::RuntimeFilterAggregateTopNKey {
                        group_key_ordinal: 4,
                        limit: 9,
                    }
                )
            )
        );
    }

    #[test]
    fn consumer_role_preserves_capabilities_activation_and_targets() {
        let Role::Consumer(blocking) = encode_role(RuntimeFilterBindingRoleFacts::Consumer {
            capabilities: vec![
                RuntimeFilterArtifactCapability::Membership,
                RuntimeFilterArtifactCapability::OrderedRange,
                RuntimeFilterArtifactCapability::EmptyDomain,
            ],
            activation: RuntimeFilterConsumerActivation::BlockingSnapshot,
            target: RuntimeFilterConsumerTarget::DirectInputOrdinal(5),
        })
        .expect("blocking consumer") else {
            panic!("consumer role");
        };
        assert_eq!(
            blocking.capabilities,
            vec![
                i32::from(plan::RuntimeFilterArtifactCapability::Membership),
                i32::from(plan::RuntimeFilterArtifactCapability::OrderedRange),
                i32::from(plan::RuntimeFilterArtifactCapability::EmptyDomain),
            ]
        );
        assert_eq!(
            blocking.activation.and_then(|activation| activation.kind),
            Some(ActivationKind::BlockingSnapshot(true))
        );
        assert_eq!(
            blocking.target,
            Some(plan::runtime_filter_consumer_role::Target::DirectInputOrdinal(5))
        );

        for (granularity, expected) in [
            (
                RuntimeFilterLateApplyGranularity::Row,
                plan::RuntimeFilterLateApplyGranularity::Row,
            ),
            (
                RuntimeFilterLateApplyGranularity::Batch,
                plan::RuntimeFilterLateApplyGranularity::Batch,
            ),
            (
                RuntimeFilterLateApplyGranularity::RowGroup,
                plan::RuntimeFilterLateApplyGranularity::RowGroup,
            ),
            (
                RuntimeFilterLateApplyGranularity::Split,
                plan::RuntimeFilterLateApplyGranularity::Split,
            ),
            (
                RuntimeFilterLateApplyGranularity::File,
                plan::RuntimeFilterLateApplyGranularity::File,
            ),
        ] {
            let Role::Consumer(live) = encode_role(RuntimeFilterBindingRoleFacts::Consumer {
                capabilities: vec![RuntimeFilterArtifactCapability::Membership],
                activation: RuntimeFilterConsumerActivation::NonBlockingLive(granularity),
                target: RuntimeFilterConsumerTarget::SourceBoundary {
                    scan_domain_target: None,
                },
            })
            .expect("live consumer") else {
                panic!("consumer role");
            };
            assert_eq!(
                live.activation.and_then(|activation| activation.kind),
                Some(ActivationKind::NonBlockingLive(i32::from(expected)))
            );
            assert_eq!(
                live.target,
                Some(
                    plan::runtime_filter_consumer_role::Target::SourceBoundaryTarget(
                        plan::RuntimeFilterSourceBoundaryTarget {
                            scan_domain_target: None,
                        }
                    )
                )
            );
        }

        let Role::Consumer(scan_domain_consumer) =
            encode_role(RuntimeFilterBindingRoleFacts::Consumer {
                capabilities: vec![RuntimeFilterArtifactCapability::Membership],
                activation: RuntimeFilterConsumerActivation::BlockingSnapshot,
                target: RuntimeFilterConsumerTarget::SourceBoundary {
                    scan_domain_target: Some(RuntimeFilterScanDomainTarget {
                        field_ordinal: 17,
                        data_type: DataType::Int32,
                        nullable: true,
                    }),
                },
            })
            .expect("scan-domain consumer role")
        else {
            panic!("consumer role");
        };
        let Some(plan::runtime_filter_consumer_role::Target::SourceBoundaryTarget(target)) =
            scan_domain_consumer.target
        else {
            panic!("structured source-boundary target");
        };
        let target = target.scan_domain_target.expect("scan-domain target");
        assert_eq!(target.field_ordinal, 17);
        assert_eq!(target.nullable, true);
        assert!(target.r#type.is_some());
    }
}
