//! Frontend-owned runtime-filter semantic encoding.
//!
//! Core exposes sealed SQL facts only.  This module is the sole owner of the
//! native runtime-filter plan-contract construction used by both fragment
//! bindings and participant deployment/install contributions.

use crate::query_execution::contract::{DistributedQueryError, DistributedQueryErrorKind};
use crate::query_execution::preparation::runtime_filter_view::{
    RuntimeFilterLogicalDomainFacts, RuntimeFilterNullOrder, RuntimeFilterNullSemantics,
    RuntimeFilterReductionFacts, RuntimeFilterSortDirection,
};
use arrow::datatypes::{DECIMAL128_MAX_PRECISION, DECIMAL128_MAX_SCALE, DataType, TimeUnit};
use novarocks_execution::runtime_filter::{
    RuntimeFilterMembershipSchema,
    RuntimeFilterNullSemantics as ExecutionRuntimeFilterNullSemantics,
};
use novarocks_plan_codec::execution_type_table::{
    EncodedExecutionTypeTable, ExecutionTypeRoot, ExecutionTypeTableBuilder,
    ExecutionTypeTableLimits,
};
use novarocks_proto_models::{common, plan};
use novarocks_type_contract::{CompileCheckpoints, CompilePhase};
use novarocks_types::largeint::LARGEINT_BYTE_WIDTH;
use sha2::{Digest, Sha256};

const ORDER_CONTRACT_DOMAIN: &[u8] = b"novarocks.runtime-filter.order-contract";
const TOPK_CONTRACT_DOMAIN: &[u8] = b"novarocks.runtime-filter.top-k-summary-contract";
const CONTRACT_VERSION: u16 = 1;

pub(crate) struct EncodedRuntimeFilterDomain {
    value_type: common::TypeDesc,
    execution_types: plan::ExecutionTypeTable,
    contract: plan::RuntimeFilterContract,
    order_contract_digest: Option<[u8; 32]>,
    membership_schema_digest: Option<[u8; 32]>,
}

impl EncodedRuntimeFilterDomain {
    pub(crate) fn value_type(&self) -> common::TypeDesc {
        self.value_type
    }

    pub(crate) fn execution_types(&self) -> plan::ExecutionTypeTable {
        self.execution_types.clone()
    }

    pub(crate) fn contract(&self) -> plan::RuntimeFilterContract {
        self.contract.clone()
    }

    /// Execution derives the same digest from the sealed value type and
    /// null-comparison semantics when it installs the participant contract.
    /// Retaining it here lets Frontend bind feedback to that exact contract
    /// without reviving a redundant digest field on the carrier.
    pub(crate) const fn membership_schema_digest(&self) -> Option<[u8; 32]> {
        self.membership_schema_digest
    }

    pub(crate) fn encode_reduction(
        &self,
        reduction: RuntimeFilterReductionFacts,
    ) -> Result<plan::RuntimeFilterReductionContract, DistributedQueryError> {
        use plan::runtime_filter_reduction_contract::Kind;

        let kind = match reduction {
            RuntimeFilterReductionFacts::SetUnion => {
                if self.order_contract_digest.is_some() {
                    return Err(contract_error(
                        "runtime filter SetUnion reduction requires a membership domain",
                    ));
                }
                Kind::SetUnion(true)
            }
            RuntimeFilterReductionFacts::TightenOrderedBound => {
                if self.order_contract_digest.is_none() {
                    return Err(contract_error(
                        "runtime filter ordered-bound reduction requires an ordered domain",
                    ));
                }
                Kind::TightenOrderedBound(true)
            }
            RuntimeFilterReductionFacts::MergeTopKSummary { k } => {
                let order_contract_digest = self.order_contract_digest.ok_or_else(|| {
                    contract_error("runtime filter TopK reduction requires an ordered domain")
                })?;
                if k == 0 {
                    return Err(contract_error("runtime filter TopK reduction has zero k"));
                }
                let mut digest = Sha256::new();
                digest.update(TOPK_CONTRACT_DOMAIN);
                digest.update(CONTRACT_VERSION.to_be_bytes());
                digest.update(order_contract_digest);
                digest.update(k.to_be_bytes());
                Kind::MergeTopkSummary(plan::RuntimeFilterTopKReduction {
                    k,
                    contract_digest: digest.finalize().to_vec(),
                })
            }
        };
        Ok(plan::RuntimeFilterReductionContract { kind: Some(kind) })
    }
}

pub(crate) fn encode_logical_domain(
    facts: RuntimeFilterLogicalDomainFacts,
) -> Result<EncodedRuntimeFilterDomain, DistributedQueryError> {
    use plan::runtime_filter_contract::Kind;

    match facts {
        RuntimeFilterLogicalDomainFacts::Membership {
            value_type,
            null_semantics,
        } => {
            if let Some(temporal) = value_type.value_type().temporal() {
                if value_type.data_type() != &temporal.carrier()
                    || !novarocks_local_program::is_semantic_filter_key_type(&value_type)
                {
                    return Err(contract_error(
                        "runtime filter membership key has an unsupported temporal declaration",
                    ));
                }
            } else {
                validate_native_type(value_type.data_type())?;
            }
            let execution_null_semantics = match null_semantics {
                RuntimeFilterNullSemantics::NeverMatches => {
                    ExecutionRuntimeFilterNullSemantics::NeverMatches
                }
                RuntimeFilterNullSemantics::NullSafeEqual => {
                    ExecutionRuntimeFilterNullSemantics::NullSafeEqual
                }
            };
            let membership_schema_digest = RuntimeFilterMembershipSchema::new(
                value_type.data_type(),
                execution_null_semantics,
            )
            .map_err(|error| {
                contract_error(format!(
                    "runtime filter membership execution schema is invalid: {error}"
                ))
            })?
            .digest();
            let (table, references) = encode_domain_type_refs(&[&value_type])?;
            let value_type_wire = references[0];
            Ok(EncodedRuntimeFilterDomain {
                value_type: value_type_wire,
                execution_types: table.clone(),
                contract: plan::RuntimeFilterContract {
                    logical_domain: Some(plan::RuntimeFilterTypeDomain {
                        value_type: Some(value_type_wire),
                        execution_types: Some(table),
                    }),
                    kind: Some(Kind::Membership(plan::RuntimeFilterMembershipContract {
                        null_semantics: match null_semantics {
                            RuntimeFilterNullSemantics::NeverMatches => {
                                i32::from(plan::RuntimeFilterMembershipNullSemantics::NeverMatches)
                            }
                            RuntimeFilterNullSemantics::NullSafeEqual => {
                                i32::from(plan::RuntimeFilterMembershipNullSemantics::NullSafeEqual)
                            }
                        },
                    })),
                },
                order_contract_digest: None,
                membership_schema_digest: Some(membership_schema_digest),
            })
        }
        RuntimeFilterLogicalDomainFacts::Ordered {
            keys,
            inclusive,
            comparator_digest,
        } => {
            if keys.is_empty() {
                return Err(contract_error(
                    "runtime filter ordered contract has no keys",
                ));
            }
            if !inclusive {
                return Err(contract_error(
                    "runtime filter ordered contract must use inclusive bounds",
                ));
            }
            let mut canonical_keys = Vec::with_capacity(64);
            canonical_keys.extend_from_slice(
                &u32::try_from(keys.len())
                    .map_err(|_| contract_error("runtime filter ordered key count overflows u32"))?
                    .to_be_bytes(),
            );
            for key in &keys {
                let data_type = key.value_type.data_type();
                if let Some(temporal) = key.value_type.value_type().temporal() {
                    if data_type != &temporal.carrier() {
                        return Err(contract_error(
                            "runtime filter ordered key has a foreign temporal carrier",
                        ));
                    }
                    canonical_keys.extend_from_slice(
                        &novarocks_local_program::temporal_order_key_type_bytes(temporal),
                    );
                } else {
                    if !ordered_key_type_supported(data_type) {
                        return Err(contract_error(
                            "runtime filter ordered contract has unsupported key type",
                        ));
                    }
                    encode_membership_schema_type(data_type, &mut canonical_keys)?;
                    validate_native_type(data_type)?;
                }
                canonical_keys.push(match key.direction {
                    RuntimeFilterSortDirection::Ascending => 1,
                    RuntimeFilterSortDirection::Descending => 2,
                });
                canonical_keys.push(match key.null_order {
                    RuntimeFilterNullOrder::First => 1,
                    RuntimeFilterNullOrder::Last => 2,
                });
            }
            let values = keys.iter().map(|key| &key.value_type).collect::<Vec<_>>();
            let (table, references) = encode_domain_type_refs(&values)?;
            let mut wire_keys = Vec::with_capacity(keys.len());
            for (key, reference) in keys.into_iter().zip(references) {
                wire_keys.push(plan::RuntimeFilterOrderKey {
                    r#type: Some(reference),
                    direction: match key.direction {
                        RuntimeFilterSortDirection::Ascending => {
                            plan::RuntimeFilterSortDirection::Ascending as i32
                        }
                        RuntimeFilterSortDirection::Descending => {
                            plan::RuntimeFilterSortDirection::Descending as i32
                        }
                    },
                    null_order: match key.null_order {
                        RuntimeFilterNullOrder::First => plan::RuntimeFilterNullOrder::First as i32,
                        RuntimeFilterNullOrder::Last => plan::RuntimeFilterNullOrder::Last as i32,
                    },
                });
            }
            // SQL seals the comparator digest with its ordering semantics.  It is
            // deliberately opaque here: Frontend validates the remaining native
            // contract shape and carries this sealed fact into the v1 order digest.
            let mut order = Sha256::new();
            order.update(ORDER_CONTRACT_DOMAIN);
            order.update(CONTRACT_VERSION.to_be_bytes());
            order.update(&canonical_keys);
            order.update([1]);
            order.update(comparator_digest);
            order.update(CONTRACT_VERSION.to_be_bytes());
            let order_contract_digest: [u8; 32] = order.finalize().into();
            let value_type = wire_keys
                .first()
                .and_then(|key| key.r#type)
                .expect("ordered keys are nonempty and encoded");
            Ok(EncodedRuntimeFilterDomain {
                value_type,
                execution_types: table.clone(),
                contract: plan::RuntimeFilterContract {
                    logical_domain: Some(plan::RuntimeFilterTypeDomain {
                        value_type: Some(value_type),
                        execution_types: Some(table),
                    }),
                    kind: Some(Kind::Ordered(plan::RuntimeFilterOrderedContract {
                        keys: wire_keys,
                        comparator_digest: comparator_digest.to_vec(),
                        order_contract_digest: order_contract_digest.to_vec(),
                    })),
                },
                order_contract_digest: Some(order_contract_digest),
                membership_schema_digest: None,
            })
        }
    }
}

// The original native RF carrier capability is independent of its complete
// declaration. Retain that capability gate without authoring semantics from it.
fn validate_native_type(data_type: &DataType) -> Result<(), DistributedQueryError> {
    match data_type {
        DataType::Boolean
        | DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::Float32
        | DataType::Float64
        | DataType::Utf8
        | DataType::Date32 => Ok(()),
        DataType::FixedSizeBinary(width) if *width == LARGEINT_BYTE_WIDTH => Ok(()),
        DataType::Timestamp(TimeUnit::Microsecond | TimeUnit::Nanosecond, _) => Ok(()),
        DataType::Timestamp(unit, _) => Err(contract_error(format!(
            "runtime filter native type does not support timestamp unit {unit:?}"
        ))),
        DataType::Decimal128(precision, scale)
            if *precision != 0
                && *precision <= DECIMAL128_MAX_PRECISION
                && *scale <= DECIMAL128_MAX_SCALE
                && (*scale <= 0 || (*scale as u8) <= *precision) =>
        {
            Ok(())
        }
        _ => Err(contract_error(format!(
            "runtime filter native type does not support {data_type:?}"
        ))),
    }
}

fn encode_domain_type_refs(
    values: &[&novarocks_physical_plan::ValueType],
) -> Result<(plan::ExecutionTypeTable, Vec<common::TypeDesc>), DistributedQueryError> {
    let control = novarocks_sql::compiler::SqlCompileControl::unbounded();
    let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Encode)
        .map_err(|error| contract_error(error.to_string()))?;
    let roots = values
        .iter()
        .enumerate()
        .map(|(index, value)| {
            Ok(ExecutionTypeRoot {
                id: u32::try_from(index)
                    .map_err(|_| contract_error("runtime filter type ID overflows"))?,
                value,
            })
        })
        .collect::<Result<Vec<_>, DistributedQueryError>>()?;
    let limits = ExecutionTypeTableLimits::for_validated_root_count(roots.len())
        .map_err(|error| contract_error(error.to_string()))?;
    // FE observes the existing structural model only. No memory factory or
    // allocation authority is introduced by this pure encoding path.
    let table: EncodedExecutionTypeTable = ExecutionTypeTableBuilder::new(&roots)
        .encode_observed(
            limits,
            &mut |_| {
                Ok::<
                    _,
                    novarocks_plan_codec::host_projection_v2::AdmissionRefusal<
                        std::convert::Infallible,
                    >,
                >(())
            },
            &mut work,
        )
        .map_err(|error| contract_error(error.to_string()))?;
    let references = roots
        .iter()
        .map(|root| {
            table
                .type_reference_observed(root.id, &mut work)
                .map_err(|error| contract_error(error.to_string()))
        })
        .collect::<Result<Vec<_>, _>>()?;
    work.flush()
        .map_err(|error| contract_error(error.to_string()))?;
    Ok((table.into_wire(), references))
}

fn encode_membership_schema_type(
    data_type: &DataType,
    output: &mut Vec<u8>,
) -> Result<(), DistributedQueryError> {
    match data_type {
        DataType::Boolean => output.push(1),
        DataType::Int8 => output.push(2),
        DataType::Int16 => output.push(3),
        DataType::Int32 => output.push(4),
        DataType::Int64 => output.push(5),
        DataType::FixedSizeBinary(width) if *width == LARGEINT_BYTE_WIDTH => output.push(6),
        DataType::Float32 => output.push(7),
        DataType::Float64 => output.push(8),
        DataType::Utf8 => output.push(9),
        DataType::Date32 => output.push(10),
        DataType::Timestamp(unit, timezone) => {
            output.extend_from_slice(&[
                11,
                match unit {
                    TimeUnit::Second => 1,
                    TimeUnit::Millisecond => 2,
                    TimeUnit::Microsecond => 3,
                    TimeUnit::Nanosecond => 4,
                },
            ]);
            match timezone {
                Some(timezone) => {
                    output.push(1);
                    let length = u32::try_from(timezone.len()).map_err(|_| {
                        contract_error("runtime filter membership timezone length overflows u32")
                    })?;
                    output.extend_from_slice(&length.to_be_bytes());
                    output.extend_from_slice(timezone.as_bytes());
                }
                None => output.push(0),
            }
        }
        DataType::Decimal128(precision, scale)
            if *precision != 0
                && *precision <= DECIMAL128_MAX_PRECISION
                && *scale <= DECIMAL128_MAX_SCALE
                && (*scale <= 0 || (*scale as u8) <= *precision) =>
        {
            output.extend_from_slice(&[12, *precision, *scale as u8]);
        }
        _ => {
            return Err(contract_error(
                "runtime filter membership has unsupported type",
            ));
        }
    }
    Ok(())
}

fn ordered_key_type_supported(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Boolean
            | DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::Utf8
            | DataType::Date32
            | DataType::Timestamp(_, _)
            | DataType::Decimal128(_, _)
    ) || matches!(data_type, DataType::FixedSizeBinary(width) if *width == LARGEINT_BYTE_WIDTH)
}

fn contract_error(message: impl Into<String>) -> DistributedQueryError {
    DistributedQueryError::new(DistributedQueryErrorKind::ContractViolation, message)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query_execution::preparation::runtime_filter_view::RuntimeFilterOrderKeyFacts;

    fn assert_complete_domain(
        encoded: &EncodedRuntimeFilterDomain,
        expected: &[novarocks_physical_plan::ValueType],
    ) {
        use novarocks_plan_codec::execution_type_table::decode_execution_type_table_observed;
        use novarocks_type_contract::ValueTypeStorageLifetime;
        let control = novarocks_sql::compiler::SqlCompileControl::unbounded();
        let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Decode).unwrap();
        let table = decode_execution_type_table_observed(
            &encoded.execution_types,
            ExecutionTypeTableLimits::for_validated_root_count(expected.len()).unwrap(),
            ValueTypeStorageLifetime::pure(),
            &mut |_| {
                Ok::<
                    _,
                    novarocks_plan_codec::host_projection_v2::AdmissionRefusal<
                        std::convert::Infallible,
                    >,
                >(())
            },
            &mut work,
        )
        .expect("the standalone RF table is complete");
        let domain = encoded
            .contract
            .logical_domain
            .as_ref()
            .expect("independent typed owner");
        assert_eq!(
            domain.execution_types.as_ref(),
            Some(&encoded.execution_types)
        );
        assert_eq!(domain.value_type.as_ref(), Some(&encoded.value_type));
        for (index, expected) in expected.iter().enumerate() {
            let actual = table
                .get_observed(index as u32, &mut work)
                .unwrap()
                .unwrap();
            assert!(
                actual
                    .exactly_equals_checked_observed(
                        expected,
                        || Ok::<_, std::convert::Infallible>(())
                    )
                    .unwrap()
            );
        }
        work.flush().unwrap();
    }

    fn digest(hex: &str) -> [u8; 32] {
        hex::decode(hex)
            .expect("fixture is hexadecimal")
            .try_into()
            .expect("SHA-256 fixture has 32 bytes")
    }

    #[test]
    fn membership_schema_encodes_typed_null_semantics_and_retains_value_type() {
        for (null_semantics, expected) in [
            (
                RuntimeFilterNullSemantics::NeverMatches,
                plan::RuntimeFilterMembershipNullSemantics::NeverMatches,
            ),
            (
                RuntimeFilterNullSemantics::NullSafeEqual,
                plan::RuntimeFilterMembershipNullSemantics::NullSafeEqual,
            ),
        ] {
            let source = novarocks_physical_plan::ValueType::new(DataType::Int32, true);
            let encoded = encode_logical_domain(RuntimeFilterLogicalDomainFacts::Membership {
                value_type: source.clone(),
                null_semantics,
            })
            .expect("membership facts are encodable");
            assert_complete_domain(&encoded, std::slice::from_ref(&source));
            assert_eq!(encoded.value_type().value_binding_id, Some(0));
            let domain = encoded
                .contract
                .logical_domain
                .as_ref()
                .expect("independent typed owner");
            assert_eq!(domain.value_type.as_ref(), Some(&encoded.value_type()));
            assert_eq!(
                domain.execution_types.as_ref(),
                Some(&encoded.execution_types)
            );
            let binding = &encoded.execution_types.value_bindings[0];
            assert_eq!(binding.id, Some(0));
            let logical = &encoded.execution_types.logical_types[0];
            assert!(logical.nullable);
            let Some(plan::runtime_filter_contract::Kind::Membership(contract)) =
                encoded.contract().kind
            else {
                panic!("membership contract")
            };
            assert_eq!(contract.null_semantics, i32::from(expected));
            let execution_null_semantics = match null_semantics {
                RuntimeFilterNullSemantics::NeverMatches => {
                    ExecutionRuntimeFilterNullSemantics::NeverMatches
                }
                RuntimeFilterNullSemantics::NullSafeEqual => {
                    ExecutionRuntimeFilterNullSemantics::NullSafeEqual
                }
            };
            assert_eq!(
                encoded.membership_schema_digest(),
                Some(
                    RuntimeFilterMembershipSchema::new(&DataType::Int32, execution_null_semantics,)
                        .expect("execution schema is valid")
                        .digest()
                )
            );
        }
    }

    #[test]
    fn membership_encodes_the_complete_zoned_timestamp_declaration() {
        use novarocks_type_contract::{
            TemporalPrecision, TemporalType, ValueKind, ValueLayout, ValueTypeRef,
            ValueTypeStorageLifetime,
        };

        let declaration = TemporalType::TimestampWithTimeZone(TemporalPrecision::PICOSECONDS);
        let carrier = declaration.carrier();
        let storage = ValueTypeStorageLifetime::pure();
        let source = novarocks_physical_plan::ValueType::try_from_value(
            ValueTypeRef::try_new(ValueKind::Temporal(declaration), true, storage.clone()).unwrap(),
            ValueLayout::try_new(&carrier, storage).unwrap(),
        )
        .unwrap();
        let encoded = encode_logical_domain(RuntimeFilterLogicalDomainFacts::Membership {
            value_type: source.clone(),
            null_semantics: RuntimeFilterNullSemantics::NeverMatches,
        })
        .unwrap();
        assert_complete_domain(&encoded, &[source]);
        assert_eq!(
            encoded.membership_schema_digest(),
            Some(
                RuntimeFilterMembershipSchema::new(
                    &carrier,
                    ExecutionRuntimeFilterNullSemantics::NeverMatches,
                )
                .unwrap()
                .digest()
            )
        );
        assert!(
            encode_logical_domain(RuntimeFilterLogicalDomainFacts::Membership {
                value_type: novarocks_physical_plan::ValueType::new(carrier, true),
                null_semantics: RuntimeFilterNullSemantics::NeverMatches,
            })
            .is_err()
        );
    }

    #[test]
    fn ordered_and_topk_keep_the_v1_digest_fixtures() {
        let comparator = digest("9e2c6e473c04c9113cb7bfe6c5a5d961fc086bc11e0b404764e27679fabc8afe");
        let source = novarocks_physical_plan::ValueType::new(DataType::Int64, true);
        let encoded = encode_logical_domain(RuntimeFilterLogicalDomainFacts::Ordered {
            keys: vec![RuntimeFilterOrderKeyFacts {
                value_type: source.clone(),
                direction: RuntimeFilterSortDirection::Ascending,
                null_order: RuntimeFilterNullOrder::First,
            }],
            inclusive: true,
            comparator_digest: comparator,
        })
        .expect("ordered facts are encodable");
        assert_complete_domain(&encoded, std::slice::from_ref(&source));
        let Some(plan::runtime_filter_contract::Kind::Ordered(contract)) =
            encoded.contract.kind.as_ref()
        else {
            panic!("ordered contract")
        };
        assert_eq!(contract.comparator_digest, comparator);
        assert_eq!(
            contract.order_contract_digest,
            digest("60347fb354b3d49b2aa393aaac71ba98f7d97bfd977e640a89d9e2d3ef0132b8").to_vec()
        );
        let reduction = encoded
            .encode_reduction(RuntimeFilterReductionFacts::MergeTopKSummary { k: 7 })
            .expect("TopK facts are encodable");
        let Some(plan::runtime_filter_reduction_contract::Kind::MergeTopkSummary(topk)) =
            reduction.kind
        else {
            panic!("TopK reduction")
        };
        assert_eq!(
            topk.contract_digest,
            digest("65b54aca8869cb0176e258f8a7bcf6dbf4090a9238f1a5f6c23b1bf115e0382a").to_vec()
        );
    }

    #[test]
    fn standalone_domain_preserves_same_carrier_declarations_and_root_null_guarantees() {
        use novarocks_type_contract::{FunctionValueType, ValueLogicalType};
        for (carrier, logical) in [
            (DataType::Int32, ValueLogicalType::Physical),
            (DataType::Utf8, ValueLogicalType::Physical),
            (DataType::Utf8, ValueLogicalType::Json),
            (DataType::FixedSizeBinary(16), ValueLogicalType::LargeInt),
            (DataType::FixedSizeBinary(16), ValueLogicalType::Uuid),
        ] {
            for nullable in [false, true] {
                let source =
                    FunctionValueType::try_with_logical_type(carrier.clone(), nullable, logical)
                        .expect("explicit complete same-carrier declaration");
                let encoded = encode_logical_domain(RuntimeFilterLogicalDomainFacts::Membership {
                    value_type: source.clone(),
                    null_semantics: RuntimeFilterNullSemantics::NeverMatches,
                })
                .expect("the original scalar RF carrier is supported");
                assert_complete_domain(&encoded, std::slice::from_ref(&source));
            }
        }
    }

    #[test]
    fn invalid_semantic_contracts_fail_before_submission() {
        let Err(err) = encode_logical_domain(RuntimeFilterLogicalDomainFacts::Ordered {
            keys: Vec::new(),
            inclusive: true,
            comparator_digest: [0; 32],
        }) else {
            panic!("empty ordered contract must fail");
        };
        assert_eq!(err.kind(), DistributedQueryErrorKind::ContractViolation);
        let opaque = novarocks_type_contract::FunctionValueType::try_with_logical_type(
            DataType::Binary,
            true,
            novarocks_type_contract::ValueLogicalType::Hll,
        )
        .expect("complete original opaque declaration");
        let error = encode_logical_domain(RuntimeFilterLogicalDomainFacts::Membership {
            value_type: opaque,
            null_semantics: RuntimeFilterNullSemantics::NeverMatches,
        })
        .err()
        .expect("the original RF Binary capability remains unsupported");
        assert_eq!(error.kind(), DistributedQueryErrorKind::ContractViolation);
        assert!(
            error
                .to_string()
                .contains("runtime filter native type does not support Binary")
        );

        let membership = encode_logical_domain(RuntimeFilterLogicalDomainFacts::Membership {
            value_type: novarocks_physical_plan::ValueType::new(DataType::Int32, true),
            null_semantics: RuntimeFilterNullSemantics::NeverMatches,
        })
        .expect("membership facts are encodable");
        let err = membership
            .encode_reduction(RuntimeFilterReductionFacts::MergeTopKSummary { k: 1 })
            .expect_err("TopK requires ordered facts");
        assert_eq!(err.kind(), DistributedQueryErrorKind::ContractViolation);
    }
}
