//! Frontend-owned runtime-filter semantic encoding.
//!
//! Core exposes sealed SQL facts only.  This module is the sole owner of the
//! native runtime-filter canonical schema and digest construction used by both
//! fragment bindings and participant deployment/install contributions.

use crate::query_execution::contract::{DistributedQueryError, DistributedQueryErrorKind};
use crate::query_execution::{
    RuntimeFilterLogicalDomainFacts, RuntimeFilterNullOrder, RuntimeFilterNullSemantics,
    RuntimeFilterReductionFacts, RuntimeFilterSortDirection,
};
use arrow::datatypes::{DECIMAL128_MAX_PRECISION, DECIMAL128_MAX_SCALE, DataType, TimeUnit};
use novarocks_protocol::{common, plan};
use novarocks_types::largeint::LARGEINT_BYTE_WIDTH;
use sha2::{Digest, Sha256};

const MEMBERSHIP_SCHEMA_DOMAIN: &[u8] = b"novarocks.runtime-filter.artifact-schema";
const ORDER_CONTRACT_DOMAIN: &[u8] = b"novarocks.runtime-filter.order-contract";
const TOPK_CONTRACT_DOMAIN: &[u8] = b"novarocks.runtime-filter.top-k-summary-contract";
const CONTRACT_VERSION: u16 = 1;

pub(crate) struct EncodedRuntimeFilterDomain {
    value_type: common::TypeDesc,
    contract: plan::RuntimeFilterContract,
    order_contract_digest: Option<[u8; 32]>,
}

impl EncodedRuntimeFilterDomain {
    pub(crate) fn value_type(&self) -> common::TypeDesc {
        self.value_type.clone()
    }

    pub(crate) fn contract(&self) -> plan::RuntimeFilterContract {
        self.contract.clone()
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
            let value_type_wire = encode_type(&value_type)?;
            let mut canonical = Vec::with_capacity(48);
            canonical.extend_from_slice(MEMBERSHIP_SCHEMA_DOMAIN);
            canonical.push(1);
            encode_membership_schema_type(&value_type, &mut canonical)?;
            canonical.push(match null_semantics {
                RuntimeFilterNullSemantics::NeverMatches => 1,
                RuntimeFilterNullSemantics::NullSafeEqual => 2,
            });
            let schema_digest = Sha256::digest(&canonical);
            Ok(EncodedRuntimeFilterDomain {
                value_type: value_type_wire,
                contract: plan::RuntimeFilterContract {
                    kind: Some(Kind::Membership(plan::RuntimeFilterMembershipContract {
                        canonical_schema: canonical,
                        schema_digest: schema_digest.to_vec(),
                    })),
                },
                order_contract_digest: None,
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
            let mut wire_keys = Vec::with_capacity(keys.len());
            for key in keys {
                if !ordered_key_type_supported(&key.data_type) {
                    return Err(contract_error(
                        "runtime filter ordered contract has unsupported key type",
                    ));
                }
                encode_membership_schema_type(&key.data_type, &mut canonical_keys)?;
                canonical_keys.push(match key.direction {
                    RuntimeFilterSortDirection::Ascending => 1,
                    RuntimeFilterSortDirection::Descending => 2,
                });
                canonical_keys.push(match key.null_order {
                    RuntimeFilterNullOrder::First => 1,
                    RuntimeFilterNullOrder::Last => 2,
                });
                wire_keys.push(plan::RuntimeFilterOrderKey {
                    r#type: Some(encode_type(&key.data_type)?),
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
                .and_then(|key| key.r#type.clone())
                .expect("ordered keys are nonempty and encoded");
            Ok(EncodedRuntimeFilterDomain {
                value_type,
                contract: plan::RuntimeFilterContract {
                    kind: Some(Kind::Ordered(plan::RuntimeFilterOrderedContract {
                        keys: wire_keys,
                        comparator_digest: comparator_digest.to_vec(),
                        order_contract_digest: order_contract_digest.to_vec(),
                    })),
                },
                order_contract_digest: Some(order_contract_digest),
            })
        }
    }
}

pub(crate) fn encode_type(data_type: &DataType) -> Result<common::TypeDesc, DistributedQueryError> {
    use common::{PrimitiveType, type_desc::Kind};

    let (primitive, precision, scale, time_unit) = match data_type {
        DataType::Boolean => (PrimitiveType::Boolean, None, None, None),
        DataType::Int8 => (PrimitiveType::Tinyint, None, None, None),
        DataType::Int16 => (PrimitiveType::Smallint, None, None, None),
        DataType::Int32 => (PrimitiveType::Int, None, None, None),
        DataType::Int64 => (PrimitiveType::Bigint, None, None, None),
        DataType::Float32 => (PrimitiveType::Float, None, None, None),
        DataType::Float64 => (PrimitiveType::Double, None, None, None),
        DataType::FixedSizeBinary(width) if *width == LARGEINT_BYTE_WIDTH => {
            (PrimitiveType::Largeint, None, None, None)
        }
        DataType::Utf8 => (PrimitiveType::Varchar, None, None, None),
        DataType::Date32 => (PrimitiveType::Date, None, None, None),
        DataType::Timestamp(TimeUnit::Microsecond, _) => {
            (PrimitiveType::Datetime, None, None, None)
        }
        DataType::Timestamp(TimeUnit::Nanosecond, _) => {
            (PrimitiveType::Datetime, None, None, Some(3))
        }
        DataType::Timestamp(unit, _) => {
            return Err(contract_error(format!(
                "runtime filter native type does not support timestamp unit {unit:?}"
            )));
        }
        DataType::Decimal128(precision, scale)
            if *precision != 0
                && *precision <= DECIMAL128_MAX_PRECISION
                && *scale <= DECIMAL128_MAX_SCALE
                && (*scale <= 0 || (*scale as u8) <= *precision) =>
        {
            (
                PrimitiveType::Decimal128,
                Some(i32::from(*precision)),
                Some(i32::from(*scale)),
                None,
            )
        }
        _ => {
            return Err(contract_error(format!(
                "runtime filter native type does not support {data_type:?}"
            )));
        }
    };
    Ok(common::TypeDesc {
        kind: Some(Kind::Scalar(common::ScalarType {
            r#type: primitive as i32,
            len: None,
            precision,
            scale,
            time_unit,
        })),
    })
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
    use crate::query_execution::RuntimeFilterOrderKeyFacts;

    fn digest(hex: &str) -> [u8; 32] {
        hex::decode(hex)
            .expect("fixture is hexadecimal")
            .try_into()
            .expect("SHA-256 fixture has 32 bytes")
    }

    #[test]
    fn membership_schema_keeps_the_v1_fixture_bytes_and_digest() {
        let encoded = encode_logical_domain(RuntimeFilterLogicalDomainFacts::Membership {
            value_type: DataType::Int32,
            null_semantics: RuntimeFilterNullSemantics::NeverMatches,
        })
        .expect("membership facts are encodable");
        let Some(plan::runtime_filter_contract::Kind::Membership(contract)) = encoded.contract.kind
        else {
            panic!("membership contract")
        };
        assert_eq!(
            contract.canonical_schema,
            b"novarocks.runtime-filter.artifact-schema\x01\x04\x01"
        );
        assert_eq!(
            contract.schema_digest,
            digest("24641ad04e80af8aacdebd06f291d49d2610497b187a02986fbb1dd84cff5d35").to_vec()
        );
    }

    #[test]
    fn ordered_and_topk_keep_the_v1_digest_fixtures() {
        let comparator = digest("9e2c6e473c04c9113cb7bfe6c5a5d961fc086bc11e0b404764e27679fabc8afe");
        let encoded = encode_logical_domain(RuntimeFilterLogicalDomainFacts::Ordered {
            keys: vec![RuntimeFilterOrderKeyFacts {
                data_type: DataType::Int64,
                direction: RuntimeFilterSortDirection::Ascending,
                null_order: RuntimeFilterNullOrder::First,
            }],
            inclusive: true,
            comparator_digest: comparator,
        })
        .expect("ordered facts are encodable");
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
    fn invalid_semantic_contracts_fail_before_submission() {
        let Err(err) = encode_logical_domain(RuntimeFilterLogicalDomainFacts::Ordered {
            keys: Vec::new(),
            inclusive: true,
            comparator_digest: [0; 32],
        }) else {
            panic!("empty ordered contract must fail");
        };
        assert_eq!(err.kind(), DistributedQueryErrorKind::ContractViolation);

        let membership = encode_logical_domain(RuntimeFilterLogicalDomainFacts::Membership {
            value_type: DataType::Int32,
            null_semantics: RuntimeFilterNullSemantics::NeverMatches,
        })
        .expect("membership facts are encodable");
        let err = membership
            .encode_reduction(RuntimeFilterReductionFacts::MergeTopKSummary { k: 1 })
            .expect_err("TopK requires ordered facts");
        assert_eq!(err.kind(), DistributedQueryErrorKind::ContractViolation);
    }
}
