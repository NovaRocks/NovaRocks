//! Ordered-bound materialization stays separate from membership materialization.

use arrow::datatypes::{DataType, TimeUnit};
use novarocks_execution::runtime_filter::{
    LogicalVersion,
    contribution::{
        OrderedScalar, OrderedTuple, RuntimeOrderContract, RuntimeOrderNullOrder,
        RuntimeOrderSortDirection,
    },
    temporal_order_key_type_bytes,
};

use crate::runtime_filter::artifact as worker_artifact;
use crate::runtime_filter::codec::leaf::ArtifactCodecError;
use worker_artifact::ArtifactKind;

const MAGIC: &[u8; 4] = b"NRRG";
const VERSION: u16 = 1;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RangeResidentLayout {
    pub key_count: usize,
    pub tuple_arity: usize,
    pub utf8_bytes: usize,
    pub timezone_bytes: usize,
}

pub fn decode_range_leaf(
    encoded: &[u8],
    expected: &RuntimeOrderContract,
    expected_version: LogicalVersion,
    max_artifact_bytes: usize,
) -> Result<
    (
        std::sync::Arc<worker_artifact::PhysicalArtifact>,
        RangeResidentLayout,
    ),
    ArtifactCodecError,
> {
    if encoded.len() > max_artifact_bytes {
        return Err(ArtifactCodecError::EncodedSizeExceeded);
    }
    let mut r = Reader::new(encoded);
    if r.take(4)? != MAGIC || r.u16()? != VERSION || r.u8()? != ArtifactKind::Range.tag() {
        return Err(ArtifactCodecError::NonCanonicalPayload);
    }
    let digest = r.array::<32>()?;
    let version =
        LogicalVersion::try_new(r.u64()?).map_err(|_| ArtifactCodecError::InvalidLogicalVersion)?;
    if digest != expected.digest() || version != expected_version {
        return Err(ArtifactCodecError::ContractViolation);
    }
    let contract_len = usize::try_from(r.u32()?).map_err(|_| ArtifactCodecError::LengthOverflow)?;
    let contract = r.take(contract_len)?;
    let tuple_len = usize::try_from(r.u64()?).map_err(|_| ArtifactCodecError::LengthOverflow)?;
    let tuple = r.take(tuple_len)?;
    if !r.empty() {
        return Err(ArtifactCodecError::TrailingBytes);
    }
    let (keys, comparator_digest, timezone_bytes) = decode_contract(contract, expected)?;
    if comparator_digest != expected.comparator_digest()
        || keys.len() != expected.keys().len()
        || !keys
            .iter()
            .zip(expected.keys())
            .all(|(actual, expected)| actual.same_physical_key(expected))
    {
        return Err(ArtifactCodecError::ContractViolation);
    }
    let bound = decode_tuple(tuple, expected)?;
    expected
        .validate_tuple(&bound)
        .map_err(|_| ArtifactCodecError::NonCanonicalPayload)?;
    if encode_range_leaf(expected, &bound, version)? != encoded {
        return Err(ArtifactCodecError::NonCanonicalPayload);
    }
    let utf8_bytes = bound
        .values()
        .iter()
        .filter_map(|value| match value {
            Some(OrderedScalar::Utf8(value)) => Some(value.len()),
            _ => None,
        })
        .try_fold(0usize, |sum, value| {
            sum.checked_add(value)
                .ok_or(ArtifactCodecError::LengthOverflow)
        })?;
    let layout = RangeResidentLayout {
        key_count: keys.len(),
        tuple_arity: bound.values().len(),
        utf8_bytes,
        timezone_bytes,
    };
    let physical = worker_artifact::PhysicalArtifact::new(
        ArtifactKind::Range,
        worker_artifact::ArtifactSchemaDigest::new(digest),
        version,
        bound.values().iter().any(Option::is_none),
        std::sync::Arc::from(encoded),
        None,
    )
    .with_range_data(worker_artifact::RangeResidentData {
        contract: std::sync::Arc::new(expected.clone()),
        bound,
    });
    Ok((std::sync::Arc::new(physical), layout))
}

fn decode_contract(
    bytes: &[u8],
    expected: &RuntimeOrderContract,
) -> Result<
    (
        Vec<novarocks_execution::runtime_filter::contribution::RuntimeOrderKey>,
        [u8; 32],
        usize,
    ),
    ArtifactCodecError,
> {
    let mut r = Reader::new(bytes);
    let count = usize::try_from(r.u32()?).map_err(|_| ArtifactCodecError::LengthOverflow)?;
    if count == 0 {
        return Err(ArtifactCodecError::NonCanonicalPayload);
    }
    if count != expected.keys().len() {
        return Err(ArtifactCodecError::ContractViolation);
    }
    let mut keys = Vec::with_capacity(count);
    let mut timezone_bytes = 0;
    for key in expected.keys() {
        let (data_type, bytes) = if let Some(declaration) = key.temporal_type() {
            if r.array::<5>()? != temporal_order_key_type_bytes(declaration) {
                return Err(ArtifactCodecError::ContractViolation);
            }
            (key.data_type().clone(), 0)
        } else {
            decode_type(&mut r)?
        };
        timezone_bytes += bytes;
        let direction = match r.u8()? {
            1 => RuntimeOrderSortDirection::Ascending,
            2 => RuntimeOrderSortDirection::Descending,
            _ => return Err(ArtifactCodecError::NonCanonicalPayload),
        };
        let null_order = match r.u8()? {
            1 => RuntimeOrderNullOrder::First,
            2 => RuntimeOrderNullOrder::Last,
            _ => return Err(ArtifactCodecError::NonCanonicalPayload),
        };
        keys.push(
            novarocks_execution::runtime_filter::contribution::RuntimeOrderKey::with_order(
                data_type, direction, null_order,
            ),
        );
    }
    let digest = r.array()?;
    if !r.empty() {
        return Err(ArtifactCodecError::TrailingBytes);
    }
    Ok((keys, digest, timezone_bytes))
}
fn decode_type(r: &mut Reader<'_>) -> Result<(DataType, usize), ArtifactCodecError> {
    Ok(match r.u8()? {
        1 => (DataType::Boolean, 0),
        2 => (DataType::Int8, 0),
        3 => (DataType::Int16, 0),
        4 => (DataType::Int32, 0),
        5 => (DataType::Int64, 0),
        6 => (DataType::FixedSizeBinary(16), 0),
        9 => (DataType::Utf8, 0),
        10 => (DataType::Date32, 0),
        11 => {
            let unit = match r.u8()? {
                1 => TimeUnit::Second,
                2 => TimeUnit::Millisecond,
                3 => TimeUnit::Microsecond,
                4 => TimeUnit::Nanosecond,
                _ => return Err(ArtifactCodecError::NonCanonicalPayload),
            };
            match r.u8()? {
                0 => (DataType::Timestamp(unit, None), 0),
                1 => {
                    let len = usize::try_from(r.u32()?)
                        .map_err(|_| ArtifactCodecError::LengthOverflow)?;
                    let timezone = std::str::from_utf8(r.take(len)?)
                        .map_err(|_| ArtifactCodecError::NonCanonicalPayload)?;
                    (DataType::Timestamp(unit, Some(timezone.into())), len)
                }
                _ => return Err(ArtifactCodecError::NonCanonicalPayload),
            }
        }
        12 => (DataType::Decimal128(r.u8()?, r.u8()? as i8), 0),
        _ => return Err(ArtifactCodecError::NonCanonicalPayload),
    })
}
fn decode_tuple(
    bytes: &[u8],
    contract: &RuntimeOrderContract,
) -> Result<OrderedTuple, ArtifactCodecError> {
    let mut r = Reader::new(bytes);
    let count = usize::try_from(r.u32()?).map_err(|_| ArtifactCodecError::LengthOverflow)?;
    if count != contract.keys().len() {
        return Err(ArtifactCodecError::NonCanonicalPayload);
    }
    let mut values = Vec::with_capacity(count);
    for key in contract.keys() {
        values.push(match r.u8()? {
            0 => None,
            1 => Some(if let Some(declaration) = key.temporal_type() {
                let value = r.array()?;
                novarocks_type_contract::TemporalRelationKey::from_order_bytes(declaration, value)
                    .map_err(|_| ArtifactCodecError::NonCanonicalPayload)?;
                OrderedScalar::Temporal(value)
            } else {
                decode_scalar(&mut r, key.data_type())?
            }),
            _ => return Err(ArtifactCodecError::NonCanonicalPayload),
        });
    }
    if !r.empty() {
        return Err(ArtifactCodecError::TrailingBytes);
    }
    Ok(OrderedTuple::new(values))
}
fn decode_scalar(r: &mut Reader<'_>, ty: &DataType) -> Result<OrderedScalar, ArtifactCodecError> {
    Ok(match ty {
        DataType::Boolean => OrderedScalar::Boolean(match r.u8()? {
            0 => false,
            1 => true,
            _ => return Err(ArtifactCodecError::NonCanonicalPayload),
        }),
        DataType::Int8 => OrderedScalar::Int8(r.u8()? as i8),
        DataType::Int16 => OrderedScalar::Int16(i16::from_be_bytes(r.array()?)),
        DataType::Int32 => OrderedScalar::Int32(i32::from_be_bytes(r.array()?)),
        DataType::Int64 => OrderedScalar::Int64(i64::from_be_bytes(r.array()?)),
        DataType::FixedSizeBinary(16) => OrderedScalar::LargeInt(i128::from_be_bytes(r.array()?)),
        DataType::Utf8 => {
            let len = usize::try_from(r.u64()?).map_err(|_| ArtifactCodecError::LengthOverflow)?;
            OrderedScalar::Utf8(
                std::str::from_utf8(r.take(len)?)
                    .map_err(|_| ArtifactCodecError::NonCanonicalPayload)?
                    .into(),
            )
        }
        DataType::Date32 => OrderedScalar::Date32(i32::from_be_bytes(r.array()?)),
        DataType::Timestamp(_, _) => OrderedScalar::Timestamp(i64::from_be_bytes(r.array()?)),
        DataType::Decimal128(_, _) => OrderedScalar::Decimal128(i128::from_be_bytes(r.array()?)),
        _ => return Err(ArtifactCodecError::ContractViolation),
    })
}
struct Reader<'a> {
    bytes: &'a [u8],
    offset: usize,
}
impl<'a> Reader<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, offset: 0 }
    }
    fn empty(&self) -> bool {
        self.offset == self.bytes.len()
    }
    fn take(&mut self, count: usize) -> Result<&'a [u8], ArtifactCodecError> {
        let end = self
            .offset
            .checked_add(count)
            .ok_or(ArtifactCodecError::LengthOverflow)?;
        let value = self
            .bytes
            .get(self.offset..end)
            .ok_or(ArtifactCodecError::Truncated)?;
        self.offset = end;
        Ok(value)
    }
    fn u8(&mut self) -> Result<u8, ArtifactCodecError> {
        Ok(self.take(1)?[0])
    }
    fn u16(&mut self) -> Result<u16, ArtifactCodecError> {
        Ok(u16::from_be_bytes(self.array()?))
    }
    fn u32(&mut self) -> Result<u32, ArtifactCodecError> {
        Ok(u32::from_be_bytes(self.array()?))
    }
    fn u64(&mut self) -> Result<u64, ArtifactCodecError> {
        Ok(u64::from_be_bytes(self.array()?))
    }
    fn array<const N: usize>(&mut self) -> Result<[u8; N], ArtifactCodecError> {
        self.take(N)?
            .try_into()
            .map_err(|_| ArtifactCodecError::Truncated)
    }
}

pub fn encode_range_leaf(
    contract: &RuntimeOrderContract,
    bound: &OrderedTuple,
    version: LogicalVersion,
) -> Result<Vec<u8>, ArtifactCodecError> {
    contract
        .validate_tuple(bound)
        .map_err(|_| ArtifactCodecError::ContractViolation)?;
    let mut contract_bytes = Vec::new();
    contract_bytes.extend_from_slice(
        &(u32::try_from(contract.keys().len()).map_err(|_| ArtifactCodecError::LengthOverflow)?)
            .to_be_bytes(),
    );
    for key in contract.keys() {
        if let Some(declaration) = key.temporal_type() {
            if key.data_type() != &declaration.carrier() {
                return Err(ArtifactCodecError::ContractViolation);
            }
            contract_bytes.extend_from_slice(&temporal_order_key_type_bytes(declaration));
        } else {
            encode_type(key.data_type(), &mut contract_bytes)?;
        }
        contract_bytes.push(match key.direction() {
            RuntimeOrderSortDirection::Ascending => 1,
            RuntimeOrderSortDirection::Descending => 2,
        });
        contract_bytes.push(match key.null_order() {
            RuntimeOrderNullOrder::First => 1,
            RuntimeOrderNullOrder::Last => 2,
        });
    }
    contract_bytes.extend_from_slice(&contract.comparator_digest());
    let mut tuple_bytes = Vec::new();
    tuple_bytes.extend_from_slice(
        &(u32::try_from(bound.values().len()).map_err(|_| ArtifactCodecError::LengthOverflow)?)
            .to_be_bytes(),
    );
    for value in bound.values() {
        match value {
            None => tuple_bytes.push(0),
            Some(value) => {
                tuple_bytes.push(1);
                encode_scalar(value, &mut tuple_bytes)?;
            }
        }
    }
    let mut output =
        Vec::with_capacity(4 + 2 + 1 + 32 + 8 + 4 + contract_bytes.len() + 8 + tuple_bytes.len());
    output.extend_from_slice(MAGIC);
    output.extend_from_slice(&VERSION.to_be_bytes());
    output.push(ArtifactKind::Range.tag());
    output.extend_from_slice(&contract.digest());
    output.extend_from_slice(&version.get().to_be_bytes());
    output.extend_from_slice(
        &(u32::try_from(contract_bytes.len()).map_err(|_| ArtifactCodecError::LengthOverflow)?)
            .to_be_bytes(),
    );
    output.extend_from_slice(&contract_bytes);
    output.extend_from_slice(
        &(u64::try_from(tuple_bytes.len()).map_err(|_| ArtifactCodecError::LengthOverflow)?)
            .to_be_bytes(),
    );
    output.extend_from_slice(&tuple_bytes);
    Ok(output)
}

pub fn materialize_range(
    channel_id: u32,
    contract: &RuntimeOrderContract,
    bound: &OrderedTuple,
    version: LogicalVersion,
    profile: &worker_artifact::ConsumerArtifactProfile,
    admission: &crate::runtime_filter::materializer::MaterializationAdmission,
) -> Result<std::sync::Arc<worker_artifact::ArtifactBundle>, ArtifactCodecError> {
    if profile.order_contract_digest() != Some(contract.digest())
        || !profile.accepts(ArtifactKind::Range)
    {
        return Err(ArtifactCodecError::ContractViolation);
    }
    let bytes = encode_range_leaf(contract, bound, version)?;
    if bytes.len() > admission.max_artifact_bytes() {
        return Err(ArtifactCodecError::EncodedSizeExceeded);
    }
    let _scratch = admission
        .reserve_scratch(bytes.len())
        .map_err(|_| ArtifactCodecError::ResourceLimit)?;
    let (artifact, _) =
        decode_range_leaf(&bytes, contract, version, admission.max_artifact_bytes())?;
    super::retain_bundle(
        channel_id,
        version,
        profile,
        vec![(ArtifactKind::Range, artifact)],
        admission,
    )
    .map(std::sync::Arc::new)
    .map_err(|error| match error {
        worker_artifact::ArtifactContractError::RetentionCapacityExceeded
        | worker_artifact::ArtifactContractError::ResidentSizeOverflow
        | worker_artifact::ArtifactContractError::LengthOverflow => {
            ArtifactCodecError::ResourceLimit
        }
        _ => ArtifactCodecError::ContractViolation,
    })
}

fn encode_type(data_type: &DataType, output: &mut Vec<u8>) -> Result<(), ArtifactCodecError> {
    match data_type {
        DataType::Boolean => output.push(1),
        DataType::Int8 => output.push(2),
        DataType::Int16 => output.push(3),
        DataType::Int32 => output.push(4),
        DataType::Int64 => output.push(5),
        DataType::FixedSizeBinary(16) => output.push(6),
        DataType::Utf8 => output.push(9),
        DataType::Date32 => output.push(10),
        DataType::Timestamp(unit, timezone) => {
            output.push(11);
            output.push(match unit {
                TimeUnit::Second => 1,
                TimeUnit::Millisecond => 2,
                TimeUnit::Microsecond => 3,
                TimeUnit::Nanosecond => 4,
            });
            match timezone {
                None => output.push(0),
                Some(timezone) => {
                    output.push(1);
                    output.extend_from_slice(
                        &(u32::try_from(timezone.len())
                            .map_err(|_| ArtifactCodecError::LengthOverflow)?)
                        .to_be_bytes(),
                    );
                    output.extend_from_slice(timezone.as_bytes());
                }
            }
        }
        DataType::Decimal128(precision, scale) => {
            output.extend_from_slice(&[12, *precision, *scale as u8])
        }
        _ => return Err(ArtifactCodecError::ContractViolation),
    }
    Ok(())
}
fn encode_scalar(value: &OrderedScalar, output: &mut Vec<u8>) -> Result<(), ArtifactCodecError> {
    match value {
        OrderedScalar::Temporal(value) => output.extend_from_slice(value),
        OrderedScalar::Boolean(value) => output.push(u8::from(*value)),
        OrderedScalar::Int8(value) => output.push(*value as u8),
        OrderedScalar::Int16(value) => output.extend_from_slice(&value.to_be_bytes()),
        OrderedScalar::Int32(value) | OrderedScalar::Date32(value) => {
            output.extend_from_slice(&value.to_be_bytes())
        }
        OrderedScalar::Int64(value) | OrderedScalar::Timestamp(value) => {
            output.extend_from_slice(&value.to_be_bytes())
        }
        OrderedScalar::LargeInt(value) | OrderedScalar::Decimal128(value) => {
            output.extend_from_slice(&value.to_be_bytes())
        }
        OrderedScalar::Utf8(value) => {
            output.extend_from_slice(
                &(u64::try_from(value.len()).map_err(|_| ArtifactCodecError::LengthOverflow)?)
                    .to_be_bytes(),
            );
            output.extend_from_slice(value.as_bytes());
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::DataType;
    use novarocks_execution::runtime_filter::contribution::{
        RuntimeOrderKey, RuntimeOrderNullOrder, RuntimeOrderSortDirection,
    };
    #[test]
    fn physical_range_frame_binds_to_an_existing_complete_typed_channel() {
        let typed = RuntimeOrderKey::with_value_type(
            novarocks_execution::runtime_filter::FunctionValueType::new(DataType::Int64, true),
            RuntimeOrderSortDirection::Ascending,
            RuntimeOrderNullOrder::Last,
        );
        let contract = RuntimeOrderContract::from_frozen([typed], [3; 32], [7; 32]);
        let tuple = OrderedTuple::new([Some(OrderedScalar::Int64(9))]);
        let bytes = encode_range_leaf(&contract, &tuple, LogicalVersion::FIRST).unwrap();
        let (_, layout) =
            decode_range_leaf(&bytes, &contract, LogicalVersion::FIRST, 4096).unwrap();
        assert_eq!(layout.key_count, 1);
        assert!(contract.keys()[0].value_type().is_some());
    }

    #[test]
    fn nrrg_is_canonical_for_an_ordered_int64_bound() {
        let contract = RuntimeOrderContract::from_frozen(
            [RuntimeOrderKey::with_order(
                DataType::Int64,
                RuntimeOrderSortDirection::Ascending,
                RuntimeOrderNullOrder::Last,
            )],
            [3; 32],
            [7; 32],
        );
        let bytes = encode_range_leaf(
            &contract,
            &OrderedTuple::new([Some(OrderedScalar::Int64(9))]),
            LogicalVersion::FIRST,
        )
        .unwrap();
        assert_eq!(&bytes[..7], b"NRRG\0\x01\x04");
        assert_eq!(&bytes[7..39], &[7; 32]);
        let (_, layout) =
            decode_range_leaf(&bytes, &contract, LogicalVersion::FIRST, 4096).unwrap();
        assert_eq!(layout.key_count, 1);
        assert_eq!(layout.tuple_arity, 1);
        assert!(matches!(
            decode_range_leaf(&bytes, &contract, LogicalVersion::new(2), 4096),
            Err(ArtifactCodecError::ContractViolation)
        ));
    }

    #[test]
    fn nrrg_rejects_zero_logical_version() {
        let contract = RuntimeOrderContract::from_frozen(
            [RuntimeOrderKey::with_order(
                DataType::Int64,
                RuntimeOrderSortDirection::Ascending,
                RuntimeOrderNullOrder::Last,
            )],
            [3; 32],
            [7; 32],
        );
        let mut bytes = encode_range_leaf(
            &contract,
            &OrderedTuple::new([Some(OrderedScalar::Int64(9))]),
            LogicalVersion::FIRST,
        )
        .unwrap();
        bytes[39..47].fill(0);
        assert!(matches!(
            decode_range_leaf(&bytes, &contract, LogicalVersion::FIRST, 4096),
            Err(ArtifactCodecError::InvalidLogicalVersion)
        ));
    }
}
