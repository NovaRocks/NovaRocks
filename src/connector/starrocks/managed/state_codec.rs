//! Per-kind VARBINARY state codec for IVM detail-state aggregates.
//!
//! All non-empty states begin with `STATE_VERSION_V1 = 0x01`. Empty state
//! is a zero-length byte slice (no version byte) and is treated as `is_empty`
//! by every kind.
//!
//! Layout by kind: see docs/superpowers/specs/2026-05-26-ivm-varbinary-state-and-distinct-count-aggregates-design.md §3.

pub(crate) const STATE_VERSION_V1: u8 = 0x01;

/// Returns `true` iff `bytes` is the empty state (zero-length).
#[inline]
pub(crate) fn is_empty_state(bytes: &[u8]) -> bool {
    bytes.is_empty()
}

pub(crate) fn write_uleb128(out: &mut Vec<u8>, mut value: u64) {
    loop {
        let byte = (value & 0x7F) as u8;
        value >>= 7;
        if value == 0 {
            out.push(byte);
            return;
        }
        out.push(byte | 0x80);
    }
}

/// Reads one canonical ULEB128 value emitted by `write_uleb128`.
///
/// Bytes already inspected are consumed even on error.
pub(crate) fn read_uleb128(cursor: &mut &[u8]) -> Result<u64, String> {
    let original = *cursor;
    let mut result: u64 = 0;
    let mut shift = 0u32;
    loop {
        let (&byte, rest) = cursor
            .split_first()
            .ok_or_else(|| "state_codec: ULEB128 truncated".to_string())?;
        *cursor = rest;
        let payload = (byte & 0x7F) as u64;
        if shift >= 64 || payload > (u64::MAX >> shift) {
            return Err("state_codec: ULEB128 overflow".to_string());
        }
        result |= payload << shift;
        if byte & 0x80 == 0 {
            let consumed_len = original.len() - cursor.len();
            let consumed = &original[..consumed_len];
            let mut canonical = Vec::new();
            write_uleb128(&mut canonical, result);
            if consumed != canonical {
                return Err("state_codec: ULEB128 non-canonical".to_string());
            }
            return Ok(result);
        }
        shift += 7;
        if shift >= 64 {
            return Err("state_codec: ULEB128 too long".to_string());
        }
    }
}

pub(crate) fn write_sleb128(out: &mut Vec<u8>, mut value: i64) {
    loop {
        let byte = (value as u8) & 0x7F;
        let high_bit_set = byte & 0x40 != 0;
        value >>= 7;
        let done = (value == 0 && !high_bit_set) || (value == -1 && high_bit_set);
        if done {
            out.push(byte);
            return;
        }
        out.push(byte | 0x80);
    }
}

/// Reads one canonical SLEB128 value emitted by `write_sleb128`.
///
/// Bytes already inspected are consumed even on error.
pub(crate) fn read_sleb128(cursor: &mut &[u8]) -> Result<i64, String> {
    let original = *cursor;
    let mut result: i64 = 0;
    let mut shift = 0u32;
    loop {
        let (&byte, rest) = cursor
            .split_first()
            .ok_or_else(|| "state_codec: SLEB128 truncated".to_string())?;
        *cursor = rest;
        let payload = byte & 0x7F;
        if shift == 63 && byte & 0x80 == 0 && payload != 0x00 && payload != 0x7F {
            return Err("state_codec: SLEB128 overflow".to_string());
        }
        if shift >= 64 {
            return Err("state_codec: SLEB128 too long".to_string());
        }
        result |= (payload as i64) << shift;
        shift += 7;
        if byte & 0x80 == 0 {
            // Sign-extend the high bit of the last group.
            if shift < 64 && byte & 0x40 != 0 {
                result |= -1i64 << shift;
            }
            let consumed_len = original.len() - cursor.len();
            let consumed = &original[..consumed_len];
            let mut canonical = Vec::new();
            write_sleb128(&mut canonical, result);
            if consumed != canonical {
                return Err("state_codec: SLEB128 non-canonical".to_string());
            }
            return Ok(result);
        }
        if shift >= 64 {
            return Err("state_codec: SLEB128 too long".to_string());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn uleb128_roundtrips() {
        for &v in &[0u64, 1, 127, 128, 16_383, 16_384, u64::MAX] {
            let mut buf = Vec::new();
            write_uleb128(&mut buf, v);
            let mut cursor = &buf[..];
            assert_eq!(read_uleb128(&mut cursor).unwrap(), v, "value {v}");
            assert!(cursor.is_empty(), "all bytes consumed for {v}");
        }
    }

    #[test]
    fn sleb128_roundtrips() {
        for &v in &[0i64, 1, -1, 63, -64, 64, -65, i64::MAX, i64::MIN] {
            let mut buf = Vec::new();
            write_sleb128(&mut buf, v);
            let mut cursor = &buf[..];
            assert_eq!(read_sleb128(&mut cursor).unwrap(), v, "value {v}");
            assert!(cursor.is_empty(), "all bytes consumed for {v}");
        }
    }

    #[test]
    fn uleb128_short_buffer_errors() {
        let mut cursor: &[u8] = &[0x80]; // continuation bit set, no follow-up
        assert!(read_uleb128(&mut cursor).is_err());
    }

    #[test]
    fn uleb128_malformed_inputs_error() {
        let mut overflow: &[u8] = &[
            0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x02,
        ];
        assert!(read_uleb128(&mut overflow).is_err());

        let mut too_long: &[u8] = &[
            0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x00,
        ];
        assert!(read_uleb128(&mut too_long).is_err());

        let mut truncated: &[u8] = &[0x81, 0x80];
        assert!(read_uleb128(&mut truncated).is_err());
    }

    #[test]
    fn uleb128_rejects_non_canonical_encodings() {
        for bytes in [&[0x80, 0x00][..], &[0x81, 0x00][..]] {
            let mut cursor = bytes;
            assert!(read_uleb128(&mut cursor).is_err());
        }
    }

    #[test]
    fn sleb128_malformed_inputs_error() {
        let mut positive_overflow: &[u8] = &[
            0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x01,
        ];
        assert!(read_sleb128(&mut positive_overflow).is_err());

        let mut negative_underflow: &[u8] = &[
            0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x01,
        ];
        assert!(read_sleb128(&mut negative_underflow).is_err());

        let mut too_long: &[u8] = &[
            0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x00,
        ];
        assert!(read_sleb128(&mut too_long).is_err());

        let mut truncated: &[u8] = &[0x80, 0x80];
        assert!(read_sleb128(&mut truncated).is_err());
    }

    #[test]
    fn sleb128_rejects_non_canonical_encodings() {
        for bytes in [&[0x80, 0x00][..], &[0xff, 0x7f][..]] {
            let mut cursor = bytes;
            assert!(read_sleb128(&mut cursor).is_err());
        }
    }
}
