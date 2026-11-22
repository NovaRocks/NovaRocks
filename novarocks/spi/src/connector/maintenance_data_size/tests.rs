// SPDX-License-Identifier: Apache-2.0

use super::*;

#[test]
fn airlift_data_size_keeps_reference_units_rounding_and_width() {
    for (text, bytes) in [
        ("0B", 0),
        ("1024B", 1024),
        ("1kB", 1024),
        ("10MB", 10_485_760),
        ("1.5MB", 1_572_864),
        (" 1.5 GB\t", 1_610_612_736),
        ("1TB", 1 << 40),
        ("1PB", 1 << 50),
        ("1EB", 1 << 60),
        ("0.5B", 1),
        ("١٢B", 12),
        ("-0B", 0),
        ("9223372036854775807B", i64::MAX as u64),
        ("8.0EB", i64::MAX as u64),
    ] {
        let parsed = ConnectorDataSize::parse(text, || Ok(()), |kind, _| kind).unwrap();
        assert_eq!(parsed.bytes(), bytes, "{text:?}");
    }
}

#[test]
fn airlift_data_size_preserves_original_failure_kinds() {
    use DataSizeParseFailure as Failure;
    for (text, expected) in [
        ("abc", Failure::Syntax),
        ("+1B", Failure::Syntax),
        ("-1B", Failure::Syntax),
        ("1e3B", Failure::Syntax),
        ("١٢kB", Failure::Syntax),
        ("𝟙B", Failure::Syntax),
        ("1KB", Failure::Unit),
        ("8EB", Failure::IntegerOverflow),
        ("9223372036854775808B", Failure::IntegerOverflow),
        ("8.000000000000002EB", Failure::DecimalOverflow),
    ] {
        assert_eq!(
            ConnectorDataSize::parse(text, || Ok(()), |kind, _| kind),
            Err(expected),
            "{text:?}",
        );
    }
}

#[test]
fn data_size_parser_preserves_the_original_control_failure() {
    let mut observed = 0;
    let error = ConnectorDataSize::parse(
        "123456789.125kB",
        || {
            observed += 1;
            if observed == 7 {
                Err("original control refusal")
            } else {
                Ok(())
            }
        },
        |_, _| panic!("an observed stop must not be replaced by a parse failure"),
    )
    .unwrap_err();
    assert_eq!(observed, 7);
    assert_eq!(error, "original control refusal");
}
