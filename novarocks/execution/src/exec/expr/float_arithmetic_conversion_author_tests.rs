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

//! Exact frozen conversion expectations for already-installed selected Cast authors.
use arrow::{
    array::{
        Array, ArrayRef, Decimal128Array, Float32Array, Float64Array, Int8Array, Int16Array,
        Int32Array, Int64Array,
    },
    datatypes::DataType,
};
use novarocks_functions::*;
use novarocks_type_contract::{
    CompileControlError, CompilePhase, DecimalOverflowPolicy, FunctionValueType, PureCompileControl,
};
use std::{sync::Arc, time::Duration};
struct Control;
impl PureCompileControl for Control {
    fn checkpoint(&self, _: CompilePhase, n: u32) -> Result<(), CompileControlError> {
        assert!(n <= 256);
        Ok(())
    }
}
impl KernelEvaluationControl for Control {
    fn checkpoint(&self, n: u32) -> Result<(), KernelFailure> {
        assert!(n <= 256);
        Ok(())
    }
    fn wait(&self, _: Duration) -> Result<(), KernelFailure> {
        panic!("numeric conversion never waits")
    }
}
fn assert_original_conversion(array: ArrayRef) {
    let expected = arrow::compute::cast(&array, &DataType::Float64).unwrap();
    let expected = expected.as_any().downcast_ref::<Float64Array>().unwrap();
    let bits = expected
        .iter()
        .map(|value| value.map(f64::to_bits))
        .collect::<Vec<_>>();
    assert_conversion_bits(array, &bits);
}
fn assert_conversion_bits(array: ArrayRef, expected: &[Option<u64>]) {
    assert_eq!(array.len(), expected.len());
    for allow in [false, true] {
        for policy in [
            DecimalOverflowPolicy::OutputNull,
            DecimalOverflowPolicy::ReportError,
        ] {
            let source = FunctionValueType::new(array.data_type().clone(), true);
            let target = FunctionValueType::new(DataType::Float64, true);
            let recipe = PreparedCastRecipe::try_new(
                CastOperation::Carrier,
                &source,
                &target,
                policy,
                allow,
                &Control,
            )
            .unwrap();
            for (row, expected) in expected.iter().copied().enumerate() {
                let actual = recipe
                    .evaluate_row(EvaluatedArgument::Column(&array), row, row, &Control)
                    .unwrap();
                if let Some(expected) = expected {
                    let CastRowResult::Float64(value) = actual else {
                        panic!("exact F64 result required: {actual:?}")
                    };
                    assert_eq!(
                        value.to_bits(),
                        expected,
                        "{:?} row {row}",
                        array.data_type()
                    );
                } else {
                    assert_eq!(actual, CastRowResult::Null);
                }
            }
        }
    }
}
#[test]
fn float_arithmetic_conversion_original_arrow_signed_and_ieee_widths() {
    let signed = [
        Some(0),
        Some(1),
        Some(-1),
        Some(i64::MIN),
        Some(i64::MAX),
        None,
    ];
    let arrays: Vec<ArrayRef> = vec![
        Arc::new(Int8Array::from(
            signed
                .iter()
                .map(|x| x.map(|x| x as i8))
                .collect::<Vec<_>>(),
        )),
        Arc::new(Int16Array::from(
            signed
                .iter()
                .map(|x| x.map(|x| x as i16))
                .collect::<Vec<_>>(),
        )),
        Arc::new(Int32Array::from(
            signed
                .iter()
                .map(|x| x.map(|x| x as i32))
                .collect::<Vec<_>>(),
        )),
        Arc::new(Int64Array::from(signed.to_vec())),
        Arc::new(Float32Array::from(vec![
            Some(0.0),
            Some(-0.0),
            Some(f32::INFINITY),
            Some(f32::NEG_INFINITY),
            Some(f32::from_bits(0x7f800001)),
            Some(f32::from_bits(0xffc12345)),
            Some(f32::MAX),
            Some(f32::MIN_POSITIVE),
            Some(f32::from_bits(1)),
            None,
        ])),
        Arc::new(Float64Array::from(vec![
            Some(0.0),
            Some(-0.0),
            Some(f64::INFINITY),
            Some(f64::NEG_INFINITY),
            Some(f64::from_bits(0x7ff0000000000001)),
            Some(f64::from_bits(0xfff8123456789abc)),
            Some(f64::MAX),
            Some(f64::MIN_POSITIVE),
            Some(f64::from_bits(1)),
            None,
        ])),
    ];
    for array in arrays {
        assert_original_conversion(array.clone());
        assert_original_conversion(array.slice(1, array.len() - 2));
        assert_original_conversion(array.slice(0, 0));
    }
}
#[test]
fn float_arithmetic_conversion_original_arrow_all_decimal128_metadata_and_raw_extremes() {
    // Raw payload precision is not silently validated or narrowed by arithmetic's original Arrow cast.
    let values = vec![
        Some(i128::MIN),
        Some(i128::MAX),
        Some(-1),
        Some(0),
        Some(1),
        None,
    ];
    for precision in 1..=38u8 {
        for scale in i8::MIN..=precision as i8 {
            let array: ArrayRef = Arc::new(
                Decimal128Array::from(values.clone())
                    .with_precision_and_scale(precision, scale)
                    .unwrap(),
            );
            let [huge, unit] = OLD_DECIMAL128_EXTREMA_BITS[(i16::from(scale) + 128) as usize];
            // Both raw extrema round to magnitude 2^127 before old division.
            let expected = [
                Some(huge | (1 << 63)),
                Some(huge),
                Some(unit | (1 << 63)),
                Some(0),
                Some(unit),
                None,
            ];
            assert_conversion_bits(array, &expected);
        }
    }
}
// Frozen Arrow 58 raw extrema expectations, independently derived with exact
// rational IEEE rounding. Rows are scales -128..=38; each is [2^127, 1]
// divided by the rounded old powi(10, scale). Precision does not mask payloads.
const OLD_DECIMAL128_EXTREMA_BITS: [[u64; 2]; 167] = [
    [0x62727748f9301d33, 0x5a827748f9301d33], // scale -128
    [0x623d8ba7f519c851, 0x5a4d8ba7f519c851], // scale -127
    [0x6207a2ecc414a041, 0x5a17a2ecc414a041], // scale -126
    [0x61d2e8bd69aa19cd, 0x59e2e8bd69aa19cd], // scale -125
    [0x619e412f0f768fae, 0x59ae412f0f768fae], // scale -124
    [0x61683425a5f872f3, 0x59783425a5f872f3], // scale -123
    [0x61335ceaeb2d28c2, 0x59435ceaeb2d28c2], // scale -122
    [0x60fefb1178484139, 0x590efb1178484139], // scale -121
    [0x60c8c8dac6a0342b, 0x58d8c8dac6a0342b], // scale -120
    [0x6093d3e2388029bc, 0x58a3d3e2388029bc], // scale -119
    [0x605fb969f40042c7, 0x586fb969f40042c7], // scale -118
    [0x602961219000356c, 0x583961219000356c], // scale -117
    [0x5ff44db473335df0, 0x58044db473335df0], // scale -116
    [0x5fc03e29f5c2b18d, 0x57d03e29f5c2b18d], // scale -115
    [0x5f89fd0fef9de8e2, 0x5799fd0fef9de8e2], // scale -114
    [0x5f54ca732617ed81, 0x5764ca732617ed81], // scale -113
    [0x5f20a1f5b8132467, 0x5730a1f5b8132467], // scale -112
    [0x5eea9cbc59b83a3f, 0x56fa9cbc59b83a3f], // scale -111
    [0x5eb54a3047c694ff, 0x56c54a3047c694ff], // scale -110
    [0x5e8108269fd210cc, 0x569108269fd210cc], // scale -109
    [0x5e4b403dcc834e14, 0x565b403dcc834e14], // scale -108
    [0x5e15ccfe3d35d80f, 0x5625ccfe3d35d80f], // scale -107
    [0x5de170cb642b1340, 0x55f170cb642b1340], // scale -106
    [0x5dabe7abd3781ecc, 0x55bbe7abd3781ecc], // scale -105
    [0x5d7652efdc6018a3, 0x558652efdc6018a3], // scale -104
    [0x5d41dbf316b346e9, 0x5551dbf316b346e9], // scale -103
    [0x5d0c931e8ab87174, 0x551c931e8ab87174], // scale -102
    [0x5cd6dc186ef9f45e, 0x54e6dc186ef9f45e], // scale -101
    [0x5ca249ad2594c37e, 0x54b249ad2594c37e], // scale -100
    [0x5c6d42aea2879f30, 0x547d42aea2879f30], // scale -99
    [0x5c37688bb5394c26, 0x5447688bb5394c26], // scale -98
    [0x5c02ba095dc7701f, 0x5412ba095dc7701f], // scale -97
    [0x5bcdf67562d8b363, 0x53ddf67562d8b363], // scale -96
    [0x5b97f85de8ad5c50, 0x53a7f85de8ad5c50], // scale -95
    [0x5b632d17ed577d0d, 0x53732d17ed577d0d], // scale -94
    [0x5b2eae8caef261ad, 0x533eae8caef261ad], // scale -93
    [0x5af88ba3bf284e25, 0x53088ba3bf284e25], // scale -92
    [0x5ac3a2e965b9d81e, 0x52d3a2e965b9d81e], // scale -91
    [0x5a8f6b0f092959c9, 0x529f6b0f092959c9], // scale -90
    [0x5a5922726dbaae3b, 0x526922726dbaae3b], // scale -89
    [0x5a241b8ebe2ef1c8, 0x52341b8ebe2ef1c8], // scale -88
    [0x59f0160bcb58c16d, 0x5200160bcb58c16d], // scale -87
    [0x59b9bcdfabc1357b, 0x51c9bcdfabc1357b], // scale -86
    [0x5984971956342ac9, 0x5194971956342ac9], // scale -85
    [0x595078e111c3556e, 0x516078e111c3556e], // scale -84
    [0x591a5b01b605557c, 0x512a5b01b605557c], // scale -83
    [0x58e5159af8044463, 0x50f5159af8044463], // scale -82
    [0x58b0de1593369d1c, 0x50c0de1593369d1c], // scale -81
    [0x587afcef51f0fb5f, 0x508afcef51f0fb5f], // scale -80
    [0x58459725db272f80, 0x50559725db272f80], // scale -79
    [0x581145b7e285bf9a, 0x502145b7e285bf9a], // scale -78
    [0x57dba2bfd0d5ff5c, 0x4feba2bfd0d5ff5c], // scale -77
    [0x57a61bcca7119917, 0x4fb61bcca7119917], // scale -76
    [0x5771afd6ec0e1412, 0x4f81afd6ec0e1412], // scale -75
    [0x573c4c8b1349b9b7, 0x4f4c4c8b1349b9b7], // scale -74
    [0x5706a3a275d49492, 0x4f16a3a275d49492], // scale -73
    [0x56d21c81f7dd43a8, 0x4ee21c81f7dd43a8], // scale -72
    [0x569cfa698c95390c, 0x4eacfa698c95390c], // scale -71
    [0x56672ebad6ddc73e, 0x4e772ebad6ddc73e], // scale -70
    [0x56328bc8abe49f64, 0x4e428bc8abe49f64], // scale -69
    [0x55fdac74463a98a1, 0x4e0dac74463a98a1], // scale -68
    [0x55c7bd29d1c87a19, 0x4dd7bd29d1c87a19], // scale -67
    [0x5592fdbb0e39fb48, 0x4da2fdbb0e39fb48], // scale -66
    [0x555e62c4e38ff875, 0x4d6e62c4e38ff875], // scale -65
    [0x55284f03e93ff9f6, 0x4d384f03e93ff9f6], // scale -64
    [0x54f3726987666191, 0x4d03726987666191], // scale -63
    [0x54bf1d75a5709c1a, 0x4ccf1d75a5709c1a], // scale -62
    [0x5488e45e1df3b015, 0x4c98e45e1df3b015], // scale -61
    [0x5453e9e4e4c2f344, 0x4c63e9e4e4c2f344], // scale -60
    [0x541fdca16e04b86d, 0x4c2fdca16e04b86d], // scale -59
    [0x53e97d4df19d6058, 0x4bf97d4df19d6058], // scale -58
    [0x53b4643e5ae44d14, 0x4bc4643e5ae44d14], // scale -57
    [0x53805031e2503da9, 0x4b905031e2503da9], // scale -56
    [0x534a19e96a19fc41, 0x4b5a19e96a19fc41], // scale -55
    [0x5314e1878814c9ce, 0x4b24e1878814c9ce], // scale -54
    [0x52e0b46c6cdd6e3e, 0x4af0b46c6cdd6e3e], // scale -53
    [0x52aaba4714957d30, 0x4ababa4714957d30], // scale -52
    [0x527561d276ddfdc0, 0x4a8561d276ddfdc0], // scale -51
    [0x52411b0ec57e649a, 0x4a511b0ec57e649a], // scale -50
    [0x520b5e7e08ca3a90, 0x4a1b5e7e08ca3a90], // scale -49
    [0x51d5e531a0a1c873, 0x49e5e531a0a1c873], // scale -48
    [0x51a18427b3b4a05c, 0x49b18427b3b4a05c], // scale -47
    [0x516c06a5ec5433c6, 0x497c06a5ec5433c6], // scale -46
    [0x51366bb7f0435c9f, 0x49466bb7f0435c9f], // scale -45
    [0x5101efc659cf7d4c, 0x4911efc659cf7d4c], // scale -44
    [0x50ccb2d6f618c878, 0x48dcb2d6f618c878], // scale -43
    [0x5096f578c4e0a061, 0x48a6f578c4e0a061], // scale -42
    [0x50625dfa371a19e7, 0x48725dfa371a19e7], // scale -41
    [0x502d6329f1c35ca5, 0x483d6329f1c35ca5], // scale -40
    [0x4ff78287f49c4a1e, 0x48078287f49c4a1e], // scale -39
    [0x4fc2ced32a16a1b1, 0x47d2ced32a16a1b1], // scale -38
    [0x4f8e17b84357691d, 0x479e17b84357691d], // scale -37
    [0x4f5812f9cf7920e3, 0x476812f9cf7920e3], // scale -36
    [0x4f23426172c74d82, 0x4733426172c74d82], // scale -35
    [0x4eeed09bead87c04, 0x46fed09bead87c04], // scale -34
    [0x4eb8a6e32246c99d, 0x46c8a6e32246c99d], // scale -33
    [0x4e83b8b5b5056e17, 0x4693b8b5b5056e17], // scale -32
    [0x4e4f8def8808b024, 0x465f8def8808b024], // scale -31
    [0x4e193e5939a08ceb, 0x46293e5939a08ceb], // scale -30
    [0x4de431e0fae6d721, 0x45f431e0fae6d721], // scale -29
    [0x4db027e72f1f1281, 0x45c027e72f1f1281], // scale -28
    [0x4d79d971e4fe8402, 0x4589d971e4fe8402], // scale -27
    [0x4d44adf4b7320335, 0x4554adf4b7320335], // scale -26
    [0x4d108b2a2c280291, 0x45208b2a2c280291], // scale -25
    [0x4cda784379d99db3, 0x44ea784379d99db3], // scale -24
    [0x4ca52d02c7e14af6, 0x44b52d02c7e14af6], // scale -23
    [0x4c70f0cf064dd592, 0x4480f0cf064dd592], // scale -22
    [0x4c3b1ae4d6e2ef51, 0x444b1ae4d6e2ef51], // scale -21
    [0x4c05af1d78b58c40, 0x4415af1d78b58c40], // scale -20
    [0x4bd158e460913d00, 0x43e158e460913d00], // scale -19
    [0x4b9bc16d674ec7ff, 0x43abc16d674ec7ff], // scale -18
    [0x4b66345785d8a000, 0x4376345785d8a000], // scale -17
    [0x4b31c37937e08000, 0x4341c37937e08000], // scale -16
    [0x4afc6bf52633ffff, 0x430c6bf52633ffff], // scale -15
    [0x4ac6bcc41e900000, 0x42d6bcc41e900000], // scale -14
    [0x4a92309ce5400000, 0x42a2309ce5400000], // scale -13
    [0x4a5d1a94a2000000, 0x426d1a94a2000000], // scale -12
    [0x4a274876e8000000, 0x42374876e8000000], // scale -11
    [0x49f2a05f20000000, 0x4202a05f20000000], // scale -10
    [0x49bdcd64ffffffff, 0x41cdcd64ffffffff], // scale -9
    [0x4987d78400000000, 0x4197d78400000000], // scale -8
    [0x495312d000000000, 0x416312d000000000], // scale -7
    [0x491e848000000000, 0x412e848000000000], // scale -6
    [0x48e869ffffffffff, 0x40f869ffffffffff], // scale -5
    [0x48b3880000000000, 0x40c3880000000000], // scale -4
    [0x487f400000000000, 0x408f400000000000], // scale -3
    [0x4849000000000000, 0x4059000000000000], // scale -2
    [0x4814000000000000, 0x4024000000000000], // scale -1
    [0x47e0000000000000, 0x3ff0000000000000], // scale 0
    [0x47a999999999999a, 0x3fb999999999999a], // scale 1
    [0x47747ae147ae147b, 0x3f847ae147ae147b], // scale 2
    [0x4740624dd2f1a9fc, 0x3f50624dd2f1a9fc], // scale 3
    [0x470a36e2eb1c432d, 0x3f1a36e2eb1c432d], // scale 4
    [0x46d4f8b588e368f1, 0x3ee4f8b588e368f1], // scale 5
    [0x46a0c6f7a0b5ed8d, 0x3eb0c6f7a0b5ed8d], // scale 6
    [0x466ad7f29abcaf48, 0x3e7ad7f29abcaf48], // scale 7
    [0x4635798ee2308c3a, 0x3e45798ee2308c3a], // scale 8
    [0x46012e0be826d695, 0x3e112e0be826d695], // scale 9
    [0x45cb7cdfd9d7bdbb, 0x3ddb7cdfd9d7bdbb], // scale 10
    [0x4595fd7fe1796495, 0x3da5fd7fe1796495], // scale 11
    [0x45619799812dea11, 0x3d719799812dea11], // scale 12
    [0x452c25c268497682, 0x3d3c25c268497682], // scale 13
    [0x44f6849b86a12b9b, 0x3d06849b86a12b9b], // scale 14
    [0x44c203af9ee75616, 0x3cd203af9ee75616], // scale 15
    [0x448cd2b297d889bc, 0x3c9cd2b297d889bc], // scale 16
    [0x44570ef54646d497, 0x3c670ef54646d497], // scale 17
    [0x4422725dd1d243ac, 0x3c32725dd1d243ac], // scale 18
    [0x43ed83c94fb6d2ac, 0x3bfd83c94fb6d2ac], // scale 19
    [0x43b79ca10c924223, 0x3bc79ca10c924223], // scale 20
    [0x4382e3b40a0e9b4f, 0x3b92e3b40a0e9b4f], // scale 21
    [0x434e392010175ee6, 0x3b5e392010175ee6], // scale 22
    [0x43182db34012b252, 0x3b282db34012b252], // scale 23
    [0x42e357c299a88ea8, 0x3af357c299a88ea8], // scale 24
    [0x42aef2d0f5da7dd8, 0x3abef2d0f5da7dd8], // scale 25
    [0x4278c240c4aecb13, 0x3a88c240c4aecb13], // scale 26
    [0x4243ce9a36f23c10, 0x3a53ce9a36f23c10], // scale 27
    [0x420fb0f6be50601a, 0x3a1fb0f6be50601a], // scale 28
    [0x41d95a5efea6b348, 0x39e95a5efea6b348], // scale 29
    [0x41a4484bfeebc29f, 0x39b4484bfeebc29f], // scale 30
    [0x417039d665896880, 0x398039d665896880], // scale 31
    [0x4139f623d5a8a732, 0x3949f623d5a8a732], // scale 32
    [0x4104c4e977ba1f5b, 0x3914c4e977ba1f5b], // scale 33
    [0x40d09d8792fb4c49, 0x38e09d8792fb4c49], // scale 34
    [0x409a95a5b7f87a0f, 0x38aa95a5b7f87a0f], // scale 35
    [0x40654484932d2e72, 0x38754484932d2e72], // scale 36
    [0x4031039d428a8b8e, 0x3841039d428a8b8e], // scale 37
    [0x3ffb38fb9daa78e4, 0x380b38fb9daa78e4], // scale 38
];
