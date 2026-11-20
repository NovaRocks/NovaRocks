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

//! Entire admitted Decimal128/256 Float64 profiles with fixed independent IEEE tables.
use arrow::array::{Array, ArrayRef, Decimal128Array, Decimal256Array};
use arrow::datatypes::DataType;
use arrow_buffer::{NullBuffer, i256};
use novarocks_functions::{
    CastOperation, CastRowResult, EvaluatedArgument, KernelEvaluationControl, KernelFailure,
    PreparedCastRecipe, SelectedValues, Selection,
};
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
        panic!("decimal float cast never waits")
    }
}
// Pure input construction only; no conversion or old evaluator.
pub(super) fn input(wide: bool, p: u8, scale: i8, raw: Vec<Option<i128>>) -> ArrayRef {
    if wide {
        Arc::new(
            Decimal256Array::from(
                raw.into_iter()
                    .map(|value| value.map(i256::from_i128))
                    .collect::<Vec<_>>(),
            )
            .with_precision_and_scale(p, scale)
            .unwrap(),
        )
    } else {
        Arc::new(
            Decimal128Array::from(raw)
                .with_precision_and_scale(p, scale)
                .unwrap(),
        )
    }
}
// Each row is fixed [raw +1, raw i128::MIN, raw i128::MAX] IEEE bits.
// Index is scale+128; precision is frozen metadata, not a value clamp.
// Binary64 staging and standard-powi witnesses are explicit in the retained
// independent rational worksheet. Decimal128 divides by signed-scale factors;
// Decimal256 multiplies at negative scales. This distinction changes bits.
const NARROW_BITS: [[Option<u64>; 3]; 205] = [
    [
        Some(0x5a827748f9301d33),
        Some(0xe2727748f9301d33),
        Some(0x62727748f9301d33),
    ], // scale -128
    [
        Some(0x5a4d8ba7f519c851),
        Some(0xe23d8ba7f519c851),
        Some(0x623d8ba7f519c851),
    ], // scale -127
    [
        Some(0x5a17a2ecc414a041),
        Some(0xe207a2ecc414a041),
        Some(0x6207a2ecc414a041),
    ], // scale -126
    [
        Some(0x59e2e8bd69aa19cd),
        Some(0xe1d2e8bd69aa19cd),
        Some(0x61d2e8bd69aa19cd),
    ], // scale -125
    [
        Some(0x59ae412f0f768fae),
        Some(0xe19e412f0f768fae),
        Some(0x619e412f0f768fae),
    ], // scale -124
    [
        Some(0x59783425a5f872f3),
        Some(0xe1683425a5f872f3),
        Some(0x61683425a5f872f3),
    ], // scale -123
    [
        Some(0x59435ceaeb2d28c2),
        Some(0xe1335ceaeb2d28c2),
        Some(0x61335ceaeb2d28c2),
    ], // scale -122
    [
        Some(0x590efb1178484139),
        Some(0xe0fefb1178484139),
        Some(0x60fefb1178484139),
    ], // scale -121
    [
        Some(0x58d8c8dac6a0342b),
        Some(0xe0c8c8dac6a0342b),
        Some(0x60c8c8dac6a0342b),
    ], // scale -120
    [
        Some(0x58a3d3e2388029bc),
        Some(0xe093d3e2388029bc),
        Some(0x6093d3e2388029bc),
    ], // scale -119
    [
        Some(0x586fb969f40042c7),
        Some(0xe05fb969f40042c7),
        Some(0x605fb969f40042c7),
    ], // scale -118
    [
        Some(0x583961219000356c),
        Some(0xe02961219000356c),
        Some(0x602961219000356c),
    ], // scale -117
    [
        Some(0x58044db473335df0),
        Some(0xdff44db473335df0),
        Some(0x5ff44db473335df0),
    ], // scale -116
    [
        Some(0x57d03e29f5c2b18d),
        Some(0xdfc03e29f5c2b18d),
        Some(0x5fc03e29f5c2b18d),
    ], // scale -115
    [
        Some(0x5799fd0fef9de8e2),
        Some(0xdf89fd0fef9de8e2),
        Some(0x5f89fd0fef9de8e2),
    ], // scale -114
    [
        Some(0x5764ca732617ed81),
        Some(0xdf54ca732617ed81),
        Some(0x5f54ca732617ed81),
    ], // scale -113
    [
        Some(0x5730a1f5b8132467),
        Some(0xdf20a1f5b8132467),
        Some(0x5f20a1f5b8132467),
    ], // scale -112
    [
        Some(0x56fa9cbc59b83a3f),
        Some(0xdeea9cbc59b83a3f),
        Some(0x5eea9cbc59b83a3f),
    ], // scale -111
    [
        Some(0x56c54a3047c694ff),
        Some(0xdeb54a3047c694ff),
        Some(0x5eb54a3047c694ff),
    ], // scale -110
    [
        Some(0x569108269fd210cc),
        Some(0xde8108269fd210cc),
        Some(0x5e8108269fd210cc),
    ], // scale -109
    [
        Some(0x565b403dcc834e14),
        Some(0xde4b403dcc834e14),
        Some(0x5e4b403dcc834e14),
    ], // scale -108
    [
        Some(0x5625ccfe3d35d80f),
        Some(0xde15ccfe3d35d80f),
        Some(0x5e15ccfe3d35d80f),
    ], // scale -107
    [
        Some(0x55f170cb642b1340),
        Some(0xdde170cb642b1340),
        Some(0x5de170cb642b1340),
    ], // scale -106
    [
        Some(0x55bbe7abd3781ecc),
        Some(0xddabe7abd3781ecc),
        Some(0x5dabe7abd3781ecc),
    ], // scale -105
    [
        Some(0x558652efdc6018a3),
        Some(0xdd7652efdc6018a3),
        Some(0x5d7652efdc6018a3),
    ], // scale -104
    [
        Some(0x5551dbf316b346e9),
        Some(0xdd41dbf316b346e9),
        Some(0x5d41dbf316b346e9),
    ], // scale -103
    [
        Some(0x551c931e8ab87174),
        Some(0xdd0c931e8ab87174),
        Some(0x5d0c931e8ab87174),
    ], // scale -102
    [
        Some(0x54e6dc186ef9f45e),
        Some(0xdcd6dc186ef9f45e),
        Some(0x5cd6dc186ef9f45e),
    ], // scale -101
    [
        Some(0x54b249ad2594c37e),
        Some(0xdca249ad2594c37e),
        Some(0x5ca249ad2594c37e),
    ], // scale -100
    [
        Some(0x547d42aea2879f30),
        Some(0xdc6d42aea2879f30),
        Some(0x5c6d42aea2879f30),
    ], // scale -99
    [
        Some(0x5447688bb5394c26),
        Some(0xdc37688bb5394c26),
        Some(0x5c37688bb5394c26),
    ], // scale -98
    [
        Some(0x5412ba095dc7701f),
        Some(0xdc02ba095dc7701f),
        Some(0x5c02ba095dc7701f),
    ], // scale -97
    [
        Some(0x53ddf67562d8b363),
        Some(0xdbcdf67562d8b363),
        Some(0x5bcdf67562d8b363),
    ], // scale -96
    [
        Some(0x53a7f85de8ad5c50),
        Some(0xdb97f85de8ad5c50),
        Some(0x5b97f85de8ad5c50),
    ], // scale -95
    [
        Some(0x53732d17ed577d0d),
        Some(0xdb632d17ed577d0d),
        Some(0x5b632d17ed577d0d),
    ], // scale -94
    [
        Some(0x533eae8caef261ad),
        Some(0xdb2eae8caef261ad),
        Some(0x5b2eae8caef261ad),
    ], // scale -93
    [
        Some(0x53088ba3bf284e25),
        Some(0xdaf88ba3bf284e25),
        Some(0x5af88ba3bf284e25),
    ], // scale -92
    [
        Some(0x52d3a2e965b9d81e),
        Some(0xdac3a2e965b9d81e),
        Some(0x5ac3a2e965b9d81e),
    ], // scale -91
    [
        Some(0x529f6b0f092959c9),
        Some(0xda8f6b0f092959c9),
        Some(0x5a8f6b0f092959c9),
    ], // scale -90
    [
        Some(0x526922726dbaae3b),
        Some(0xda5922726dbaae3b),
        Some(0x5a5922726dbaae3b),
    ], // scale -89
    [
        Some(0x52341b8ebe2ef1c8),
        Some(0xda241b8ebe2ef1c8),
        Some(0x5a241b8ebe2ef1c8),
    ], // scale -88
    [
        Some(0x5200160bcb58c16d),
        Some(0xd9f0160bcb58c16d),
        Some(0x59f0160bcb58c16d),
    ], // scale -87
    [
        Some(0x51c9bcdfabc1357b),
        Some(0xd9b9bcdfabc1357b),
        Some(0x59b9bcdfabc1357b),
    ], // scale -86
    [
        Some(0x5194971956342ac9),
        Some(0xd984971956342ac9),
        Some(0x5984971956342ac9),
    ], // scale -85
    [
        Some(0x516078e111c3556e),
        Some(0xd95078e111c3556e),
        Some(0x595078e111c3556e),
    ], // scale -84
    [
        Some(0x512a5b01b605557c),
        Some(0xd91a5b01b605557c),
        Some(0x591a5b01b605557c),
    ], // scale -83
    [
        Some(0x50f5159af8044463),
        Some(0xd8e5159af8044463),
        Some(0x58e5159af8044463),
    ], // scale -82
    [
        Some(0x50c0de1593369d1c),
        Some(0xd8b0de1593369d1c),
        Some(0x58b0de1593369d1c),
    ], // scale -81
    [
        Some(0x508afcef51f0fb5f),
        Some(0xd87afcef51f0fb5f),
        Some(0x587afcef51f0fb5f),
    ], // scale -80
    [
        Some(0x50559725db272f80),
        Some(0xd8459725db272f80),
        Some(0x58459725db272f80),
    ], // scale -79
    [
        Some(0x502145b7e285bf9a),
        Some(0xd81145b7e285bf9a),
        Some(0x581145b7e285bf9a),
    ], // scale -78
    [
        Some(0x4feba2bfd0d5ff5c),
        Some(0xd7dba2bfd0d5ff5c),
        Some(0x57dba2bfd0d5ff5c),
    ], // scale -77
    [
        Some(0x4fb61bcca7119917),
        Some(0xd7a61bcca7119917),
        Some(0x57a61bcca7119917),
    ], // scale -76
    [
        Some(0x4f81afd6ec0e1412),
        Some(0xd771afd6ec0e1412),
        Some(0x5771afd6ec0e1412),
    ], // scale -75
    [
        Some(0x4f4c4c8b1349b9b7),
        Some(0xd73c4c8b1349b9b7),
        Some(0x573c4c8b1349b9b7),
    ], // scale -74
    [
        Some(0x4f16a3a275d49492),
        Some(0xd706a3a275d49492),
        Some(0x5706a3a275d49492),
    ], // scale -73
    [
        Some(0x4ee21c81f7dd43a8),
        Some(0xd6d21c81f7dd43a8),
        Some(0x56d21c81f7dd43a8),
    ], // scale -72
    [
        Some(0x4eacfa698c95390c),
        Some(0xd69cfa698c95390c),
        Some(0x569cfa698c95390c),
    ], // scale -71
    [
        Some(0x4e772ebad6ddc73e),
        Some(0xd6672ebad6ddc73e),
        Some(0x56672ebad6ddc73e),
    ], // scale -70
    [
        Some(0x4e428bc8abe49f64),
        Some(0xd6328bc8abe49f64),
        Some(0x56328bc8abe49f64),
    ], // scale -69
    [
        Some(0x4e0dac74463a98a1),
        Some(0xd5fdac74463a98a1),
        Some(0x55fdac74463a98a1),
    ], // scale -68
    [
        Some(0x4dd7bd29d1c87a19),
        Some(0xd5c7bd29d1c87a19),
        Some(0x55c7bd29d1c87a19),
    ], // scale -67
    [
        Some(0x4da2fdbb0e39fb48),
        Some(0xd592fdbb0e39fb48),
        Some(0x5592fdbb0e39fb48),
    ], // scale -66
    [
        Some(0x4d6e62c4e38ff875),
        Some(0xd55e62c4e38ff875),
        Some(0x555e62c4e38ff875),
    ], // scale -65
    [
        Some(0x4d384f03e93ff9f6),
        Some(0xd5284f03e93ff9f6),
        Some(0x55284f03e93ff9f6),
    ], // scale -64
    [
        Some(0x4d03726987666191),
        Some(0xd4f3726987666191),
        Some(0x54f3726987666191),
    ], // scale -63
    [
        Some(0x4ccf1d75a5709c1a),
        Some(0xd4bf1d75a5709c1a),
        Some(0x54bf1d75a5709c1a),
    ], // scale -62
    [
        Some(0x4c98e45e1df3b015),
        Some(0xd488e45e1df3b015),
        Some(0x5488e45e1df3b015),
    ], // scale -61
    [
        Some(0x4c63e9e4e4c2f344),
        Some(0xd453e9e4e4c2f344),
        Some(0x5453e9e4e4c2f344),
    ], // scale -60
    [
        Some(0x4c2fdca16e04b86d),
        Some(0xd41fdca16e04b86d),
        Some(0x541fdca16e04b86d),
    ], // scale -59
    [
        Some(0x4bf97d4df19d6058),
        Some(0xd3e97d4df19d6058),
        Some(0x53e97d4df19d6058),
    ], // scale -58
    [
        Some(0x4bc4643e5ae44d14),
        Some(0xd3b4643e5ae44d14),
        Some(0x53b4643e5ae44d14),
    ], // scale -57
    [
        Some(0x4b905031e2503da9),
        Some(0xd3805031e2503da9),
        Some(0x53805031e2503da9),
    ], // scale -56
    [
        Some(0x4b5a19e96a19fc41),
        Some(0xd34a19e96a19fc41),
        Some(0x534a19e96a19fc41),
    ], // scale -55
    [
        Some(0x4b24e1878814c9ce),
        Some(0xd314e1878814c9ce),
        Some(0x5314e1878814c9ce),
    ], // scale -54
    [
        Some(0x4af0b46c6cdd6e3e),
        Some(0xd2e0b46c6cdd6e3e),
        Some(0x52e0b46c6cdd6e3e),
    ], // scale -53
    [
        Some(0x4ababa4714957d30),
        Some(0xd2aaba4714957d30),
        Some(0x52aaba4714957d30),
    ], // scale -52
    [
        Some(0x4a8561d276ddfdc0),
        Some(0xd27561d276ddfdc0),
        Some(0x527561d276ddfdc0),
    ], // scale -51
    [
        Some(0x4a511b0ec57e649a),
        Some(0xd2411b0ec57e649a),
        Some(0x52411b0ec57e649a),
    ], // scale -50
    [
        Some(0x4a1b5e7e08ca3a90),
        Some(0xd20b5e7e08ca3a90),
        Some(0x520b5e7e08ca3a90),
    ], // scale -49
    [
        Some(0x49e5e531a0a1c873),
        Some(0xd1d5e531a0a1c873),
        Some(0x51d5e531a0a1c873),
    ], // scale -48
    [
        Some(0x49b18427b3b4a05c),
        Some(0xd1a18427b3b4a05c),
        Some(0x51a18427b3b4a05c),
    ], // scale -47
    [
        Some(0x497c06a5ec5433c6),
        Some(0xd16c06a5ec5433c6),
        Some(0x516c06a5ec5433c6),
    ], // scale -46
    [
        Some(0x49466bb7f0435c9f),
        Some(0xd1366bb7f0435c9f),
        Some(0x51366bb7f0435c9f),
    ], // scale -45
    [
        Some(0x4911efc659cf7d4c),
        Some(0xd101efc659cf7d4c),
        Some(0x5101efc659cf7d4c),
    ], // scale -44
    [
        Some(0x48dcb2d6f618c878),
        Some(0xd0ccb2d6f618c878),
        Some(0x50ccb2d6f618c878),
    ], // scale -43
    [
        Some(0x48a6f578c4e0a061),
        Some(0xd096f578c4e0a061),
        Some(0x5096f578c4e0a061),
    ], // scale -42
    [
        Some(0x48725dfa371a19e7),
        Some(0xd0625dfa371a19e7),
        Some(0x50625dfa371a19e7),
    ], // scale -41
    [
        Some(0x483d6329f1c35ca5),
        Some(0xd02d6329f1c35ca5),
        Some(0x502d6329f1c35ca5),
    ], // scale -40
    [
        Some(0x48078287f49c4a1e),
        Some(0xcff78287f49c4a1e),
        Some(0x4ff78287f49c4a1e),
    ], // scale -39
    [
        Some(0x47d2ced32a16a1b1),
        Some(0xcfc2ced32a16a1b1),
        Some(0x4fc2ced32a16a1b1),
    ], // scale -38
    [
        Some(0x479e17b84357691d),
        Some(0xcf8e17b84357691d),
        Some(0x4f8e17b84357691d),
    ], // scale -37
    [
        Some(0x476812f9cf7920e3),
        Some(0xcf5812f9cf7920e3),
        Some(0x4f5812f9cf7920e3),
    ], // scale -36
    [
        Some(0x4733426172c74d82),
        Some(0xcf23426172c74d82),
        Some(0x4f23426172c74d82),
    ], // scale -35
    [
        Some(0x46fed09bead87c04),
        Some(0xceeed09bead87c04),
        Some(0x4eeed09bead87c04),
    ], // scale -34
    [
        Some(0x46c8a6e32246c99d),
        Some(0xceb8a6e32246c99d),
        Some(0x4eb8a6e32246c99d),
    ], // scale -33
    [
        Some(0x4693b8b5b5056e17),
        Some(0xce83b8b5b5056e17),
        Some(0x4e83b8b5b5056e17),
    ], // scale -32
    [
        Some(0x465f8def8808b024),
        Some(0xce4f8def8808b024),
        Some(0x4e4f8def8808b024),
    ], // scale -31
    [
        Some(0x46293e5939a08ceb),
        Some(0xce193e5939a08ceb),
        Some(0x4e193e5939a08ceb),
    ], // scale -30
    [
        Some(0x45f431e0fae6d721),
        Some(0xcde431e0fae6d721),
        Some(0x4de431e0fae6d721),
    ], // scale -29
    [
        Some(0x45c027e72f1f1281),
        Some(0xcdb027e72f1f1281),
        Some(0x4db027e72f1f1281),
    ], // scale -28
    [
        Some(0x4589d971e4fe8402),
        Some(0xcd79d971e4fe8402),
        Some(0x4d79d971e4fe8402),
    ], // scale -27
    [
        Some(0x4554adf4b7320335),
        Some(0xcd44adf4b7320335),
        Some(0x4d44adf4b7320335),
    ], // scale -26
    [
        Some(0x45208b2a2c280291),
        Some(0xcd108b2a2c280291),
        Some(0x4d108b2a2c280291),
    ], // scale -25
    [
        Some(0x44ea784379d99db3),
        Some(0xccda784379d99db3),
        Some(0x4cda784379d99db3),
    ], // scale -24
    [
        Some(0x44b52d02c7e14af6),
        Some(0xcca52d02c7e14af6),
        Some(0x4ca52d02c7e14af6),
    ], // scale -23
    [
        Some(0x4480f0cf064dd592),
        Some(0xcc70f0cf064dd592),
        Some(0x4c70f0cf064dd592),
    ], // scale -22
    [
        Some(0x444b1ae4d6e2ef51),
        Some(0xcc3b1ae4d6e2ef51),
        Some(0x4c3b1ae4d6e2ef51),
    ], // scale -21
    [
        Some(0x4415af1d78b58c40),
        Some(0xcc05af1d78b58c40),
        Some(0x4c05af1d78b58c40),
    ], // scale -20
    [
        Some(0x43e158e460913d00),
        Some(0xcbd158e460913d00),
        Some(0x4bd158e460913d00),
    ], // scale -19
    [
        Some(0x43abc16d674ec7ff),
        Some(0xcb9bc16d674ec7ff),
        Some(0x4b9bc16d674ec7ff),
    ], // scale -18
    [
        Some(0x4376345785d8a000),
        Some(0xcb66345785d8a000),
        Some(0x4b66345785d8a000),
    ], // scale -17
    [
        Some(0x4341c37937e08000),
        Some(0xcb31c37937e08000),
        Some(0x4b31c37937e08000),
    ], // scale -16
    [
        Some(0x430c6bf52633ffff),
        Some(0xcafc6bf52633ffff),
        Some(0x4afc6bf52633ffff),
    ], // scale -15
    [
        Some(0x42d6bcc41e900000),
        Some(0xcac6bcc41e900000),
        Some(0x4ac6bcc41e900000),
    ], // scale -14
    [
        Some(0x42a2309ce5400000),
        Some(0xca92309ce5400000),
        Some(0x4a92309ce5400000),
    ], // scale -13
    [
        Some(0x426d1a94a2000000),
        Some(0xca5d1a94a2000000),
        Some(0x4a5d1a94a2000000),
    ], // scale -12
    [
        Some(0x42374876e8000000),
        Some(0xca274876e8000000),
        Some(0x4a274876e8000000),
    ], // scale -11
    [
        Some(0x4202a05f20000000),
        Some(0xc9f2a05f20000000),
        Some(0x49f2a05f20000000),
    ], // scale -10
    [
        Some(0x41cdcd64ffffffff),
        Some(0xc9bdcd64ffffffff),
        Some(0x49bdcd64ffffffff),
    ], // scale -9
    [
        Some(0x4197d78400000000),
        Some(0xc987d78400000000),
        Some(0x4987d78400000000),
    ], // scale -8
    [
        Some(0x416312d000000000),
        Some(0xc95312d000000000),
        Some(0x495312d000000000),
    ], // scale -7
    [
        Some(0x412e848000000000),
        Some(0xc91e848000000000),
        Some(0x491e848000000000),
    ], // scale -6
    [
        Some(0x40f869ffffffffff),
        Some(0xc8e869ffffffffff),
        Some(0x48e869ffffffffff),
    ], // scale -5
    [
        Some(0x40c3880000000000),
        Some(0xc8b3880000000000),
        Some(0x48b3880000000000),
    ], // scale -4
    [
        Some(0x408f400000000000),
        Some(0xc87f400000000000),
        Some(0x487f400000000000),
    ], // scale -3
    [
        Some(0x4059000000000000),
        Some(0xc849000000000000),
        Some(0x4849000000000000),
    ], // scale -2
    [
        Some(0x4024000000000000),
        Some(0xc814000000000000),
        Some(0x4814000000000000),
    ], // scale -1
    [
        Some(0x3ff0000000000000),
        Some(0xc7e0000000000000),
        Some(0x47e0000000000000),
    ], // scale 0
    [
        Some(0x3fb999999999999a),
        Some(0xc7a999999999999a),
        Some(0x47a999999999999a),
    ], // scale 1
    [
        Some(0x3f847ae147ae147b),
        Some(0xc7747ae147ae147b),
        Some(0x47747ae147ae147b),
    ], // scale 2
    [
        Some(0x3f50624dd2f1a9fc),
        Some(0xc740624dd2f1a9fc),
        Some(0x4740624dd2f1a9fc),
    ], // scale 3
    [
        Some(0x3f1a36e2eb1c432d),
        Some(0xc70a36e2eb1c432d),
        Some(0x470a36e2eb1c432d),
    ], // scale 4
    [
        Some(0x3ee4f8b588e368f1),
        Some(0xc6d4f8b588e368f1),
        Some(0x46d4f8b588e368f1),
    ], // scale 5
    [
        Some(0x3eb0c6f7a0b5ed8d),
        Some(0xc6a0c6f7a0b5ed8d),
        Some(0x46a0c6f7a0b5ed8d),
    ], // scale 6
    [
        Some(0x3e7ad7f29abcaf48),
        Some(0xc66ad7f29abcaf48),
        Some(0x466ad7f29abcaf48),
    ], // scale 7
    [
        Some(0x3e45798ee2308c3a),
        Some(0xc635798ee2308c3a),
        Some(0x4635798ee2308c3a),
    ], // scale 8
    [
        Some(0x3e112e0be826d695),
        Some(0xc6012e0be826d695),
        Some(0x46012e0be826d695),
    ], // scale 9
    [
        Some(0x3ddb7cdfd9d7bdbb),
        Some(0xc5cb7cdfd9d7bdbb),
        Some(0x45cb7cdfd9d7bdbb),
    ], // scale 10
    [
        Some(0x3da5fd7fe1796495),
        Some(0xc595fd7fe1796495),
        Some(0x4595fd7fe1796495),
    ], // scale 11
    [
        Some(0x3d719799812dea11),
        Some(0xc5619799812dea11),
        Some(0x45619799812dea11),
    ], // scale 12
    [
        Some(0x3d3c25c268497682),
        Some(0xc52c25c268497682),
        Some(0x452c25c268497682),
    ], // scale 13
    [
        Some(0x3d06849b86a12b9b),
        Some(0xc4f6849b86a12b9b),
        Some(0x44f6849b86a12b9b),
    ], // scale 14
    [
        Some(0x3cd203af9ee75616),
        Some(0xc4c203af9ee75616),
        Some(0x44c203af9ee75616),
    ], // scale 15
    [
        Some(0x3c9cd2b297d889bc),
        Some(0xc48cd2b297d889bc),
        Some(0x448cd2b297d889bc),
    ], // scale 16
    [
        Some(0x3c670ef54646d497),
        Some(0xc4570ef54646d497),
        Some(0x44570ef54646d497),
    ], // scale 17
    [
        Some(0x3c32725dd1d243ac),
        Some(0xc422725dd1d243ac),
        Some(0x4422725dd1d243ac),
    ], // scale 18
    [
        Some(0x3bfd83c94fb6d2ac),
        Some(0xc3ed83c94fb6d2ac),
        Some(0x43ed83c94fb6d2ac),
    ], // scale 19
    [
        Some(0x3bc79ca10c924223),
        Some(0xc3b79ca10c924223),
        Some(0x43b79ca10c924223),
    ], // scale 20
    [
        Some(0x3b92e3b40a0e9b4f),
        Some(0xc382e3b40a0e9b4f),
        Some(0x4382e3b40a0e9b4f),
    ], // scale 21
    [
        Some(0x3b5e392010175ee6),
        Some(0xc34e392010175ee6),
        Some(0x434e392010175ee6),
    ], // scale 22
    [
        Some(0x3b282db34012b252),
        Some(0xc3182db34012b252),
        Some(0x43182db34012b252),
    ], // scale 23
    [
        Some(0x3af357c299a88ea8),
        Some(0xc2e357c299a88ea8),
        Some(0x42e357c299a88ea8),
    ], // scale 24
    [
        Some(0x3abef2d0f5da7dd8),
        Some(0xc2aef2d0f5da7dd8),
        Some(0x42aef2d0f5da7dd8),
    ], // scale 25
    [
        Some(0x3a88c240c4aecb13),
        Some(0xc278c240c4aecb13),
        Some(0x4278c240c4aecb13),
    ], // scale 26
    [
        Some(0x3a53ce9a36f23c10),
        Some(0xc243ce9a36f23c10),
        Some(0x4243ce9a36f23c10),
    ], // scale 27
    [
        Some(0x3a1fb0f6be50601a),
        Some(0xc20fb0f6be50601a),
        Some(0x420fb0f6be50601a),
    ], // scale 28
    [
        Some(0x39e95a5efea6b348),
        Some(0xc1d95a5efea6b348),
        Some(0x41d95a5efea6b348),
    ], // scale 29
    [
        Some(0x39b4484bfeebc29f),
        Some(0xc1a4484bfeebc29f),
        Some(0x41a4484bfeebc29f),
    ], // scale 30
    [
        Some(0x398039d665896880),
        Some(0xc17039d665896880),
        Some(0x417039d665896880),
    ], // scale 31
    [
        Some(0x3949f623d5a8a732),
        Some(0xc139f623d5a8a732),
        Some(0x4139f623d5a8a732),
    ], // scale 32
    [
        Some(0x3914c4e977ba1f5b),
        Some(0xc104c4e977ba1f5b),
        Some(0x4104c4e977ba1f5b),
    ], // scale 33
    [
        Some(0x38e09d8792fb4c49),
        Some(0xc0d09d8792fb4c49),
        Some(0x40d09d8792fb4c49),
    ], // scale 34
    [
        Some(0x38aa95a5b7f87a0f),
        Some(0xc09a95a5b7f87a0f),
        Some(0x409a95a5b7f87a0f),
    ], // scale 35
    [
        Some(0x38754484932d2e72),
        Some(0xc0654484932d2e72),
        Some(0x40654484932d2e72),
    ], // scale 36
    [
        Some(0x3841039d428a8b8e),
        Some(0xc031039d428a8b8e),
        Some(0x4031039d428a8b8e),
    ], // scale 37
    [
        Some(0x380b38fb9daa78e4),
        Some(0xbffb38fb9daa78e4),
        Some(0x3ffb38fb9daa78e4),
    ], // scale 38
    [
        Some(0x37d5c72fb1552d83),
        Some(0xbfc5c72fb1552d83),
        Some(0x3fc5c72fb1552d83),
    ], // scale 39
    [
        Some(0x37a16c262777579c),
        Some(0xbf916c262777579c),
        Some(0x3f916c262777579c),
    ], // scale 40
    [
        Some(0x376be03d0bf225c7),
        Some(0xbf5be03d0bf225c7),
        Some(0x3f5be03d0bf225c7),
    ], // scale 41
    [
        Some(0x37364cfda3281e38),
        Some(0xbf264cfda3281e38),
        Some(0x3f264cfda3281e38),
    ], // scale 42
    [
        Some(0x3701d7314f534b61),
        Some(0xbef1d7314f534b61),
        Some(0x3ef1d7314f534b61),
    ], // scale 43
    [
        Some(0x36cc8b8218854567),
        Some(0xbebc8b8218854567),
        Some(0x3ebc8b8218854567),
    ], // scale 44
    [
        Some(0x3696d601ad376ab9),
        Some(0xbe86d601ad376ab9),
        Some(0x3e86d601ad376ab9),
    ], // scale 45
    [
        Some(0x366244ce242c5561),
        Some(0xbe5244ce242c5561),
        Some(0x3e5244ce242c5561),
    ], // scale 46
    [
        Some(0x362d3ae36d13bbce),
        Some(0xbe1d3ae36d13bbce),
        Some(0x3e1d3ae36d13bbce),
    ], // scale 47
    [
        Some(0x35f7624f8a762fd8),
        Some(0xbde7624f8a762fd8),
        Some(0x3de7624f8a762fd8),
    ], // scale 48
    [
        Some(0x35c2b50c6ec4f313),
        Some(0xbdb2b50c6ec4f313),
        Some(0x3db2b50c6ec4f313),
    ], // scale 49
    [
        Some(0x358dee7a4ad4b81e),
        Some(0xbd7dee7a4ad4b81e),
        Some(0x3d7dee7a4ad4b81e),
    ], // scale 50
    [
        Some(0x3557f1fb6f10934c),
        Some(0xbd47f1fb6f10934c),
        Some(0x3d47f1fb6f10934c),
    ], // scale 51
    [
        Some(0x352327fc58da0f70),
        Some(0xbd1327fc58da0f70),
        Some(0x3d1327fc58da0f70),
    ], // scale 52
    [
        Some(0x34eea6608e29b24d),
        Some(0xbcdea6608e29b24d),
        Some(0x3cdea6608e29b24d),
    ], // scale 53
    [
        Some(0x34b8851a0b548ea3),
        Some(0xbca8851a0b548ea3),
        Some(0x3ca8851a0b548ea3),
    ], // scale 54
    [
        Some(0x34839dae6f76d883),
        Some(0xbc739dae6f76d883),
        Some(0x3c739dae6f76d883),
    ], // scale 55
    [
        Some(0x344f62b0b257c0d1),
        Some(0xbc3f62b0b257c0d1),
        Some(0x3c3f62b0b257c0d1),
    ], // scale 56
    [
        Some(0x34191bc08eac9a40),
        Some(0xbc091bc08eac9a40),
        Some(0x3c091bc08eac9a40),
    ], // scale 57
    [
        Some(0x33e41633a556e1cd),
        Some(0xbbd41633a556e1cd),
        Some(0x3bd41633a556e1cd),
    ], // scale 58
    [
        Some(0x33b011c2eaabe7d8),
        Some(0xbba011c2eaabe7d8),
        Some(0x3ba011c2eaabe7d8),
    ], // scale 59
    [
        Some(0x3379b604aaaca627),
        Some(0xbb69b604aaaca627),
        Some(0x3b69b604aaaca627),
    ], // scale 60
    [
        Some(0x3344919d5556eb52),
        Some(0xbb34919d5556eb52),
        Some(0x3b34919d5556eb52),
    ], // scale 61
    [
        Some(0x3310747ddddf22a8),
        Some(0xbb00747ddddf22a8),
        Some(0x3b00747ddddf22a8),
    ], // scale 62
    [
        Some(0x32da53fc9631d10c),
        Some(0xbaca53fc9631d10c),
        Some(0x3aca53fc9631d10c),
    ], // scale 63
    [
        Some(0x32a50ffd44f4a73c),
        Some(0xba950ffd44f4a73c),
        Some(0x3a950ffd44f4a73c),
    ], // scale 64
    [
        Some(0x3270d9976a5d5296),
        Some(0xba60d9976a5d5296),
        Some(0x3a60d9976a5d5296),
    ], // scale 65
    [
        Some(0x323af5bf109550f1),
        Some(0xba2af5bf109550f1),
        Some(0x3a2af5bf109550f1),
    ], // scale 66
    [
        Some(0x32059165a6ddda5b),
        Some(0xb9f59165a6ddda5b),
        Some(0x39f59165a6ddda5b),
    ], // scale 67
    [
        Some(0x31d1411e1f17e1e2),
        Some(0xb9c1411e1f17e1e2),
        Some(0x39c1411e1f17e1e2),
    ], // scale 68
    [
        Some(0x319b9b6364f30304),
        Some(0xb98b9b6364f30304),
        Some(0x398b9b6364f30304),
    ], // scale 69
    [
        Some(0x316615e91d8f359c),
        Some(0xb95615e91d8f359c),
        Some(0x395615e91d8f359c),
    ], // scale 70
    [
        Some(0x3131ab20e472914a),
        Some(0xb921ab20e472914a),
        Some(0x3921ab20e472914a),
    ], // scale 71
    [
        Some(0x30fc45016d841ba9),
        Some(0xb8ec45016d841ba9),
        Some(0x38ec45016d841ba9),
    ], // scale 72
    [
        Some(0x30c69d9abe034954),
        Some(0xb8b69d9abe034954),
        Some(0x38b69d9abe034954),
    ], // scale 73
    [
        Some(0x309217aefe690776),
        Some(0xb88217aefe690776),
        Some(0x388217aefe690776),
    ], // scale 74
    [
        Some(0x305cf2b1970e7257),
        Some(0xb84cf2b1970e7257),
        Some(0x384cf2b1970e7257),
    ], // scale 75
    [
        Some(0x3027288e1271f512),
        Some(0xb817288e1271f512),
        Some(0x3817288e1271f512),
    ], // scale 76
];
const WIDE_BITS: [[Option<u64>; 3]; 205] = [
    [
        Some(0x5a827748f9301d33),
        Some(0xe2727748f9301d33),
        Some(0x62727748f9301d33),
    ], // scale -128
    [
        Some(0x5a4d8ba7f519c851),
        Some(0xe23d8ba7f519c851),
        Some(0x623d8ba7f519c851),
    ], // scale -127
    [
        Some(0x5a17a2ecc414a041),
        Some(0xe207a2ecc414a041),
        Some(0x6207a2ecc414a041),
    ], // scale -126
    [
        Some(0x59e2e8bd69aa19cd),
        Some(0xe1d2e8bd69aa19cd),
        Some(0x61d2e8bd69aa19cd),
    ], // scale -125
    [
        Some(0x59ae412f0f768fae),
        Some(0xe19e412f0f768fae),
        Some(0x619e412f0f768fae),
    ], // scale -124
    [
        Some(0x59783425a5f872f3),
        Some(0xe1683425a5f872f3),
        Some(0x61683425a5f872f3),
    ], // scale -123
    [
        Some(0x59435ceaeb2d28c2),
        Some(0xe1335ceaeb2d28c2),
        Some(0x61335ceaeb2d28c2),
    ], // scale -122
    [
        Some(0x590efb1178484138),
        Some(0xe0fefb1178484138),
        Some(0x60fefb1178484138),
    ], // scale -121
    [
        Some(0x58d8c8dac6a0342c),
        Some(0xe0c8c8dac6a0342c),
        Some(0x60c8c8dac6a0342c),
    ], // scale -120
    [
        Some(0x58a3d3e2388029bc),
        Some(0xe093d3e2388029bc),
        Some(0x6093d3e2388029bc),
    ], // scale -119
    [
        Some(0x586fb969f40042c7),
        Some(0xe05fb969f40042c7),
        Some(0x605fb969f40042c7),
    ], // scale -118
    [
        Some(0x583961219000356c),
        Some(0xe02961219000356c),
        Some(0x602961219000356c),
    ], // scale -117
    [
        Some(0x58044db473335df0),
        Some(0xdff44db473335df0),
        Some(0x5ff44db473335df0),
    ], // scale -116
    [
        Some(0x57d03e29f5c2b18d),
        Some(0xdfc03e29f5c2b18d),
        Some(0x5fc03e29f5c2b18d),
    ], // scale -115
    [
        Some(0x5799fd0fef9de8e2),
        Some(0xdf89fd0fef9de8e2),
        Some(0x5f89fd0fef9de8e2),
    ], // scale -114
    [
        Some(0x5764ca732617ed81),
        Some(0xdf54ca732617ed81),
        Some(0x5f54ca732617ed81),
    ], // scale -113
    [
        Some(0x5730a1f5b8132467),
        Some(0xdf20a1f5b8132467),
        Some(0x5f20a1f5b8132467),
    ], // scale -112
    [
        Some(0x56fa9cbc59b83a3f),
        Some(0xdeea9cbc59b83a3f),
        Some(0x5eea9cbc59b83a3f),
    ], // scale -111
    [
        Some(0x56c54a3047c694ff),
        Some(0xdeb54a3047c694ff),
        Some(0x5eb54a3047c694ff),
    ], // scale -110
    [
        Some(0x569108269fd210cc),
        Some(0xde8108269fd210cc),
        Some(0x5e8108269fd210cc),
    ], // scale -109
    [
        Some(0x565b403dcc834e14),
        Some(0xde4b403dcc834e14),
        Some(0x5e4b403dcc834e14),
    ], // scale -108
    [
        Some(0x5625ccfe3d35d80f),
        Some(0xde15ccfe3d35d80f),
        Some(0x5e15ccfe3d35d80f),
    ], // scale -107
    [
        Some(0x55f170cb642b1340),
        Some(0xdde170cb642b1340),
        Some(0x5de170cb642b1340),
    ], // scale -106
    [
        Some(0x55bbe7abd3781ecc),
        Some(0xddabe7abd3781ecc),
        Some(0x5dabe7abd3781ecc),
    ], // scale -105
    [
        Some(0x558652efdc6018a3),
        Some(0xdd7652efdc6018a3),
        Some(0x5d7652efdc6018a3),
    ], // scale -104
    [
        Some(0x5551dbf316b346e9),
        Some(0xdd41dbf316b346e9),
        Some(0x5d41dbf316b346e9),
    ], // scale -103
    [
        Some(0x551c931e8ab87174),
        Some(0xdd0c931e8ab87174),
        Some(0x5d0c931e8ab87174),
    ], // scale -102
    [
        Some(0x54e6dc186ef9f45e),
        Some(0xdcd6dc186ef9f45e),
        Some(0x5cd6dc186ef9f45e),
    ], // scale -101
    [
        Some(0x54b249ad2594c37e),
        Some(0xdca249ad2594c37e),
        Some(0x5ca249ad2594c37e),
    ], // scale -100
    [
        Some(0x547d42aea2879f2f),
        Some(0xdc6d42aea2879f2f),
        Some(0x5c6d42aea2879f2f),
    ], // scale -99
    [
        Some(0x5447688bb5394c27),
        Some(0xdc37688bb5394c27),
        Some(0x5c37688bb5394c27),
    ], // scale -98
    [
        Some(0x5412ba095dc7701f),
        Some(0xdc02ba095dc7701f),
        Some(0x5c02ba095dc7701f),
    ], // scale -97
    [
        Some(0x53ddf67562d8b364),
        Some(0xdbcdf67562d8b364),
        Some(0x5bcdf67562d8b364),
    ], // scale -96
    [
        Some(0x53a7f85de8ad5c50),
        Some(0xdb97f85de8ad5c50),
        Some(0x5b97f85de8ad5c50),
    ], // scale -95
    [
        Some(0x53732d17ed577d0d),
        Some(0xdb632d17ed577d0d),
        Some(0x5b632d17ed577d0d),
    ], // scale -94
    [
        Some(0x533eae8caef261ad),
        Some(0xdb2eae8caef261ad),
        Some(0x5b2eae8caef261ad),
    ], // scale -93
    [
        Some(0x53088ba3bf284e25),
        Some(0xdaf88ba3bf284e25),
        Some(0x5af88ba3bf284e25),
    ], // scale -92
    [
        Some(0x52d3a2e965b9d81e),
        Some(0xdac3a2e965b9d81e),
        Some(0x5ac3a2e965b9d81e),
    ], // scale -91
    [
        Some(0x529f6b0f092959c9),
        Some(0xda8f6b0f092959c9),
        Some(0x5a8f6b0f092959c9),
    ], // scale -90
    [
        Some(0x526922726dbaae3b),
        Some(0xda5922726dbaae3b),
        Some(0x5a5922726dbaae3b),
    ], // scale -89
    [
        Some(0x52341b8ebe2ef1c8),
        Some(0xda241b8ebe2ef1c8),
        Some(0x5a241b8ebe2ef1c8),
    ], // scale -88
    [
        Some(0x5200160bcb58c16d),
        Some(0xd9f0160bcb58c16d),
        Some(0x59f0160bcb58c16d),
    ], // scale -87
    [
        Some(0x51c9bcdfabc1357b),
        Some(0xd9b9bcdfabc1357b),
        Some(0x59b9bcdfabc1357b),
    ], // scale -86
    [
        Some(0x5194971956342ac9),
        Some(0xd984971956342ac9),
        Some(0x5984971956342ac9),
    ], // scale -85
    [
        Some(0x516078e111c3556e),
        Some(0xd95078e111c3556e),
        Some(0x595078e111c3556e),
    ], // scale -84
    [
        Some(0x512a5b01b605557c),
        Some(0xd91a5b01b605557c),
        Some(0x591a5b01b605557c),
    ], // scale -83
    [
        Some(0x50f5159af8044463),
        Some(0xd8e5159af8044463),
        Some(0x58e5159af8044463),
    ], // scale -82
    [
        Some(0x50c0de1593369d1c),
        Some(0xd8b0de1593369d1c),
        Some(0x58b0de1593369d1c),
    ], // scale -81
    [
        Some(0x508afcef51f0fb60),
        Some(0xd87afcef51f0fb60),
        Some(0x587afcef51f0fb60),
    ], // scale -80
    [
        Some(0x50559725db272f80),
        Some(0xd8459725db272f80),
        Some(0x58459725db272f80),
    ], // scale -79
    [
        Some(0x502145b7e285bf9a),
        Some(0xd81145b7e285bf9a),
        Some(0x581145b7e285bf9a),
    ], // scale -78
    [
        Some(0x4feba2bfd0d5ff5c),
        Some(0xd7dba2bfd0d5ff5c),
        Some(0x57dba2bfd0d5ff5c),
    ], // scale -77
    [
        Some(0x4fb61bcca7119917),
        Some(0xd7a61bcca7119917),
        Some(0x57a61bcca7119917),
    ], // scale -76
    [
        Some(0x4f81afd6ec0e1412),
        Some(0xd771afd6ec0e1412),
        Some(0x5771afd6ec0e1412),
    ], // scale -75
    [
        Some(0x4f4c4c8b1349b9b7),
        Some(0xd73c4c8b1349b9b7),
        Some(0x573c4c8b1349b9b7),
    ], // scale -74
    [
        Some(0x4f16a3a275d49492),
        Some(0xd706a3a275d49492),
        Some(0x5706a3a275d49492),
    ], // scale -73
    [
        Some(0x4ee21c81f7dd43a8),
        Some(0xd6d21c81f7dd43a8),
        Some(0x56d21c81f7dd43a8),
    ], // scale -72
    [
        Some(0x4eacfa698c95390d),
        Some(0xd69cfa698c95390d),
        Some(0x569cfa698c95390d),
    ], // scale -71
    [
        Some(0x4e772ebad6ddc73e),
        Some(0xd6672ebad6ddc73e),
        Some(0x56672ebad6ddc73e),
    ], // scale -70
    [
        Some(0x4e428bc8abe49f64),
        Some(0xd6328bc8abe49f64),
        Some(0x56328bc8abe49f64),
    ], // scale -69
    [
        Some(0x4e0dac74463a98a1),
        Some(0xd5fdac74463a98a1),
        Some(0x55fdac74463a98a1),
    ], // scale -68
    [
        Some(0x4dd7bd29d1c87a1a),
        Some(0xd5c7bd29d1c87a1a),
        Some(0x55c7bd29d1c87a1a),
    ], // scale -67
    [
        Some(0x4da2fdbb0e39fb48),
        Some(0xd592fdbb0e39fb48),
        Some(0x5592fdbb0e39fb48),
    ], // scale -66
    [
        Some(0x4d6e62c4e38ff874),
        Some(0xd55e62c4e38ff874),
        Some(0x555e62c4e38ff874),
    ], // scale -65
    [
        Some(0x4d384f03e93ff9f6),
        Some(0xd5284f03e93ff9f6),
        Some(0x55284f03e93ff9f6),
    ], // scale -64
    [
        Some(0x4d03726987666191),
        Some(0xd4f3726987666191),
        Some(0x54f3726987666191),
    ], // scale -63
    [
        Some(0x4ccf1d75a5709c1b),
        Some(0xd4bf1d75a5709c1b),
        Some(0x54bf1d75a5709c1b),
    ], // scale -62
    [
        Some(0x4c98e45e1df3b015),
        Some(0xd488e45e1df3b015),
        Some(0x5488e45e1df3b015),
    ], // scale -61
    [
        Some(0x4c63e9e4e4c2f344),
        Some(0xd453e9e4e4c2f344),
        Some(0x5453e9e4e4c2f344),
    ], // scale -60
    [
        Some(0x4c2fdca16e04b86e),
        Some(0xd41fdca16e04b86e),
        Some(0x541fdca16e04b86e),
    ], // scale -59
    [
        Some(0x4bf97d4df19d6058),
        Some(0xd3e97d4df19d6058),
        Some(0x53e97d4df19d6058),
    ], // scale -58
    [
        Some(0x4bc4643e5ae44d14),
        Some(0xd3b4643e5ae44d14),
        Some(0x53b4643e5ae44d14),
    ], // scale -57
    [
        Some(0x4b905031e2503da9),
        Some(0xd3805031e2503da9),
        Some(0x53805031e2503da9),
    ], // scale -56
    [
        Some(0x4b5a19e96a19fc41),
        Some(0xd34a19e96a19fc41),
        Some(0x534a19e96a19fc41),
    ], // scale -55
    [
        Some(0x4b24e1878814c9ce),
        Some(0xd314e1878814c9ce),
        Some(0x5314e1878814c9ce),
    ], // scale -54
    [
        Some(0x4af0b46c6cdd6e3e),
        Some(0xd2e0b46c6cdd6e3e),
        Some(0x52e0b46c6cdd6e3e),
    ], // scale -53
    [
        Some(0x4ababa4714957d30),
        Some(0xd2aaba4714957d30),
        Some(0x52aaba4714957d30),
    ], // scale -52
    [
        Some(0x4a8561d276ddfdc0),
        Some(0xd27561d276ddfdc0),
        Some(0x527561d276ddfdc0),
    ], // scale -51
    [
        Some(0x4a511b0ec57e649a),
        Some(0xd2411b0ec57e649a),
        Some(0x52411b0ec57e649a),
    ], // scale -50
    [
        Some(0x4a1b5e7e08ca3a90),
        Some(0xd20b5e7e08ca3a90),
        Some(0x520b5e7e08ca3a90),
    ], // scale -49
    [
        Some(0x49e5e531a0a1c873),
        Some(0xd1d5e531a0a1c873),
        Some(0x51d5e531a0a1c873),
    ], // scale -48
    [
        Some(0x49b18427b3b4a05c),
        Some(0xd1a18427b3b4a05c),
        Some(0x51a18427b3b4a05c),
    ], // scale -47
    [
        Some(0x497c06a5ec5433c6),
        Some(0xd16c06a5ec5433c6),
        Some(0x516c06a5ec5433c6),
    ], // scale -46
    [
        Some(0x49466bb7f0435c9f),
        Some(0xd1366bb7f0435c9f),
        Some(0x51366bb7f0435c9f),
    ], // scale -45
    [
        Some(0x4911efc659cf7d4c),
        Some(0xd101efc659cf7d4c),
        Some(0x5101efc659cf7d4c),
    ], // scale -44
    [
        Some(0x48dcb2d6f618c879),
        Some(0xd0ccb2d6f618c879),
        Some(0x50ccb2d6f618c879),
    ], // scale -43
    [
        Some(0x48a6f578c4e0a061),
        Some(0xd096f578c4e0a061),
        Some(0x5096f578c4e0a061),
    ], // scale -42
    [
        Some(0x48725dfa371a19e7),
        Some(0xd0625dfa371a19e7),
        Some(0x50625dfa371a19e7),
    ], // scale -41
    [
        Some(0x483d6329f1c35ca5),
        Some(0xd02d6329f1c35ca5),
        Some(0x502d6329f1c35ca5),
    ], // scale -40
    [
        Some(0x48078287f49c4a1e),
        Some(0xcff78287f49c4a1e),
        Some(0x4ff78287f49c4a1e),
    ], // scale -39
    [
        Some(0x47d2ced32a16a1b1),
        Some(0xcfc2ced32a16a1b1),
        Some(0x4fc2ced32a16a1b1),
    ], // scale -38
    [
        Some(0x479e17b84357691c),
        Some(0xcf8e17b84357691c),
        Some(0x4f8e17b84357691c),
    ], // scale -37
    [
        Some(0x476812f9cf7920e3),
        Some(0xcf5812f9cf7920e3),
        Some(0x4f5812f9cf7920e3),
    ], // scale -36
    [
        Some(0x4733426172c74d82),
        Some(0xcf23426172c74d82),
        Some(0x4f23426172c74d82),
    ], // scale -35
    [
        Some(0x46fed09bead87c04),
        Some(0xceeed09bead87c04),
        Some(0x4eeed09bead87c04),
    ], // scale -34
    [
        Some(0x46c8a6e32246c99d),
        Some(0xceb8a6e32246c99d),
        Some(0x4eb8a6e32246c99d),
    ], // scale -33
    [
        Some(0x4693b8b5b5056e17),
        Some(0xce83b8b5b5056e17),
        Some(0x4e83b8b5b5056e17),
    ], // scale -32
    [
        Some(0x465f8def8808b024),
        Some(0xce4f8def8808b024),
        Some(0x4e4f8def8808b024),
    ], // scale -31
    [
        Some(0x46293e5939a08cea),
        Some(0xce193e5939a08cea),
        Some(0x4e193e5939a08cea),
    ], // scale -30
    [
        Some(0x45f431e0fae6d721),
        Some(0xcde431e0fae6d721),
        Some(0x4de431e0fae6d721),
    ], // scale -29
    [
        Some(0x45c027e72f1f1281),
        Some(0xcdb027e72f1f1281),
        Some(0x4db027e72f1f1281),
    ], // scale -28
    [
        Some(0x4589d971e4fe8402),
        Some(0xcd79d971e4fe8402),
        Some(0x4d79d971e4fe8402),
    ], // scale -27
    [
        Some(0x4554adf4b7320335),
        Some(0xcd44adf4b7320335),
        Some(0x4d44adf4b7320335),
    ], // scale -26
    [
        Some(0x45208b2a2c280291),
        Some(0xcd108b2a2c280291),
        Some(0x4d108b2a2c280291),
    ], // scale -25
    [
        Some(0x44ea784379d99db4),
        Some(0xccda784379d99db4),
        Some(0x4cda784379d99db4),
    ], // scale -24
    [
        Some(0x44b52d02c7e14af6),
        Some(0xcca52d02c7e14af6),
        Some(0x4ca52d02c7e14af6),
    ], // scale -23
    [
        Some(0x4480f0cf064dd592),
        Some(0xcc70f0cf064dd592),
        Some(0x4c70f0cf064dd592),
    ], // scale -22
    [
        Some(0x444b1ae4d6e2ef50),
        Some(0xcc3b1ae4d6e2ef50),
        Some(0x4c3b1ae4d6e2ef50),
    ], // scale -21
    [
        Some(0x4415af1d78b58c40),
        Some(0xcc05af1d78b58c40),
        Some(0x4c05af1d78b58c40),
    ], // scale -20
    [
        Some(0x43e158e460913d00),
        Some(0xcbd158e460913d00),
        Some(0x4bd158e460913d00),
    ], // scale -19
    [
        Some(0x43abc16d674ec800),
        Some(0xcb9bc16d674ec800),
        Some(0x4b9bc16d674ec800),
    ], // scale -18
    [
        Some(0x4376345785d8a000),
        Some(0xcb66345785d8a000),
        Some(0x4b66345785d8a000),
    ], // scale -17
    [
        Some(0x4341c37937e08000),
        Some(0xcb31c37937e08000),
        Some(0x4b31c37937e08000),
    ], // scale -16
    [
        Some(0x430c6bf526340000),
        Some(0xcafc6bf526340000),
        Some(0x4afc6bf526340000),
    ], // scale -15
    [
        Some(0x42d6bcc41e900000),
        Some(0xcac6bcc41e900000),
        Some(0x4ac6bcc41e900000),
    ], // scale -14
    [
        Some(0x42a2309ce5400000),
        Some(0xca92309ce5400000),
        Some(0x4a92309ce5400000),
    ], // scale -13
    [
        Some(0x426d1a94a2000000),
        Some(0xca5d1a94a2000000),
        Some(0x4a5d1a94a2000000),
    ], // scale -12
    [
        Some(0x42374876e8000000),
        Some(0xca274876e8000000),
        Some(0x4a274876e8000000),
    ], // scale -11
    [
        Some(0x4202a05f20000000),
        Some(0xc9f2a05f20000000),
        Some(0x49f2a05f20000000),
    ], // scale -10
    [
        Some(0x41cdcd6500000000),
        Some(0xc9bdcd6500000000),
        Some(0x49bdcd6500000000),
    ], // scale -9
    [
        Some(0x4197d78400000000),
        Some(0xc987d78400000000),
        Some(0x4987d78400000000),
    ], // scale -8
    [
        Some(0x416312d000000000),
        Some(0xc95312d000000000),
        Some(0x495312d000000000),
    ], // scale -7
    [
        Some(0x412e848000000000),
        Some(0xc91e848000000000),
        Some(0x491e848000000000),
    ], // scale -6
    [
        Some(0x40f86a0000000000),
        Some(0xc8e86a0000000000),
        Some(0x48e86a0000000000),
    ], // scale -5
    [
        Some(0x40c3880000000000),
        Some(0xc8b3880000000000),
        Some(0x48b3880000000000),
    ], // scale -4
    [
        Some(0x408f400000000000),
        Some(0xc87f400000000000),
        Some(0x487f400000000000),
    ], // scale -3
    [
        Some(0x4059000000000000),
        Some(0xc849000000000000),
        Some(0x4849000000000000),
    ], // scale -2
    [
        Some(0x4024000000000000),
        Some(0xc814000000000000),
        Some(0x4814000000000000),
    ], // scale -1
    [
        Some(0x3ff0000000000000),
        Some(0xc7e0000000000000),
        Some(0x47e0000000000000),
    ], // scale 0
    [
        Some(0x3fb999999999999a),
        Some(0xc7a999999999999a),
        Some(0x47a999999999999a),
    ], // scale 1
    [
        Some(0x3f847ae147ae147b),
        Some(0xc7747ae147ae147b),
        Some(0x47747ae147ae147b),
    ], // scale 2
    [
        Some(0x3f50624dd2f1a9fc),
        Some(0xc740624dd2f1a9fc),
        Some(0x4740624dd2f1a9fc),
    ], // scale 3
    [
        Some(0x3f1a36e2eb1c432d),
        Some(0xc70a36e2eb1c432d),
        Some(0x470a36e2eb1c432d),
    ], // scale 4
    [
        Some(0x3ee4f8b588e368f1),
        Some(0xc6d4f8b588e368f1),
        Some(0x46d4f8b588e368f1),
    ], // scale 5
    [
        Some(0x3eb0c6f7a0b5ed8d),
        Some(0xc6a0c6f7a0b5ed8d),
        Some(0x46a0c6f7a0b5ed8d),
    ], // scale 6
    [
        Some(0x3e7ad7f29abcaf48),
        Some(0xc66ad7f29abcaf48),
        Some(0x466ad7f29abcaf48),
    ], // scale 7
    [
        Some(0x3e45798ee2308c3a),
        Some(0xc635798ee2308c3a),
        Some(0x4635798ee2308c3a),
    ], // scale 8
    [
        Some(0x3e112e0be826d695),
        Some(0xc6012e0be826d695),
        Some(0x46012e0be826d695),
    ], // scale 9
    [
        Some(0x3ddb7cdfd9d7bdbb),
        Some(0xc5cb7cdfd9d7bdbb),
        Some(0x45cb7cdfd9d7bdbb),
    ], // scale 10
    [
        Some(0x3da5fd7fe1796495),
        Some(0xc595fd7fe1796495),
        Some(0x4595fd7fe1796495),
    ], // scale 11
    [
        Some(0x3d719799812dea11),
        Some(0xc5619799812dea11),
        Some(0x45619799812dea11),
    ], // scale 12
    [
        Some(0x3d3c25c268497682),
        Some(0xc52c25c268497682),
        Some(0x452c25c268497682),
    ], // scale 13
    [
        Some(0x3d06849b86a12b9b),
        Some(0xc4f6849b86a12b9b),
        Some(0x44f6849b86a12b9b),
    ], // scale 14
    [
        Some(0x3cd203af9ee75616),
        Some(0xc4c203af9ee75616),
        Some(0x44c203af9ee75616),
    ], // scale 15
    [
        Some(0x3c9cd2b297d889bc),
        Some(0xc48cd2b297d889bc),
        Some(0x448cd2b297d889bc),
    ], // scale 16
    [
        Some(0x3c670ef54646d497),
        Some(0xc4570ef54646d497),
        Some(0x44570ef54646d497),
    ], // scale 17
    [
        Some(0x3c32725dd1d243ac),
        Some(0xc422725dd1d243ac),
        Some(0x4422725dd1d243ac),
    ], // scale 18
    [
        Some(0x3bfd83c94fb6d2ac),
        Some(0xc3ed83c94fb6d2ac),
        Some(0x43ed83c94fb6d2ac),
    ], // scale 19
    [
        Some(0x3bc79ca10c924223),
        Some(0xc3b79ca10c924223),
        Some(0x43b79ca10c924223),
    ], // scale 20
    [
        Some(0x3b92e3b40a0e9b4f),
        Some(0xc382e3b40a0e9b4f),
        Some(0x4382e3b40a0e9b4f),
    ], // scale 21
    [
        Some(0x3b5e392010175ee6),
        Some(0xc34e392010175ee6),
        Some(0x434e392010175ee6),
    ], // scale 22
    [
        Some(0x3b282db34012b252),
        Some(0xc3182db34012b252),
        Some(0x43182db34012b252),
    ], // scale 23
    [
        Some(0x3af357c299a88ea8),
        Some(0xc2e357c299a88ea8),
        Some(0x42e357c299a88ea8),
    ], // scale 24
    [
        Some(0x3abef2d0f5da7dd8),
        Some(0xc2aef2d0f5da7dd8),
        Some(0x42aef2d0f5da7dd8),
    ], // scale 25
    [
        Some(0x3a88c240c4aecb13),
        Some(0xc278c240c4aecb13),
        Some(0x4278c240c4aecb13),
    ], // scale 26
    [
        Some(0x3a53ce9a36f23c10),
        Some(0xc243ce9a36f23c10),
        Some(0x4243ce9a36f23c10),
    ], // scale 27
    [
        Some(0x3a1fb0f6be50601a),
        Some(0xc20fb0f6be50601a),
        Some(0x420fb0f6be50601a),
    ], // scale 28
    [
        Some(0x39e95a5efea6b348),
        Some(0xc1d95a5efea6b348),
        Some(0x41d95a5efea6b348),
    ], // scale 29
    [
        Some(0x39b4484bfeebc29f),
        Some(0xc1a4484bfeebc29f),
        Some(0x41a4484bfeebc29f),
    ], // scale 30
    [
        Some(0x398039d665896880),
        Some(0xc17039d665896880),
        Some(0x417039d665896880),
    ], // scale 31
    [
        Some(0x3949f623d5a8a732),
        Some(0xc139f623d5a8a732),
        Some(0x4139f623d5a8a732),
    ], // scale 32
    [
        Some(0x3914c4e977ba1f5b),
        Some(0xc104c4e977ba1f5b),
        Some(0x4104c4e977ba1f5b),
    ], // scale 33
    [
        Some(0x38e09d8792fb4c49),
        Some(0xc0d09d8792fb4c49),
        Some(0x40d09d8792fb4c49),
    ], // scale 34
    [
        Some(0x38aa95a5b7f87a0f),
        Some(0xc09a95a5b7f87a0f),
        Some(0x409a95a5b7f87a0f),
    ], // scale 35
    [
        Some(0x38754484932d2e72),
        Some(0xc0654484932d2e72),
        Some(0x40654484932d2e72),
    ], // scale 36
    [
        Some(0x3841039d428a8b8e),
        Some(0xc031039d428a8b8e),
        Some(0x4031039d428a8b8e),
    ], // scale 37
    [
        Some(0x380b38fb9daa78e4),
        Some(0xbffb38fb9daa78e4),
        Some(0x3ffb38fb9daa78e4),
    ], // scale 38
    [
        Some(0x37d5c72fb1552d83),
        Some(0xbfc5c72fb1552d83),
        Some(0x3fc5c72fb1552d83),
    ], // scale 39
    [
        Some(0x37a16c262777579c),
        Some(0xbf916c262777579c),
        Some(0x3f916c262777579c),
    ], // scale 40
    [
        Some(0x376be03d0bf225c7),
        Some(0xbf5be03d0bf225c7),
        Some(0x3f5be03d0bf225c7),
    ], // scale 41
    [
        Some(0x37364cfda3281e38),
        Some(0xbf264cfda3281e38),
        Some(0x3f264cfda3281e38),
    ], // scale 42
    [
        Some(0x3701d7314f534b61),
        Some(0xbef1d7314f534b61),
        Some(0x3ef1d7314f534b61),
    ], // scale 43
    [
        Some(0x36cc8b8218854567),
        Some(0xbebc8b8218854567),
        Some(0x3ebc8b8218854567),
    ], // scale 44
    [
        Some(0x3696d601ad376ab9),
        Some(0xbe86d601ad376ab9),
        Some(0x3e86d601ad376ab9),
    ], // scale 45
    [
        Some(0x366244ce242c5561),
        Some(0xbe5244ce242c5561),
        Some(0x3e5244ce242c5561),
    ], // scale 46
    [
        Some(0x362d3ae36d13bbce),
        Some(0xbe1d3ae36d13bbce),
        Some(0x3e1d3ae36d13bbce),
    ], // scale 47
    [
        Some(0x35f7624f8a762fd8),
        Some(0xbde7624f8a762fd8),
        Some(0x3de7624f8a762fd8),
    ], // scale 48
    [
        Some(0x35c2b50c6ec4f313),
        Some(0xbdb2b50c6ec4f313),
        Some(0x3db2b50c6ec4f313),
    ], // scale 49
    [
        Some(0x358dee7a4ad4b81e),
        Some(0xbd7dee7a4ad4b81e),
        Some(0x3d7dee7a4ad4b81e),
    ], // scale 50
    [
        Some(0x3557f1fb6f10934c),
        Some(0xbd47f1fb6f10934c),
        Some(0x3d47f1fb6f10934c),
    ], // scale 51
    [
        Some(0x352327fc58da0f70),
        Some(0xbd1327fc58da0f70),
        Some(0x3d1327fc58da0f70),
    ], // scale 52
    [
        Some(0x34eea6608e29b24d),
        Some(0xbcdea6608e29b24d),
        Some(0x3cdea6608e29b24d),
    ], // scale 53
    [
        Some(0x34b8851a0b548ea3),
        Some(0xbca8851a0b548ea3),
        Some(0x3ca8851a0b548ea3),
    ], // scale 54
    [
        Some(0x34839dae6f76d883),
        Some(0xbc739dae6f76d883),
        Some(0x3c739dae6f76d883),
    ], // scale 55
    [
        Some(0x344f62b0b257c0d1),
        Some(0xbc3f62b0b257c0d1),
        Some(0x3c3f62b0b257c0d1),
    ], // scale 56
    [
        Some(0x34191bc08eac9a40),
        Some(0xbc091bc08eac9a40),
        Some(0x3c091bc08eac9a40),
    ], // scale 57
    [
        Some(0x33e41633a556e1cd),
        Some(0xbbd41633a556e1cd),
        Some(0x3bd41633a556e1cd),
    ], // scale 58
    [
        Some(0x33b011c2eaabe7d8),
        Some(0xbba011c2eaabe7d8),
        Some(0x3ba011c2eaabe7d8),
    ], // scale 59
    [
        Some(0x3379b604aaaca627),
        Some(0xbb69b604aaaca627),
        Some(0x3b69b604aaaca627),
    ], // scale 60
    [
        Some(0x3344919d5556eb52),
        Some(0xbb34919d5556eb52),
        Some(0x3b34919d5556eb52),
    ], // scale 61
    [
        Some(0x3310747ddddf22a8),
        Some(0xbb00747ddddf22a8),
        Some(0x3b00747ddddf22a8),
    ], // scale 62
    [
        Some(0x32da53fc9631d10c),
        Some(0xbaca53fc9631d10c),
        Some(0x3aca53fc9631d10c),
    ], // scale 63
    [
        Some(0x32a50ffd44f4a73c),
        Some(0xba950ffd44f4a73c),
        Some(0x3a950ffd44f4a73c),
    ], // scale 64
    [
        Some(0x3270d9976a5d5296),
        Some(0xba60d9976a5d5296),
        Some(0x3a60d9976a5d5296),
    ], // scale 65
    [
        Some(0x323af5bf109550f1),
        Some(0xba2af5bf109550f1),
        Some(0x3a2af5bf109550f1),
    ], // scale 66
    [
        Some(0x32059165a6ddda5b),
        Some(0xb9f59165a6ddda5b),
        Some(0x39f59165a6ddda5b),
    ], // scale 67
    [
        Some(0x31d1411e1f17e1e2),
        Some(0xb9c1411e1f17e1e2),
        Some(0x39c1411e1f17e1e2),
    ], // scale 68
    [
        Some(0x319b9b6364f30304),
        Some(0xb98b9b6364f30304),
        Some(0x398b9b6364f30304),
    ], // scale 69
    [
        Some(0x316615e91d8f359c),
        Some(0xb95615e91d8f359c),
        Some(0x395615e91d8f359c),
    ], // scale 70
    [
        Some(0x3131ab20e472914a),
        Some(0xb921ab20e472914a),
        Some(0x3921ab20e472914a),
    ], // scale 71
    [
        Some(0x30fc45016d841ba9),
        Some(0xb8ec45016d841ba9),
        Some(0x38ec45016d841ba9),
    ], // scale 72
    [
        Some(0x30c69d9abe034954),
        Some(0xb8b69d9abe034954),
        Some(0x38b69d9abe034954),
    ], // scale 73
    [
        Some(0x309217aefe690776),
        Some(0xb88217aefe690776),
        Some(0x388217aefe690776),
    ], // scale 74
    [
        Some(0x305cf2b1970e7257),
        Some(0xb84cf2b1970e7257),
        Some(0x384cf2b1970e7257),
    ], // scale 75
    [
        Some(0x3027288e1271f512),
        Some(0xb817288e1271f512),
        Some(0x3817288e1271f512),
    ], // scale 76
];
fn expected_bits(a: &ArrayRef, row: usize) -> Option<u64> {
    if a.is_null(row) {
        return None;
    }
    let (wide, scale, raw) = match a.data_type() {
        DataType::Decimal128(_, scale) => (
            false,
            *scale,
            a.as_any()
                .downcast_ref::<Decimal128Array>()
                .unwrap()
                .value(row),
        ),
        DataType::Decimal256(_, scale) => {
            let raw = a
                .as_any()
                .downcast_ref::<Decimal256Array>()
                .unwrap()
                .value(row);
            if raw == i256::MIN {
                assert_eq!(*scale, 0);
                return Some(0xcfe0000000000000);
            }
            if raw == i256::MAX {
                assert_eq!(*scale, 0);
                return Some(0x4fe0000000000000);
            }
            // The remaining authored corpus has exact i128 source payloads.
            (
                true,
                *scale,
                raw.to_i128().expect("fixed corpus i128 payload"),
            )
        }
        _ => panic!("fixed decimal carrier required"),
    };
    let row_bits = if wide {
        WIDE_BITS[(i16::from(scale) + 128) as usize]
    } else {
        NARROW_BITS[(i16::from(scale) + 128) as usize]
    };
    match raw {
        0 => Some(0),
        1 => row_bits[0],
        -1 => row_bits[0].map(|bits| bits ^ 0x8000000000000000),
        i128::MIN => row_bits[1],
        i128::MAX => row_bits[2],
        12345 => {
            assert_eq!(scale, 2);
            Some(0x405edccccccccccd)
        }
        _ => panic!("fixed decimal coefficient needs an explicit table entry"),
    }
}
fn expected_row(a: &ArrayRef, row: usize) -> CastRowResult {
    expected_bits(a, row).map_or(CastRowResult::Null, |bits| {
        CastRowResult::Float64(f64::from_bits(bits))
    })
}
fn r(
    a: &ArrayRef,
    nullable: bool,
    policy: DecimalOverflowPolicy,
    allow: bool,
) -> PreparedCastRecipe {
    PreparedCastRecipe::try_new(
        CastOperation::Carrier,
        &FunctionValueType::new(a.data_type().clone(), nullable),
        &FunctionValueType::new(DataType::Float64, nullable),
        policy,
        allow,
        None,
        &Control,
    )
    .unwrap()
}
fn value(
    r: &PreparedCastRecipe,
    a: &ArrayRef,
    ordinal: usize,
    row: usize,
    policy: DecimalOverflowPolicy,
    allow: bool,
) {
    let _ = (policy, allow);
    let pure = r
        .evaluate_row(EvaluatedArgument::Column(a), ordinal, row, &Control)
        .unwrap();
    match expected_bits(a, row) {
        None => assert_eq!(pure, CastRowResult::Null),
        Some(expected) => {
            let CastRowResult::Float64(got) = pure else {
                panic!("exact float output");
            };
            assert_eq!(got.to_bits(), expected);
        }
    }
}

fn complete(wide: bool) {
    let max = if wide { 76 } else { 38 };
    // False->true result widening is separately checked through the same exact profile below.
    for p in 1..=max {
        for scale in i8::MIN..=p as i8 {
            for nullable in [false, true] {
                let a = input(
                    wide,
                    p,
                    scale,
                    if nullable {
                        vec![Some(1), Some(-1), None, Some(i128::MIN), Some(i128::MAX)]
                    } else {
                        vec![Some(1), Some(-1), Some(0), Some(i128::MIN), Some(i128::MAX)]
                    },
                );
                for policy in [
                    DecimalOverflowPolicy::OutputNull,
                    DecimalOverflowPolicy::ReportError,
                ] {
                    for allow in [false, true] {
                        let recipe = r(&a, nullable, policy, allow);
                        for row in 0..a.len() {
                            value(&recipe, &a, row, row, policy, allow);
                        }
                    }
                }
            }
        }
    }
    for policy in [
        DecimalOverflowPolicy::OutputNull,
        DecimalOverflowPolicy::ReportError,
    ] {
        for allow in [false, true] {
            let a = input(wide, max, 2, vec![Some(0), Some(1), None, Some(-1)]).slice(1, 3);
            let recipe = r(&a, true, policy, allow);
            for (ordinal, row) in Selection::try_sparse(3, &[0, 2])
                .unwrap()
                .iter()
                .enumerate()
            {
                value(&recipe, &a, ordinal, row, policy, allow);
            }
            let compact = SelectedValues::try_new(
                Selection::try_sparse(3, &[0, 2]).unwrap(),
                a.data_type(),
                input(wide, max, 2, vec![Some(1), Some(-1)]),
                Box::default(),
            )
            .unwrap();
            for (ordinal, row) in [(0, 0), (1, 2)] {
                let expected = expected_row(&a, row);
                assert_eq!(
                    recipe
                        .evaluate_row(
                            EvaluatedArgument::SelectedColumn(&compact),
                            ordinal,
                            row,
                            &Control
                        )
                        .unwrap(),
                    expected
                );
            }
            let scalar = a.slice(2, 1);
            assert_eq!(
                recipe
                    .evaluate_row(EvaluatedArgument::Scalar(&scalar), 99, 500, &Control)
                    .unwrap(),
                expected_row(&a, 2)
            );
        }
    }
    let a = input(wide, max, 2, vec![Some(12345)]);
    for policy in [
        DecimalOverflowPolicy::OutputNull,
        DecimalOverflowPolicy::ReportError,
    ] {
        for allow in [false, true] {
            let recipe = PreparedCastRecipe::try_new(
                CastOperation::Carrier,
                &FunctionValueType::new(a.data_type().clone(), false),
                &FunctionValueType::new(DataType::Float64, true),
                policy,
                allow,
                None,
                &Control,
            )
            .unwrap();
            value(&recipe, &a, 0, 0, policy, allow);
        }
    }
    if wide {
        for p in [1, 76] {
            let a = input(true, p, i8::MIN, vec![Some(1)]);
            for policy in [
                DecimalOverflowPolicy::OutputNull,
                DecimalOverflowPolicy::ReportError,
            ] {
                for allow in [false, true] {
                    value(&r(&a, false, policy, allow), &a, 0, 0, policy, allow);
                }
            }
        }
        let a: ArrayRef = Arc::new(
            Decimal256Array::new(
                vec![i256::MIN, i256::MAX, i256::MIN].into(),
                Some(NullBuffer::from(vec![true, true, false])),
            )
            .with_precision_and_scale(76, 0)
            .unwrap(),
        );
        for row in 0..3 {
            value(
                &r(&a, true, DecimalOverflowPolicy::ReportError, true),
                &a,
                row,
                row,
                DecimalOverflowPolicy::ReportError,
                true,
            );
        }
    }
}
#[test]
fn decimal_float_oracle_complete_decimal128_float64_profile() {
    complete(false);
}
#[test]
fn decimal_float_oracle_complete_decimal256_float64_profile() {
    complete(true);
}
