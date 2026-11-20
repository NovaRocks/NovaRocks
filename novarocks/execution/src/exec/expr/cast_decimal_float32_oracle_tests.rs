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

//! Entire admitted Decimal128/256 Float32 profiles with fixed independent IEEE tables.
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
const NARROW_BITS: [[Option<u32>; 3]; 205] = [
    [None, None, None],                                     // scale -128
    [None, None, None],                                     // scale -127
    [None, None, None],                                     // scale -126
    [None, None, None],                                     // scale -125
    [None, None, None],                                     // scale -124
    [None, None, None],                                     // scale -123
    [None, None, None],                                     // scale -122
    [None, None, None],                                     // scale -121
    [None, None, None],                                     // scale -120
    [None, None, None],                                     // scale -119
    [None, None, None],                                     // scale -118
    [None, None, None],                                     // scale -117
    [None, None, None],                                     // scale -116
    [None, None, None],                                     // scale -115
    [None, None, None],                                     // scale -114
    [None, None, None],                                     // scale -113
    [None, None, None],                                     // scale -112
    [None, None, None],                                     // scale -111
    [None, None, None],                                     // scale -110
    [None, None, None],                                     // scale -109
    [None, None, None],                                     // scale -108
    [None, None, None],                                     // scale -107
    [None, None, None],                                     // scale -106
    [None, None, None],                                     // scale -105
    [None, None, None],                                     // scale -104
    [None, None, None],                                     // scale -103
    [None, None, None],                                     // scale -102
    [None, None, None],                                     // scale -101
    [None, None, None],                                     // scale -100
    [None, None, None],                                     // scale -99
    [None, None, None],                                     // scale -98
    [None, None, None],                                     // scale -97
    [None, None, None],                                     // scale -96
    [None, None, None],                                     // scale -95
    [None, None, None],                                     // scale -94
    [None, None, None],                                     // scale -93
    [None, None, None],                                     // scale -92
    [None, None, None],                                     // scale -91
    [None, None, None],                                     // scale -90
    [None, None, None],                                     // scale -89
    [None, None, None],                                     // scale -88
    [None, None, None],                                     // scale -87
    [None, None, None],                                     // scale -86
    [None, None, None],                                     // scale -85
    [None, None, None],                                     // scale -84
    [None, None, None],                                     // scale -83
    [None, None, None],                                     // scale -82
    [None, None, None],                                     // scale -81
    [None, None, None],                                     // scale -80
    [None, None, None],                                     // scale -79
    [None, None, None],                                     // scale -78
    [None, None, None],                                     // scale -77
    [None, None, None],                                     // scale -76
    [None, None, None],                                     // scale -75
    [None, None, None],                                     // scale -74
    [None, None, None],                                     // scale -73
    [None, None, None],                                     // scale -72
    [None, None, None],                                     // scale -71
    [None, None, None],                                     // scale -70
    [None, None, None],                                     // scale -69
    [None, None, None],                                     // scale -68
    [None, None, None],                                     // scale -67
    [None, None, None],                                     // scale -66
    [None, None, None],                                     // scale -65
    [None, None, None],                                     // scale -64
    [None, None, None],                                     // scale -63
    [None, None, None],                                     // scale -62
    [None, None, None],                                     // scale -61
    [None, None, None],                                     // scale -60
    [None, None, None],                                     // scale -59
    [None, None, None],                                     // scale -58
    [None, None, None],                                     // scale -57
    [None, None, None],                                     // scale -56
    [None, None, None],                                     // scale -55
    [None, None, None],                                     // scale -54
    [None, None, None],                                     // scale -53
    [None, None, None],                                     // scale -52
    [None, None, None],                                     // scale -51
    [None, None, None],                                     // scale -50
    [None, None, None],                                     // scale -49
    [None, None, None],                                     // scale -48
    [None, None, None],                                     // scale -47
    [None, None, None],                                     // scale -46
    [None, None, None],                                     // scale -45
    [None, None, None],                                     // scale -44
    [None, None, None],                                     // scale -43
    [None, None, None],                                     // scale -42
    [None, None, None],                                     // scale -41
    [None, None, None],                                     // scale -40
    [None, None, None],                                     // scale -39
    [Some(0x7e967699), None, None],                         // scale -38
    [Some(0x7cf0bdc2), None, None],                         // scale -37
    [Some(0x7b4097ce), None, None],                         // scale -36
    [Some(0x799a130c), None, None],                         // scale -35
    [Some(0x77f684df), None, None],                         // scale -34
    [Some(0x76453719), None, None],                         // scale -33
    [Some(0x749dc5ae), None, None],                         // scale -32
    [Some(0x72fc6f7c), None, None],                         // scale -31
    [Some(0x7149f2ca), None, None],                         // scale -30
    [Some(0x6fa18f08), None, None],                         // scale -29
    [Some(0x6e013f39), None, None],                         // scale -28
    [Some(0x6c4ecb8f), None, None],                         // scale -27
    [Some(0x6aa56fa6), None, None],                         // scale -26
    [Some(0x69045951), None, None],                         // scale -25
    [Some(0x6753c21c), None, None],                         // scale -24
    [Some(0x65a96816), None, None],                         // scale -23
    [Some(0x64078678), None, None],                         // scale -22
    [Some(0x6258d727), None, None],                         // scale -21
    [Some(0x60ad78ec), None, None],                         // scale -20
    [Some(0x5f0ac723), None, None],                         // scale -19
    [Some(0x5d5e0b6b), None, None],                         // scale -18
    [Some(0x5bb1a2bc), None, None],                         // scale -17
    [Some(0x5a0e1bca), None, None],                         // scale -16
    [Some(0x58635fa9), None, None],                         // scale -15
    [Some(0x56b5e621), None, None],                         // scale -14
    [Some(0x551184e7), None, None],                         // scale -13
    [Some(0x5368d4a5), None, None],                         // scale -12
    [Some(0x51ba43b7), None, None],                         // scale -11
    [Some(0x501502f9), None, None],                         // scale -10
    [Some(0x4e6e6b28), None, None],                         // scale -9
    [Some(0x4cbebc20), None, None],                         // scale -8
    [Some(0x4b189680), None, None],                         // scale -7
    [Some(0x49742400), None, None],                         // scale -6
    [Some(0x47c35000), None, None],                         // scale -5
    [Some(0x461c4000), None, None],                         // scale -4
    [Some(0x447a0000), None, None],                         // scale -3
    [Some(0x42c80000), None, None],                         // scale -2
    [Some(0x41200000), None, None],                         // scale -1
    [Some(0x3f800000), Some(0xff000000), Some(0x7f000000)], // scale 0
    [Some(0x3dcccccd), Some(0xfd4ccccd), Some(0x7d4ccccd)], // scale 1
    [Some(0x3c23d70a), Some(0xfba3d70a), Some(0x7ba3d70a)], // scale 2
    [Some(0x3a83126f), Some(0xfa03126f), Some(0x7a03126f)], // scale 3
    [Some(0x38d1b717), Some(0xf851b717), Some(0x7851b717)], // scale 4
    [Some(0x3727c5ac), Some(0xf6a7c5ac), Some(0x76a7c5ac)], // scale 5
    [Some(0x358637bd), Some(0xf50637bd), Some(0x750637bd)], // scale 6
    [Some(0x33d6bf95), Some(0xf356bf95), Some(0x7356bf95)], // scale 7
    [Some(0x322bcc77), Some(0xf1abcc77), Some(0x71abcc77)], // scale 8
    [Some(0x3089705f), Some(0xf009705f), Some(0x7009705f)], // scale 9
    [Some(0x2edbe6ff), Some(0xee5be6ff), Some(0x6e5be6ff)], // scale 10
    [Some(0x2d2febff), Some(0xecafebff), Some(0x6cafebff)], // scale 11
    [Some(0x2b8cbccc), Some(0xeb0cbccc), Some(0x6b0cbccc)], // scale 12
    [Some(0x29e12e13), Some(0xe9612e13), Some(0x69612e13)], // scale 13
    [Some(0x283424dc), Some(0xe7b424dc), Some(0x67b424dc)], // scale 14
    [Some(0x26901d7d), Some(0xe6101d7d), Some(0x66101d7d)], // scale 15
    [Some(0x24e69595), Some(0xe4669595), Some(0x64669595)], // scale 16
    [Some(0x233877aa), Some(0xe2b877aa), Some(0x62b877aa)], // scale 17
    [Some(0x219392ef), Some(0xe11392ef), Some(0x611392ef)], // scale 18
    [Some(0x1fec1e4a), Some(0xdf6c1e4a), Some(0x5f6c1e4a)], // scale 19
    [Some(0x1e3ce508), Some(0xddbce508), Some(0x5dbce508)], // scale 20
    [Some(0x1c971da0), Some(0xdc171da0), Some(0x5c171da0)], // scale 21
    [Some(0x1af1c901), Some(0xda71c901), Some(0x5a71c901)], // scale 22
    [Some(0x19416d9a), Some(0xd8c16d9a), Some(0x58c16d9a)], // scale 23
    [Some(0x179abe15), Some(0xd71abe15), Some(0x571abe15)], // scale 24
    [Some(0x15f79688), Some(0xd5779688), Some(0x55779688)], // scale 25
    [Some(0x14461206), Some(0xd3c61206), Some(0x53c61206)], // scale 26
    [Some(0x129e74d2), Some(0xd21e74d2), Some(0x521e74d2)], // scale 27
    [Some(0x10fd87b6), Some(0xd07d87b6), Some(0x507d87b6)], // scale 28
    [Some(0x0f4ad2f8), Some(0xcecad2f8), Some(0x4ecad2f8)], // scale 29
    [Some(0x0da24260), Some(0xcd224260), Some(0x4d224260)], // scale 30
    [Some(0x0c01ceb3), Some(0xcb81ceb3), Some(0x4b81ceb3)], // scale 31
    [Some(0x0a4fb11f), Some(0xc9cfb11f), Some(0x49cfb11f)], // scale 32
    [Some(0x08a6274c), Some(0xc826274c), Some(0x4826274c)], // scale 33
    [Some(0x0704ec3d), Some(0xc684ec3d), Some(0x4684ec3d)], // scale 34
    [Some(0x0554ad2e), Some(0xc4d4ad2e), Some(0x44d4ad2e)], // scale 35
    [Some(0x03aa2425), Some(0xc32a2425), Some(0x432a2425)], // scale 36
    [Some(0x02081cea), Some(0xc1881cea), Some(0x41881cea)], // scale 37
    [Some(0x006ce3ee), Some(0xbfd9c7dd), Some(0x3fd9c7dd)], // scale 38
    [Some(0x000ae398), Some(0xbe2e397e), Some(0x3e2e397e)], // scale 39
    [Some(0x000116c2), Some(0xbc8b6131), Some(0x3c8b6131)], // scale 40
    [Some(0x00001be0), Some(0xbadf01e8), Some(0x3adf01e8)], // scale 41
    [Some(0x000002ca), Some(0xb93267ed), Some(0x393267ed)], // scale 42
    [Some(0x00000047), Some(0xb78eb98a), Some(0x378eb98a)], // scale 43
    [Some(0x00000007), Some(0xb5e45c11), Some(0x35e45c11)], // scale 44
    [Some(0x00000001), Some(0xb436b00d), Some(0x3436b00d)], // scale 45
    [Some(0x00000000), Some(0xb2922671), Some(0x32922671)], // scale 46
    [Some(0x00000000), Some(0xb0e9d71b), Some(0x30e9d71b)], // scale 47
    [Some(0x00000000), Some(0xaf3b127c), Some(0x2f3b127c)], // scale 48
    [Some(0x00000000), Some(0xad95a863), Some(0x2d95a863)], // scale 49
    [Some(0x00000000), Some(0xabef73d2), Some(0x2bef73d2)], // scale 50
    [Some(0x00000000), Some(0xaa3f8fdb), Some(0x2a3f8fdb)], // scale 51
    [Some(0x00000000), Some(0xa8993fe3), Some(0x28993fe3)], // scale 52
    [Some(0x00000000), Some(0xa6f53304), Some(0x26f53304)], // scale 53
    [Some(0x00000000), Some(0xa54428d0), Some(0x254428d0)], // scale 54
    [Some(0x00000000), Some(0xa39ced73), Some(0x239ced73)], // scale 55
    [Some(0x00000000), Some(0xa1fb1586), Some(0x21fb1586)], // scale 56
    [Some(0x00000000), Some(0xa048de04), Some(0x2048de04)], // scale 57
    [Some(0x00000000), Some(0x9ea0b19d), Some(0x1ea0b19d)], // scale 58
    [Some(0x00000000), Some(0x9d008e17), Some(0x1d008e17)], // scale 59
    [Some(0x00000000), Some(0x9b4db025), Some(0x1b4db025)], // scale 60
    [Some(0x00000000), Some(0x99a48ceb), Some(0x19a48ceb)], // scale 61
    [Some(0x00000000), Some(0x9803a3ef), Some(0x1803a3ef)], // scale 62
    [Some(0x00000000), Some(0x96529fe5), Some(0x16529fe5)], // scale 63
    [Some(0x00000000), Some(0x94a87fea), Some(0x14a87fea)], // scale 64
    [Some(0x00000000), Some(0x9306ccbb), Some(0x1306ccbb)], // scale 65
    [Some(0x00000000), Some(0x9157adf9), Some(0x1157adf9)], // scale 66
    [Some(0x00000000), Some(0x8fac8b2d), Some(0x0fac8b2d)], // scale 67
    [Some(0x00000000), Some(0x8e0a08f1), Some(0x0e0a08f1)], // scale 68
    [Some(0x00000000), Some(0x8c5cdb1b), Some(0x0c5cdb1b)], // scale 69
    [Some(0x00000000), Some(0x8ab0af49), Some(0x0ab0af49)], // scale 70
    [Some(0x00000000), Some(0x890d5907), Some(0x090d5907)], // scale 71
    [Some(0x00000000), Some(0x8762280b), Some(0x0762280b)], // scale 72
    [Some(0x00000000), Some(0x85b4ecd6), Some(0x05b4ecd6)], // scale 73
    [Some(0x00000000), Some(0x8410bd78), Some(0x0410bd78)], // scale 74
    [Some(0x00000000), Some(0x8267958d), Some(0x0267958d)], // scale 75
    [Some(0x00000000), Some(0x80b94471), Some(0x00b94471)], // scale 76
];
const WIDE_BITS: [[Option<u32>; 3]; 205] = [
    [Some(0x7f800000), Some(0xff800000), Some(0x7f800000)], // scale -128
    [Some(0x7f800000), Some(0xff800000), Some(0x7f800000)], // scale -127
    [Some(0x7f800000), Some(0xff800000), Some(0x7f800000)], // scale -126
    [Some(0x7f800000), Some(0xff800000), Some(0x7f800000)], // scale -125
    [Some(0x7f800000), Some(0xff800000), Some(0x7f800000)], // scale -124
    [Some(0x7f800000), Some(0xff800000), Some(0x7f800000)], // scale -123
    [Some(0x7f800000), Some(0xff800000), Some(0x7f800000)], // scale -122
    [Some(0x7f800000), Some(0xff800000), Some(0x7f800000)], // scale -121
    [Some(0x7f800000), Some(0xff800000), Some(0x7f800000)], // scale -120
    [Some(0x7f800000), Some(0xff800000), Some(0x7f800000)], // scale -119
    [Some(0x7f800000), Some(0xff800000), Some(0x7f800000)], // scale -118
    [Some(0x7f800000), Some(0xff800000), Some(0x7f800000)], // scale -117
    [Some(0x7f800000), Some(0xff800000), Some(0x7f800000)], // scale -116
    [Some(0x7f800000), Some(0xff800000), Some(0x7f800000)], // scale -115
    [Some(0x7f800000), Some(0xff800000), Some(0x7f800000)], // scale -114
    [Some(0x7f800000), Some(0xff800000), Some(0x7f800000)], // scale -113
    [Some(0x7f800000), Some(0xff800000), Some(0x7f800000)], // scale -112
    [Some(0x7f800000), Some(0xff800000), Some(0x7f800000)], // scale -111
    [Some(0x7f800000), Some(0xff800000), Some(0x7f800000)], // scale -110
    [Some(0x7f800000), Some(0xff800000), Some(0x7f800000)], // scale -109
    [Some(0x7f800000), Some(0xff800000), Some(0x7f800000)], // scale -108
    [Some(0x7f800000), Some(0xff800000), Some(0x7f800000)], // scale -107
    [Some(0x7f800000), Some(0xff800000), Some(0x7f800000)], // scale -106
    [Some(0x7f800000), Some(0xff800000), Some(0x7f800000)], // scale -105
    [Some(0x7f800000), Some(0xff800000), Some(0x7f800000)], // scale -104
    [Some(0x7f800000), Some(0xff800000), Some(0x7f800000)], // scale -103
    [Some(0x7f800000), Some(0xff800000), Some(0x7f800000)], // scale -102
    [Some(0x7f800000), Some(0xff800000), Some(0x7f800000)], // scale -101
    [Some(0x7f800000), Some(0xff800000), Some(0x7f800000)], // scale -100
    [Some(0x7f800000), Some(0xff800000), Some(0x7f800000)], // scale -99
    [Some(0x7f800000), Some(0xff800000), Some(0x7f800000)], // scale -98
    [Some(0x7f800000), Some(0xff800000), Some(0x7f800000)], // scale -97
    [Some(0x7f800000), Some(0xff800000), Some(0x7f800000)], // scale -96
    [Some(0x7f800000), Some(0xff800000), Some(0x7f800000)], // scale -95
    [Some(0x7f800000), Some(0xff800000), Some(0x7f800000)], // scale -94
    [Some(0x7f800000), Some(0xff800000), Some(0x7f800000)], // scale -93
    [Some(0x7f800000), Some(0xff800000), Some(0x7f800000)], // scale -92
    [Some(0x7f800000), Some(0xff800000), Some(0x7f800000)], // scale -91
    [Some(0x7f800000), Some(0xff800000), Some(0x7f800000)], // scale -90
    [Some(0x7f800000), Some(0xff800000), Some(0x7f800000)], // scale -89
    [Some(0x7f800000), Some(0xff800000), Some(0x7f800000)], // scale -88
    [Some(0x7f800000), Some(0xff800000), Some(0x7f800000)], // scale -87
    [Some(0x7f800000), Some(0xff800000), Some(0x7f800000)], // scale -86
    [Some(0x7f800000), Some(0xff800000), Some(0x7f800000)], // scale -85
    [Some(0x7f800000), Some(0xff800000), Some(0x7f800000)], // scale -84
    [Some(0x7f800000), Some(0xff800000), Some(0x7f800000)], // scale -83
    [Some(0x7f800000), Some(0xff800000), Some(0x7f800000)], // scale -82
    [Some(0x7f800000), Some(0xff800000), Some(0x7f800000)], // scale -81
    [Some(0x7f800000), Some(0xff800000), Some(0x7f800000)], // scale -80
    [Some(0x7f800000), Some(0xff800000), Some(0x7f800000)], // scale -79
    [Some(0x7f800000), Some(0xff800000), Some(0x7f800000)], // scale -78
    [Some(0x7f800000), Some(0xff800000), Some(0x7f800000)], // scale -77
    [Some(0x7f800000), Some(0xff800000), Some(0x7f800000)], // scale -76
    [Some(0x7f800000), Some(0xff800000), Some(0x7f800000)], // scale -75
    [Some(0x7f800000), Some(0xff800000), Some(0x7f800000)], // scale -74
    [Some(0x7f800000), Some(0xff800000), Some(0x7f800000)], // scale -73
    [Some(0x7f800000), Some(0xff800000), Some(0x7f800000)], // scale -72
    [Some(0x7f800000), Some(0xff800000), Some(0x7f800000)], // scale -71
    [Some(0x7f800000), Some(0xff800000), Some(0x7f800000)], // scale -70
    [Some(0x7f800000), Some(0xff800000), Some(0x7f800000)], // scale -69
    [Some(0x7f800000), Some(0xff800000), Some(0x7f800000)], // scale -68
    [Some(0x7f800000), Some(0xff800000), Some(0x7f800000)], // scale -67
    [Some(0x7f800000), Some(0xff800000), Some(0x7f800000)], // scale -66
    [Some(0x7f800000), Some(0xff800000), Some(0x7f800000)], // scale -65
    [Some(0x7f800000), Some(0xff800000), Some(0x7f800000)], // scale -64
    [Some(0x7f800000), Some(0xff800000), Some(0x7f800000)], // scale -63
    [Some(0x7f800000), Some(0xff800000), Some(0x7f800000)], // scale -62
    [Some(0x7f800000), Some(0xff800000), Some(0x7f800000)], // scale -61
    [Some(0x7f800000), Some(0xff800000), Some(0x7f800000)], // scale -60
    [Some(0x7f800000), Some(0xff800000), Some(0x7f800000)], // scale -59
    [Some(0x7f800000), Some(0xff800000), Some(0x7f800000)], // scale -58
    [Some(0x7f800000), Some(0xff800000), Some(0x7f800000)], // scale -57
    [Some(0x7f800000), Some(0xff800000), Some(0x7f800000)], // scale -56
    [Some(0x7f800000), Some(0xff800000), Some(0x7f800000)], // scale -55
    [Some(0x7f800000), Some(0xff800000), Some(0x7f800000)], // scale -54
    [Some(0x7f800000), Some(0xff800000), Some(0x7f800000)], // scale -53
    [Some(0x7f800000), Some(0xff800000), Some(0x7f800000)], // scale -52
    [Some(0x7f800000), Some(0xff800000), Some(0x7f800000)], // scale -51
    [Some(0x7f800000), Some(0xff800000), Some(0x7f800000)], // scale -50
    [Some(0x7f800000), Some(0xff800000), Some(0x7f800000)], // scale -49
    [Some(0x7f800000), Some(0xff800000), Some(0x7f800000)], // scale -48
    [Some(0x7f800000), Some(0xff800000), Some(0x7f800000)], // scale -47
    [Some(0x7f800000), Some(0xff800000), Some(0x7f800000)], // scale -46
    [Some(0x7f800000), Some(0xff800000), Some(0x7f800000)], // scale -45
    [Some(0x7f800000), Some(0xff800000), Some(0x7f800000)], // scale -44
    [Some(0x7f800000), Some(0xff800000), Some(0x7f800000)], // scale -43
    [Some(0x7f800000), Some(0xff800000), Some(0x7f800000)], // scale -42
    [Some(0x7f800000), Some(0xff800000), Some(0x7f800000)], // scale -41
    [Some(0x7f800000), Some(0xff800000), Some(0x7f800000)], // scale -40
    [Some(0x7f800000), Some(0xff800000), Some(0x7f800000)], // scale -39
    [Some(0x7e967699), Some(0xff800000), Some(0x7f800000)], // scale -38
    [Some(0x7cf0bdc2), Some(0xff800000), Some(0x7f800000)], // scale -37
    [Some(0x7b4097ce), Some(0xff800000), Some(0x7f800000)], // scale -36
    [Some(0x799a130c), Some(0xff800000), Some(0x7f800000)], // scale -35
    [Some(0x77f684df), Some(0xff800000), Some(0x7f800000)], // scale -34
    [Some(0x76453719), Some(0xff800000), Some(0x7f800000)], // scale -33
    [Some(0x749dc5ae), Some(0xff800000), Some(0x7f800000)], // scale -32
    [Some(0x72fc6f7c), Some(0xff800000), Some(0x7f800000)], // scale -31
    [Some(0x7149f2ca), Some(0xff800000), Some(0x7f800000)], // scale -30
    [Some(0x6fa18f08), Some(0xff800000), Some(0x7f800000)], // scale -29
    [Some(0x6e013f39), Some(0xff800000), Some(0x7f800000)], // scale -28
    [Some(0x6c4ecb8f), Some(0xff800000), Some(0x7f800000)], // scale -27
    [Some(0x6aa56fa6), Some(0xff800000), Some(0x7f800000)], // scale -26
    [Some(0x69045951), Some(0xff800000), Some(0x7f800000)], // scale -25
    [Some(0x6753c21c), Some(0xff800000), Some(0x7f800000)], // scale -24
    [Some(0x65a96816), Some(0xff800000), Some(0x7f800000)], // scale -23
    [Some(0x64078678), Some(0xff800000), Some(0x7f800000)], // scale -22
    [Some(0x6258d727), Some(0xff800000), Some(0x7f800000)], // scale -21
    [Some(0x60ad78ec), Some(0xff800000), Some(0x7f800000)], // scale -20
    [Some(0x5f0ac723), Some(0xff800000), Some(0x7f800000)], // scale -19
    [Some(0x5d5e0b6b), Some(0xff800000), Some(0x7f800000)], // scale -18
    [Some(0x5bb1a2bc), Some(0xff800000), Some(0x7f800000)], // scale -17
    [Some(0x5a0e1bca), Some(0xff800000), Some(0x7f800000)], // scale -16
    [Some(0x58635fa9), Some(0xff800000), Some(0x7f800000)], // scale -15
    [Some(0x56b5e621), Some(0xff800000), Some(0x7f800000)], // scale -14
    [Some(0x551184e7), Some(0xff800000), Some(0x7f800000)], // scale -13
    [Some(0x5368d4a5), Some(0xff800000), Some(0x7f800000)], // scale -12
    [Some(0x51ba43b7), Some(0xff800000), Some(0x7f800000)], // scale -11
    [Some(0x501502f9), Some(0xff800000), Some(0x7f800000)], // scale -10
    [Some(0x4e6e6b28), Some(0xff800000), Some(0x7f800000)], // scale -9
    [Some(0x4cbebc20), Some(0xff800000), Some(0x7f800000)], // scale -8
    [Some(0x4b189680), Some(0xff800000), Some(0x7f800000)], // scale -7
    [Some(0x49742400), Some(0xff800000), Some(0x7f800000)], // scale -6
    [Some(0x47c35000), Some(0xff800000), Some(0x7f800000)], // scale -5
    [Some(0x461c4000), Some(0xff800000), Some(0x7f800000)], // scale -4
    [Some(0x447a0000), Some(0xff800000), Some(0x7f800000)], // scale -3
    [Some(0x42c80000), Some(0xff800000), Some(0x7f800000)], // scale -2
    [Some(0x41200000), Some(0xff800000), Some(0x7f800000)], // scale -1
    [Some(0x3f800000), Some(0xff000000), Some(0x7f000000)], // scale 0
    [Some(0x3dcccccd), Some(0xfd4ccccd), Some(0x7d4ccccd)], // scale 1
    [Some(0x3c23d70a), Some(0xfba3d70a), Some(0x7ba3d70a)], // scale 2
    [Some(0x3a83126f), Some(0xfa03126f), Some(0x7a03126f)], // scale 3
    [Some(0x38d1b717), Some(0xf851b717), Some(0x7851b717)], // scale 4
    [Some(0x3727c5ac), Some(0xf6a7c5ac), Some(0x76a7c5ac)], // scale 5
    [Some(0x358637bd), Some(0xf50637bd), Some(0x750637bd)], // scale 6
    [Some(0x33d6bf95), Some(0xf356bf95), Some(0x7356bf95)], // scale 7
    [Some(0x322bcc77), Some(0xf1abcc77), Some(0x71abcc77)], // scale 8
    [Some(0x3089705f), Some(0xf009705f), Some(0x7009705f)], // scale 9
    [Some(0x2edbe6ff), Some(0xee5be6ff), Some(0x6e5be6ff)], // scale 10
    [Some(0x2d2febff), Some(0xecafebff), Some(0x6cafebff)], // scale 11
    [Some(0x2b8cbccc), Some(0xeb0cbccc), Some(0x6b0cbccc)], // scale 12
    [Some(0x29e12e13), Some(0xe9612e13), Some(0x69612e13)], // scale 13
    [Some(0x283424dc), Some(0xe7b424dc), Some(0x67b424dc)], // scale 14
    [Some(0x26901d7d), Some(0xe6101d7d), Some(0x66101d7d)], // scale 15
    [Some(0x24e69595), Some(0xe4669595), Some(0x64669595)], // scale 16
    [Some(0x233877aa), Some(0xe2b877aa), Some(0x62b877aa)], // scale 17
    [Some(0x219392ef), Some(0xe11392ef), Some(0x611392ef)], // scale 18
    [Some(0x1fec1e4a), Some(0xdf6c1e4a), Some(0x5f6c1e4a)], // scale 19
    [Some(0x1e3ce508), Some(0xddbce508), Some(0x5dbce508)], // scale 20
    [Some(0x1c971da0), Some(0xdc171da0), Some(0x5c171da0)], // scale 21
    [Some(0x1af1c901), Some(0xda71c901), Some(0x5a71c901)], // scale 22
    [Some(0x19416d9a), Some(0xd8c16d9a), Some(0x58c16d9a)], // scale 23
    [Some(0x179abe15), Some(0xd71abe15), Some(0x571abe15)], // scale 24
    [Some(0x15f79688), Some(0xd5779688), Some(0x55779688)], // scale 25
    [Some(0x14461206), Some(0xd3c61206), Some(0x53c61206)], // scale 26
    [Some(0x129e74d2), Some(0xd21e74d2), Some(0x521e74d2)], // scale 27
    [Some(0x10fd87b6), Some(0xd07d87b6), Some(0x507d87b6)], // scale 28
    [Some(0x0f4ad2f8), Some(0xcecad2f8), Some(0x4ecad2f8)], // scale 29
    [Some(0x0da24260), Some(0xcd224260), Some(0x4d224260)], // scale 30
    [Some(0x0c01ceb3), Some(0xcb81ceb3), Some(0x4b81ceb3)], // scale 31
    [Some(0x0a4fb11f), Some(0xc9cfb11f), Some(0x49cfb11f)], // scale 32
    [Some(0x08a6274c), Some(0xc826274c), Some(0x4826274c)], // scale 33
    [Some(0x0704ec3d), Some(0xc684ec3d), Some(0x4684ec3d)], // scale 34
    [Some(0x0554ad2e), Some(0xc4d4ad2e), Some(0x44d4ad2e)], // scale 35
    [Some(0x03aa2425), Some(0xc32a2425), Some(0x432a2425)], // scale 36
    [Some(0x02081cea), Some(0xc1881cea), Some(0x41881cea)], // scale 37
    [Some(0x006ce3ee), Some(0xbfd9c7dd), Some(0x3fd9c7dd)], // scale 38
    [Some(0x000ae398), Some(0xbe2e397e), Some(0x3e2e397e)], // scale 39
    [Some(0x000116c2), Some(0xbc8b6131), Some(0x3c8b6131)], // scale 40
    [Some(0x00001be0), Some(0xbadf01e8), Some(0x3adf01e8)], // scale 41
    [Some(0x000002ca), Some(0xb93267ed), Some(0x393267ed)], // scale 42
    [Some(0x00000047), Some(0xb78eb98a), Some(0x378eb98a)], // scale 43
    [Some(0x00000007), Some(0xb5e45c11), Some(0x35e45c11)], // scale 44
    [Some(0x00000001), Some(0xb436b00d), Some(0x3436b00d)], // scale 45
    [Some(0x00000000), Some(0xb2922671), Some(0x32922671)], // scale 46
    [Some(0x00000000), Some(0xb0e9d71b), Some(0x30e9d71b)], // scale 47
    [Some(0x00000000), Some(0xaf3b127c), Some(0x2f3b127c)], // scale 48
    [Some(0x00000000), Some(0xad95a863), Some(0x2d95a863)], // scale 49
    [Some(0x00000000), Some(0xabef73d2), Some(0x2bef73d2)], // scale 50
    [Some(0x00000000), Some(0xaa3f8fdb), Some(0x2a3f8fdb)], // scale 51
    [Some(0x00000000), Some(0xa8993fe3), Some(0x28993fe3)], // scale 52
    [Some(0x00000000), Some(0xa6f53304), Some(0x26f53304)], // scale 53
    [Some(0x00000000), Some(0xa54428d0), Some(0x254428d0)], // scale 54
    [Some(0x00000000), Some(0xa39ced73), Some(0x239ced73)], // scale 55
    [Some(0x00000000), Some(0xa1fb1586), Some(0x21fb1586)], // scale 56
    [Some(0x00000000), Some(0xa048de04), Some(0x2048de04)], // scale 57
    [Some(0x00000000), Some(0x9ea0b19d), Some(0x1ea0b19d)], // scale 58
    [Some(0x00000000), Some(0x9d008e17), Some(0x1d008e17)], // scale 59
    [Some(0x00000000), Some(0x9b4db025), Some(0x1b4db025)], // scale 60
    [Some(0x00000000), Some(0x99a48ceb), Some(0x19a48ceb)], // scale 61
    [Some(0x00000000), Some(0x9803a3ef), Some(0x1803a3ef)], // scale 62
    [Some(0x00000000), Some(0x96529fe5), Some(0x16529fe5)], // scale 63
    [Some(0x00000000), Some(0x94a87fea), Some(0x14a87fea)], // scale 64
    [Some(0x00000000), Some(0x9306ccbb), Some(0x1306ccbb)], // scale 65
    [Some(0x00000000), Some(0x9157adf9), Some(0x1157adf9)], // scale 66
    [Some(0x00000000), Some(0x8fac8b2d), Some(0x0fac8b2d)], // scale 67
    [Some(0x00000000), Some(0x8e0a08f1), Some(0x0e0a08f1)], // scale 68
    [Some(0x00000000), Some(0x8c5cdb1b), Some(0x0c5cdb1b)], // scale 69
    [Some(0x00000000), Some(0x8ab0af49), Some(0x0ab0af49)], // scale 70
    [Some(0x00000000), Some(0x890d5907), Some(0x090d5907)], // scale 71
    [Some(0x00000000), Some(0x8762280b), Some(0x0762280b)], // scale 72
    [Some(0x00000000), Some(0x85b4ecd6), Some(0x05b4ecd6)], // scale 73
    [Some(0x00000000), Some(0x8410bd78), Some(0x0410bd78)], // scale 74
    [Some(0x00000000), Some(0x8267958d), Some(0x0267958d)], // scale 75
    [Some(0x00000000), Some(0x80b94471), Some(0x00b94471)], // scale 76
];
fn expected_bits(a: &ArrayRef, row: usize) -> Option<u32> {
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
                return Some(0xff800000);
            }
            if raw == i256::MAX {
                assert_eq!(*scale, 0);
                return Some(0x7f800000);
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
        -1 => row_bits[0].map(|bits| bits ^ 0x80000000),
        i128::MIN => row_bits[1],
        i128::MAX => row_bits[2],
        12345 => {
            assert_eq!(scale, 2);
            Some(0x42f6e666)
        }
        _ => panic!("fixed decimal coefficient needs an explicit table entry"),
    }
}
fn expected_row(a: &ArrayRef, row: usize) -> CastRowResult {
    expected_bits(a, row).map_or(CastRowResult::Null, |bits| {
        CastRowResult::Float32(f32::from_bits(bits))
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
        &FunctionValueType::new(
            DataType::Float32,
            nullable || matches!(a.data_type(), DataType::Decimal128(_, s) if *s < 0),
        ),
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
            let CastRowResult::Float32(got) = pure else {
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
                &FunctionValueType::new(DataType::Float32, true),
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
fn decimal_float32_oracle_complete_decimal128_float32_profile() {
    complete(false);
}
#[test]
fn decimal_float32_oracle_complete_decimal256_float32_profile() {
    complete(true);
}
