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

use novarocks_type_contract::{CallEffects, DecimalOverflowPolicy, ExpressionEffectContext};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExpressionCallClaim {
    pub context: ExpressionEffectContext,
    pub effects: CallEffects,
    /// Authored by this call's SQL scope, independently of effect claims and
    /// environment parameters. Compilation must not infer a package default.
    pub decimal_overflow_policy: DecimalOverflowPolicy,
    /// Exact original emitted source roles and independent use occurrences.
    /// Required only by nominal TemporalSource controls; absence is no fact.
    pub temporal_source: Option<novarocks_type_contract::TemporalSourcePlan<crate::ExprId>>,
    /// Present only for the exact REGEXP_COUNT scalar; Dynamic is positive
    /// authored evidence, never the default for an absent source receipt.
    pub regexp_count_pattern_source: Option<novarocks_type_contract::RegexpCountPatternSource>,
    /// Exact immediate emitted-source receipt; Ordinary must be positively authored.
    pub to_base64_byte_source: Option<novarocks_type_contract::ToBase64ByteSource>,
}
