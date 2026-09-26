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

//! Pattern-argument compilation microbenchmark.
//!
//! This is a measurement harness, not a latency gate. It evaluates regexp and
//! JSON path functions over one chunk whose pattern argument is either a
//! literal or a low-cardinality column: the shapes where compiling the
//! pattern once per row, rather than once per distinct pattern, dominates the
//! cost of an evaluation.

use std::sync::Arc;
use std::time::Instant;

use arrow::array::{ArrayRef, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use novarocks_execution::exec::chunk::{Chunk, ChunkSchema};
use novarocks_execution::exec::expr::function::matching::eval_regexp;
use novarocks_execution::exec::expr::function::string::eval_string_function;
use novarocks_execution::exec::expr::function::variant::eval_variant_function;
use novarocks_execution::exec::expr::{ExprArena, ExprId, ExprNode, LiteralValue};
use novarocks_types::SlotId;

const TEXT_SLOT: u32 = 1;
const PATTERN_SLOT: u32 = 2;
const JSON_SLOT: u32 = 3;
const DISTINCT_COLUMN_PATTERNS: [&str; 8] = [
    "abc.*",
    "^xyz\\d+",
    "_package_\\d+$",
    "\\d{3}",
    "(abc|xyz)[0-9]+",
    "^[a-z]+\\d",
    "package",
    "z$",
];

#[derive(Clone, Copy, Debug)]
struct Config {
    rows: usize,
    warmup_rounds: usize,
    measurement_rounds: usize,
}

impl Config {
    fn from_env() -> Result<Self, String> {
        let rows = env_usize("NOVAROCKS_BENCH_ROWS", 4_096)?;
        let warmup_rounds = env_usize("NOVAROCKS_BENCH_WARMUP_ROUNDS", 2)?;
        let measurement_rounds = env_usize("NOVAROCKS_BENCH_MEASUREMENT_ROUNDS", 15)?;
        if rows == 0 || measurement_rounds == 0 {
            return Err("rows and measurement rounds must be positive".to_string());
        }
        Ok(Self {
            rows,
            warmup_rounds,
            measurement_rounds,
        })
    }
}

#[derive(Clone, Copy, Debug)]
enum Case {
    RegexpLiteral,
    RegexpLowCardinalityColumn,
    RegexpReplaceLiteral,
    RegexpCountLiteral,
    GetJsonStringLiteralPath,
}

impl Case {
    const ALL: [Self; 5] = [
        Self::RegexpLiteral,
        Self::RegexpLowCardinalityColumn,
        Self::RegexpReplaceLiteral,
        Self::RegexpCountLiteral,
        Self::GetJsonStringLiteralPath,
    ];

    const fn name(self) -> &'static str {
        match self {
            Self::RegexpLiteral => "regexp(text, 'abc.*')",
            Self::RegexpLowCardinalityColumn => "regexp(text, pattern_column[8 distinct])",
            Self::RegexpReplaceLiteral => "regexp_replace(text, '_package_.*', '')",
            Self::RegexpCountLiteral => "regexp_count(text, '[0-9]')",
            Self::GetJsonStringLiteralPath => "get_json_string(json, '$.a.b')",
        }
    }
}

fn main() {
    if let Err(error) = run() {
        eprintln!("regexp_pattern_compile benchmark failed: {error}");
        std::process::exit(1);
    }
}

fn run() -> Result<(), String> {
    let config = Config::from_env()?;
    let chunk = build_chunk(config.rows)?;
    println!(
        "rows_per_chunk={} warmup_rounds={} measurement_rounds={}",
        config.rows, config.warmup_rounds, config.measurement_rounds
    );
    for case in Case::ALL {
        let (arena, expr) = build_case(case);
        for _ in 0..config.warmup_rounds {
            evaluate(case, &arena, &expr, &chunk)?;
        }
        let mut samples = Vec::with_capacity(config.measurement_rounds);
        for _ in 0..config.measurement_rounds {
            let started = Instant::now();
            evaluate(case, &arena, &expr, &chunk)?;
            samples.push(started.elapsed());
        }
        samples.sort_unstable();
        let median = samples[samples.len() / 2];
        println!(
            "{:<44} median={:>12?} min={:>12?} median_per_row={:>10?}",
            case.name(),
            median,
            samples[0],
            median / config.rows as u32
        );
    }
    Ok(())
}

/// Arguments of one case, as expression ids in its arena.
struct CaseExpr {
    args: Vec<ExprId>,
}

fn build_case(case: Case) -> (ExprArena, CaseExpr) {
    let mut arena = ExprArena::default();
    let text = slot(&mut arena, TEXT_SLOT);
    let args = match case {
        Case::RegexpLiteral => vec![text, literal(&mut arena, "abc.*")],
        Case::RegexpLowCardinalityColumn => vec![text, slot(&mut arena, PATTERN_SLOT)],
        Case::RegexpReplaceLiteral => vec![
            text,
            literal(&mut arena, "_package_.*"),
            literal(&mut arena, ""),
        ],
        Case::RegexpCountLiteral => vec![text, literal(&mut arena, "[0-9]")],
        Case::GetJsonStringLiteralPath => {
            vec![slot(&mut arena, JSON_SLOT), literal(&mut arena, "$.a.b")]
        }
    };
    (arena, CaseExpr { args })
}

fn evaluate(case: Case, arena: &ExprArena, expr: &CaseExpr, chunk: &Chunk) -> Result<(), String> {
    let args = expr.args.as_slice();
    let out = match case {
        Case::RegexpLiteral | Case::RegexpLowCardinalityColumn => {
            eval_regexp(arena, args[0], args, chunk)?
        }
        Case::RegexpReplaceLiteral => {
            eval_string_function("regexp_replace", arena, args[0], args, chunk)?
        }
        Case::RegexpCountLiteral => {
            eval_string_function("regexp_count", arena, args[0], args, chunk)?
        }
        Case::GetJsonStringLiteralPath => {
            eval_variant_function("get_json_string", arena, args[0], args, chunk)?
        }
    };
    if out.len() != chunk.len() {
        return Err(format!(
            "{} returned {} rows for a {}-row chunk",
            case.name(),
            out.len(),
            chunk.len()
        ));
    }
    std::hint::black_box(out);
    Ok(())
}

fn build_chunk(rows: usize) -> Result<Chunk, String> {
    let text = (0..rows)
        .map(|i| {
            if i % 2 == 0 {
                format!("abc{i}xyz_package_{}", i % 7)
            } else {
                format!("xyz{i}abc")
            }
        })
        .collect::<Vec<_>>();
    let patterns = (0..rows)
        .map(|i| DISTINCT_COLUMN_PATTERNS[i % DISTINCT_COLUMN_PATTERNS.len()])
        .collect::<Vec<_>>();
    let json = (0..rows)
        .map(|i| format!(r#"{{"a":{{"b":"v{i}"}},"n":{i}}}"#))
        .collect::<Vec<_>>();
    let columns = vec![
        Arc::new(StringArray::from(text)) as ArrayRef,
        Arc::new(StringArray::from(patterns)) as ArrayRef,
        Arc::new(StringArray::from(json)) as ArrayRef,
    ];
    let schema = Arc::new(Schema::new(vec![
        Field::new("text", DataType::Utf8, true),
        Field::new("pattern", DataType::Utf8, true),
        Field::new("json", DataType::Utf8, true),
    ]));
    let batch = RecordBatch::try_new(schema, columns).map_err(|e| e.to_string())?;
    let chunk_schema = ChunkSchema::try_ref_from_schema_and_slot_ids(
        batch.schema().as_ref(),
        &[
            SlotId::new(TEXT_SLOT),
            SlotId::new(PATTERN_SLOT),
            SlotId::new(JSON_SLOT),
        ],
    )?;
    Ok(Chunk::new_with_chunk_schema(batch, chunk_schema))
}

fn slot(arena: &mut ExprArena, slot: u32) -> ExprId {
    arena.push_typed(ExprNode::SlotId(SlotId::new(slot)), DataType::Utf8)
}

fn literal(arena: &mut ExprArena, value: &str) -> ExprId {
    arena.push(ExprNode::Literal(LiteralValue::Utf8(value.to_string())))
}

fn env_usize(name: &str, default: usize) -> Result<usize, String> {
    std::env::var(name)
        .unwrap_or_else(|_| default.to_string())
        .parse::<usize>()
        .map_err(|error| format!("parse {name}: {error}"))
}
