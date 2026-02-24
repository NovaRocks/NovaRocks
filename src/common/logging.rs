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
use std::fmt;
use std::sync::OnceLock;

use chrono::{Datelike, Local, Timelike};
use tracing_subscriber::fmt::format::{FormatEvent, FormatFields, Writer};
use tracing_subscriber::registry::LookupSpan;
use tracing_subscriber::{EnvFilter, fmt as tracing_fmt};

static INIT: OnceLock<()> = OnceLock::new();

struct StarRocksFormatter;

impl<S, N> FormatEvent<S, N> for StarRocksFormatter
where
    S: tracing::Subscriber + for<'a> LookupSpan<'a>,
    N: for<'a> FormatFields<'a> + 'static,
{
    fn format_event(
        &self,
        ctx: &tracing_fmt::FmtContext<'_, S, N>,
        mut writer: Writer<'_>,
        event: &tracing::Event<'_>,
    ) -> fmt::Result {
        let metadata = event.metadata();

        // Level (single character like glog)
        let level_char = match *metadata.level() {
            tracing::Level::ERROR => 'E',
            tracing::Level::WARN => 'W',
            tracing::Level::INFO => 'I',
            tracing::Level::DEBUG => 'D',
            tracing::Level::TRACE => 'T',
        };

        // Timestamp (Yyyyymmdd HH:MM:SS.microseconds)
        let now = Local::now();
        let timestamp = format!(
            "{}{:02}{:02} {:02}:{:02}:{:02}.{:06}",
            now.year() % 10000,
            now.month(),
            now.day(),
            now.hour(),
            now.minute(),
            now.second(),
            now.timestamp_subsec_micros()
        );

        // Thread ID
        let thread_id = format!("{:?}", std::thread::current().id())
            .trim_start_matches("ThreadId(")
            .trim_end_matches(")")
            .parse::<u64>()
            .unwrap_or(0);

        // File and line
        let file = metadata.file().unwrap_or("unknown");
        let line = metadata.line().unwrap_or(0);

        // Write in glog format: Lyyyymmdd hh:mm:ss.uuuuuu threadid file:line] message
        write!(
            writer,
            "{}{} {} {}:{}] ",
            level_char, timestamp, thread_id, file, line
        )?;

        // Write the message fields
        ctx.field_format().format_fields(writer.by_ref(), event)?;

        writeln!(writer)
    }
}

pub fn init_with_level(level: &str) {
    INIT.get_or_init(|| {
        // Build filter directly from the provided level/filter string.
        // The caller (via config) is responsible for specifying per-target filters,
        // such as disabling verbose system libraries like h2/hyper/tonic.
        let env_filter = EnvFilter::new(level);

        // Auto-detect if stderr is a TTY (terminal) to decide whether to use ANSI colors
        // If stderr is redirected to a file, ANSI codes would appear as garbage
        let use_ansi = atty::is(atty::Stream::Stderr);
        let _ = tracing_fmt()
            .with_env_filter(env_filter)
            .with_writer(std::io::stderr)
            .with_ansi(use_ansi)
            .event_format(StarRocksFormatter)
            .try_init();
    });
}

pub fn init() {
    init_with_level("info");
}

pub use tracing::instrument;
pub use tracing::{debug, error, info, trace, warn};
