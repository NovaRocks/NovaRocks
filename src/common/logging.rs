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
use std::fs::{self, OpenOptions};
use std::io;
use std::path::PathBuf;
use std::sync::OnceLock;
use std::sync::{Arc, Mutex};

use chrono::{Datelike, Local, Timelike};
use tracing_subscriber::fmt::MakeWriter;
use tracing_subscriber::fmt::format::{FormatEvent, FormatFields, Writer};
use tracing_subscriber::registry::LookupSpan;
use tracing_subscriber::{EnvFilter, fmt as tracing_fmt};

static INIT: OnceLock<()> = OnceLock::new();

struct StarRocksFormatter;

#[derive(Clone)]
struct SharedFileMakeWriter {
    file: Arc<Mutex<std::fs::File>>,
}

struct SharedFileWriter {
    file: Arc<Mutex<std::fs::File>>,
}

impl<'a> MakeWriter<'a> for SharedFileMakeWriter {
    type Writer = SharedFileWriter;

    fn make_writer(&'a self) -> Self::Writer {
        SharedFileWriter {
            file: Arc::clone(&self.file),
        }
    }
}

impl io::Write for SharedFileWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let mut file = self
            .file
            .lock()
            .map_err(|_| io::Error::other("log file lock poisoned"))?;
        file.write(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        let mut file = self
            .file
            .lock()
            .map_err(|_| io::Error::other("log file lock poisoned"))?;
        file.flush()
    }
}

fn resolve_log_file_path() -> PathBuf {
    if let Ok(log_path) = std::env::var("NOVAROCKS_LOG_FILE") {
        let trimmed = log_path.trim();
        if !trimmed.is_empty() {
            return PathBuf::from(trimmed);
        }
    }

    let log_dir = if let Ok(log_dir) = std::env::var("NOVAROCKS_LOG_DIR") {
        let trimmed = log_dir.trim();
        if !trimmed.is_empty() {
            trimmed.to_string()
        } else {
            "log".to_string()
        }
    } else if let Ok(log_dir) = std::env::var("LOG_DIR") {
        let trimmed = log_dir.trim();
        if !trimmed.is_empty() {
            trimmed.to_string()
        } else {
            "log".to_string()
        }
    } else {
        "log".to_string()
    };

    PathBuf::from(log_dir).join("novarocks.log")
}

fn open_log_writer() -> Option<SharedFileMakeWriter> {
    let log_file_path = resolve_log_file_path();
    if let Some(parent) = log_file_path.parent()
        && let Err(err) = fs::create_dir_all(parent)
    {
        eprintln!(
            "failed to create log directory {}: {}, fallback to stderr",
            parent.display(),
            err
        );
        return None;
    }

    match OpenOptions::new()
        .create(true)
        .append(true)
        .open(&log_file_path)
    {
        Ok(file) => Some(SharedFileMakeWriter {
            file: Arc::new(Mutex::new(file)),
        }),
        Err(err) => {
            eprintln!(
                "failed to open log file {}: {}, fallback to stderr",
                log_file_path.display(),
                err
            );
            None
        }
    }
}

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

        if let Some(make_writer) = open_log_writer() {
            let _ = tracing_fmt()
                .with_env_filter(env_filter)
                .with_writer(make_writer)
                .with_ansi(false)
                .event_format(StarRocksFormatter)
                .try_init();
            return;
        }

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
