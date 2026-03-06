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
use std::fs::{self, File, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::OnceLock;
use std::sync::{Arc, Mutex};

use chrono::{DateTime, Datelike, Local, Timelike};
use tracing::Level;
use tracing_subscriber::filter::filter_fn;
use tracing_subscriber::fmt::MakeWriter;
use tracing_subscriber::fmt::format::{FormatEvent, FormatFields, Writer};
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::prelude::*;
use tracing_subscriber::registry::LookupSpan;
use tracing_subscriber::{EnvFilter, Registry, fmt as tracing_fmt};

use crate::novarocks_config;

static INIT: OnceLock<()> = OnceLock::new();

const DEFAULT_BASENAME: &str = "novarocks";
const DEFAULT_LOG_DIR: &str = "log";
const DEFAULT_ROLL_MODE: &str = "SIZE-MB-1024";
const DEFAULT_ROLL_NUM: usize = 10;

#[derive(Clone, Copy)]
struct StarRocksFormatter;

#[derive(Clone, Debug)]
struct LogSettings {
    dir: PathBuf,
    basename: String,
    roll_mode: RollMode,
    roll_num: usize,
}

#[derive(Clone, Debug)]
enum RollMode {
    TimeDay,
    TimeHour,
    SizeBytes(u64),
}

#[derive(Clone, Copy, Debug)]
enum SeverityFile {
    Info,
    Warning,
    Error,
}

#[derive(Clone)]
struct RotatingFileMakeWriter {
    state: Arc<Mutex<RotatingFileSet>>,
}

struct RotatingFileWriter {
    state: Arc<Mutex<RotatingFileSet>>,
    buffer: Vec<u8>,
}

struct RotatingFileSet {
    active_path: PathBuf,
    mode: RollMode,
    roll_num: usize,
    file: Option<File>,
    current_size: u64,
    current_period_key: String,
}

struct LevelWriters {
    info: RotatingFileMakeWriter,
    warning: RotatingFileMakeWriter,
    error: RotatingFileMakeWriter,
}

impl RollMode {
    fn parse(raw: &str) -> Self {
        let trimmed = raw.trim();
        if trimmed.eq_ignore_ascii_case("TIME-DAY") {
            return Self::TimeDay;
        }
        if trimmed.eq_ignore_ascii_case("TIME-HOUR") {
            return Self::TimeHour;
        }
        let prefix = "SIZE-MB-";
        if trimmed.len() >= prefix.len()
            && trimmed[..prefix.len()].eq_ignore_ascii_case(prefix)
            && let Ok(size_mb) = trimmed[prefix.len()..].parse::<u64>()
            && size_mb > 0
        {
            return Self::SizeBytes(size_mb * 1024 * 1024);
        }
        eprintln!(
            "invalid sys_log_roll_mode='{}', fallback to {}",
            raw, DEFAULT_ROLL_MODE
        );
        Self::SizeBytes(1024 * 1024 * 1024)
    }

    fn period_key(&self, dt: DateTime<Local>) -> String {
        match self {
            Self::TimeDay => dt.format("%Y%m%d").to_string(),
            Self::TimeHour => dt.format("%Y%m%d%H").to_string(),
            Self::SizeBytes(_) => String::new(),
        }
    }

    fn archive_name(
        &self,
        active_name: &str,
        current_period_key: &str,
        now: DateTime<Local>,
    ) -> String {
        match self {
            Self::TimeDay | Self::TimeHour => {
                format!("{active_name}.log.{current_period_key}")
            }
            Self::SizeBytes(_) => {
                format!("{}.log.{}", active_name, now.format("%Y%m%d-%H%M%S-%6f"))
            }
        }
    }
}

impl SeverityFile {
    fn suffix(self) -> &'static str {
        match self {
            Self::Info => "INFO",
            Self::Warning => "WARNING",
            Self::Error => "ERROR",
        }
    }
}

impl LogSettings {
    fn from_config_and_env() -> Self {
        let cfg = novarocks_config::config().ok();
        let dir = std::env::var("NOVAROCKS_LOG_DIR")
            .ok()
            .filter(|v| !v.trim().is_empty())
            .or_else(|| {
                std::env::var("LOG_DIR")
                    .ok()
                    .filter(|v| !v.trim().is_empty())
            })
            .or_else(|| cfg.as_ref().map(|c| c.sys_log_dir.clone()))
            .unwrap_or_else(|| DEFAULT_LOG_DIR.to_string());
        let basename = std::env::var("NOVAROCKS_LOG_BASENAME")
            .ok()
            .filter(|v| !v.trim().is_empty())
            .map(|v| v.trim().to_string())
            .or_else(|| {
                std::env::var("NOVAROCKS_LOG_FILE")
                    .ok()
                    .and_then(|raw| derive_basename_from_legacy_path(Path::new(raw.trim())))
            })
            .unwrap_or_else(|| DEFAULT_BASENAME.to_string());
        let roll_mode = std::env::var("NOVAROCKS_LOG_ROLL_MODE")
            .ok()
            .filter(|v| !v.trim().is_empty())
            .or_else(|| cfg.as_ref().map(|c| c.sys_log_roll_mode.clone()))
            .map(|v| RollMode::parse(&v))
            .unwrap_or_else(|| RollMode::parse(DEFAULT_ROLL_MODE));
        let roll_num = std::env::var("NOVAROCKS_LOG_ROLL_NUM")
            .ok()
            .and_then(|v| v.trim().parse::<usize>().ok())
            .or_else(|| cfg.as_ref().map(|c| c.sys_log_roll_num))
            .unwrap_or(DEFAULT_ROLL_NUM)
            .max(1);

        Self {
            dir: PathBuf::from(dir),
            basename,
            roll_mode,
            roll_num,
        }
    }

    fn active_log_path(&self, severity: SeverityFile) -> PathBuf {
        self.dir
            .join(format!("{}.{}", self.basename, severity.suffix()))
    }

    fn stdout_log_path(&self) -> PathBuf {
        self.dir.join(format!("{}.out", self.basename))
    }
}

impl RotatingFileSet {
    fn new(settings: &LogSettings, severity: SeverityFile) -> io::Result<Self> {
        fs::create_dir_all(&settings.dir)?;
        let active_path = settings.active_log_path(severity);
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&active_path)?;
        let current_size = file.metadata().map(|m| m.len()).unwrap_or(0);
        let current_period_key = settings
            .roll_mode
            .period_key(file_modified_local(&active_path));
        Ok(Self {
            active_path,
            mode: settings.roll_mode.clone(),
            roll_num: settings.roll_num,
            file: Some(file),
            current_size,
            current_period_key,
        })
    }

    fn append(&mut self, bytes: &[u8]) -> io::Result<()> {
        if bytes.is_empty() {
            return Ok(());
        }
        self.rotate_if_needed(bytes.len() as u64)?;
        let file = self
            .file
            .as_mut()
            .ok_or_else(|| io::Error::other("log file is not open"))?;
        file.write_all(bytes)?;
        self.current_size += bytes.len() as u64;
        Ok(())
    }

    fn flush(&mut self) -> io::Result<()> {
        let file = self
            .file
            .as_mut()
            .ok_or_else(|| io::Error::other("log file is not open"))?;
        file.flush()
    }

    fn rotate_if_needed(&mut self, incoming_bytes: u64) -> io::Result<()> {
        let now = Local::now();
        match self.mode {
            RollMode::SizeBytes(limit) => {
                if self.current_size > 0 && self.current_size.saturating_add(incoming_bytes) > limit
                {
                    self.rotate(now)?;
                }
            }
            RollMode::TimeDay | RollMode::TimeHour => {
                let next_period_key = self.mode.period_key(now);
                if self.current_period_key != next_period_key {
                    if self.current_size > 0 {
                        self.rotate(now)?;
                    }
                    self.current_period_key = next_period_key;
                }
            }
        }
        Ok(())
    }

    fn rotate(&mut self, now: DateTime<Local>) -> io::Result<()> {
        let active_name = self
            .active_path
            .file_name()
            .and_then(|name| name.to_str())
            .ok_or_else(|| io::Error::other("active log path has no file name"))?
            .to_string();
        let archive_name = self
            .mode
            .archive_name(&active_name, &self.current_period_key, now);
        let archive_path = unique_archive_path(self.active_path.with_file_name(archive_name));

        let Some(mut file) = self.file.take() else {
            return Err(io::Error::other("log file is not open"));
        };
        file.flush()?;
        drop(file);

        if self.active_path.exists() {
            fs::rename(&self.active_path, &archive_path)?;
        }
        self.cleanup_archives()?;

        self.file = Some(
            OpenOptions::new()
                .create(true)
                .append(true)
                .open(&self.active_path)?,
        );
        self.current_size = 0;
        self.current_period_key = self.mode.period_key(now);
        Ok(())
    }

    fn cleanup_archives(&self) -> io::Result<()> {
        let dir = self
            .active_path
            .parent()
            .ok_or_else(|| io::Error::other("active log path has no parent"))?;
        let active_name = self
            .active_path
            .file_name()
            .and_then(|name| name.to_str())
            .ok_or_else(|| io::Error::other("active log path has no file name"))?;
        let archive_prefix = format!("{active_name}.log.");
        let mut archives = fs::read_dir(dir)?
            .filter_map(Result::ok)
            .filter_map(|entry| {
                let file_name = entry.file_name();
                let file_name = file_name.to_string_lossy();
                file_name
                    .starts_with(&archive_prefix)
                    .then_some(entry.path())
            })
            .collect::<Vec<_>>();
        archives.sort_by(|lhs, rhs| lhs.file_name().cmp(&rhs.file_name()));
        if archives.len() <= self.roll_num {
            return Ok(());
        }
        let delete_count = archives.len() - self.roll_num;
        for path in archives.into_iter().take(delete_count) {
            let _ = fs::remove_file(path);
        }
        Ok(())
    }
}

impl<'a> MakeWriter<'a> for RotatingFileMakeWriter {
    type Writer = RotatingFileWriter;

    fn make_writer(&'a self) -> Self::Writer {
        RotatingFileWriter {
            state: Arc::clone(&self.state),
            buffer: Vec::with_capacity(256),
        }
    }
}

impl Write for RotatingFileWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.buffer.extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        if self.buffer.is_empty() {
            return Ok(());
        }
        let mut guard = self
            .state
            .lock()
            .map_err(|_| io::Error::other("log file lock poisoned"))?;
        guard.append(&self.buffer)?;
        guard.flush()?;
        self.buffer.clear();
        Ok(())
    }
}

impl Drop for RotatingFileWriter {
    fn drop(&mut self) {
        let _ = self.flush();
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
        let level_char = match *metadata.level() {
            Level::ERROR => 'E',
            Level::WARN => 'W',
            Level::INFO => 'I',
            Level::DEBUG => 'D',
            Level::TRACE => 'T',
        };

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

        let thread_id = format!("{:?}", std::thread::current().id())
            .trim_start_matches("ThreadId(")
            .trim_end_matches(")")
            .parse::<u64>()
            .unwrap_or(0);
        let file = metadata.file().unwrap_or("unknown");
        let line = metadata.line().unwrap_or(0);

        write!(
            writer,
            "{}{} {} {}:{}] ",
            level_char, timestamp, thread_id, file, line
        )?;
        ctx.field_format().format_fields(writer.by_ref(), event)?;
        writeln!(writer)
    }
}

fn derive_basename_from_legacy_path(path: &Path) -> Option<String> {
    path.file_stem()
        .and_then(|stem| stem.to_str())
        .map(str::trim)
        .filter(|stem| !stem.is_empty())
        .map(ToOwned::to_owned)
}

fn file_modified_local(path: &Path) -> DateTime<Local> {
    fs::metadata(path)
        .and_then(|m| m.modified())
        .map(DateTime::<Local>::from)
        .unwrap_or_else(|_| Local::now())
}

fn unique_archive_path(base_path: PathBuf) -> PathBuf {
    if !base_path.exists() {
        return base_path;
    }
    let parent = base_path
        .parent()
        .map(Path::to_path_buf)
        .unwrap_or_default();
    let file_name = base_path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("log.archive")
        .to_string();
    for idx in 1usize.. {
        let candidate = parent.join(format!("{file_name}.{idx}"));
        if !candidate.exists() {
            return candidate;
        }
    }
    unreachable!("archive path search must terminate");
}

fn open_log_writers(settings: &LogSettings) -> io::Result<LevelWriters> {
    Ok(LevelWriters {
        info: RotatingFileMakeWriter {
            state: Arc::new(Mutex::new(RotatingFileSet::new(
                settings,
                SeverityFile::Info,
            )?)),
        },
        warning: RotatingFileMakeWriter {
            state: Arc::new(Mutex::new(RotatingFileSet::new(
                settings,
                SeverityFile::Warning,
            )?)),
        },
        error: RotatingFileMakeWriter {
            state: Arc::new(Mutex::new(RotatingFileSet::new(
                settings,
                SeverityFile::Error,
            )?)),
        },
    })
}

pub fn resolve_stdout_log_path() -> PathBuf {
    LogSettings::from_config_and_env().stdout_log_path()
}

pub fn init_with_level(level: &str) {
    INIT.get_or_init(|| {
        let env_filter = EnvFilter::new(level);
        let settings = LogSettings::from_config_and_env();

        if let Ok(writers) = open_log_writers(&settings) {
            let info_layer = tracing_fmt::layer()
                .with_ansi(false)
                .with_writer(writers.info)
                .event_format(StarRocksFormatter);
            let warning_layer = tracing_fmt::layer()
                .with_ansi(false)
                .with_writer(writers.warning)
                .event_format(StarRocksFormatter)
                .with_filter(filter_fn(|metadata| {
                    matches!(*metadata.level(), Level::WARN | Level::ERROR)
                }));
            let error_layer = tracing_fmt::layer()
                .with_ansi(false)
                .with_writer(writers.error)
                .event_format(StarRocksFormatter)
                .with_filter(filter_fn(|metadata| {
                    matches!(*metadata.level(), Level::ERROR)
                }));

            let _ = Registry::default()
                .with(env_filter)
                .with(info_layer)
                .with(warning_layer)
                .with(error_layer)
                .try_init();
            return;
        }

        eprintln!(
            "failed to initialize log files under {}, fallback to stderr",
            settings.dir.display()
        );
        let use_ansi = atty::is(atty::Stream::Stderr);
        let stderr_layer = tracing_fmt::layer()
            .with_ansi(use_ansi)
            .with_writer(std::io::stderr)
            .event_format(StarRocksFormatter);
        let _ = Registry::default()
            .with(env_filter)
            .with(stderr_layer)
            .try_init();
    });
}

pub fn init() {
    init_with_level("info");
}

pub use tracing::instrument;
pub use tracing::{debug, error, info, trace, warn};

#[cfg(test)]
mod tests {
    use super::{
        LogSettings, RollMode, RotatingFileSet, SeverityFile, derive_basename_from_legacy_path,
    };
    use tempfile::tempdir;

    #[test]
    fn derive_basename_uses_legacy_log_file_stem() {
        let base = derive_basename_from_legacy_path(std::path::Path::new("/tmp/novarocks.log"))
            .expect("basename");
        assert_eq!(base, "novarocks");
    }

    #[test]
    fn parse_roll_mode_supports_starrocks_values() {
        assert!(matches!(RollMode::parse("TIME-DAY"), RollMode::TimeDay));
        assert!(matches!(RollMode::parse("TIME-HOUR"), RollMode::TimeHour));
        match RollMode::parse("SIZE-MB-2") {
            RollMode::SizeBytes(bytes) => assert_eq!(bytes, 2 * 1024 * 1024),
            other => panic!("unexpected roll mode: {other:?}"),
        }
    }

    #[test]
    fn size_rotation_keeps_only_configured_archives() {
        let dir = tempdir().expect("tempdir");
        let settings = LogSettings {
            dir: dir.path().to_path_buf(),
            basename: "novarocks".to_string(),
            roll_mode: RollMode::SizeBytes(16),
            roll_num: 2,
        };
        let mut file_set =
            RotatingFileSet::new(&settings, SeverityFile::Info).expect("create rotating set");

        for idx in 0..5 {
            let payload = format!("line-{idx}-0123456789\n");
            file_set
                .append(payload.as_bytes())
                .expect("append log line");
            file_set.flush().expect("flush log line");
        }

        let mut archived = std::fs::read_dir(dir.path())
            .expect("read dir")
            .filter_map(Result::ok)
            .filter_map(|entry| {
                let name = entry.file_name().to_string_lossy().to_string();
                name.starts_with("novarocks.INFO.log.").then_some(name)
            })
            .collect::<Vec<_>>();
        archived.sort();
        assert_eq!(archived.len(), 2);
        assert!(dir.path().join("novarocks.INFO").exists());
    }
}
