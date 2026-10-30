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

//! Private real-I/O fixture: every payload belongs to the existing ManagedProcess owner.
use anyhow::{Context, Result, ensure};
use bytes::Bytes;
use novarocks_fs::{FileCancellation, FileIdentity, FileReadRange, FsAccessResolver};
use novarocks_spi::connector::StorageAccessDomainId;
use novarocks_test_support::{ManagedCommand, ManagedProcess, ReadyMarker};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::fs;
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4, TcpStream};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

fn document(path: &Path) -> Result<BTreeMap<String, String>> {
    let bytes = fs::read(path)?;
    ensure!(bytes.len() <= 16 * 1024, "fixture document exceeds bound");
    let text = std::str::from_utf8(&bytes)?;
    let mut fields = BTreeMap::new();
    for line in text.lines() {
        let (key, value) = line.split_once('=').context("invalid fixture document")?;
        ensure!(
            fields.insert(key.to_owned(), value.to_owned()).is_none(),
            "duplicate field"
        );
    }
    Ok(fields)
}

fn hash_inputs(root: &Path) -> Result<String> {
    fn visit(root: &Path, path: &Path, out: &mut Vec<(String, String)>) -> Result<()> {
        for entry in fs::read_dir(path)? {
            let entry = entry?;
            let path = entry.path();
            let kind = entry.file_type()?;
            ensure!(
                !kind.is_symlink(),
                "classpath symlink requires explicit identity: {}",
                path.display()
            );
            if kind.is_dir() {
                visit(root, &path, out)?;
            } else if kind.is_file() {
                let hash = format!("{:x}", Sha256::digest(fs::read(&path)?));
                out.push((path.strip_prefix(root)?.display().to_string(), hash));
            }
        }
        Ok(())
    }
    let mut entries = Vec::new();
    visit(root, root, &mut entries)?;
    entries.sort();
    Ok(entries
        .into_iter()
        .map(|(path, hash)| format!("{hash}  {path}\n"))
        .collect())
}

fn payload(program: &Path, run: &Path, complete: Instant) -> ManagedCommand {
    let mut command = ManagedCommand::new(program);
    command
        .env_clear()
        .env("PATH", "/usr/bin:/bin")
        .env("HOME", run)
        .env("TMPDIR", run.join("tmp"))
        .current_dir(run)
        .terminate_at(complete - Duration::from_secs(15), complete);
    command
}

fn main() -> Result<()> {
    match std::env::args().nth(1).as_deref() {
        Some("--consumer") => hdfs_public_service_consumer_helper(),
        None => hdfs_public_service_real_io(),
        Some(_) => anyhow::bail!("expected no arguments or the private --consumer mode"),
    }
}

fn hdfs_public_service_real_io() -> Result<()> {
    let repo = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()?;
    let java = PathBuf::from(std::env::var_os("NOVA_HDFS_JAVA").context("NOVA_HDFS_JAVA missing")?)
        .canonicalize()?;
    let build = repo.join("target/hdfs-real-io/java");
    ensure!(
        build
            .join("classes/org/novarocks/test/NovaMiniDfs.class")
            .is_file(),
        "prepare fixture classes first"
    );
    ensure!(
        build.join("gav.txt").is_file(),
        "prepare resolved GAV inventory first"
    );
    let runs = repo.join("target/hdfs-real-io/runs");
    fs::create_dir_all(&runs)?;
    // Retain failed runs; delete only after both original process owners report actual exit.
    let run = tempfile::Builder::new()
        .prefix("run-")
        .tempdir_in(runs)?
        .keep();
    fs::create_dir(run.join("tmp"))?;
    let token = run
        .file_name()
        .context("missing run token")?
        .to_str()
        .context("invalid token")?
        .to_owned();
    fs::write(run.join("classpath.sha256"), hash_inputs(&build)?)?;
    fs::write(
        run.join("java.sha256"),
        format!("{:x}\n", Sha256::digest(fs::read(&java)?)),
    )?;
    let complete = Instant::now() + Duration::from_secs(180);
    let mut java_command = payload(&java, &run, complete);
    java_command
        .args([
            "-Xms64m",
            "-Xmx512m",
            "-XX:MaxDirectMemorySize=128m",
            "-XX:ActiveProcessorCount=2",
            "-Djava.awt.headless=true",
            "-cp",
        ])
        .args([
            format!(
                "{}:{}/*",
                build.join("classes").display(),
                build.join("deps").display()
            ),
            "org.novarocks.test.NovaMiniDfs".into(),
            run.display().to_string(),
            token.clone(),
        ]);
    let mut service = ManagedProcess::spawn(
        "private-hdfs-1nn1dn".into(),
        java_command,
        ReadyMarker::FileContains {
            path: run.join("ready"),
            needle: format!("READY={token}\n"),
        },
        Duration::from_secs(60),
        run.join("java.log"),
    )?;
    let exercise = (|| -> Result<()> {
        let ready = document(&run.join("ready"))?;
        ensure!(ready.get("READY") == Some(&token), "wrong run generation");
        let ports = ready
            .get("ports")
            .context("missing actual listener ports")?
            .split(',')
            .map(str::parse::<u16>)
            .collect::<std::result::Result<Vec<_>, _>>()?;
        ensure!(
            ports.len() == 5 && ports.iter().all(|p| *p != 0),
            "wrong actual listener inventory"
        );
        for port in ports {
            TcpStream::connect_timeout(
                &SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, port)),
                Duration::from_secs(2),
            )
            .context("actual loopback listener probe failed")?;
        }
        let mut command = payload(&std::env::current_exe()?, &run, complete);
        command
            .args(["--consumer"])
            .env("HADOOP_CONF_DIR", run.join("conf"))
            .env("NOVA_HDFS_RUN", &run)
            .env("NOVA_HDFS_TOKEN", &token);
        let remaining = (complete - Duration::from_secs(25))
            .checked_duration_since(Instant::now())
            .context("consumer execution deadline expired")?;
        ManagedProcess::run_to_completion(
            "private-hdfs-fs-consumer".into(),
            command,
            ReadyMarker::StdoutContains("CONSUMER_DONE".into()),
            remaining,
            run.join("consumer.log"),
        )?;
        Ok(())
    })();
    let stop = (|| -> std::io::Result<()> {
        fs::write(
            run.join("stop.new"),
            if exercise.is_ok() { "VERIFY" } else { "ABORT" },
        )?;
        fs::rename(run.join("stop.new"), run.join("stop"))
    })();
    let exit = service.wait_for_successful_exit_until(complete - Duration::from_secs(5));
    let cleanup = if exit.is_err() {
        service.stop()
    } else {
        Ok(())
    };
    // No failed run is erased; primary and physical-cleanup diagnostics remain separate.
    let diagnostics = format!(
        "run={} stop={stop:?} service_exit={exit:?} cleanup={cleanup:?}",
        run.display()
    );
    exercise.context(diagnostics.clone())?;
    stop.context(diagnostics.clone())?;
    exit.context(diagnostics.clone())?;
    cleanup.context(diagnostics)?;
    ensure!(service.cleanup_settled(), "service cleanup not observed");
    let witness = document(&run.join("owner-witness"))?;
    ensure!(
        witness.get("RUN") == Some(&token),
        "wrong owner witness generation"
    );
    ensure!(
        witness.get("physical").map(String::as_str) == Some("alice"),
        "physical owner not proven"
    );
    ensure!(
        witness.get("logical").map(String::as_str) == Some("alice"),
        "logical owner not proven"
    );
    // Keep the small input/owner receipts and process diagnostics in the owning target.
    fs::remove_dir_all(run.join("data"))?;
    Ok(())
}

fn hdfs_public_service_consumer_helper() -> Result<()> {
    let run = PathBuf::from(std::env::var_os("NOVA_HDFS_RUN").context("private run missing")?);
    let token = std::env::var("NOVA_HDFS_TOKEN")?;
    let ready = document(&run.join("ready"))?;
    ensure!(ready.get("READY") == Some(&token), "wrong ready generation");
    println!("CONSUMER_READY");
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    let resolver = FsAccessResolver::with_runtime(runtime.handle().clone());
    runtime.block_on(async {
        for (route, user) in [("physical", "%61lice"), ("logical", "alice")] {
            let mut base = url::Url::parse(ready.get(route).context("missing route")?)?;
            base.set_username(user)
                .map_err(|()| anyhow::anyhow!("invalid userinfo"))?;
            let root = ready.get("root").context("missing root")?;
            base.set_path(&format!("{root}/{route}/"));
            let domain = StorageAccessDomainId::from_bytes([47; 32]);
            let access = resolver.resolve_location(domain, base.as_str(), None)?;
            let operator = access.operator();
            let directory = access.paths()[0].operator_relative_path();
            operator.create_dir(directory).await?;
            let path = format!("{directory}payload");
            let renamed = format!("{directory}renamed");
            let empty = format!("{directory}empty");
            operator.writer(&empty).await?.close().await?;
            ensure!(
                operator.stat(&empty).await?.content_length() == 0,
                "empty close did not create an empty file"
            );
            operator.delete(&empty).await?;
            let mut old = operator.writer(&path).await?;
            old.write(Bytes::from_static(
                b"a longer previously closed file that must be truncated",
            ))
            .await?;
            old.close().await?;
            let mut writer = operator.writer(&path).await?;
            writer
                .write(Bytes::from_static(b"0123456789abcdef"))
                .await?;
            writer.close().await?;
            ensure!(
                operator.stat(&path).await?.content_length() == 16,
                "wrong stat size"
            );
            let object_url = format!("{}payload", base.as_str());
            let file = resolver.resolve_location(domain, &object_url, None)?;
            let bound = file.bind(0, FileIdentity::new(object_url, 16, None))?;
            let cancel = FileCancellation::new();
            ensure!(
                bound
                    .read(FileReadRange::WholeFile, &cancel)
                    .await?
                    .as_ref()
                    == b"0123456789abcdef",
                "whole read mismatch"
            );
            ensure!(
                bound
                    .read(
                        FileReadRange::Bounded {
                            offset: 3,
                            length: 5
                        },
                        &cancel
                    )
                    .await?
                    .as_ref()
                    == b"34567",
                "range read mismatch"
            );
            let entries = operator.list(directory).await?;
            ensure!(
                entries.len() == 1 && entries[0].path() == path,
                "listing did not contain exactly the closed payload"
            );
            operator.rename(&path, &renamed).await?;
            ensure!(
                !operator.exists(&path).await?
                    && operator.stat(&renamed).await?.content_length() == 16,
                "rename mismatch"
            );
            ensure!(
                operator.read(&renamed).await?.to_bytes().as_ref() == b"0123456789abcdef",
                "renamed content mismatch"
            );
            operator.delete(&renamed).await?;
            ensure!(
                !operator.exists(&renamed).await?,
                "delete did not take effect"
            );
            let witness = format!("{directory}owner-witness");
            let mut writer = operator.writer(&witness).await?;
            writer
                .write(Bytes::from_static(b"effective-user-witness"))
                .await?;
            writer.close().await?;
        }
        Ok::<(), anyhow::Error>(())
    })?;
    println!("CONSUMER_DONE");
    Ok(())
}
