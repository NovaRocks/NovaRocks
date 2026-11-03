// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

//! Admission of the already prepared readonly HMS input before any role spawn.
//! Reuses original bounded file/command owners; creates no fixture or provider.
use super::*;
use crate::scenarios::hms_bulk_readonly_native::Binding as HmsBinding;
use serde_json::Value;

const FILE_BYTES: u64 = 1_048_576;
const ARGUMENT: &str = "--mem-1-m07-hms-listing-build-identity";

fn clock_domain(domain: &str) -> Result<()> {
    #[cfg(target_os = "macos")]
    let expected = "python-monotonic-darwin-mach-absolute-time-v1";
    #[cfg(not(target_os = "macos"))]
    let expected = "python-monotonic-linux-clock-monotonic-v1";
    ensure!(
        domain == expected,
        "HMS external original monotonic clock domain differs"
    );
    Ok(())
}

pub(crate) struct Admission {
    repository: PathBuf,
    original_source: Source,
    git_stamp: Stamp,
    files: Vec<InputOwner>,
    deadline: Instant,
}
impl Admission {
    pub(crate) fn deadline(&self) -> Instant {
        self.deadline
    }
    pub(crate) fn recheck(&self, deadline: Instant) -> Result<()> {
        ensure!(
            deadline == self.deadline,
            "HMS original admission deadline changed"
        );
        self.recheck_until(deadline)
    }
    fn recheck_until(&self, deadline: Instant) -> Result<()> {
        ensure!(
            deadline <= self.deadline,
            "HMS original recheck deadline extended"
        );
        for file in &self.files {
            file.recheck(deadline)?;
        }
        ensure!(
            source(&self.repository, deadline)? == self.original_source
                && Stamp::from(&fs::symlink_metadata(self.repository.join(".git"))?)
                    == self.git_stamp,
            "HMS original source/Git owner changed"
        );
        check(deadline)
    }
}

// Python's time.monotonic_ns uses mach_absolute_time on Darwin and
// CLOCK_MONOTONIC on Linux. CLOCK_UPTIME_RAW projects that same Darwin clock.
fn monotonic_nanos() -> Result<u64> {
    #[cfg(target_os = "macos")]
    let clock = libc::CLOCK_UPTIME_RAW;
    #[cfg(not(target_os = "macos"))]
    let clock = libc::CLOCK_MONOTONIC;
    let mut sample = libc::timespec {
        tv_sec: 0,
        tv_nsec: 0,
    };
    let result = unsafe { libc::clock_gettime(clock, &mut sample) };
    ensure!(result == 0, "HMS original monotonic clock read failed");
    ensure!(
        sample.tv_sec >= 0 && (0..1_000_000_000).contains(&sample.tv_nsec),
        "HMS original monotonic sample is malformed"
    );
    (sample.tv_sec as u64)
        .checked_mul(1_000_000_000)
        .and_then(|seconds| seconds.checked_add(sample.tv_nsec as u64))
        .ok_or_else(|| anyhow::anyhow!("HMS original monotonic sample overflow"))
}
fn project_deadline(
    before_sample: Instant,
    now_nanos: u64,
    end_nanos: u64,
    maximum_millis: u64,
) -> Result<Instant> {
    ensure!(
        maximum_millis > 0 && maximum_millis <= 3_600_000,
        "HMS Native original budget differs"
    );
    let left = end_nanos
        .checked_sub(now_nanos)
        .ok_or_else(|| anyhow::anyhow!("HMS external original deadline expired"))?;
    ensure!(
        left > 0 && left <= maximum_millis * 1_000_000,
        "HMS external remaining deadline exceeds original budget"
    );
    // Anchor on the Instant sampled BEFORE the foreign-clock read. Sampling
    // latency can only shorten this deadline; admission never renews it.
    before_sample
        .checked_add(Duration::from_nanos(left))
        .ok_or_else(|| anyhow::anyhow!("HMS original Instant deadline overflow"))
}
fn identity(raw: &[u8], expected: &str) -> Result<()> {
    revision(expected)?;
    ensure!(
        raw == format!(
            "{} commit={expected} build_identity={expected} hms_listing_observe=true\n",
            novarocks_failpoint::observation_marker::MEM_1_M07_HMS_LISTING_BUILD
        )
        .as_bytes(),
        "HMS actual server feature/full-clean-build identity differs"
    );
    Ok(())
}
fn read_pin(
    path: &Path,
    expected: &str,
    files: &mut Vec<InputOwner>,
    deadline: Instant,
) -> Result<Vec<u8>> {
    hash(expected)?;
    let mut owner = InputOwner::open(path, FILE_BYTES, deadline)?;
    let raw = owner.read_small(FILE_BYTES, deadline)?;
    ensure!(
        digest(&raw) == expected,
        "HMS original input raw pin differs"
    );
    files.push(owner);
    Ok(raw)
}
fn freeze(raw: &[u8], revision: &str, original: &str, purpose: &str) -> Result<Value> {
    let value: Value = serde_json::from_slice(raw)?;
    ensure!(
        value["source_revision"].as_str() == Some(revision)
            && value["frozen_before_execution"].as_bool() == Some(true)
            && value["schema_version"].as_u64() == Some(1)
            && value["spec_revision"].as_u64() == Some(7)
            && value["task"].as_str() == Some("MEM-1-M07")
            && value["purpose"].as_str() == Some(purpose)
            && value["review_status"].as_str() == Some("reviewed"),
        "HMS external freeze belongs to another source/execution"
    );
    if purpose != "private-stock-hms-capability-only" {
        ensure!(
            value["original_input_sha256"].as_str() == Some(original),
            "HMS external original CL input differs"
        );
    }
    Ok(value)
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ExternalClockReceipt {
    schema_version: u32,
    clock_freeze_raw_sha256: String,
    native_clock_domain: String,
    original_anchor_seconds: f64,
    preparation_until_seconds: f64,
    verification_until_seconds: f64,
    cleanup_until_seconds: f64,
}
// Exact floor of the original binary64 seconds, matching Python's
// numerator*1e9//denominator. A rounded float multiplication can extend it.
fn floor_nanos(seconds: f64) -> Result<u64> {
    ensure!(
        seconds.is_finite() && seconds >= 0.,
        "HMS original clock seconds are invalid"
    );
    let bits = seconds.to_bits();
    let exponent = ((bits >> 52) & 0x7ff) as i32;
    let mantissa =
        u128::from(bits & ((1u64 << 52) - 1)) + if exponent == 0 { 0 } else { 1u128 << 52 };
    let shift = if exponent == 0 {
        -1074
    } else {
        exponent - 1023 - 52
    };
    let value = mantissa * 1_000_000_000;
    let nanos = if shift >= 0 {
        let shift = shift as u32;
        ensure!(
            shift < 128 && value <= (u128::MAX >> shift),
            "HMS original clock nanos overflow"
        );
        value << shift
    } else {
        value.checked_shr((-shift) as u32).unwrap_or(0)
    };
    u64::try_from(nanos).map_err(|_| anyhow::anyhow!("HMS original clock nanos overflow"))
}
fn external_clock(
    raw: &[u8],
    frozen: &Value,
    clock_pin: &str,
    domain: &str,
    native_deadline: u64,
    now: u64,
) -> Result<()> {
    let receipt: ExternalClockReceipt = serde_json::from_slice(raw)?;
    ensure!(
        receipt.schema_version == 1
            && receipt.clock_freeze_raw_sha256 == clock_pin
            && receipt.native_clock_domain == domain,
        "HMS original external clock receipt binding differs"
    );
    let offsets = &frozen["offset_seconds_from_original_anchor"];
    let mut previous = 0.;
    for (key, end) in [
        ("preparation_until", receipt.preparation_until_seconds),
        ("verification_until", receipt.verification_until_seconds),
        ("cleanup_until", receipt.cleanup_until_seconds),
    ] {
        let offset = offsets[key]
            .as_f64()
            .ok_or_else(|| anyhow::anyhow!("HMS frozen clock offset is absent"))?;
        ensure!(
            offset.is_finite()
                && offset > 0.
                && offset >= previous
                && end.is_finite()
                && end.to_bits() == (receipt.original_anchor_seconds + offset).to_bits(),
            "HMS original external anchor/offset/deadline differs"
        );
        previous = offset;
    }
    ensure!(
        floor_nanos(receipt.original_anchor_seconds)? <= now
            && native_deadline <= floor_nanos(receipt.verification_until_seconds)?,
        "HMS Native deadline extends the original external verification clock"
    );
    Ok(())
}
fn canonical(value: Value) -> Value {
    match value {
        Value::Object(object) => Value::Object(
            object
                .into_iter()
                .map(|(key, value)| (key, canonical(value)))
                .collect::<std::collections::BTreeMap<_, _>>()
                .into_iter()
                .collect(),
        ),
        Value::Array(array) => Value::Array(array.into_iter().map(canonical).collect()),
        value => value,
    }
}

pub(crate) type HmsNativeAdmission = (HmsBinding, Vec<u8>, Vec<u8>, Vec<u8>, Admission);

pub(crate) fn admit(
    path: &Path,
    server: &Path,
    config: &Path,
    build: &crate::build_information::BuildInformation,
) -> Result<HmsNativeAdmission> {
    let preparation = Instant::now() + Duration::from_millis(PREP_MS);
    let mut binding_owner = InputOwner::open(path, BINDING_BYTES, preparation)?;
    let binding_raw = binding_owner.read_small(BINDING_BYTES, preparation)?;
    let binding: HmsBinding = serde_json::from_slice(&binding_raw)?;
    ensure!(
        binding.schema_version == 1 && binding.runnable && binding.frozen_before_execution,
        "HMS binding has no frozen runnable admission"
    );
    clock_domain(&binding.native_clock_domain)?;
    let before_clock = Instant::now();
    let deadline = project_deadline(
        before_clock,
        monotonic_nanos()?,
        binding.native_deadline_monotonic_nanos,
        binding.native_budget_millis,
    )?;
    let prep = preparation.min(deadline);
    check(prep)?;
    let repository = Path::new(env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .nth(2)
        .ok_or_else(|| anyhow::anyhow!("HMS source workspace is absent"))?
        .canonicalize()?;
    let git_metadata = fs::symlink_metadata(repository.join(".git"))?;
    ensure!(
        !git_metadata.file_type().is_symlink(),
        "HMS Git locator is a symlink"
    );
    let original_source = source(&repository, prep)?;
    ensure!(
        original_source.revision == binding.clean_source_revision
            && original_source.tree == binding.source_tree_sha256,
        "HMS actual clean source differs from original freeze"
    );
    require_compiled_runner(
        build.source_commit,
        build.native_build_identity,
        &original_source.revision,
    )?;
    let mut files = vec![binding_owner];
    for (path, cap, pin) in [
        (server.to_path_buf(), BINARY_BYTES, &binding.server_sha256),
        (
            std::env::current_exe()?,
            BINARY_BYTES,
            &binding.runner_sha256,
        ),
        (
            config.to_path_buf(),
            CONFIG_BYTES,
            &binding.base_config_sha256,
        ),
        (
            repository.join("Cargo.lock"),
            CONFIG_BYTES,
            &binding.cargo_lock_sha256,
        ),
    ] {
        hash(pin)?;
        let mut file = InputOwner::open(&path, cap, prep)?;
        ensure!(
            file.stream_hash(prep)? == *pin,
            "HMS actual binary/config/lock pin differs"
        );
        files.push(file);
    }
    let original = read_pin(
        &binding.original_cl_freeze_path,
        &binding.original_cl_freeze_sha256,
        &mut files,
        prep,
    )?;
    ensure!(
        digest(&original) == "e279724dc4ab2ce34dfdef5f3a939ad3f0f05ed076c36c5c60a7b7d7c6c1a3d3",
        "HMS original accepted CL input changed"
    );
    let owner_raw = read_pin(
        &binding.owner_freeze_path,
        &binding.owner_freeze_raw_sha256,
        &mut files,
        prep,
    )?;
    let bulk_raw = read_pin(
        &binding.bulk_freeze_path,
        &binding.bulk_freeze_raw_sha256,
        &mut files,
        prep,
    )?;
    let clock_raw = read_pin(
        &binding.clock_freeze_path,
        &binding.clock_freeze_raw_sha256,
        &mut files,
        prep,
    )?;
    let owner = freeze(
        &owner_raw,
        &binding.clean_source_revision,
        &binding.original_cl_freeze_sha256,
        "private-stock-hms-capability-only",
    )?;
    let bulk = freeze(
        &bulk_raw,
        &binding.clean_source_revision,
        &binding.original_cl_freeze_sha256,
        "private-stock-hms-external-readonly-cl-input-only",
    )?;
    let clock = freeze(
        &clock_raw,
        &binding.clean_source_revision,
        &binding.original_cl_freeze_sha256,
        "external-stock-hms-bulk-clock-review-only",
    )?;
    let clock_receipt = read_pin(
        &binding.external_clock_receipt_path,
        &binding.external_clock_receipt_sha256,
        &mut files,
        prep,
    )?;
    external_clock(
        &clock_receipt,
        &clock,
        &binding.clock_freeze_raw_sha256,
        &binding.native_clock_domain,
        binding.native_deadline_monotonic_nanos,
        monotonic_nanos()?,
    )?;
    ensure!(
        digest(&serde_json::to_vec(&canonical(bulk.clone()))?)
            == binding.bulk_freeze_canonical_sha256
            && bulk["base_freeze_sha256"].as_str()
                == Some(digest(&serde_json::to_vec(&canonical(owner))?).as_str()),
        "HMS original bulk/owner canonical linkage differs"
    );
    let ready = read_pin(
        &binding.input_ready_path,
        &binding.input_ready_sha256,
        &mut files,
        prep,
    )?;
    let manifest = read_pin(
        &binding.before_oracle_manifest_path,
        &binding.before_oracle_manifest_sha256,
        &mut files,
        prep,
    )?;
    let index: Value = serde_json::from_slice(&manifest)?;
    let first = &index["shards"][0];
    let root_path = first["path"]
        .as_str()
        .ok_or_else(|| anyhow::anyhow!("HMS root oracle path is absent"))?;
    let root_pin = first["sha256"]
        .as_str()
        .ok_or_else(|| anyhow::anyhow!("HMS root oracle pin is absent"))?;
    let root_oracle = read_pin(Path::new(root_path), root_pin, &mut files, prep)?;
    read_pin(
        &Path::new(&binding.fixture.publication).join("manifest.json"),
        &binding.fixture.manifest_sha256,
        &mut files,
        prep,
    )?;
    read_pin(
        &binding.hms_manifest_path,
        &binding.fixture.hms_manifest_sha256,
        &mut files,
        prep,
    )?;
    // files[1] is the original server file whose bytes were just hashed.
    files[1].recheck(prep)?;
    let mut command = Command::new(&files[1].canonical);
    command.env_clear().arg(ARGUMENT);
    identity(
        &bounded_command(command, IDENTITY_STDOUT_BYTES, prep)?,
        &original_source.revision,
    )?;
    let admitted = Admission {
        repository,
        original_source,
        files,
        git_stamp: Stamp::from(&git_metadata),
        deadline,
    };
    admitted.recheck_until(prep)?;
    check(prep)?;
    Ok((binding, ready, manifest, root_oracle, admitted))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn foreign_deadline_preserves_original_clock_and_rejects_late_or_extended_entry() {
        let before = Instant::now();
        assert_eq!(
            project_deadline(before, 1_000, 2_000, 1).unwrap(),
            before + Duration::from_nanos(1_000)
        );
        assert!(project_deadline(before, 2_000, 2_000, 1).is_err());
        assert!(project_deadline(before, 2_001, 2_000, 1).is_err());
        assert!(project_deadline(before, 0, 1_000_001, 1).is_err());
        assert!(project_deadline(before, 0, 1, 0).is_err());
    }
    #[test]
    fn default_or_foreign_server_never_implies_hms_feature() {
        let revision = "a".repeat(40);
        let valid = format!(
            "{} commit={revision} build_identity={revision} hms_listing_observe=true\n",
            novarocks_failpoint::observation_marker::MEM_1_M07_HMS_LISTING_BUILD
        );
        assert!(identity(valid.as_bytes(), &revision).is_ok());
        for raw in [
            b"".as_slice(),
            b"standalone\n",
            b"hms_listing_observe=true\n",
        ] {
            assert!(identity(raw, &revision).is_err());
        }
        assert!(identity(valid.as_bytes(), &"b".repeat(40)).is_err());
    }
    #[test]
    fn actual_monotonic_source_advances_without_new_anchor() {
        let first = monotonic_nanos().unwrap();
        assert!(first > 0 && monotonic_nanos().unwrap() >= first);
        assert!(clock_domain("wall-clock-unix-nanos").is_err());
        assert!(clock_domain("").is_err());
    }
    #[test]
    fn original_float_clock_is_floored_exactly_without_rounded_multiply() {
        assert_eq!(floor_nanos(0.5).unwrap(), 500_000_000);
        // Binary64 0.1 is slightly above 0.1. Its exact ns floor is 100000000.
        assert_eq!(floor_nanos(0.1).unwrap(), 100_000_000);
        assert_eq!(floor_nanos(f64::from_bits(1)).unwrap(), 0);
        for value in [f64::NAN, f64::INFINITY, -1., f64::MAX] {
            assert!(floor_nanos(value).is_err());
        }
    }
    #[test]
    fn external_freeze_refuses_draft_wrong_scope_and_missing_original() {
        let revision = "a".repeat(40);
        let original = "b".repeat(64);
        let purpose = "external-stock-hms-bulk-clock-review-only";
        let valid = serde_json::json!({"schema_version":1,"spec_revision":7,"task":"MEM-1-M07",
            "source_revision":revision,"original_input_sha256":original,"purpose":purpose,
            "review_status":"reviewed","frozen_before_execution":true});
        let encode = |value: &Value| serde_json::to_vec(value).unwrap();
        assert!(freeze(&encode(&valid), &revision, &original, purpose).is_ok());
        let mut changed = valid.clone();
        changed["review_status"] = Value::String("draft".into());
        assert!(freeze(&encode(&changed), &revision, &original, purpose).is_err());
        let mut changed = valid.clone();
        changed
            .as_object_mut()
            .unwrap()
            .remove("original_input_sha256");
        assert!(freeze(&encode(&changed), &revision, &original, purpose).is_err());
        assert!(freeze(&encode(&valid), &revision, &original, "different-scope").is_err());
    }
    #[test]
    fn original_external_anchor_refuses_null_unordered_changed_and_extended_clocks() {
        let pin = "a".repeat(64);
        let frozen = serde_json::json!({"offset_seconds_from_original_anchor":{
            "preparation_until":10,"verification_until":20,"cleanup_until":30}});
        let receipt = serde_json::json!({"schema_version":1,"clock_freeze_raw_sha256":pin,
            "native_clock_domain":"test-domain","original_anchor_seconds":10.0,
            "preparation_until_seconds":20.0,"verification_until_seconds":30.0,"cleanup_until_seconds":40.0});
        let raw = serde_json::to_vec(&receipt).unwrap();
        let check = |raw: &[u8], frozen: &Value, end: u64| {
            external_clock(raw, frozen, &pin, "test-domain", end, 15_000_000_000)
        };
        assert!(check(&raw, &frozen, 25_000_000_000).is_ok());
        assert!(check(&raw, &frozen, 30_000_000_001).is_err());
        let mut changed = frozen.clone();
        changed["offset_seconds_from_original_anchor"]["preparation_until"] = Value::Null;
        assert!(check(&raw, &changed, 25_000_000_000).is_err());
        let mut changed = frozen.clone();
        changed["offset_seconds_from_original_anchor"]["verification_until"] = serde_json::json!(9);
        assert!(check(&raw, &changed, 25_000_000_000).is_err());
        let mut changed = receipt.clone();
        changed["original_anchor_seconds"] = serde_json::json!(11.0);
        assert!(
            check(
                &serde_json::to_vec(&changed).unwrap(),
                &frozen,
                25_000_000_000
            )
            .is_err()
        );
        let mut changed = receipt.clone();
        changed["extra"] = Value::Bool(true);
        assert!(
            check(
                &serde_json::to_vec(&changed).unwrap(),
                &frozen,
                25_000_000_000
            )
            .is_err()
        );
        assert!(
            external_clock(
                &raw,
                &frozen,
                &pin,
                "foreign-domain",
                25_000_000_000,
                15_000_000_000
            )
            .is_err()
        );
    }
}
