#!/usr/bin/env python3
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements. See the NOTICE file for additional
# information. Licensed under the Apache License, Version 2.0.
"""No-Docker bootstrap producer identity tests and lifecycle fixture setup."""
import importlib.util
import json
import os
from pathlib import Path
import shlex
import shutil
import subprocess
import sys
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[5]
sys.path.insert(0, str(ROOT / "docker/fixture-inputs"))
from fixture_inputs import load_lock, definition_sha256
spec = importlib.util.spec_from_file_location("resolver", ROOT / "tests/sql/fixtures/benchmarks/resolve_benchmark_fixture.py")
resolver = importlib.util.module_from_spec(spec)
spec.loader.exec_module(resolver)


def setup(root, directory):
    directory.mkdir(parents=True, exist_ok=True)
    publication = directory / "r2-publication"
    publication.mkdir(exist_ok=True)
    lock, lock_hash = load_lock(root / "docker/fixture-inputs/lock.json")
    derived = lock["derived_images"]["iceberg-spark"]
    receipt = {"alias": derived["alias"], "platform": derived["platform"],
               "definition_sha256": definition_sha256(root, derived["definition_files"]), "image_id": "sha256:spark-r2"}
    bom = {"lock_sha256": lock_hash, "derived_images": {"iceberg-spark": receipt}}
    (directory / "bom.json").write_text(json.dumps(bom))
    project = "nr-test-r2"
    for name in ("compose.env", "compose.yml"):
        (publication / name).touch()
    manifest = {"ready": True, "fixture_inputs": {"bom": str(directory / "bom.json")},
                "runtime": {"publication_dir": str(publication), "producer_receipt": {"lock_sha256": lock_hash, **receipt},
                "catalog": {"images": {"spark": {"image_id": receipt["image_id"], "receipt": receipt}},
                            "containers": {"spark": "r2-exact-container"}}}}
    (publication / "manifest.json").write_text(json.dumps(manifest))
    env = {"NOVA_ENV_READY": "true", "NOVA_ENV_COMPOSE_ENV": str(publication / "compose.env"),
           "NOVA_ENV_COMPOSE_PROJECT": project, "NOVA_ENV_COMPOSE_FILE": str(publication / "compose.yml"),
           "NOVA_ENV_SHARED_BENCHMARK_ROOT": "s3://fixture/shared/benchmarks", "NOVA_ENV_BENCHMARK_BUILD_TIMEOUT_SECONDS": "16",
           "AWS_S3_ENDPOINT": "http://unused", "AWS_S3_ACCESS_KEY_ID": "test-key", "AWS_S3_SECRET_ACCESS_KEY": "test-secret"}
    (publication / "env.sh").write_text("".join(f"export {key}={shlex.quote(value)}\n" for key, value in env.items()))
    (directory / "r1-env.sh").write_text("echo 'STALE_R1_ENV_READ' >&2; exit 87\n")
    (directory / "unbound-env.sh").write_text("export NOVA_ENV_READY=false\n")
    state = directory / "docker-state.json"
    state.write_text(json.dumps({"image": receipt["image_id"]}))
    log = directory / "calls.jsonl"
    bin_dir = directory / "bin"
    bin_dir.mkdir(exist_ok=True)
    executable = bin_dir / "docker"
    executable.write_text(f'''#!{sys.executable}
import json, sys
from pathlib import Path
args=sys.argv[1:]
with open({str(log)!r}, 'a') as out: out.write(json.dumps(args)+'\\n')
if args[0]=='compose':
    assert args[args.index('-p')+1]=={project!r}, args
    assert args[args.index('--env-file')+1]=={str(publication / 'compose.env')!r}, args
    assert args[-3:]==['ps','-q','spark'], args
    print('r2-exact-container')
elif args[:2]==['inspect','r2-exact-container']:
    print(json.loads(Path({str(state)!r}).read_text())['image'])
else:
    raise SystemExit('unexpected Docker call: '+repr(args))
''')
    executable.chmod(0o755)
    binder = directory / "bind"
    binder.write_text(f'''#!{sys.executable}
import json
with open({str(log)!r}, 'a') as out: out.write(json.dumps(['bind'])+'\\n')
print(json.dumps({{"published_dir": {str(publication)!r}}}))
''')
    binder.chmod(0o755)
    return publication


def populate_ready(resolved, storage):
    def path(uri):
        return storage / uri.removeprefix("s3://")
    warehouse = resolved["staging_parent"] + "/writer-r2/warehouse"
    manifest = warehouse + "/manifest"
    path(manifest).mkdir(parents=True)
    path(manifest + "/_SUCCESS").touch()
    tables = []
    for table in resolved["contract"]["tables"]:
        metadata = warehouse + f"/{table}/metadata/v1.metadata.json"
        statistics = warehouse + f"/{table}/metadata/stats.puffin"
        for uri in (metadata, statistics):
            path(uri).parent.mkdir(parents=True, exist_ok=True)
            path(uri).write_text(table)
        tables.append({"name": table, "metadata_uri": metadata, "statistics_file": statistics})
    path(manifest + "/part-00000").write_text(json.dumps({"dataset_key": resolved["dataset_key"], "fixture_contract": resolved["contract"],
        "producer_fingerprint": resolved["producer_fingerprint"], "tables": tables}))
    path(resolved["ready_uri"]).write_text(json.dumps({"schema_version": 1, "dataset_key": resolved["dataset_key"], "state": "ReadyValid",
        "exact_warehouse": warehouse, "manifest_uri": manifest, "contract": resolved["contract"], "producer_fingerprint": resolved["producer_fingerprint"],
        "publication": {"ready_uri": resolved["ready_uri"], "identity": "writer-r2"}}))


class ProducerBindingTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.directory = Path(self.temp.name)
        self.root = self.directory / "source"
        model = resolver.load_model(ROOT / "tests/sql/fixtures/benchmarks/benchmark_tools.toml")
        sources = set(model["fixture"]["producer_inputs"].values()) | {
            "tests/sql/fixtures/benchmarks/benchmark_tools.toml", "docker/fixture-inputs/lock.json",
            "docker/fixture-inputs/fixture_inputs.py", "docker/iceberg-rest/runtime_entry.py", "docker/iceberg-rest/fixture_runtime.py"}
        for name in sources:
            target = self.root / name
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(ROOT / name, target)
        self.r1 = resolver.resolve_fixture(model, self.root, "ssb", "1", "s3://fixture/shared/benchmarks")
        dockerfile = self.root / "docker/iceberg-rest/spark/Dockerfile"
        dockerfile.write_bytes(dockerfile.read_bytes() + b'\nLABEL test.generation="r2"\n')
        self.resolved = resolver.resolve_fixture(model, self.root, "ssb", "1", "s3://fixture/shared/benchmarks")
        self.assertNotEqual(self.r1["producer_fingerprint"], self.resolved["producer_fingerprint"])
        self.fixture = self.directory / "fixture"
        self.publication = setup(self.root, self.fixture)
        self.resolved_path = self.directory / "resolved.json"
        self.resolved_path.write_text(json.dumps(self.resolved))
        self.storage = self.directory / "storage"
        populate_ready(self.resolved, self.storage)
        self.environment = {**os.environ, "NOVAROCKS_WORKSPACE_ROOT": str(self.root),
            "NOVA_ENV_REST_ENV_FILE": str(self.fixture / "r1-env.sh"), "BENCHMARK_FIXTURE_UP_COMMAND": str(self.fixture / "bind"),
            "BENCHMARK_FIXTURE_STORAGE_DIR": str(self.storage), "PATH": str(self.fixture / "bin") + os.pathsep + os.environ["PATH"]}

    def run_bootstrap(self, mode="--check"):
        return subprocess.run(["bash", str(self.root / "tests/sql/fixtures/benchmarks/bootstrap_benchmark_data.sh"),
            "--suite", "ssb", "--scale", "1", "--resolved-dataset", str(self.resolved_path), mode],
            env=self.environment, capture_output=True, text=True, timeout=20)

    def test_r1_cached_entry_is_ignored_after_current_r2_bind(self):
        result = self.run_bootstrap()
        self.assertEqual(result.returncode, 0, result.stderr)
        value = json.loads(result.stdout)
        self.assertEqual(value["dataset_key"], self.resolved["dataset_key"])
        calls = [json.loads(line) for line in (self.fixture / "calls.jsonl").read_text().splitlines()]
        self.assertEqual(calls[0], ["bind"])
        self.assertEqual(len(calls), 3)
        self.assertNotIn("STALE_R1_ENV_READ", result.stderr)
        self.assertNotIn("test-secret", result.stdout)

    def test_declaration_checkout_is_independent_of_worktree_data_root(self):
        workspace = self.directory / "different-worktree"
        workspace.mkdir()
        self.environment["NOVAROCKS_WORKSPACE_ROOT"] = str(workspace)
        result = self.run_bootstrap()
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(json.loads(result.stdout)["dataset_key"], self.resolved["dataset_key"])

    def test_first_unbound_entry_binds_and_dry_run_uses_no_docker(self):
        self.environment["NOVA_ENV_REST_ENV_FILE"] = str(self.fixture / "unbound-env.sh")
        result = self.run_bootstrap("--dry-run")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("producer_identity=not_checked_dry_run", result.stderr)
        self.assertFalse((self.fixture / "calls.jsonl").exists())
        result = self.run_bootstrap()
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_mismatched_actual_image_or_bom_cannot_reuse_valid_ready(self):
        ready = self.storage / self.resolved["ready_uri"].removeprefix("s3://")
        original = ready.read_bytes()
        for mismatch in ("image", "bom"):
            with self.subTest(mismatch=mismatch):
                setup(self.root, self.fixture)
                if mismatch == "image":
                    (self.fixture / "docker-state.json").write_text(json.dumps({"image": "sha256:old-r1"}))
                else:
                    path = self.fixture / "bom.json"
                    bom = json.loads(path.read_text())
                    bom["derived_images"]["iceberg-spark"]["definition_sha256"] = "old-r1"
                    path.write_text(json.dumps(bom))
                result = self.run_bootstrap()
                self.assertNotEqual(result.returncode, 0)
                self.assertIn("ProducerIdentityMismatch", json.loads(result.stdout)["message"])
                self.assertEqual(ready.read_bytes(), original)


if __name__ == "__main__":
    if len(sys.argv) == 4 and sys.argv[1] == "--prepare":
        setup(Path(sys.argv[2]), Path(sys.argv[3]))
    else:
        unittest.main()
