# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import importlib.util
import json
import subprocess
import sys
import tempfile
import unittest
from argparse import Namespace
from contextlib import redirect_stdout
from io import StringIO
from pathlib import Path
from unittest import mock


ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location("paimon_fixture", ROOT / "fixture.py")
assert SPEC and SPEC.loader
fixture = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = fixture
SPEC.loader.exec_module(fixture)


class FixtureContractTest(unittest.TestCase):
    @staticmethod
    def runtime(root: Path) -> fixture.Runtime:
        return fixture.Runtime(
            env_id="test-env",
            compose_project="test-project",
            compose_file=root / "compose.yml",
            compose_env=root / "compose.env",
            minio_endpoint_host="http://127.0.0.1:9000",
            minio_endpoint_container="http://minio:9000",
            docker_network="test-network",
            access_key="test-access",
            secret_key="test-secret",
            credential_name="test-data",
            credential_generation="v1",
            fe_config=root / "fe.toml",
            sql_config=root / "sql.toml",
        )

    def test_run_scope_is_stable_and_isolated(self) -> None:
        first = fixture.make_scope("run-a", "env-a", "a" * 64)
        repeated = fixture.make_scope("run-a", "env-a", "a" * 64)
        second = fixture.make_scope("run-b", "env-a", "a" * 64)
        self.assertEqual(first, repeated)
        self.assertNotEqual(first.prefix, second.prefix)
        self.assertEqual(fixture.validate_warehouse_uri(first.warehouse_uri), (first.bucket, first.prefix))

    def test_run_scope_rejects_path_traversal(self) -> None:
        for invalid in ("../escape", "/absolute", "space value", "", "a" * 65):
            with self.subTest(invalid=invalid), self.assertRaises(fixture.FixtureError):
                fixture.make_scope(invalid, "env-a", "a" * 64)
        for uri in (
            "s3://novarocks/shared/benchmarks",
            "s3://warehouse/fixtures/paimon-read/env/run-000000000000",
            "s3://novarocks/fixtures/paimon-read/env/../escape-000000000000",
        ):
            with self.subTest(uri=uri), self.assertRaises(fixture.FixtureError):
                fixture.validate_warehouse_uri(uri)

    def test_writer_versions_are_exact(self) -> None:
        versions = fixture.load_versions()
        self.assertEqual(versions["SPARK_VERSION"], "3.5.3")

    def test_image_repository_override_keeps_the_digest_separate(self) -> None:
        versions = fixture.load_versions()
        with mock.patch.dict(
            "os.environ",
            {"PAIMON_SPARK_IMAGE_REPOSITORY": "dockerproxy.net/apache/spark"},
        ):
            self.assertEqual(
                fixture.select_image_repository(versions),
                "dockerproxy.net/apache/spark",
            )
        with mock.patch.dict(
            "os.environ", {"PAIMON_SPARK_IMAGE_REPOSITORY": "mirror/spark@sha256:bad"}
        ):
            with self.assertRaises(fixture.FixtureError):
                fixture.select_image_repository(versions)
        self.assertEqual(versions["PAIMON_VERSION"], "1.3.1")
        self.assertEqual(
            versions["SPARK_IMAGE_MANIFEST_DIGEST"],
            "sha256:b2da01c5855fdf791328a6fa1267b406336a535d39abc05699214a48bee95955",
        )
        self.assertEqual(versions["PAIMON_SPARK_JAR_SIZE"], "41895267")
        self.assertEqual(versions["PAIMON_S3_JAR_SIZE"], "31776897")
        dockerfile = (ROOT / "Dockerfile").read_text()
        self.assertIn(versions["SPARK_IMAGE_MANIFEST_DIGEST"], dockerfile)
        self.assertNotIn(":latest", dockerfile)

    def test_rendered_sql_covers_fixed_matrix_and_has_no_secret(self) -> None:
        sql = "\n".join(fixture.render_stage(stage) for stage in fixture.STAGES)
        for required in (
            "'file.compression' = 'uncompressed'",
            "'file.compression' = 'snappy'",
            "'file.compression' = 'zstd'",
            "'file.compression' = 'lz4_raw'",
            "'manifest.compression' = 'null'",
            "'manifest.compression' = 'snappy'",
            "'manifest.compression' = 'zstd'",
            "'sequence.field' = 'seq'",
            "'deletion-vectors.enabled' = 'false'",
            "CALL paimon.sys.compact",
            "RENAME COLUMN old_name TO name",
            "DROP COLUMN retired",
            "unsupported_dv",
            "unsupported_nested",
            "unsupported_timestamp_ltz",
            "'postpone.batch-write-fixed-bucket' = 'false'",
            "NR_ORACLE|s1|unsupported_postpone|",
            "'count', count(*)",
        ):
            self.assertIn(required, sql)
        self.assertNotIn("admin123", sql)
        self.assertNotIn("AWS_SECRET_ACCESS_KEY=", sql)
        self.assertNotIn("'manifest.compression' = 'none'", sql)
        self.assertIn(
            {"table": "unsupported_postpone", "value": {"count": 0}},
            fixture.load_expected("s1"),
        )
        for stage in fixture.STAGES:
            rendered = fixture.render_stage(stage)
            for marker in ("SNAPSHOT", "SCHEMA", "FILE", "MANIFEST"):
                for table in fixture.ALL_TABLES:
                    self.assertIn(f"NR_{marker}|{stage}|{table}|", rendered)

    def test_marker_parser_keeps_external_rows_and_metadata_separate(self) -> None:
        output = "\n".join(
            (
                'NR_ORACLE|s2|pk|{"id":1,"value":"winner"}',
                'NR_FILE|s2|pk|{"file_path":"data-a.parquet","level":0}',
                'ignored log line',
            )
        )
        parsed = fixture.parse_markers(output, "s2")
        self.assertEqual(parsed["oracle"], [{"table": "pk", "value": {"id": 1, "value": "winner"}}])
        self.assertEqual(parsed["file"], [{"table": "pk", "value": {"file_path": "data-a.parquet", "level": 0}}])
        self.assertEqual(parsed["snapshot"], [])

    def test_marker_parser_accepts_spark_timing_suffix_only(self) -> None:
        output = 'NR_SNAPSHOT|s2|pk|{"snapshot_id":1}Time taken: 1.87 seconds, Fetched 4 row(s)'
        parsed = fixture.parse_markers(output, "s2")
        self.assertEqual(
            parsed["snapshot"],
            [{"table": "pk", "value": {"snapshot_id": 1}}],
        )
        with self.assertRaises(fixture.FixtureError):
            fixture.parse_markers(
                'NR_SNAPSHOT|s2|pk|{"snapshot_id":1}corrupt',
                "s2",
            )

    def test_marker_validation_rejects_incomplete_metadata(self) -> None:
        markers = {marker.lower(): [] for marker in fixture.MARKERS}
        markers["oracle"] = fixture.load_expected("s1")
        with tempfile.TemporaryDirectory() as temporary, self.assertRaises(
            fixture.FixtureError
        ):
            fixture.validate_stage_markers(Path(temporary), "s1", markers)

    def test_stage_transition_requires_snapshot_advances(self) -> None:
        def markers(stage: str, snapshot_id: int) -> dict[str, list[dict[str, object]]]:
            result = {marker.lower(): [] for marker in fixture.MARKERS}
            result["oracle"] = fixture.load_expected(stage)
            for marker in ("snapshot", "schema", "file", "manifest"):
                for table in fixture.ALL_TABLES:
                    value = {
                        "summary": True,
                        "count": 1,
                        "max_snapshot_id": snapshot_id,
                        "max_schema_id": 0,
                        "commit_kinds": ["APPEND"],
                    }
                    result[marker].append({"table": table, "value": value})
            return result

        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            (root / "stages").mkdir()
            fixture.write_json(root / "stages" / "s1.json", markers("s1", 1))
            unchanged = markers("s2", 1)
            with self.assertRaises(fixture.FixtureError):
                fixture.validate_stage_markers(root, "s2", unchanged)
            advanced = markers("s2", 1)
            for entry in advanced["snapshot"]:
                if entry["table"] in fixture.SNAPSHOT_ADVANCES["s2"]:
                    entry["value"]["max_snapshot_id"] = 2
            fixture.validate_stage_markers(root, "s2", advanced)

    def test_command_failure_is_redacted_before_it_is_raised(self) -> None:
        with self.assertRaises(fixture.FixtureError) as caught:
            fixture.run_command(
                [sys.executable, "-c", "print('test-secret'); raise SystemExit(7)"],
                redactions=("test-secret",),
            )
        self.assertNotIn("test-secret", str(caught.exception))
        self.assertIn("<redacted>", str(caught.exception))

    def test_published_configs_replace_and_scan_credentials(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            runtime = self.runtime(root)
            runtime.fe_config.write_text('access = "test-access"\nsecret = "test-secret"\n')
            runtime.sql_config.write_text('oss_ak = "test-access"\noss_sk = "test-secret"\n')
            scope = fixture.make_scope("unit-run", "test-env", "a" * 64)
            output = root / "fixture"
            output.mkdir()
            fixture.publish_configs(output, runtime, scope)
            fixture.assert_artifacts_secret_free(output, runtime)
            published = (output / "base-server.toml").read_text() + (
                output / "sql-runner.toml"
            ).read_text()
            self.assertNotIn("test-access", published)
            self.assertNotIn("test-secret", published)
            self.assertIn("${ENV:AWS_S3_ACCESS_KEY_ID}", published)
            (output / "leak.txt").write_text("test-secret")
            with self.assertRaises(fixture.FixtureError):
                fixture.assert_artifacts_secret_free(output, runtime)

    def test_remote_inventory_requires_exact_key_size_and_etag(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            runtime = self.runtime(root)
            scope = fixture.make_scope("unit-run", "test-env", "a" * 64)
            objects = [{"key": "fixture.db/t/data.parquet", "size": 10, "etag": "one"}]
            fixture.write_json(root / "objects.json", objects)
            with mock.patch.object(fixture, "collect_objects", return_value=objects):
                self.assertEqual(
                    fixture.verify_remote_inventory(root, runtime, scope.warehouse_uri),
                    objects,
                )
            changed = [{"key": "fixture.db/t/data.parquet", "size": 10, "etag": "two"}]
            with (
                mock.patch.object(fixture, "collect_objects", return_value=changed),
                self.assertRaises(fixture.FixtureError),
            ):
                fixture.verify_remote_inventory(root, runtime, scope.warehouse_uri)

    def test_format_report_comes_from_object_bytes(self) -> None:
        import fastavro
        import pyarrow.parquet

        runtime = self.runtime(Path("/tmp"))
        scope = fixture.make_scope("unit-run", "test-env", "a" * 64)
        payloads: dict[str, bytes] = {}
        objects = []
        for table, codec in (
            ("append_none", "UNCOMPRESSED"),
            ("append_snappy", "SNAPPY"),
            ("append_zstd", "ZSTD"),
            ("append_lz4", "LZ4_RAW"),
        ):
            key = f"fixture.db/{table}/bucket-0/data.parquet"
            payloads[key] = codec.encode()
            objects.append({"key": key, "size": len(codec), "etag": table})
        for table, codec in (
            ("append_none", "null"),
            ("append_snappy", "snappy"),
            ("append_zstd", "zstandard"),
        ):
            key = f"fixture.db/{table}/manifest/manifest-1"
            payloads[key] = codec.encode()
            objects.append({"key": key, "size": len(codec), "etag": table})

        class Column:
            def __init__(self, codec: str) -> None:
                self.compression = codec

        class RowGroup:
            num_columns = 1

            def __init__(self, codec: str) -> None:
                self.codec = codec

            def column(self, _: int) -> Column:
                return Column(self.codec)

        class Metadata:
            num_row_groups = 1

            def __init__(self, codec: str) -> None:
                self.codec = codec

            def row_group(self, _: int) -> RowGroup:
                return RowGroup(self.codec)

        class ParquetFile:
            def __init__(self, source: object) -> None:
                self.metadata = Metadata(source.read().decode())

        class AvroReader:
            def __init__(self, source: object) -> None:
                self.metadata = {"avro.codec": source.read().decode()}

        with (
            mock.patch.object(
                fixture,
                "read_remote_object",
                side_effect=lambda _runtime, _uri, key: payloads[key],
            ),
            mock.patch.object(pyarrow.parquet, "ParquetFile", ParquetFile),
            mock.patch.object(fastavro, "reader", AvroReader),
        ):
            report = fixture.inspect_object_formats(
                runtime, scope.warehouse_uri, objects
            )
        self.assertEqual(report["parquet"]["append_lz4"], ["LZ4"])
        self.assertEqual(report["avro_manifest"]["append_zstd"], ["zstandard"])
        self.assertEqual(len(report["inspected"]), 7)

    def test_dry_run_never_publishes_ready(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            env_file = root / "env.sh"
            env_file.write_text('export NOVA_ENV_ID="test-env"\n')
            output_dir = root / "fixture"
            result = subprocess.run(
                [
                    sys.executable,
                    str(ROOT / "fixture.py"),
                    "prepare",
                    "--run-id",
                    "unit-run",
                    "--output-dir",
                    str(output_dir),
                    "--env-file",
                    str(env_file),
                    "--stop-after",
                    "s2",
                    "--dry-run",
                ],
                check=False,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertFalse((output_dir / "READY").exists())
            dry_run = json.loads((output_dir / "dry-run.json").read_text())
            self.assertFalse(dry_run["ready_published"])
            self.assertEqual(dry_run["target_stage"], "s2")
            self.assertEqual(
                sorted(path.name for path in (output_dir / "rendered").iterdir()),
                ["s1.sql", "s2.sql"],
            )

    def test_prepare_wrapper_accepts_equals_env_file_without_sourcing_it(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            env_file = root / "env.sh"
            env_file.write_text('export NOVA_ENV_ID="wrapper-env"\n')
            output_dir = root / "fixture"
            result = subprocess.run(
                [
                    str(ROOT / "prepare.sh"),
                    f"--env-file={env_file}",
                    "--run-id",
                    "wrapper-run",
                    "--output-dir",
                    str(output_dir),
                    "--dry-run",
                ],
                check=False,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertTrue((output_dir / "dry-run.json").is_file())
        for name in ("prepare.sh", "cleanup.sh"):
            script = (ROOT / name).read_text()
            self.assertIn("--env-file=*", script)
            self.assertNotIn('source "$ENV_FILE"', script)

    def test_ready_is_content_addressed_and_rejects_tampering(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            output_dir = Path(temporary)
            scope = fixture.make_scope("unit-run", "test-env", "a" * 64)
            artifacts = []
            for name, value in (
                ("state.json", {}),
                ("objects.json", []),
                ("formats.json", {}),
            ):
                path = output_dir / name
                fixture.write_json(path, value)
                artifacts.append(fixture.artifact_entry(path, output_dir))
            manifest = {
                "fixture_kind": fixture.FIXTURE_KIND,
                "warehouse_uri": scope.warehouse_uri,
                "secret_scan": "passed",
                "artifacts": artifacts,
            }
            fixture.write_json(output_dir / "manifest.json", manifest)
            (output_dir / "READY").write_text(
                f"sha256:{fixture.sha256_file(output_dir / 'manifest.json')}\n"
            )
            fixture.verify_ready(output_dir)
            (output_dir / "objects.json").write_text("tampered\n")
            with self.assertRaises(fixture.FixtureError):
                fixture.verify_ready(output_dir)

    def test_prepare_is_idempotent_at_an_exact_ready_stage(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            env_file = root / "env.sh"
            env_file.write_text('export NOVA_ENV_ID="test-env"\n')
            output_dir = root / "fixture"
            runtime = self.runtime(root)
            expected = fixture.load_expected("s1")
            lines = [
                f"NR_ORACLE|s1|{entry['table']}|"
                + json.dumps(entry["value"], separators=(",", ":"))
                for entry in expected
            ]
            for marker in ("SNAPSHOT", "SCHEMA", "FILE", "MANIFEST"):
                for table in fixture.ALL_TABLES:
                    value = {
                        "summary": True,
                        "count": 1,
                        "max_snapshot_id": 1,
                        "max_schema_id": 0,
                        "commit_kinds": ["APPEND"],
                    }
                    lines.append(
                        f"NR_{marker}|s1|{table}|"
                        + json.dumps(value, separators=(",", ":"))
                    )
            spark_output = "\n".join(lines)

            def publish_configs(target: Path, *_: object) -> None:
                for name in ("catalog.sql", "base-server.toml", "sql-runner.toml"):
                    (target / name).write_text(f"{name}\n")

            args = Namespace(
                run_id="unit-run",
                output_dir=str(output_dir),
                env_file=str(env_file),
                stop_after="s1",
                dry_run=False,
            )
            with (
                mock.patch.object(fixture, "load_runtime", return_value=runtime),
                mock.patch.object(fixture, "build_image", return_value="fixture:image") as build,
                mock.patch.object(fixture, "run_spark_stage", return_value=spark_output),
                mock.patch.object(
                    fixture,
                    "collect_objects",
                    return_value=[{"key": "fixture.db/table/schema/schema-0", "size": 1, "etag": "e"}],
                ) as inventory,
                mock.patch.object(
                    fixture,
                    "inspect_object_formats",
                    return_value={"parquet": {}, "avro_manifest": {}, "inspected": []},
                ),
                mock.patch.object(fixture, "publish_configs", side_effect=publish_configs),
                redirect_stdout(StringIO()),
            ):
                self.assertEqual(fixture.execute_prepare(args), 0)
                first_ready = (output_dir / "READY").read_text()
                self.assertEqual(fixture.execute_prepare(args), 0)
                self.assertEqual((output_dir / "READY").read_text(), first_ready)
                self.assertEqual(build.call_count, 1)
                self.assertEqual(inventory.call_count, 2)

    def test_cleanup_asserts_owned_prefix_is_empty(self) -> None:
        runtime = self.runtime(Path("/tmp"))
        scope = fixture.make_scope("unit-run", "test-env", "a" * 64)
        with (
            mock.patch.object(fixture, "cleanup_prefix") as cleanup,
            mock.patch.object(
                fixture,
                "collect_objects",
                return_value=[{"key": "leftover", "size": 1, "etag": "e"}],
            ) as inventory,
            self.assertRaises(fixture.FixtureError),
        ):
            fixture.cleanup_and_assert_empty(runtime, scope.warehouse_uri)
        cleanup.assert_called_once_with(runtime, scope.warehouse_uri)
        inventory.assert_called_once_with(runtime, scope.warehouse_uri, allow_empty=True)

    def test_cleanup_dry_run_checks_exact_run_without_docker(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            output_dir = Path(temporary)
            scope = fixture.make_scope("unit-run", "test-env", "a" * 64)
            artifacts = []
            for name, value in (
                ("state.json", {}),
                ("objects.json", []),
                ("formats.json", {}),
            ):
                path = output_dir / name
                fixture.write_json(path, value)
                artifacts.append(fixture.artifact_entry(path, output_dir))
            manifest = {
                "fixture_kind": fixture.FIXTURE_KIND,
                "run_id": "unit-run",
                "warehouse_uri": scope.warehouse_uri,
                "secret_scan": "passed",
                "artifacts": artifacts,
            }
            fixture.write_json(output_dir / "manifest.json", manifest)
            (output_dir / "READY").write_text(
                f"sha256:{fixture.sha256_file(output_dir / 'manifest.json')}\n"
            )
            arguments = Namespace(
                output_dir=str(output_dir),
                env_file="unused",
                run_id="wrong-run",
                dry_run=True,
            )
            with self.assertRaises(fixture.FixtureError):
                fixture.execute_cleanup(arguments)
            arguments.run_id = "unit-run"
            with redirect_stdout(StringIO()):
                self.assertEqual(fixture.execute_cleanup(arguments), 0)
            self.assertTrue((output_dir / "READY").is_file())


if __name__ == "__main__":
    unittest.main()
