from __future__ import annotations

import hashlib
import importlib.util
import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock


ROOT = Path(__file__).resolve().parents[1]
for module_name in ("fixture_inputs", "python_environment", "provision", "verify"):
    spec = importlib.util.spec_from_file_location(module_name, ROOT / f"{module_name}.py")
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)

fixture_inputs = sys.modules["fixture_inputs"]
verify_module = sys.modules["verify"]
provision_module = sys.modules["provision"]


def source_lock(effective):
    """Test input serializer: production accepts only explicit schema-2 variants."""
    import copy
    source = copy.deepcopy(effective)
    source["schema"] = 2
    source.pop("platform", None)
    for kind in ("images", "derived_images"):
        for name, item in source[kind].items():
            target = item.pop("platform")
            variant = {"platform": target}
            for field in (("alias", "manifest_digest") if kind == "images" else ("alias",)):
                variant[field] = item.pop(field)
            item["variants"] = {target: variant}
    return source


def setUpModule():
    # Most tests use a synthetic daemon; native agreement is tested separately.
    global daemon_patches
    daemon_patches = [mock.patch.object(verify_module, "require_native_daemon"),
                      mock.patch.object(provision_module, "require_native_daemon"),
                      mock.patch.object(fixture_inputs, "native_platform", return_value="linux/arm64"),
                      mock.patch.object(provision_module, "native_platform", return_value="linux/arm64")]
    for patch in daemon_patches:
        patch.start()


def tearDownModule():
    for patch in reversed(daemon_patches):
        patch.stop()


class FixtureInputsTest(unittest.TestCase):
    def test_oss_closure_excludes_minio_and_builds_original_recipes_first(self):
        lock, _ = fixture_inputs.load_lock(ROOT / "lock.json", platform="linux/amd64", consumer="iceberg-rest-oss")
        self.assertNotIn("minio", lock["derived_images"])
        self.assertNotIn("minio-mc", lock["derived_images"])
        self.assertEqual(set(lock["python_environments"]), {"oss-sdk"})
        order = fixture_inputs.derived_build_order(lock)
        self.assertLess(order.index("iceberg-spark"), order.index("iceberg-spark-oss"))
        self.assertLess(order.index("rest-mv"), order.index("rest-mv-oss"))
        original, _ = fixture_inputs.load_lock(ROOT / "lock.json", platform="linux/arm64", consumer="iceberg-rest")
        self.assertEqual(original["python_environments"], {})
        with self.assertRaisesRegex(fixture_inputs.FixtureInputError, "unsupported fixture input platform"):
            fixture_inputs.load_lock(ROOT / "lock.json", platform="linux/arm64", consumer="iceberg-rest-oss")

    def test_derived_recipe_cycle_is_rejected_before_build(self):
        lock = {"images": {}, "artifacts": {}, "derived_images": {
            "first": {"bases": {}, "artifacts": [], "derived_bases": {"BASE": "second"}},
            "second": {"bases": {}, "artifacts": [], "derived_bases": {"BASE": "first"}},
        }}
        with self.assertRaisesRegex(fixture_inputs.FixtureInputError, "cycle"):
            fixture_inputs.selected_input_names(lock, "derived:first")

    def test_python_receipt_verification_is_read_only_and_detects_changed_sdk(self):
        sdk = sys.modules["python_environment"]
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            (root / "definition.py").write_text("# SDK environment definition\n")
            directory = root / "generations/one/python-environments/sdk"
            (directory / "bin").mkdir(parents=True)
            (directory / "bin/python").write_bytes(b"interpreter")
            (directory / "sdk.py").write_bytes(b"original")
            item = {"definition_files": ["definition.py"], "packages": {"sdk": "1.0"}, "python_version": "3.14"}
            receipt = {"path": str(directory.relative_to(root)), "definition_sha256": sdk.definition(root, item),
                "packages": item["packages"], "python_version": "3.14", "files": sdk.environment_files(directory)}
            lock = {"python_environments": {"sdk": item}}
            bom = {"python_environments": {"sdk": receipt}}
            before = {str(p): p.read_bytes() for p in root.rglob("*") if p.is_file()}
            with mock.patch.object(sdk, "run", side_effect=AssertionError("verify dispatched a process")):
                sdk.verify_environments(root, root, lock, bom)
            self.assertEqual(before, {str(p): p.read_bytes() for p in root.rglob("*") if p.is_file()})
            (directory / "sdk.py").write_bytes(b"changed")
            with self.assertRaisesRegex(fixture_inputs.FixtureInputError, "content mismatch"):
                sdk.verify_environments(root, root, lock, bom)

    def test_default_arm64_variant_matches_locked_platform(self) -> None:
        item = {"platform": "linux/arm64", "manifest_digest": "sha256:expected"}
        image = {
            "Os": "linux",
            "Architecture": "arm64",
            "Variant": "v8",
            "Id": "sha256:expected",
        }
        fixture_inputs.verify_image(image, item)
        with self.assertRaisesRegex(
            fixture_inputs.FixtureInputError, "platform mismatch"
        ):
            fixture_inputs.verify_image({**image, "Variant": "v7"}, item)

    def test_lock_is_immutable_and_covers_all_current_inputs(self) -> None:
        lock, digest = fixture_inputs.load_lock(ROOT / "lock.json", platform="linux/amd64")
        self.assertEqual(lock["schema"], 2)
        self.assertEqual(
            lock["images"]["paimon-spark-base"]["platform"], "linux/amd64"
        )
        self.assertIn("paimon-writer", lock["derived_images"])
        self.assertIn("iceberg-spark", lock["derived_images"])
        self.assertEqual(len(digest), 64)
        self.assertNotIn("latest", str(lock))

    def test_artifact_validation_requires_size_and_checksum(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            artifact = Path(temporary) / "artifact.jar"
            artifact.write_bytes(b"fixture")
            item = {"bytes": 7, "sha1": hashlib.sha1(b"fixture").hexdigest()}
            receipt = fixture_inputs.validate_artifact(artifact, item)
            self.assertEqual(receipt["sha256"], hashlib.sha256(b"fixture").hexdigest())
            artifact.write_bytes(b"changed")
            with self.assertRaises(fixture_inputs.FixtureInputError):
                fixture_inputs.validate_artifact(artifact, item)

    def test_definition_rejects_escape(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            with self.assertRaises(fixture_inputs.FixtureInputError):
                fixture_inputs.definition_sha256(Path(temporary), ["../outside"])

    def test_store_lock_uses_exclusive_publish_and_shared_verify_modes(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            store = Path(temporary)
            with mock.patch.object(fixture_inputs.fcntl, "flock") as flock:
                with fixture_inputs.fixture_store_lock(
                    store, exclusive=True, create=True
                ):
                    self.assertTrue((store / ".provision.lock").is_file())
                self.assertEqual(
                    flock.call_args_list,
                    [
                        mock.call(mock.ANY, fixture_inputs.fcntl.LOCK_EX),
                        mock.call(mock.ANY, fixture_inputs.fcntl.LOCK_UN),
                    ],
                )
            with mock.patch.object(fixture_inputs.fcntl, "flock") as flock:
                with fixture_inputs.fixture_store_lock(store, exclusive=False):
                    pass
                self.assertEqual(
                    flock.call_args_list,
                    [
                        mock.call(mock.ANY, fixture_inputs.fcntl.LOCK_SH),
                        mock.call(mock.ANY, fixture_inputs.fcntl.LOCK_UN),
                    ],
                )

    def test_store_lock_does_not_reclassify_body_oserror(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            store = Path(temporary)
            (store / ".provision.lock").touch()
            failure = FileNotFoundError("protected artifact disappeared")
            with mock.patch.object(fixture_inputs.fcntl, "flock") as flock:
                with self.assertRaises(FileNotFoundError) as raised:
                    with fixture_inputs.fixture_store_lock(store, exclusive=False):
                        raise failure
                self.assertIs(raised.exception, failure)
                self.assertEqual(
                    flock.call_args_list[-1],
                    mock.call(mock.ANY, fixture_inputs.fcntl.LOCK_UN),
                )

    def test_missing_lock_still_reports_its_own_prerequisite(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            with self.assertRaisesRegex(
                fixture_inputs.FixtureInputError, "cannot acquire fixture store lock"
            ):
                with fixture_inputs.fixture_store_lock(Path(temporary), exclusive=False):
                    self.fail("missing lock must not admit the protected body")

    def test_command_launch_failure_inside_lock_names_the_command(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            store = Path(temporary)
            (store / ".provision.lock").touch()
            with mock.patch.object(
                fixture_inputs.subprocess,
                "run",
                side_effect=FileNotFoundError(2, "No such file or directory", "docker"),
            ):
                with self.assertRaisesRegex(
                    fixture_inputs.FixtureInputError,
                    "cannot start fixture command docker",
                ) as raised:
                    with fixture_inputs.fixture_store_lock(store, exclusive=False):
                        fixture_inputs.inspect_image("locked-image")
            self.assertNotIn(
                "cannot acquire fixture store lock", str(raised.exception)
            )

    def test_missing_bom_blocks_without_docker_or_network(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            with mock.patch.object(verify_module, "inspect_image") as inspect:
                with self.assertRaises(fixture_inputs.FixtureInputError) as raised:
                    verify_module.verify(
                        Path(temporary), ROOT.parents[1], ROOT / "lock.json", consumer="iceberg-rest"
                    )
            inspect.assert_not_called()
            self.assertIn("BOM is missing", str(raised.exception))

    def test_verify_rejects_base_image_bom_identity_tampering_before_inspect(self) -> None:
        lock = {
            "schema": 2, "platform": "linux/amd64",
            "images": {
                "spark": {
                    "alias": "novarocks/fixture-spark:current",
                    "manifest_digest": "sha256:expected",
                    "platform": "linux/amd64",
                }
            },
            "artifacts": {},
            "derived_images": {},
        }
        with tempfile.TemporaryDirectory() as temporary:
            store = Path(temporary)
            (store / ".provision.lock").touch()
            (store / "READY").write_text("sha256:lock-sha\n")
            (store / "bom.json").write_text(
                '{"schema":2,"platform":"linux/amd64","lock_sha256":"lock-sha","artifact_dir":"artifacts",'
                '"images":{"spark":{"alias":"novarocks/fixture-spark:current",'
                '"manifest_digest":"sha256:tampered","platform":"linux/arm64"}},'
                '"artifacts":{},"derived_images":{}}'
            )
            with mock.patch.object(
                verify_module, "load_lock", return_value=(lock, "lock-sha")
            ), mock.patch.object(verify_module, "inspect_image") as inspect:
                with self.assertRaises(fixture_inputs.FixtureInputError) as raised:
                    verify_module.verify(store, ROOT.parents[1], ROOT / "lock.json")
            inspect.assert_not_called()
            self.assertIn("image BOM receipt mismatch", str(raised.exception))

    def test_verify_rejects_derived_image_id_bom_tampering(self) -> None:
        lock = {
            "schema": 2, "platform": "linux/amd64",
            "images": {},
            "artifacts": {},
            "derived_images": {
                "writer": {
                    "alias": "novarocks/fixture-writer:current",
                    "platform": "linux/amd64",
                    "definition_files": ["docker/paimon-read/Dockerfile"],
                }
            },
        }
        with tempfile.TemporaryDirectory() as temporary:
            store = Path(temporary)
            (store / ".provision.lock").touch()
            (store / "READY").write_text("sha256:lock-sha\n")
            (store / "bom.json").write_text(
                '{"schema":2,"platform":"linux/amd64","lock_sha256":"lock-sha","artifact_dir":"artifacts",'
                '"images":{},"artifacts":{},"derived_images":{"writer":'
                '{"alias":"novarocks/fixture-writer:current","platform":"linux/amd64",'
                '"definition_sha256":"definition-sha","image_id":"sha256:tampered"}}}'
            )
            image = {
                "Id": "sha256:actual",
                "Os": "linux",
                "Architecture": "amd64",
                "Config": {
                    "Labels": {
                        "novarocks.fixture.lock.sha256": "lock-sha",
                        "novarocks.fixture.definition.sha256": "definition-sha",
                    }
                },
            }
            with mock.patch.object(
                verify_module, "load_lock", return_value=(lock, "lock-sha")
            ), mock.patch.object(
                verify_module, "definition_sha256", return_value="definition-sha"
            ), mock.patch.object(
                verify_module, "inspect_image", return_value=image
            ):
                with self.assertRaises(fixture_inputs.FixtureInputError) as raised:
                    verify_module.verify(store, ROOT.parents[1], ROOT / "lock.json")
            self.assertIn("derived image BOM receipt mismatch", str(raised.exception))

    def test_image_source_override_changes_transport_but_not_identity(self) -> None:
        lock, _ = fixture_inputs.load_lock(ROOT / "lock.json", platform="linux/amd64")
        item = lock["images"]["paimon-spark-base"]
        overrides = provision_module.parse_image_sources(
            ["paimon-spark-base=dockerproxy.net/apache/spark"], set(lock["images"])
        )
        source, reference = provision_module.image_reference(
            "paimon-spark-base", item, overrides
        )
        self.assertEqual(source, "dockerproxy.net/apache/spark")
        self.assertEqual(reference, f"{source}@{item['manifest_digest']}")
        with self.assertRaises(fixture_inputs.FixtureInputError):
            provision_module.parse_image_sources(
                ["unknown=example.invalid/spark"], set(lock["images"])
            )
        with self.assertRaises(fixture_inputs.FixtureInputError):
            provision_module.parse_image_sources(
                ["paimon-spark-base=example.invalid/spark@sha256:override"],
                set(lock["images"]),
            )

    def test_artifact_url_prefixes_require_closed_https_directory_urls(self):
        valid = "https://upstream.invalid/maven2/=https://mirror.invalid/public/"
        parsed = provision_module.parse_artifact_url_prefixes([valid])
        self.assertEqual({"https://upstream.invalid/maven2/": "https://mirror.invalid/public/"}, parsed)
        invalid = ["missing-separator", "http://upstream.invalid/path/=https://mirror.invalid/",
                   "https://upstream.invalid/path/=http://mirror.invalid/",
                   "https://user:secret@upstream.invalid/path/=https://mirror.invalid/",
                   "https://upstream.invalid/path/=https://user@mirror.invalid/",
                   "https://upstream.invalid/path?query/=https://mirror.invalid/",
                   "https://upstream.invalid/path/=https://mirror.invalid/path/#fragment/",
                   "https://upstream.invalid/path/=https://mirror.invalid/path?/",
                   "https://upstream.invalid/path=https://mirror.invalid/",
                   "https://upstream.invalid/path/=https://mirror.invalid/path",
                   "https://upstream.invalid/pa\tth/=https://mirror.invalid/",
                   "https://upstream.invalid/path/=https://mirror.invalid:70000/",
                   "https:///path/=https://mirror.invalid/", "=https://mirror.invalid/"]
        for spec in invalid:
            with self.subTest(spec=spec), self.assertRaises(fixture_inputs.FixtureInputError):
                provision_module.parse_artifact_url_prefixes([spec])
        for specs in ([valid, valid], [valid, "https://upstream.invalid/maven2/nested/=https://other.invalid/"],
                      [valid, "https://upstream.invalid/=https://other.invalid/"]):
            with self.assertRaisesRegex(fixture_inputs.FixtureInputError, "overlapping"):
                provision_module.parse_artifact_url_prefixes(specs)

    def test_artifact_transport_rewrite_matches_original_once(self):
        prefixes = provision_module.parse_artifact_url_prefixes([
            "https://upstream.invalid/maven2/=https://mirror.invalid/public/",
            "https://mirror.invalid/public/=https://another.invalid/maven/"])
        self.assertEqual("https://mirror.invalid/public/group/artifact.jar",
            provision_module.artifact_transport_url("https://upstream.invalid/maven2/group/artifact.jar", prefixes))
        self.assertEqual("https://upstream.invalid/maven20/a.jar",
            provision_module.artifact_transport_url("https://upstream.invalid/maven20/a.jar", prefixes))

    def test_invalid_artifact_transport_fails_before_daemon_or_store(self):
        with tempfile.TemporaryDirectory() as temporary, mock.patch.object(provision_module, "require_native_daemon") as daemon:
            store = Path(temporary) / "absent-store"
            with self.assertRaises(fixture_inputs.FixtureInputError):
                provision_module.provision(store, ROOT.parents[1], ROOT / "lock.json", [], 30,
                    artifact_url_prefix_specs=["https://user@host.invalid/=https://mirror.invalid/"])
            daemon.assert_not_called()
            self.assertFalse(store.exists())

    def test_mirrored_artifact_keeps_locked_identity_and_content_cache(self):
        data = b"exact locked upstream bytes"
        item = {"url": "https://upstream.invalid/maven2/group/input.jar", "bytes": len(data),
                "sha1": hashlib.sha1(data).hexdigest(), "sha256": hashlib.sha256(data).hexdigest()}
        lock = {"artifacts": {"input.jar": item}}
        original = json.dumps(lock, sort_keys=True)
        prefixes = provision_module.parse_artifact_url_prefixes([
            "https://upstream.invalid/maven2/=https://mirror.invalid/public/"])
        with tempfile.TemporaryDirectory() as temporary:
            store = Path(temporary)
            first = store / ".staging-first"; first.mkdir()
            with mock.patch.object(provision_module, "download", side_effect=lambda _, path: path.write_bytes(data)) as download:
                receipt = provision_module.prepare_artifacts(lock, first, prefixes)
            download.assert_called_once_with("https://mirror.invalid/public/group/input.jar", first / "artifacts/input.jar")
            self.assertEqual(original, json.dumps(lock, sort_keys=True))
            cache = store / "downloads" / f"sha256-{item['sha256']}-{len(data)}"
            self.assertEqual(data, cache.read_bytes())
            second = store / ".staging-second"; second.mkdir()
            with mock.patch.object(provision_module, "download") as download:
                reused = provision_module.prepare_artifacts(lock, second)
            download.assert_not_called()
            self.assertEqual(receipt, reused)
            self.assertEqual([cache], list((store / "downloads").iterdir()))
            self.assertFalse((store / "READY").exists())

    def test_wrong_mirror_bytes_fail_original_size_sha1_and_sha256_without_fallback(self):
        data = b"locked-content"
        base = {"url": "https://upstream.invalid/maven2/input.jar", "bytes": len(data),
                "sha1": hashlib.sha1(data).hexdigest(), "sha256": hashlib.sha256(data).hexdigest()}
        prefixes = provision_module.parse_artifact_url_prefixes([
            "https://upstream.invalid/maven2/=https://mirror.invalid/public/"])
        for fault in ("mirror-content", "bytes", "sha1", "sha256"):
            item = dict(base)
            mirror_bytes = data[::-1] if fault == "mirror-content" else data
            if fault != "mirror-content":
                item[fault] = len(data) + 1 if fault == "bytes" else "0" * len(item[fault])
            with self.subTest(fault=fault), tempfile.TemporaryDirectory() as temporary:
                staging = Path(temporary) / ".staging-failed"; staging.mkdir()
                with mock.patch.object(provision_module, "download", side_effect=lambda _, path: path.write_bytes(mirror_bytes)) as download:
                    with self.assertRaises(fixture_inputs.FixtureInputError):
                        provision_module.prepare_artifacts({"artifacts": {"input.jar": item}}, staging, prefixes)
                self.assertEqual(1, download.call_count)
                self.assertFalse((staging.parent / "downloads").exists())
                self.assertFalse((staging.parent / "READY").exists())
        with tempfile.TemporaryDirectory() as temporary, mock.patch.object(provision_module, "download",
                side_effect=fixture_inputs.FixtureInputError("mirror unavailable")) as download:
            with self.assertRaisesRegex(fixture_inputs.FixtureInputError, "mirror unavailable"):
                provision_module.prepare_artifacts({"artifacts": {"input.jar": base}}, Path(temporary), prefixes)
            self.assertEqual(1, download.call_count)
            self.assertEqual("https://mirror.invalid/public/input.jar", download.call_args.args[0])

    def test_provision_cli_forwards_repeatable_artifact_transports(self):
        prefixes = ["https://upstream.invalid/maven2/=https://mirror.invalid/public/",
                    "https://files.invalid/packages/=https://mirror.invalid/packages/"]
        args = ["provision.py", "--consumer", "iceberg-rest-oss", "--platform", "linux/amd64"]
        for spec in prefixes:
            args += ["--artifact-url-prefix", spec]
        with mock.patch.object(sys, "argv", args), mock.patch.object(provision_module, "provision") as provision:
            self.assertEqual(0, provision_module.main())
        self.assertEqual(prefixes, provision.call_args.kwargs["artifact_url_prefix_specs"])
        context_args = args + ["--context-only", "rest-mv", "--out", "/example/output"]
        with mock.patch.object(sys, "argv", context_args), mock.patch.object(provision_module, "context_only") as context:
            self.assertEqual(0, provision_module.main())
        self.assertEqual(prefixes, context.call_args.kwargs["artifact_url_prefix_specs"])

    def test_provision_reuses_a_verified_local_image_before_pulling(self) -> None:
        item = {
            "source": "example.invalid/spark",
            "platform": "linux/amd64",
            "manifest_digest": "sha256:fixture",
            "alias": "novarocks/fixture:test",
        }
        with mock.patch.object(provision_module, "inspect_image", return_value={}), mock.patch.object(
            provision_module, "verify_image"
        ), mock.patch.object(provision_module, "run") as run:
            provision_module.prepare_images({"images": {"spark": item}}, {}, 30)
        self.assertEqual(
            run.call_args_list,
            [mock.call(["docker", "tag", "example.invalid/spark@sha256:fixture", item["alias"]])],
        )


class ConsumerClosureTests(unittest.TestCase):
    def fixture(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        root = Path(temporary.name)
        store, repo = root / "store", root / "repo"
        store.mkdir(); repo.mkdir(); (store / ".provision.lock").touch()
        artifacts = store / "artifacts"; artifacts.mkdir()
        lock = {"schema": 2, "platform": "linux/arm64", "images": {}, "artifacts": {}, "derived_images": {}}
        bom = {"schema": 2, "platform": "linux/arm64", "artifact_dir": "artifacts", "images": {}, "artifacts": {}, "derived_images": {}}
        infos = {}
        for name in ("minio-runtime", "minio-go-builder", "iceberg-rest", "iceberg-spark-base", "paimon-spark-base", "gradle-builder"):
            item = {"alias": f"fixture/{name}:locked", "platform": "linux/arm64", "manifest_digest": f"sha256:{name}"}
            lock["images"][name] = item
            bom["images"][name] = item.copy()
            infos[item["alias"]] = {"Id": item["manifest_digest"], "Os": "linux", "Architecture": "arm64"}
        for name in ("spark.jar", "paimon.jar", "source.tar.gz"):
            path = artifacts / name; path.write_bytes(b"fixture")
            item = {"url": "https://example.invalid/" + name, "bytes": 7, "sha1": hashlib.sha1(b"fixture").hexdigest()}
            lock["artifacts"][name] = item
            bom["artifacts"][name] = fixture_inputs.validate_artifact(path, item)
        for name, base, jar in (("iceberg-spark", "iceberg-spark-base", "spark.jar"), ("paimon-writer", "paimon-spark-base", "paimon.jar"), ("rest-mv", "gradle-builder", "source.tar.gz"), ("minio", "minio-go-builder", "source.tar.gz"), ("minio-mc", "minio-runtime", "source.tar.gz")):
            definition = repo / f"{name}.txt"; definition.write_text(name)
            item = {"alias": f"fixture/{name}:current", "platform": "linux/arm64", "bases": ({"BUILDER": base, "REST_BASE": "iceberg-rest"} if name == "rest-mv" else {"SPARK_BASE": base}), "artifacts": [jar], "definition_files": [definition.name]}
            lock["derived_images"][name] = item
            digest = fixture_inputs.definition_sha256(repo, item["definition_files"])
            bom["derived_images"][name] = {"alias": item["alias"], "platform": item["platform"], "definition_sha256": digest, "image_id": f"sha256:{name}-id"}
        raw = source_lock(lock)
        lock_sha = fixture_inputs.resolve_lock(raw, "linux/arm64")[1]
        bom["lock_sha256"] = lock_sha
        for name, item in lock["derived_images"].items():
            receipt = bom["derived_images"][name]
            infos[item["alias"]] = {"Id": receipt["image_id"], "Os": "linux", "Architecture": "arm64", "Config": {"Labels": {"novarocks.fixture.lock.sha256": lock_sha, "novarocks.fixture.definition.sha256": receipt["definition_sha256"]}}}
        lock_path = root / "lock.json"
        lock_path.write_bytes(fixture_inputs.canonical_bytes(raw))
        (store / "bom.json").write_bytes(fixture_inputs.canonical_bytes(bom))
        (store / "READY").write_text(f"sha256:{lock_sha}\n")
        return store, repo, lock_path, infos

    def test_actual_iceberg_consumer_closure_is_exact(self):
        lock, _ = fixture_inputs.load_lock(ROOT / "lock.json", platform="linux/amd64")
        images, artifacts, derived = fixture_inputs.required_input_names(lock, "iceberg-rest")
        self.assertEqual(images, {"minio-runtime", "iceberg-rest", "iceberg-spark-base", "gradle-builder"})
        self.assertEqual(derived, {"iceberg-spark", "rest-mv", "minio", "minio-mc"})
        self.assertEqual(artifacts, set().union(*(lock["derived_images"][name]["artifacts"] for name in derived)))
        self.assertNotIn("paimon-writer", derived)
        self.assertNotIn("paimon-spark-base", images)

    def test_declared_dependency_changes_are_followed_without_name_based_exclusions(self):
        lock, _ = fixture_inputs.load_lock(ROOT / "lock.json", platform="linux/amd64")
        lock["derived_images"]["iceberg-spark"]["bases"]["BUILDER"] = "paimon-spark-base"
        lock["derived_images"]["iceberg-spark"]["artifacts"].append("paimon-s3.jar")
        images, artifacts, derived = fixture_inputs.required_input_names(lock, "iceberg-rest")
        self.assertIn("paimon-spark-base", images)
        self.assertIn("iceberg-spark-base", images)
        self.assertIn("paimon-s3.jar", artifacts)
        self.assertEqual(derived, {"iceberg-spark", "rest-mv", "minio", "minio-mc"})

    def test_full_verifier_rejects_unrelated_drift_but_iceberg_consumer_does_not_read_it(self):
        store, repo, lock, infos = self.fixture()
        (repo / "paimon-writer.txt").write_text("new unrelated definition")
        with mock.patch.object(verify_module, "inspect_image", side_effect=infos.__getitem__) as inspect:
            verified = verify_module.verify(store, repo, lock, consumer="iceberg-rest")
            self.assertEqual(verified["schema"], 2)
            self.assertEqual(set(call.args[0] for call in inspect.call_args_list), {"fixture/minio-runtime:locked", "fixture/minio-go-builder:locked", "fixture/minio:current", "fixture/minio-mc:current", "fixture/iceberg-rest:locked", "fixture/iceberg-spark-base:locked", "fixture/iceberg-spark:current", "fixture/gradle-builder:locked", "fixture/rest-mv:current"})
            with self.assertRaisesRegex(fixture_inputs.FixtureInputError, "definition mismatch: paimon-writer"):
                verify_module.verify(store, repo, lock)

    def test_required_definition_checksum_identity_and_platform_stay_strict(self):
        for fault in ("definition", "artifact", "image-id", "platform", "base-digest",
                      "mv-definition", "mv-artifact", "mv-image-id", "mv-platform", "builder-digest"):
            with self.subTest(fault=fault):
                store, repo, lock, infos = self.fixture()
                if fault == "definition": (repo / "iceberg-spark.txt").write_text("changed")
                elif fault == "artifact": (store / "artifacts/spark.jar").write_bytes(b"changed")
                elif fault == "image-id": infos["fixture/iceberg-spark:current"]["Id"] = "sha256:wrong"
                elif fault == "platform": infos["fixture/iceberg-spark:current"]["Architecture"] = "amd64"
                elif fault == "mv-definition": (repo / "rest-mv.txt").write_text("changed")
                elif fault == "mv-artifact": (store / "artifacts/source.tar.gz").write_bytes(b"changed")
                elif fault == "mv-image-id": infos["fixture/rest-mv:current"]["Id"] = "sha256:wrong"
                elif fault == "mv-platform": infos["fixture/rest-mv:current"]["Architecture"] = "amd64"
                elif fault == "builder-digest": infos["fixture/gradle-builder:locked"]["Id"] = "sha256:wrong"
                else: infos["fixture/iceberg-spark-base:locked"]["Id"] = "sha256:wrong"
                with mock.patch.object(verify_module, "inspect_image", side_effect=infos.__getitem__), self.assertRaises(fixture_inputs.FixtureInputError):
                    verify_module.verify(store, repo, lock, consumer="iceberg-rest")

    def test_global_publication_anchor_remains_required_before_inspection(self):
        for fault in ("READY", "BOM"):
            with self.subTest(fault=fault):
                store, repo, lock, infos = self.fixture()
                if fault == "READY": (store / "READY").write_text("sha256:wrong\n")
                else:
                    bom = fixture_inputs.read_json(store / "bom.json")
                    bom["lock_sha256"] = "wrong"
                    (store / "bom.json").write_bytes(fixture_inputs.canonical_bytes(bom))
                with mock.patch.object(verify_module, "inspect_image") as inspect, self.assertRaises(fixture_inputs.FixtureInputError):
                    verify_module.verify(store, repo, lock, consumer="iceberg-rest")
                inspect.assert_not_called()

    def test_unknown_consumer_or_missing_declared_dependency_is_not_a_smaller_scope(self):
        lock, _ = fixture_inputs.load_lock(ROOT / "lock.json", platform="linux/amd64")
        with self.assertRaisesRegex(fixture_inputs.FixtureInputError, "unknown fixture input consumer"):
            fixture_inputs.required_input_names(lock, "unregistered")
        for category, name in (("images", "iceberg-spark-base"), ("artifacts", "hadoop-aws-3.3.4.jar"), ("derived_images", "iceberg-spark")):
            with self.subTest(category=category):
                selected = {key: value.copy() if isinstance(value, dict) else value for key, value in lock.items()}
                selected[category].pop(name)
                with self.assertRaises(fixture_inputs.FixtureInputError):
                    fixture_inputs.required_input_names(selected, "iceberg-rest")


    def test_partial_provision_does_not_regress_or_republish_verified_all(self):
        store, repo, lock, infos = self.fixture()
        original = (store / "bom.json").read_bytes()
        ready = (store / "READY").read_bytes()
        with mock.patch.object(verify_module, "inspect_image", side_effect=infos.__getitem__), \
                mock.patch.object(provision_module, "prepare_images") as images, \
                mock.patch.object(provision_module, "prepare_artifacts") as artifacts, \
                mock.patch.object(provision_module, "build_derived_images") as derived:
            provision_module.provision(store, repo, lock, [], 30, consumer="object-store")
            provision_module.provision(store, repo, lock, [], 30, consumer="iceberg-rest")
            provision_module.provision(store, repo, lock, [], 30)
            verify_module.verify(store, repo, lock)
        images.assert_not_called(); artifacts.assert_not_called(); derived.assert_not_called()
        self.assertEqual((store / "bom.json").read_bytes(), original)
        self.assertEqual((store / "READY").read_bytes(), ready)

    def test_supplement_rechecks_and_copies_valid_other_consumer_inputs(self):
        store, repo, lock, infos = self.fixture()
        complete = fixture_inputs.read_json(store / "bom.json")
        partial = json.loads(json.dumps(complete))
        del partial["artifacts"]["paimon.jar"]
        del partial["derived_images"]["paimon-writer"]
        (store / "bom.json").write_bytes(fixture_inputs.canonical_bytes(partial))
        def images(selected, *_):
            return {name: complete["images"][name] for name in selected["images"]}
        def download(_, path):
            path.write_bytes(b"fixture")
        def derived(selected, *_):
            return {name: complete["derived_images"][name] for name in selected["derived_images"]}
        with mock.patch.object(verify_module, "inspect_image", side_effect=infos.__getitem__), \
                mock.patch.object(provision_module, "prepare_images", side_effect=images), \
                mock.patch.object(provision_module, "download", side_effect=download), \
                mock.patch.object(provision_module, "build_derived_images", side_effect=derived), \
                mock.patch.object(provision_module, "run"):
            provision_module.provision(store, repo, lock, [], 30)
            current = verify_module.verify(store, repo, lock)
            self.assertEqual(current["images"], complete["images"])
            self.assertEqual(current["derived_images"], complete["derived_images"])
            for name in complete["artifacts"]:
                self.assertEqual((store / current["artifact_dir"] / name).read_bytes(), b"fixture")
            self.assertNotEqual(current["artifact_dir"], complete["artifact_dir"])
            original = (store / "bom.json").read_bytes()
            provision_module.provision(store, repo, lock, [], 30, consumer="object-store")
            provision_module.provision(store, repo, lock, [], 30, consumer="iceberg-rest")
            provision_module.provision(store, repo, lock, [], 30)
            self.assertEqual((store / "bom.json").read_bytes(), original)


class MinioSupplyTests(unittest.TestCase):
    def test_object_store_resolves_native_source_and_client_without_rest(self):
        for architecture in ("amd64", "arm64"):
            lock, identity = fixture_inputs.load_lock(ROOT / "lock.json", platform="linux/" + architecture,
                                                      consumer="object-store")
            self.assertEqual(set(lock["images"]), {"minio-runtime"})
            self.assertEqual(set(lock["derived_images"]), {"minio", "minio-mc"})
            self.assertIn("mc-source.tar.gz", lock["artifacts"])
            self.assertIn("go-linux-" + architecture + ".tar.gz", lock["artifacts"])
            other = "arm64" if architecture == "amd64" else "amd64"
            self.assertNotIn("go-linux-" + other + ".tar.gz", lock["artifacts"])
            self.assertEqual(identity, fixture_inputs.load_lock(ROOT / "lock.json", platform="linux/" + architecture,
                                                               consumer="iceberg-rest")[1])

    def test_failed_provision_reuses_only_revalidated_artifact_transfers(self):
        with tempfile.TemporaryDirectory() as temporary:
            store = Path(temporary)
            data = b"upstream content"
            item = {"url": "https://example.invalid/input", "bytes": len(data),
                    "sha1": hashlib.sha1(data).hexdigest(), "sha256": hashlib.sha256(data).hexdigest()}
            lock = {"artifacts": {"input": item}}
            first = store / ".staging-first"
            first.mkdir()
            with mock.patch.object(provision_module, "download", side_effect=lambda _, p: p.write_bytes(data)) as download:
                provision_module.prepare_artifacts(lock, first)
                self.assertEqual(download.call_count, 1)
            second = store / ".staging-second"
            second.mkdir()
            with mock.patch.object(provision_module, "download") as download:
                provision_module.prepare_artifacts(lock, second)
                download.assert_not_called()
            self.assertEqual((second / "artifacts/input").read_bytes(), data)
            for path in store.glob(".staging-*/artifacts/input"):
                path.write_bytes(b"invalid")
            for path in (store / "downloads").iterdir():
                path.write_bytes(b"invalid")
            third = store / ".staging-third"
            third.mkdir()
            with mock.patch.object(provision_module, "download", side_effect=lambda _, p: p.write_bytes(data)) as download:
                provision_module.prepare_artifacts(lock, third)
                self.assertEqual(download.call_count, 1)
            self.assertEqual((third / "artifacts/input").read_bytes(), data)
            self.assertFalse((store / "READY").exists())

    def test_declared_sha256_is_required_even_when_size_and_sha1_match(self):
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "binary"
            path.write_bytes(b"verified binary")
            item = {"bytes": path.stat().st_size, "sha1": hashlib.sha1(path.read_bytes()).hexdigest(),
                    "sha256": "0" * 64}
            with self.assertRaisesRegex(fixture_inputs.FixtureInputError, "integrity mismatch"):
                fixture_inputs.validate_artifact(path, item)
            item["sha256"] = hashlib.sha256(path.read_bytes()).hexdigest()
            self.assertEqual(fixture_inputs.validate_artifact(path, item)["sha256"], item["sha256"])


class NativePlatformTests(unittest.TestCase):
    def test_platform_normalization_and_native_daemon_agreement(self):
        self.assertEqual(fixture_inputs.normalize_platform("linux/aarch64"), "linux/arm64")
        self.assertEqual(fixture_inputs.normalize_platform("linux/x86_64"), "linux/amd64")
        with mock.patch.object(fixture_inputs, "native_platform", return_value="linux/amd64"), \
                mock.patch.object(fixture_inputs, "run", return_value="linux/x86_64\n"):
            fixture_inputs.require_native_daemon("linux/amd64")
        with mock.patch.object(fixture_inputs, "native_platform", return_value="linux/amd64"), \
                mock.patch.object(fixture_inputs, "run", return_value="linux/arm64\n"), \
                self.assertRaisesRegex(fixture_inputs.FixtureInputError, "native platform mismatch"):
            fixture_inputs.require_native_daemon("linux/amd64")

    def test_platform_identity_is_consumer_independent_and_all_never_shrinks(self):
        path = ROOT / "lock.json"
        _, all_sha = fixture_inputs.load_lock(path, platform="linux/amd64")
        _, consumer_sha = fixture_inputs.load_lock(path, platform="linux/amd64", consumer="iceberg-rest")
        arm, arm_sha = fixture_inputs.load_lock(path, platform="linux/arm64", consumer="iceberg-rest")
        self.assertEqual(all_sha, consumer_sha)
        self.assertNotEqual(all_sha, arm_sha)
        self.assertTrue(all(item["platform"] == "linux/arm64" for item in arm["images"].values()))
        with tempfile.TemporaryDirectory() as temporary, \
                mock.patch.object(provision_module, "require_native_daemon") as daemon, \
                mock.patch.object(provision_module, "prepare_images") as images:
            store = Path(temporary) / "uncreated"
            with self.assertRaisesRegex(fixture_inputs.FixtureInputError, "paimon-spark-base.*no linux/arm64"):
                provision_module.provision(store, ROOT.parents[1], path, [], 30, platform="linux/arm64")
            self.assertFalse(store.exists()); daemon.assert_not_called(); images.assert_not_called()

    def test_variant_recipe_dependencies_are_resolved_after_platform_selection(self):
        source = fixture_inputs.read_source_lock(ROOT / "lock.json")
        source["derived_images"]["iceberg-spark"]["variants"]["linux/amd64"]["bases"] = {
            "SPARK_BASE": "paimon-spark-base"
        }
        amd, _ = fixture_inputs.resolve_lock(source, "linux/amd64", consumer="derived:iceberg-spark")
        arm, _ = fixture_inputs.resolve_lock(source, "linux/arm64", consumer="derived:iceberg-spark")
        self.assertEqual(set(amd["images"]), {"paimon-spark-base"})
        self.assertEqual(set(arm["images"]), {"iceberg-spark-base"})

    def test_old_source_and_bom_are_explicit_reprovision_errors(self):
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "lock.json"
            path.write_text('{"schema":1}')
            with self.assertRaisesRegex(fixture_inputs.FixtureInputError, "reprovision"):
                fixture_inputs.load_lock(path)
            store = Path(temporary) / "store"
            store.mkdir(); (store / ".provision.lock").touch()
            (store / "READY").write_text("sha256:old-lock\n")
            (store / "bom.json").write_text('{"schema":1}')
            with self.assertRaisesRegex(fixture_inputs.FixtureInputError, "reprovision"):
                verify_module.verify(store, ROOT.parents[1], ROOT / "lock.json", consumer="iceberg-rest")
            source = {"schema": 2, "platform": "linux/arm64", "images": {}, "artifacts": {}, "derived_images": {}}
            with self.assertRaisesRegex(fixture_inputs.FixtureInputError, "reprovision"):
                verify_module.verify_contents(Path(temporary), ROOT.parents[1], source, "identity", {"schema":1})


class DerivedImageBuildTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.repo = self.root / "repo"
        self.recipe = self.repo / "recipe"
        self.recipe.mkdir(parents=True)
        (self.recipe / "Dockerfile").write_text("ARG BUILDER\nARG REST_BASE\nFROM ${REST_BASE}\n")
        (self.recipe / "patches").mkdir()
        (self.recipe / "patches/0001.patch").write_text("fixture patch\n")
        self.staging = self.root / "staging"
        (self.staging / "artifacts").mkdir(parents=True)
        (self.staging / "artifacts/source.tar.gz").write_bytes(b"source")
        self.lock = {
            "schema": 2, "platform": "linux/arm64",
            "images": {
                name: {"source": f"example.invalid/{name}", "platform": "linux/arm64",
                       "manifest_digest": f"sha256:{name}", "alias": f"fixture/{name}:locked"}
                for name in ("builder", "rest", "unrelated")
            },
            "artifacts": {
                "source.tar.gz": {"url": "https://example.invalid/source.tar.gz", "bytes": 6,
                                  "sha1": hashlib.sha1(b"source").hexdigest()},
                "unrelated.jar": {"url": "https://example.invalid/unrelated.jar", "bytes": 5,
                                  "sha1": hashlib.sha1(b"other").hexdigest()},
            },
            "derived_images": {
                "rest-mv": {"platform": "linux/arm64", "alias": "fixture/rest-mv:current",
                            "bases": {"BUILDER": "builder", "REST_BASE": "rest"},
                            "dockerfile": "recipe/Dockerfile", "context": "recipe",
                            "definition_files": ["recipe/Dockerfile", "recipe/patches/0001.patch"],
                            "artifacts": ["source.tar.gz"]},
            },
        }
        self.lock_path = self.root / "lock.json"
        self.write_lock()

    def write_lock(self):
        self.lock_path.write_text(json.dumps(source_lock(self.lock)))

    def image_info(self, identity):
        return {"Id": identity, "Os": "linux", "Architecture": "arm64"}

    def build_command(self, command):
        if command[:2] == ["docker", "build"]:
            iidfile = Path(command[command.index("--iidfile") + 1])
            iidfile.write_text(f"sha256:{iidfile.stem}\n")

    def test_lock_rejects_old_base_and_invalid_named_bases(self):
        item = self.lock["derived_images"]["rest-mv"]
        item["base"] = "rest"
        self.write_lock()
        with self.assertRaisesRegex(fixture_inputs.FixtureInputError, "unsupported base field"):
            fixture_inputs.load_lock(self.lock_path)
        del item["base"]
        for bases in ({}, [], {"BUILDER": "missing"}, {"BAD=ARG": "builder"}):
            with self.subTest(bases=bases):
                item["bases"] = bases
                self.write_lock()
                with self.assertRaises(fixture_inputs.FixtureInputError):
                    fixture_inputs.load_lock(self.lock_path)

    def test_recipe_transport_argument_requires_one_closed_unshared_name(self):
        item = self.lock["derived_images"]["rest-mv"]
        for argument in (None, "", "lowercase", "BAD=ARG", "NOVA-FIXTURE", "REST_BASE", "BUILDER"):
            with self.subTest(argument=argument):
                item["artifact_url_prefix_build_arg"] = argument
                self.write_lock()
                with self.assertRaisesRegex(fixture_inputs.FixtureInputError, "artifact transport build argument"):
                    fixture_inputs.load_lock(self.lock_path)
        item["build_args"] = {"NOVA_FIXTURE_URL_PREFIXES": "fixed"}
        item["artifact_url_prefix_build_arg"] = "NOVA_FIXTURE_URL_PREFIXES"
        self.write_lock()
        with self.assertRaisesRegex(fixture_inputs.FixtureInputError, "artifact transport build argument"):
            fixture_inputs.load_lock(self.lock_path)
        del item["build_args"]
        self.write_lock()
        fixture_inputs.load_lock(self.lock_path)

    def test_only_declared_recipe_receives_transport_without_source_identity_change(self):
        prefixes = {"https://upstream.invalid/maven2/": "https://mirror.invalid/public/"}
        item = self.lock["derived_images"]["rest-mv"]
        item["artifact_url_prefix_build_arg"] = "NOVA_FIXTURE_URL_PREFIXES"
        self.write_lock()
        original = self.lock_path.read_bytes()
        definition = fixture_inputs.definition_sha256(self.repo, item["definition_files"])
        with mock.patch.object(provision_module, "run", side_effect=self.build_command) as run, \
                mock.patch.object(provision_module, "inspect_image", side_effect=self.image_info):
            receipt = provision_module.build_derived_images(self.lock, self.repo, self.staging, "lock-sha", url_prefixes=prefixes)
        command = run.call_args.args[0]
        payload = next(value.split("=", 1)[1] for value in command if value.startswith("NOVA_FIXTURE_URL_PREFIXES="))
        self.assertEqual(prefixes, json.loads(payload))
        self.assertEqual(original, self.lock_path.read_bytes())
        self.assertEqual(definition, receipt["rest-mv"]["definition_sha256"])
        self.assertNotIn("url_prefixes", receipt["rest-mv"])
        self.assertEqual([], provision_module.artifact_url_prefix_build_args({}, prefixes))
        self.assertEqual(["--build-arg", "NOVA_FIXTURE_URL_PREFIXES={}"],
            provision_module.artifact_url_prefix_build_args(item, None))

    def test_provision_forwards_verified_transport_to_original_builder(self):
        self.lock["derived_images"]["rest-mv"]["artifact_url_prefix_build_arg"] = "NOVA_FIXTURE_URL_PREFIXES"
        self.write_lock()
        prefixes = {"https://upstream.invalid/maven2/": "https://mirror.invalid/public/"}
        with mock.patch.object(provision_module, "prepare_images", return_value={}), \
                mock.patch.object(provision_module, "download", side_effect=lambda _, path: path.write_bytes(
                    b"source" if path.name == "source.tar.gz" else b"other")), \
                mock.patch.object(provision_module, "run", side_effect=self.build_command), \
                mock.patch.object(provision_module, "inspect_image", side_effect=self.image_info), \
                mock.patch.object(provision_module, "build_derived_images", wraps=provision_module.build_derived_images) as build:
            provision_module.provision(self.root / "store", self.repo, self.lock_path, [], 30,
                artifact_url_prefix_specs=[f"{original}={transport}" for original, transport in prefixes.items()])
        self.assertEqual(prefixes, build.call_args.kwargs["url_prefixes"])

    def test_metadata_maintenance_uses_the_same_declared_transport_argument(self):
        self.lock["derived_images"]["rest-mv"]["artifact_url_prefix_build_arg"] = "NOVA_FIXTURE_URL_PREFIXES"
        script = (ROOT.parent / "iceberg-rest/rest-mv/update-verification-metadata.sh").read_text()
        source = script.split("<<'PYTHON'\n", 1)[1].rsplit("\nPYTHON", 1)[0]
        commands = []
        def run(command, **kwargs):
            commands.append(command)
            if command[0] == "docker":
                directory = Path(next(value.split("dest=", 1)[1] for value in command if value.startswith("type=local,dest=")))
                directory.mkdir()
                (directory / "verification-metadata.xml").write_text("contract-only metadata")
        prefix = "https://upstream.invalid/maven2/=https://mirror.invalid/public/"
        with mock.patch.object(sys, "argv", ["-", str(self.repo), "--artifact-url-prefix", prefix]), \
                mock.patch.object(fixture_inputs, "load_lock", return_value=(self.lock, "lock-sha")), \
                mock.patch("subprocess.run", side_effect=run):
            exec(compile(source, "metadata-maintenance-contract", "exec"), {})
        self.assertEqual(2, len(commands))
        self.assertIn("--artifact-url-prefix", commands[0])
        self.assertIn(prefix, commands[0])
        payload = next(value.split("=", 1)[1] for value in commands[1] if value.startswith("NOVA_FIXTURE_URL_PREFIXES="))
        self.assertEqual({"https://upstream.invalid/maven2/": "https://mirror.invalid/public/"}, json.loads(payload))
        self.assertIn("verification-metadata-export", commands[1])

    def test_lock_rejects_context_outside_repository(self):
        for context in ("../outside", "/outside", "", None):
            with self.subTest(context=context):
                self.lock["derived_images"]["rest-mv"]["context"] = context
                self.write_lock()
                with self.assertRaises(fixture_inputs.FixtureInputError):
                    fixture_inputs.load_lock(self.lock_path)

    def test_context_copies_declared_inputs_with_relative_layout(self):
        out = self.root / "out"
        provision_module.build_context(self.lock, "rest-mv", self.repo, self.staging / "artifacts", out)
        self.assertEqual((out / "Dockerfile").read_bytes(), (self.recipe / "Dockerfile").read_bytes())
        self.assertEqual((out / "patches/0001.patch").read_bytes(), b"fixture patch\n")
        self.assertEqual((out / "artifacts/source.tar.gz").read_bytes(), b"source")
        self.assertFalse((out / "artifacts/unrelated.jar").exists())

    def test_context_rejects_unlisted_files_and_missing_dockerfile_definition(self):
        (self.recipe / "extra.txt").write_text("unlocked input")
        with self.assertRaisesRegex(fixture_inputs.FixtureInputError, "not a definition input: recipe/extra.txt"):
            provision_module.build_context(self.lock, "rest-mv", self.repo, self.staging / "artifacts", self.root / "out")
        self.assertFalse((self.root / "out").exists())
        (self.recipe / "extra.txt").unlink()
        self.lock["derived_images"]["rest-mv"]["definition_files"].remove("recipe/Dockerfile")
        with self.assertRaisesRegex(fixture_inputs.FixtureInputError, "Dockerfile is not a definition input"):
            provision_module.build_context(self.lock, "rest-mv", self.repo, self.staging / "artifacts", self.root / "out")

    def test_context_rejects_symlinks_and_artifact_overlay(self):
        (self.recipe / "escape").symlink_to(self.root)
        with self.assertRaisesRegex(fixture_inputs.FixtureInputError, "must not contain symlinks"):
            provision_module.build_context(self.lock, "rest-mv", self.repo, self.staging / "artifacts", self.root / "out")
        (self.recipe / "escape").unlink()
        (self.recipe / "artifacts").mkdir()
        (self.recipe / "artifacts/source.tar.gz").write_bytes(b"overlay")
        self.lock["derived_images"]["rest-mv"]["definition_files"].append("recipe/artifacts/source.tar.gz")
        with self.assertRaisesRegex(fixture_inputs.FixtureInputError, "reserves artifacts"):
            provision_module.build_context(self.lock, "rest-mv", self.repo, self.staging / "artifacts", self.root / "out")

    def test_named_bases_build_without_alias_and_return_exact_image_id(self):
        with mock.patch.object(provision_module, "run", side_effect=self.build_command) as run, mock.patch.object(
            provision_module, "inspect_image", side_effect=self.image_info
        ) as inspect:
            receipts = provision_module.build_derived_images(self.lock, self.repo, self.staging, "lock-sha")
        command = run.call_args.args[0]
        arguments = [command[index + 1] for index, value in enumerate(command) if value == "--build-arg"]
        self.assertEqual(arguments, ["BUILDER=fixture/builder:locked", "REST_BASE=fixture/rest:locked"])
        self.assertNotIn("-t", command)
        self.assertNotIn("fixture/rest-mv:current", command)
        inspect.assert_called_once_with("sha256:rest-mv")
        self.assertEqual(receipts["rest-mv"]["image_id"], "sha256:rest-mv")
        self.assertEqual(receipts["rest-mv"]["definition_sha256"], fixture_inputs.definition_sha256(self.repo, self.lock["derived_images"]["rest-mv"]["definition_files"]))

    def test_derived_base_uses_verified_content_tag_before_child_build(self):
        import copy
        parent = "sha256:" + "a" * 64
        reference = "novarocks/fixture-build-base:" + "a" * 64
        self.lock["derived_images"]["wrapper"] = copy.deepcopy(self.lock["derived_images"]["rest-mv"])
        self.lock["derived_images"]["wrapper"].update(bases={"BUILDER": "builder"}, derived_bases={"REST_BASE": "rest-mv"})
        self.lock["_build_names"] = ["wrapper"]
        self.lock["_verified_derived_images"] = {"rest-mv": {"image_id": parent}}
        def inspect(identity):
            return self.image_info(parent if identity == reference else identity)
        with mock.patch.object(provision_module, "run", side_effect=self.build_command) as run, \
                mock.patch.object(provision_module, "inspect_image", side_effect=inspect):
            receipts = provision_module.build_derived_images(self.lock, self.repo, self.staging, "lock-sha")
        self.assertEqual(run.call_args_list[0].args[0], ["docker", "tag", parent, reference])
        build = run.call_args_list[1].args[0]
        self.assertIn("REST_BASE=" + reference, build)
        self.assertNotIn("REST_BASE=" + parent, build)
        self.assertEqual(set(receipts), {"wrapper"})

    def test_derived_base_tag_mismatch_cannot_start_child_build(self):
        import copy
        self.lock["derived_images"]["wrapper"] = copy.deepcopy(self.lock["derived_images"]["rest-mv"])
        self.lock["derived_images"]["wrapper"].update(bases={}, derived_bases={"REST_BASE": "rest-mv"})
        self.lock["_build_names"] = ["wrapper"]
        self.lock["_verified_derived_images"] = {"rest-mv": {"image_id": "sha256:" + "a" * 64}}
        with mock.patch.object(provision_module, "run") as run, \
                mock.patch.object(provision_module, "inspect_image", return_value=self.image_info("sha256:" + "b" * 64)), \
                self.assertRaisesRegex(fixture_inputs.FixtureInputError, "tag differs"):
            provision_module.build_derived_images(self.lock, self.repo, self.staging, "lock-sha")
        self.assertEqual(len(run.call_args_list), 1)

    def test_definition_digest_changes_when_a_context_input_changes(self):
        definitions = self.lock["derived_images"]["rest-mv"]["definition_files"]
        before = fixture_inputs.definition_sha256(self.repo, definitions)
        (self.recipe / "patches/0001.patch").write_text("changed patch\n")
        after = fixture_inputs.definition_sha256(self.repo, definitions)
        self.assertNotEqual(before, after)

    def test_failed_second_build_does_not_tag_or_publish(self):
        self.lock["derived_images"]["second"] = {
            **self.lock["derived_images"]["rest-mv"], "alias": "fixture/second:current"
        }
        self.write_lock()
        store = self.root / "store"
        store.mkdir()
        (store / "bom.json").write_bytes(b"old BOM\n")
        (store / "READY").write_bytes(b"old READY\n")
        def fake_artifacts(lock, staging):
            (staging / "artifacts").mkdir(exist_ok=True)
            (staging / "artifacts/source.tar.gz").write_bytes(b"source")
            return {}
        def fail_second(command):
            self.build_command(command)
            if command[:2] == ["docker", "build"] and Path(command[-1]).name == "second":
                raise fixture_inputs.FixtureInputError("build failed")
        with mock.patch.object(provision_module, "prepare_images", return_value={}), mock.patch.object(
            provision_module, "prepare_artifacts", side_effect=fake_artifacts
        ), mock.patch.object(provision_module, "run", side_effect=fail_second) as run, mock.patch.object(
            provision_module, "inspect_image", side_effect=self.image_info
        ), self.assertRaisesRegex(fixture_inputs.FixtureInputError, "build failed"):
            provision_module.provision(store, self.repo, self.lock_path, [], 30)
        self.assertEqual([call.args[0][:2] for call in run.call_args_list], [["docker", "build"], ["docker", "build"]])
        self.assertEqual((store / "bom.json").read_bytes(), b"old BOM\n")
        self.assertEqual((store / "READY").read_bytes(), b"old READY\n")
        self.assertFalse((store / "generations").exists())

    def test_successful_provision_tags_all_images_after_builds_then_publishes(self):
        self.lock["derived_images"]["second"] = {
            **self.lock["derived_images"]["rest-mv"], "alias": "fixture/second:current"
        }
        self.write_lock()
        store = self.root / "store"
        def fake_download(url, output):
            output.write_bytes(b"source" if output.name == "source.tar.gz" else b"other")
        def assert_publication_order(command):
            self.build_command(command)
            self.assertFalse((store / "READY").exists())
            self.assertFalse((store / "bom.json").exists())
        with mock.patch.object(provision_module, "prepare_images", return_value={}), mock.patch.object(
            provision_module, "download", side_effect=fake_download
        ), mock.patch.object(provision_module, "run", side_effect=assert_publication_order) as run, mock.patch.object(
            provision_module, "inspect_image", side_effect=self.image_info
        ):
            provision_module.provision(store, self.repo, self.lock_path, [], 30)
        self.assertEqual([call.args[0][:2] for call in run.call_args_list], [["docker", "build"], ["docker", "build"], ["docker", "tag"], ["docker", "tag"]])
        bom = fixture_inputs.read_json(store / "bom.json")
        self.assertEqual(bom["derived_images"]["rest-mv"]["image_id"], "sha256:rest-mv")
        self.assertEqual(bom["derived_images"]["second"]["image_id"], "sha256:second")
        self.assertEqual((store / "READY").read_text(), f"sha256:{bom['lock_sha256']}\n")
        self.assertTrue((store / bom["artifact_dir"] / "source.tar.gz").is_file())

    def test_context_only_selects_dependencies_without_accessing_store_or_building(self):
        out = self.root / "out"
        store = self.root / "store"
        store.mkdir()
        (store / "bom.json").write_bytes(b"old BOM\n")
        (store / "READY").write_bytes(b"old READY\n")
        def fake_download(url, output):
            output.write_bytes(b"source")
        with mock.patch.object(provision_module, "prepare_images", return_value={}) as images, mock.patch.object(
            provision_module, "download", side_effect=fake_download
        ) as download, mock.patch.object(provision_module, "run") as run, mock.patch.object(
            provision_module, "fixture_store_lock"
        ) as lock, mock.patch.object(provision_module, "fixture_store") as fixture_store, mock.patch.object(
            sys, "argv", ["provision.py", "--repo-root", str(self.repo), "--lock", str(self.lock_path),
                          "--store", str(store), "--context-only", "rest-mv", "--out", str(out)]
        ):
            self.assertEqual(provision_module.main(), 0)
        self.assertEqual(set(images.call_args.args[0]["images"]), {"builder", "rest"})
        self.assertEqual(download.call_count, 1)
        self.assertEqual(download.call_args.args[0], "https://example.invalid/source.tar.gz")
        run.assert_not_called()
        lock.assert_not_called()
        fixture_store.assert_not_called()
        self.assertEqual((store / "bom.json").read_bytes(), b"old BOM\n")
        self.assertEqual((store / "READY").read_bytes(), b"old READY\n")
        self.assertEqual(set(path.name for path in store.iterdir()), {"bom.json", "READY"})
        self.assertEqual((out / "patches/0001.patch").read_bytes(), b"fixture patch\n")


if __name__ == "__main__":
    unittest.main()
