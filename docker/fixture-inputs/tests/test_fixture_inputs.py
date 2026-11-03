from __future__ import annotations

import json
import hashlib
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
import fixture_inputs as inputs
import provision


class FixtureInputsTest(unittest.TestCase):
    """Build only absent targets; environment differences are not admission facts."""

    def test_platform_selection_keeps_recipe_dependencies(self):
        arm = inputs.load_inputs(ROOT / 'versions.json', platform='linux/arm64', consumer='iceberg-rest')
        self.assertEqual(arm['images']['iceberg-rest']['version'], '1.10.1')
        self.assertFalse(arm['python_environments'])
        oss = inputs.load_inputs(ROOT / 'versions.json', platform='linux/amd64', consumer='iceberg-rest-oss')
        self.assertNotIn('minio', oss['derived_images'])
        self.assertEqual(set(oss['python_environments']), {'oss-sdk'})
        order = inputs.derived_build_order(oss)
        self.assertLess(order.index('iceberg-spark'), order.index('iceberg-spark-oss'))
        with self.assertRaisesRegex(inputs.FixtureInputError, 'unsupported fixture input platform'):
            inputs.load_inputs(ROOT / 'versions.json', platform='linux/arm64', consumer='iceberg-rest-oss')

    def test_recipe_cycle_is_an_input_error(self):
        recipe = {'images': {}, 'artifacts': {}, 'derived_images': {
            'one': {'bases': {}, 'derived_bases': {'BASE': 'two'}, 'artifacts': []},
            'two': {'bases': {}, 'derived_bases': {'BASE': 'one'}, 'artifacts': []}}}
        with self.assertRaisesRegex(inputs.FixtureInputError, 'cycle'):
            inputs.selected_input_names(recipe, 'derived:one')

    def builder(self, root, recipe=None):
        if recipe is None:
            recipe = inputs.load_inputs(ROOT / 'versions.json', platform='linux/arm64', consumer='iceberg-rest')
        (root / 'builds').mkdir()
        return provision.InputBuilder(root, ROOT.parents[1], recipe)

    def test_existing_derived_target_does_not_resolve_or_prepare_its_dependencies(self):
        with tempfile.TemporaryDirectory() as temporary:
            builder = self.builder(Path(temporary))
            with mock.patch.object(provision, 'inspect_image', return_value={'Id': 'old-actual-image'}) as inspect, \
                 mock.patch.object(provision, 'run') as run, mock.patch.object(builder, 'artifact') as artifact:
                result = builder.image('derived_images', 'iceberg-spark')
            self.assertEqual('old-actual-image', result['image_id'])
            inspect.assert_called_once_with(builder.inputs['derived_images']['iceberg-spark']['alias'])
            run.assert_not_called()
            artifact.assert_not_called()

    def test_only_explicit_absence_fetches_recommended_version(self):
        with tempfile.TemporaryDirectory() as temporary:
            builder = self.builder(Path(temporary))
            item = builder.inputs['images']['iceberg-rest']
            with mock.patch.object(provision, 'inspect_image', side_effect=[None, None, {'Id': 'acquired'}]), \
                 mock.patch.object(provision, 'run') as run:
                result = builder.image('images', 'iceberg-rest')
            self.assertEqual('acquired', result['image_id'])
            self.assertEqual(run.call_args_list, [
                mock.call(['docker', 'pull', '--platform', 'linux/arm64', 'apache/iceberg-rest-fixture:1.10.1'], timeout_seconds=180),
                mock.call(['docker', 'tag', 'acquired', item['alias']])])

    def test_unavailable_docker_does_not_trigger_pull_or_build(self):
        with tempfile.TemporaryDirectory() as temporary:
            builder = self.builder(Path(temporary))
            with mock.patch.object(provision, 'inspect_image', side_effect=inputs.FixtureInputError('permission denied')), \
                 mock.patch.object(provision, 'run') as run:
                with self.assertRaisesRegex(inputs.FixtureInputError, 'permission denied'):
                    builder.image('images', 'iceberg-rest')
            run.assert_not_called()

    def test_docker_absence_is_distinct_from_failed_observation(self):
        with mock.patch.object(inputs.subprocess, 'run', return_value=mock.Mock(returncode=1, stderr='Error response from daemon: No such image: fixture:v1')):
            self.assertIsNone(inputs.inspect_image('fixture:v1'))
        with mock.patch.object(inputs.subprocess, 'run', return_value=mock.Mock(returncode=1, stderr='Cannot connect to the Docker daemon')):
            with self.assertRaisesRegex(inputs.FixtureInputError, 'Cannot connect'):
                inputs.inspect_image('fixture:v1')

    def test_corrupt_build_input_is_not_overwritten(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            builder = self.builder(root)
            data = b'correct'
            builder.inputs['artifacts']['input.jar'] = {'url': 'https://example.invalid/input.jar', 'bytes': len(data), 'sha1': hashlib.sha1(data).hexdigest()}
            (root / 'artifacts').mkdir()
            (root / 'artifacts/input.jar').write_bytes(b'corrupt')
            with mock.patch.object(provision, 'download') as download:
                with self.assertRaisesRegex(inputs.FixtureInputError, 'integrity mismatch'):
                    builder.artifact('input.jar')
            download.assert_not_called()
            self.assertEqual(b'corrupt', (root / 'artifacts/input.jar').read_bytes())

    def test_failed_download_keeps_its_partial_and_does_not_publish_target(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            builder = self.builder(root)
            builder.inputs['artifacts']['input.jar'] = {'url': 'https://example.invalid/input.jar', 'bytes': 4, 'sha1': 'unused'}
            def failed(url, path):
                path.write_bytes(b'partial')
                raise inputs.FixtureInputError('transfer failed')
            with mock.patch.object(provision, 'download', side_effect=failed):
                with self.assertRaisesRegex(inputs.FixtureInputError, 'transfer failed'):
                    builder.artifact('input.jar')
            self.assertFalse((root / 'artifacts/input.jar').exists())
            self.assertEqual(1, len(list((root / 'builds').glob('download-*/payload'))))

    def test_existing_sdk_is_used_without_package_or_directory_verification(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            builder = self.builder(root)
            builder.inputs['python_environments']['sdk'] = {'python_version': '3.14', 'artifacts': []}
            (root / 'python-environments/sdk/bin').mkdir(parents=True)
            (root / 'python-environments/sdk/bin/python').write_bytes(b'existing interpreter')
            with mock.patch('python_environment.prepare_environment') as prepare:
                value = builder.python('sdk')
            prepare.assert_not_called()
            self.assertEqual(str(root / 'python-environments/sdk/bin/python'), value['path'])

    def test_partial_existing_sdk_is_not_rebuilt(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            builder = self.builder(root)
            builder.inputs['python_environments']['sdk'] = {'python_version': '3.14', 'artifacts': []}
            (root / 'python-environments/sdk').mkdir(parents=True)
            with mock.patch('python_environment.prepare_environment') as prepare:
                with self.assertRaisesRegex(inputs.FixtureInputError, 'interpreter is unavailable'):
                    builder.python('sdk')
            prepare.assert_not_called()

    def test_build_context_keeps_declared_source_and_artifact_layout(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            (root / 'repo').mkdir()
            (root / 'repo/Dockerfile').write_text('FROM scratch\nCOPY artifacts/input.jar /input.jar\n')
            (root / 'artifacts').mkdir()
            (root / 'artifacts/input.jar').write_bytes(b'input')
            recipe = {'derived_images': {'image': {'definition_files': ['Dockerfile'], 'dockerfile': 'Dockerfile', 'artifacts': ['input.jar']}}}
            out = provision.build_context(recipe, 'image', root / 'repo', root / 'artifacts', root / 'context')
            self.assertEqual(b'input', (out / 'artifacts/input.jar').read_bytes())
            self.assertIn('FROM scratch', (out / 'Dockerfile').read_text())

    def test_transport_changes_repository_not_release(self):
        sources = provision.parse_image_sources(['iceberg-rest=mirror.example/iceberg'], {'iceberg-rest'})
        self.assertEqual({'iceberg-rest': 'mirror.example/iceberg'}, sources)
        prefixes = provision.parse_artifact_url_prefixes(['https://repo.maven.apache.org/maven2/=https://mirror.example/maven/'])
        self.assertEqual('https://mirror.example/maven/org/a.jar', provision.artifact_transport_url('https://repo.maven.apache.org/maven2/org/a.jar', prefixes))


if __name__ == '__main__':
    unittest.main()
