from __future__ import annotations

from contextlib import contextmanager
import json
import os
import signal
import subprocess
import time
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

    @contextmanager
    def fake_docker(self, root, mode="ok", *, alias=False):
        """Exercise acquisition and custody with observable fake Docker resources."""
        command = root / 'docker'
        command.write_text(r'''#!/usr/bin/env python3
import json,os,sys,time
from pathlib import Path
root=Path(os.environ['FAKE_DOCKER_ROOT']); args=sys.argv[1:]
with (root/'commands').open('a') as f: f.write(json.dumps(args)+'\n')
mode=os.environ['FAKE_DOCKER_MODE']; cid='a'*64
if args[:2]==['image','inspect']:
    if (root/'alias').exists() or (args[2]=='source:42' and (root/'source').exists()):
        print(json.dumps({'Id':'sha256:actual'}))
    else:
        print('Error: No such image: '+args[2],file=sys.stderr);sys.exit(1)
elif args[0]=='pull':
    print('pull progress from Docker',flush=True);(root/'source').touch()
elif args[0]=='tag': (root/'alias').touch()
elif args[0]=='run':
    if mode=='no-cid':
        print('creation transport failed',file=sys.stderr);sys.exit(9)
    Path(args[args.index('--cidfile')+1]).write_text(cid);(root/'present').touch()
    if mode in ('timeout','cleanup-fail','signal','daemon-lost'): time.sleep(30)
    if mode=='command-fail':
        print('check failed',file=sys.stderr);sys.exit(7)
    if mode!='remains': (root/'present').unlink()
    print('wrong version' if mode=='mismatch' else 'Trino CLI 42')
elif args[:2]==['rm','-f']:
    if mode=='cleanup-fail':
        print('cannot remove owned container',file=sys.stderr);sys.exit(3)
    (root/'present').unlink(missing_ok=True)
elif args[:2]==['container','inspect']:
    if mode in ('cleanup-fail','daemon-lost'):
        print('Cannot connect to Docker daemon',file=sys.stderr);sys.exit(1)
    if (root/'present').exists(): print(json.dumps({'Id':cid}))
    else:
        print('Error: No such container: '+cid,file=sys.stderr);sys.exit(1)
else: sys.exit(13)
''')
        command.chmod(0o755)
        if alias: (root / 'alias').touch()
        with mock.patch.dict(os.environ, {'PATH':str(root)+os.pathsep+os.environ['PATH'],
                'FAKE_DOCKER_ROOT':str(root), 'FAKE_DOCKER_MODE':mode}):
            yield

    def trino_recipe(self):
        return {'schema':1, 'images':{'trino':{'source':'source', 'version':'42',
            'creation_check':'trino-cli-version', 'jdbc_artifact':'driver.jar',
            'variants':{'linux/arm64':{'platform':'linux/arm64','alias':'fixture:42'}}}},
            'artifacts':{'driver.jar':{'url':'https://example.invalid/42/driver.jar'}},
            'derived_images':{}, 'python_environments':{}}

    def commands(self, root):
        return [json.loads(line) for line in (root / 'commands').read_text().splitlines()]

    def test_missing_reference_alias_is_checked_before_tagging(self):
        with tempfile.TemporaryDirectory() as temporary:
            root=Path(temporary)
            recipe=inputs.resolve_inputs(self.trino_recipe(),'linux/arm64',consumer='image:trino')
            builder=self.builder(root,recipe)
            with self.fake_docker(root): result=builder.image('images','trino')
            self.assertEqual(result, {'alias':'fixture:42','image_id':'sha256:actual','platform':'linux/arm64','version':'42'})
            commands=self.commands(root)
            self.assertEqual([c[0] for c in commands],['image','image','pull','image','run','container','tag'])
            self.assertIn('--pull',commands[4]);self.assertIn('never',commands[4])
            self.assertFalse((root/'present').exists())
            self.assertFalse(list((root/'builds').glob('fixture-image-check-*')))

    def test_reference_creation_mismatch_does_not_tag(self):
        with tempfile.TemporaryDirectory() as temporary:
            root=Path(temporary)
            builder=self.builder(root,inputs.resolve_inputs(self.trino_recipe(),'linux/arm64',consumer='image:trino'))
            with self.fake_docker(root,'mismatch'):
                with self.assertRaisesRegex(inputs.FixtureInputError,'mismatch.*wrong version.*cleanup=confirmed absent'):
                    builder.image('images','trino')
            self.assertNotIn('tag',[c[0] for c in self.commands(root)])

    def test_existing_reference_alias_is_not_checked(self):
        with tempfile.TemporaryDirectory() as temporary:
            root=Path(temporary)
            builder=self.builder(root,inputs.resolve_inputs(self.trino_recipe(),'linux/arm64',consumer='image:trino'))
            with self.fake_docker(root,alias=True): builder.image('images','trino')
            self.assertEqual([c[0] for c in self.commands(root)],['image'])

    def test_unknown_creation_check_is_rejected(self):
        recipe=self.trino_recipe();recipe['images']['trino']['creation_check']='guess'
        with mock.patch.object(inputs,'read_json',return_value=recipe):
            with self.assertRaisesRegex(inputs.FixtureInputError,'unknown fixture creation_check'):
                inputs.read_recommendations(Path('unused'))

    def test_jdbc_artifact_must_exist_and_match_image_version(self):
        for artifacts in ({}, {'driver.jar':{'url':'https://example.invalid/41/driver.jar'}}):
            recipe=self.trino_recipe();recipe['artifacts']=artifacts
            with mock.patch.object(inputs,'read_json',return_value=recipe):
                with self.assertRaises(inputs.FixtureInputError): inputs.read_recommendations(Path('unused'))

    def test_owned_check_waits_for_explicit_absence(self):
        with tempfile.TemporaryDirectory() as temporary:
            root=Path(temporary)
            with self.fake_docker(root):
                result=inputs.run_owned_check('sha256:actual','linux/arm64',work_dir=root)
            self.assertEqual(result.stdout,'Trino CLI 42\n')
            self.assertEqual([c[0] for c in self.commands(root)],['run','container'])

    def test_owned_check_timeout_removes_exact_cid_and_keeps_primary(self):
        with tempfile.TemporaryDirectory() as temporary:
            root=Path(temporary)
            with self.fake_docker(root,'timeout'):
                with self.assertRaisesRegex(inputs.FixtureInputError,'timed out.*cleanup=confirmed absent: '+ 'a'*64):
                    inputs.run_owned_check('sha256:actual','linux/arm64',timeout_seconds=3,work_dir=root)
            self.assertEqual(self.commands(root)[1],['rm','-f','a'*64])
            self.assertFalse((root/'present').exists())

    def test_owned_check_cleanup_failure_keeps_original_and_identity(self):
        with tempfile.TemporaryDirectory() as temporary:
            root=Path(temporary)
            with self.fake_docker(root,'cleanup-fail'):
                with self.assertRaisesRegex(inputs.FixtureInputError,'timed out.*cleanup=failed:.*Cannot connect'):
                    inputs.run_owned_check('sha256:actual','linux/arm64',timeout_seconds=3,work_dir=root)
            self.assertEqual(next(root.glob('fixture-image-check-*/container-id')).read_text(),'a'*64)
            self.assertTrue((root/'present').exists())

    def test_owned_check_without_cid_does_not_guess_cleanup_target(self):
        with tempfile.TemporaryDirectory() as temporary:
            root=Path(temporary)
            with self.fake_docker(root,'no-cid'):
                with self.assertRaisesRegex(inputs.FixtureInputError,'command failed.*creation transport failed.*creation outcome unknown'):
                    inputs.run_owned_check('sha256:actual','linux/arm64',work_dir=root)
            self.assertEqual([c[0] for c in self.commands(root)],['run'])

    def test_owned_check_failed_command_is_removed(self):
        with tempfile.TemporaryDirectory() as temporary:
            root=Path(temporary)
            with self.fake_docker(root,'command-fail'):
                with self.assertRaisesRegex(inputs.FixtureInputError,'command failed.*check failed.*cleanup=confirmed absent'):
                    inputs.run_owned_check('sha256:actual','linux/arm64',work_dir=root)
            self.assertFalse((root/'present').exists())

    def test_owned_check_unconfirmed_normal_exit_is_reclaimed_and_rejected(self):
        with tempfile.TemporaryDirectory() as temporary:
            root=Path(temporary)
            with self.fake_docker(root,'remains'):
                with self.assertRaisesRegex(inputs.FixtureInputError,'initial removal confirmation failed.*cleanup=confirmed absent'):
                    inputs.run_owned_check('image','linux/arm64',timeout_seconds=3,work_dir=root)
            self.assertFalse((root/'present').exists())
            self.assertIn(['rm','-f','a'*64],self.commands(root))

    def test_owned_check_daemon_loss_never_claims_absence(self):
        with tempfile.TemporaryDirectory() as temporary:
            root=Path(temporary)
            with self.fake_docker(root,'daemon-lost'):
                with self.assertRaisesRegex(inputs.FixtureInputError,'timed out.*cleanup=failed:.*Cannot connect'):
                    inputs.run_owned_check('image','linux/arm64',timeout_seconds=3,work_dir=root)
            self.assertTrue(list(root.glob('fixture-image-check-*/container-id')))

    def check_signal_settles_custody(self, cancellation):
        with tempfile.TemporaryDirectory() as temporary:
            root=Path(temporary)
            with self.fake_docker(root,'signal'):
                env={**os.environ,'PYTHONPATH':str(ROOT)}
                code="from pathlib import Path; from fixture_inputs import run_owned_check; run_owned_check('image','linux/arm64',timeout_seconds=3,work_dir=Path("+repr(str(root))+"))"
                child=subprocess.Popen([sys.executable,'-c',code],env=env,stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True)
                try:
                    until=time.monotonic()+5
                    while not (root/'present').exists() and child.poll() is None and time.monotonic()<until: time.sleep(.01)
                    self.assertTrue((root/'present').exists())
                    child.send_signal(cancellation)
                    _,err=child.communicate(timeout=5)
                    self.assertNotEqual(child.returncode,0)
                    self.assertIn(signal.Signals(cancellation).name,err);self.assertIn('cleanup=confirmed absent',err)
                    self.assertFalse((root/'present').exists())
                finally:
                    if child.poll() is None: child.kill();child.communicate()

    def test_owned_check_sigterm_settles_custody(self):
        self.check_signal_settles_custody(signal.SIGTERM)

    def test_owned_check_sigint_settles_custody(self):
        self.check_signal_settles_custody(signal.SIGINT)

    def test_result_file_is_complete_despite_cold_pull_diagnostics(self):
        with tempfile.TemporaryDirectory() as temporary:
            root=Path(temporary);recipe=root/'versions.json';recipe.write_text(json.dumps(self.trino_recipe()))
            result=root/'result.json'
            command=[sys.executable,str(ROOT/'provision.py'),'--store',str(root/'store'),'--versions',str(recipe),
                     '--consumer','image:trino','--platform','linux/arm64']
            with self.fake_docker(root):
                completed=subprocess.run([*command,'--result',str(result)],text=True,capture_output=True)
                self.assertEqual(completed.returncode,0,completed.stderr)
                value=json.loads(result.read_text())
                self.assertEqual(value['images']['trino']['version'],'42')
                self.assertEqual(completed.stdout,'pull progress from Docker\n')
                reused=subprocess.run(command,text=True,capture_output=True)
                self.assertEqual(json.loads(reused.stdout),value)
                (root/'alias').unlink()
                failed=subprocess.run([*command,'--result',str(root/'failed.json')],env={**os.environ,'FAKE_DOCKER_MODE':'mismatch'},text=True,capture_output=True)
                self.assertNotEqual(failed.returncode,0)
                self.assertFalse((root/'failed.json').exists())
            self.assertFalse(list(root.glob('.result.json.*.tmp')))

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
