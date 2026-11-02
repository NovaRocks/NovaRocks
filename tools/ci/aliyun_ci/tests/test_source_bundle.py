# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements. See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership. The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License. You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied. See the License for the
# specific language governing permissions and limitations
# under the License.

"""Source transport correctness: real Git repositories, independent identities.

CI maintainers run these low-cost tests on source/control changes. Temporary
repositories and files are owned and removed by unittest's attempt lifetime.
"""
import hashlib
import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest

from aliyun_ci import source_bundle


class SourceBundleTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.repo = self.root / 'repo'
        self.repo.mkdir()
        self.git('init', '-q')
        self.git('config', 'user.name', 'Source Test')
        self.git('config', 'user.email', 'source@example.invalid')
        self.git('config', 'remote.origin.url', 'https://private-token@example.invalid/private')
        (self.repo / '.gitignore').write_text('ignored\n')
        (self.repo / 'one.txt').write_text('committed contents\n')
        (self.repo / 'run.sh').write_text('#!/bin/sh\nexit 0\n')
        (self.repo / 'run.sh').chmod(0o755)
        (self.repo / 'linked').symlink_to('one.txt')
        self.git('add', '.')
        self.git('commit', '-qm', 'Initial source')
        (self.repo / 'ignored').write_text('ignored secret')

    def git(self, *args):
        return subprocess.check_output(['git', '-C', str(self.repo), *args], stderr=subprocess.DEVNULL).decode().strip()

    def test_unpublished_commit_imports_without_config_credentials_or_ignored_files(self):
        (self.repo / 'one.txt').write_text('unpublished contents\n')
        self.git('add', 'one.txt')
        self.git('commit', '-qm', 'Unpublished source')
        sha, tree = self.git('rev-parse', 'HEAD'), self.git('rev-parse', 'HEAD^{tree}')
        manifest = source_bundle.create(self.repo, self.root / 'carrier', sha)
        # No original repository is needed after the self-contained carrier exists.
        self.repo.rename(self.root / 'unavailable-original')
        imported = self.root / 'imported'
        fact = source_bundle.import_bundle(self.root / 'carrier/source.bundle', manifest, imported)
        self.assertEqual((sha, tree), (fact['sha'], fact['tree']))
        self.assertEqual('unpublished contents\n', (imported / 'one.txt').read_text())
        self.assertTrue((imported / 'linked').is_symlink())
        self.assertTrue((imported / 'run.sh').stat().st_mode & 0o111)
        self.assertFalse((imported / 'ignored').exists())
        self.assertNotIn('private-token', (imported / '.git/config').read_text())
        self.assertNotIn('remote', (imported / '.git/config').read_text())
        row = next(row for row in manifest['members'] if row['path'] == 'one.txt')
        self.assertEqual(hashlib.sha256(b'unpublished contents\n').hexdigest(), row['sha256'])
        self.assertEqual(4, fact['members_verified'])
        self.assertEqual(manifest, json.loads((self.root / 'carrier/source.json').read_text()))

    def test_dirty_or_wrong_sha_is_rejected_before_creating_carrier(self):
        for dirty in ('tracked', 'untracked'):
            with self.subTest(dirty=dirty):
                path = self.repo / ('one.txt' if dirty == 'tracked' else 'extra')
                original = path.read_bytes() if path.exists() else None
                path.write_text('not committed')
                with self.assertRaisesRegex(ValueError, 'clean'):
                    source_bundle.create(self.repo, self.root / 'carrier')
                self.assertFalse((self.root / 'carrier').exists())
                if original is None:
                    path.unlink()
                else:
                    path.write_bytes(original)
        with self.assertRaisesRegex(ValueError, 'clean HEAD'):
            source_bundle.create(self.repo, self.root / 'carrier', '0' * 40)
        self.assertFalse((self.root / 'carrier').exists())

    def test_explicit_blob_hash_mismatch_is_rejected_after_git_import(self):
        manifest = source_bundle.create(self.repo, self.root / 'carrier')
        manifest['members'][0]['sha256'] = '0' * 64
        with self.assertRaisesRegex(ValueError, 'member manifest'):
            source_bundle.import_bundle(self.root / 'carrier/source.bundle', manifest, self.root / 'imported')

    def test_wrong_bundle_digest_is_rejected_before_import(self):
        manifest = source_bundle.create(self.repo, self.root / 'carrier')
        manifest['bundle_sha256'] = '0' * 64
        with self.assertRaisesRegex(ValueError, 'SHA-256'):
            source_bundle.import_bundle(self.root / 'carrier/source.bundle', manifest, self.root / 'imported')
        self.assertFalse((self.root / 'imported').exists())
