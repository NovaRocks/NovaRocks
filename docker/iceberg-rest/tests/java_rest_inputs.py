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

"""The Java REST fourteen-test input owner shared by the two original drivers.

Only fixture_run admits budgets, owns storage, waits for commands and cleans a
run. Importing this module never admits a run or creates an artifact directory.
"""
import argparse
from contextlib import contextmanager
import hashlib
import json
import os
from pathlib import Path
import shlex
import signal
import subprocess
import sys
import uuid

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import fixture_run

OWNER = 'iceberg-java-rest-interop'
SCOPE = 'rust-owner:' + OWNER
EXPECTED = frozenset({
    'commit::staging::interop_tests::iru13a_java_rest_namespace_table_lifecycle_and_interleaved_reads',
    'commit::staging::interop_tests::iru13a_java_rest_recreated_table_rejects_stale_partition_evolution',
    'commit::staging::interop_tests::iru13a_java_rest_stage_create_and_ctas_final_commit',
    'commit::staging::interop_tests::iru13a_java_rest_table_commit_shapes',
    'commit::staging::interop_tests::iru13a_java_rest_view_create_replace_drop',
    'commit::staging::interop_tests::iru13b_java_rest_ctas_publishes_staged_metadata_for_every_format_version',
    'commit::staging::interop_tests::iru13b_java_rest_schema_changes_keep_the_column_high_water_mark',
    'commit::staging::interop_tests::iru13b_java_rest_view_replacement_keeps_dialects_unless_allowed',
    'commit::staging::interop_tests::iru13b_java_rest_view_replacement_survives_version_id_reuse',
    'commit::staging::interop_tests::iru13b_java_rest_view_replacements_always_add_the_selected_version',
    'commit::staging::interop_tests::iru5_java_rest_attempt_runner_reloads_and_reprepares_after_real_cas',
    'commit::staging::interop_tests::iru5_java_rest_d12_inheritance_and_physical_rewrite',
    'commit::staging::interop_tests::iru5_java_rest_three_shapes_historic_spec_and_composition',
    'commit::staging::interop_tests::iru5_java_rest_two_canonical_clients_real_cas',
})
# These are replaced by the actual publication/run at group execution. Other
# environment inputs (including Java's PATH) remain in the static target binding.
OWNED_ENV = frozenset({
    'NOVA_FIXTURE_RUN_CONTEXT', 'NOVA_FIXTURE_RUN_STORAGE', 'NOVA_ENV_MANIFEST',
    'NOVA_ENV_REST_ENV_FILE', 'NOVA_FIXTURE_INPUT_BOM', 'AWS_S3_ENDPOINT',
    'AWS_S3_ACCESS_KEY_ID', 'AWS_S3_SECRET_ACCESS_KEY',
    'NOVAROCKS_ICEBERG_REST_URI', 'NOVAROCKS_ICEBERG_REST_WAREHOUSE',
    'NR_JAVA_REST_INTEROP_RUN_ROOT',
    *('NR_' + family + '_INTEROP_' + suffix for family in ('IRU5', 'IRU13A', 'IRU13B')
      for suffix in ('NAMESPACE', 'ARTIFACT_DIR')),
})


def write(path, value):
    Path(path).write_text(json.dumps(value, indent=2, sort_keys=True) + '\n')


def static_identity(value):
    return dict(value, env={key: item for key, item in value.get('env', {}).items()
                            if key not in OWNED_ENV})


def validate_group(names):
    if len(names) != len(EXPECTED) or set(names) != EXPECTED:
        raise ValueError('Java REST input owner requires the complete original fourteen tests')


def publication_environment(env_file, inherited):
    # The generated immutable file contains shell-quoted literals and explicit
    # unset lines. Reuse the original parser, honoring those unsets in the parent.
    result = dict(inherited)
    for line in Path(env_file).read_text().splitlines():
        words = shlex.split(line, comments=True)
        if words[:1] == ['unset']:
            for key in words[1:]:
                result.pop(key, None)
    result.update(fixture_run.env_values(env_file))
    if result.get('NOVA_ENV_REST_ENV_FILE') != str(env_file):
        raise ValueError('sourced environment differs from pinned publication')
    return result


class JavaRestInputs:
    def __init__(self, repo, publication, root, budget, owns_run):
        self.repo, self.publication, self.root = repo, publication, root
        self.budget, self.owns_run = budget, owns_run
        self.environment = dict(os.environ)
        self.input_files = {str(path): hashlib.sha256(path.read_bytes()).hexdigest()
                            for path in (publication / 'env.sh', publication / 'manifest.json',
                                         Path(self.environment['NOVA_FIXTURE_INPUT_BOM']))}
        self.status = dict(schema=1, owner=OWNER, owns_fixture_run=owns_run, command_attempted=False,
                           command_returned=False, exit_code=None,
                           cleanup=dict(summary='unknown', disposition='not_started'))
        self.save()

    def save(self):
        write(self.root / 'owner-result.json', self.status)

    def run(self, command, *, stdout=None, stderr=None):
        # communicate retains the original cumulative work deadline and waits
        # for the original process group after interruption/timeout.
        if self.status['command_attempted']:
            raise ValueError('Java REST input owner already attempted its complete command')
        self.status['command_attempted'] = True
        self.save()
        handler = signal.getsignal(signal.SIGTERM)
        fixture_run.runtime.install_signal_handlers()
        try:
            result = fixture_run.communicate(list(map(str, command)),
                timeout=self.budget.remaining_seconds(work=True) if self.budget else 3600,
                environment=self.environment, stdout=stdout, stderr=stderr, cwd=self.repo)
            self.status.update(command_returned=True, exit_code=result.returncode)
            self.save()
            return result.returncode
        except BaseException as error:
            self.status['interrupted'] = type(error).__name__
            self.save()
            raise
        finally:
            signal.signal(signal.SIGTERM, handler)

    def finish(self):
        if (not self.status['command_returned'] or self.status['exit_code'] != 0
                or 'owner_scope_error' in self.status):
            self.status['cleanup'] = dict(summary='unknown', disposition='retained_after_failed_or_cancelled_command')
            self.save()
            return
        if not self.owns_run or self.budget is None:
            self.status['cleanup'] = dict(summary='ok', disposition='inherited_owner' if not self.owns_run else 'private_publication', cleanup_rights=False)
            self.save()
            return
        previous = os.environ.get('NOVA_FIXTURE_CLEANUP_MODE')
        os.environ['NOVA_FIXTURE_CLEANUP_MODE'] = 'true'
        try:
            report = fixture_run.cleanup_run(self.publication / 'env.sh', budget=self.budget)
            self.status['cleanup'] = dict(summary='ok' if report.get('complete') is True else 'violation',
                                         disposition='original_run_owner', report=report)
            self.save()
            if report.get('complete') is not True:
                raise ValueError('original Java fixture run cleanup is incomplete')
        except BaseException:
            if self.status['cleanup'].get('disposition') != 'original_run_owner':
                self.status['cleanup'] = dict(summary='violation', disposition='original_cleanup_failed')
            self.save()
            raise
        finally:
            if previous is None:
                os.environ.pop('NOVA_FIXTURE_CLEANUP_MODE', None)
            else:
                os.environ['NOVA_FIXTURE_CLEANUP_MODE'] = previous


@contextmanager
def own_inputs(repo, requested_artifact_root=None, environment=None):
    repo = Path(repo).resolve()
    publication = (repo / 'docker/iceberg-rest/runtime/current/published').resolve(strict=True)
    manifest = json.loads((publication / 'manifest.json').read_text())
    if not manifest.get('ready') or not manifest.get('fixture_inputs', {}).get('verified'):
        raise ValueError('pinned fixture publication is not ready with verified inputs')
    env_file = publication / 'env.sh'
    supplied = dict(os.environ if environment is None else environment)
    environment = publication_environment(env_file, supplied)
    store = Path(manifest['fixture_inputs']['bom']).parent
    subprocess.run([str(repo / 'docker/fixture-inputs/verify.sh'), '--store', str(store),
                    '--repo-root', str(repo), '--consumer', 'iceberg-rest'], env=environment, check=True)
    bom = store / 'bom.json'
    if manifest['fixture_inputs']['lock_sha256'] != json.loads(bom.read_text())['lock_sha256']:
        raise ValueError('pinned publication differs from verified fixture input lock')
    for key in ('NOVAROCKS_ICEBERG_REST_URI', 'NOVAROCKS_ICEBERG_REST_WAREHOUSE',
                'AWS_S3_ENDPOINT', 'AWS_S3_ACCESS_KEY_ID', 'AWS_S3_SECRET_ACCESS_KEY'):
        if not environment.get(key):
            raise ValueError('pinned Java fixture input is missing: ' + key)
    previous = dict(os.environ)
    os.environ.clear()
    os.environ.update(environment)
    owner = None
    try:
        owns_run = environment.get('NOVA_FIXTURE_RUN_CONTEXT', 'null') == 'null'
        _, _, budget = fixture_run.admit(env_file)
        # Identical admission/storage author for the shell and formal owner.
        if budget is None:
            os.environ['NOVA_FIXTURE_RUN_CONTEXT'] = 'null'
            os.environ['NOVA_FIXTURE_RUN_STORAGE'] = 'null'
        fixture_run.startup_sweep(env_file, budget=budget)
        run_id = budget.identity.run_id if budget else uuid.uuid4().hex
        root = Path(requested_artifact_root or environment.get('NR_IRU13A_INTEROP_ARTIFACT_DIR')
                    or Path(environment['NOVA_ENV_RUNTIME_DIR']) / ('iru13a-java-rest-' + run_id)).resolve()
        root.mkdir()  # Exclusive, including on resume; never reuse case artifacts.
        os.environ['NR_JAVA_REST_INTEROP_RUN_ROOT'] = str(root)
        os.environ['NOVA_FIXTURE_INPUT_BOM'] = str(bom.resolve())
        os.environ['NOVA_FIXTURE_JAVA_DRIVER_OWNER'] = 'true'
        os.environ['NR_JAVA_REST_INTEROP_EXPECTED_TESTS'] = json.dumps(sorted(EXPECTED))
        for family in ('IRU5', 'IRU13A', 'IRU13B'):
            folder = root / family
            folder.mkdir()
            os.environ['NR_' + family + '_INTEROP_ARTIFACT_DIR'] = str(folder)
            os.environ['NR_' + family + '_INTEROP_NAMESPACE'] = run_id + '_' + family.lower()[-4:]
        os.environ.setdefault('CARGO_TARGET_DIR', str(repo / 'target'))
        write(root / 'fixture.json', dict(publication=str(publication),
            run_context=json.loads(os.environ['NOVA_FIXTURE_RUN_CONTEXT']),
            run_storage=json.loads(os.environ['NOVA_FIXTURE_RUN_STORAGE']),
            fixture_input_lock_sha256=manifest['fixture_inputs']['lock_sha256'],
            publication_sha256=hashlib.sha256((publication / 'manifest.json').read_bytes()).hexdigest(),
            bom_sha256=hashlib.sha256(bom.read_bytes()).hexdigest(),
            boundary='Java REST acceptance of NovaRocks-issued REST requests; native 1FE+3BE SQL acceptance is separate'))
        owner = JavaRestInputs(repo, publication, root, budget, owns_run)
        yield owner
    except BaseException as error:
        if owner is not None:
            owner.status['owner_scope_error'] = type(error).__name__
            owner.save()
        raise
    finally:
        try:
            if owner is not None:
                owner.finish()
        finally:
            os.environ.clear()
            os.environ.update(previous)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('operation', choices=['legacy'])
    args, rest = parser.parse_known_args()
    repo = Path(__file__).resolve().parents[3]
    with own_inputs(repo) as owner:
        return owner.run(['bash', str(repo / 'docker/iceberg-rest/tests/iru13a-rest-interop-test.sh'), *rest])


if __name__ == '__main__':
    raise SystemExit(main())
