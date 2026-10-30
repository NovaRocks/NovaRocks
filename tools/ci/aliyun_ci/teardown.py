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

"""Exact resource release and independent archive verification, even after test failure."""
from datetime import datetime, timedelta, timezone
import json
import io
import tarfile
import re
import time
from .ports import APIError
from .plan import parse_time
from .start import NOT_FOUND, describe_instances, find_run, owned
from .github_api import delete_runner, runners, runner_tags
from .summary import run_summary


from .lifecycle import delete_exact


def secret_parameters(config, oos):
    token, result = None, []
    while True:
        params = {'RegionId': config['region'], 'MaxResults': 100}
        if token:
            params['NextToken'] = token
        response = oos.call('ListSecretParameters', **params)
        result.extend(response.get('Parameters', []))
        token = response.get('NextToken')
        if not token:
            return result


def delete_bootstrap(config, plan, oos):
    # List exact names first, including partially published chunks with no root.
    exact_prefix = plan.secret_name + '-part-'
    removed = []
    for row in secret_parameters(config, oos):
        name = row.get('Name', '')
        if name != plan.secret_name and not re.fullmatch(re.escape(exact_prefix) + r'[0-9]{6}', name):
            continue
        tags = row.get('Tags', {})
        if isinstance(tags, str):
            tags = json.loads(tags)
        expected = {k: v for k, v in plan.tags.items() if k != 'deadline'}
        if any(tags.get(k) != v for k, v in expected.items()):
            raise ValueError(f'bootstrap parameter ownership mismatch: {name}')
        try:
            oos.call('DeleteSecretParameter', RegionId=config['region'], Name=name)
        except APIError as error:
            if error.code not in NOT_FOUND:
                raise
        removed.append(name)
    return removed


def delete_run_runners(config, plan, github):
    pattern = re.compile(rf'g16-{re.escape(plan.run_id)}-{plan.run_attempt}-[0-9a-f]{{12}}')
    removed = []
    for row in runners(config, github):
        if row['name'] == plan.label or pattern.fullmatch(row['name']):
            actual = runner_tags(row)
            expected = {k: v for k, v in plan.tags.items() if k != 'deadline'}
            expected.update(account_id=str(config['account_id']), region=config['region'])
            if any(actual.get(key) != value for key, value in expected.items()):
                raise ValueError('runner ownership labels do not match this run')
            removed.extend(delete_runner(config, github, row['name']))
    if any(row['name'] == plan.label or pattern.fullmatch(row['name']) for row in runners(config, github)):
        raise APIError('RunnerReleaseUnconfirmed', f'runner registration remains for {plan.run_id}/{plan.run_attempt}')
    return removed


def native_archive_facts(raw, *, kind, key, digest):
    """Read exact native JSON members only after the caller verified all bytes."""
    facts = {}
    members = {'execution_index': 'attempt/rust/execution-index.json',
               'tools_check_result': 'attempt/tools-check-result.json'}
    with tarfile.open(fileobj=io.BytesIO(raw), mode='r:gz') as archive:
        for field, name in members.items():
            try:
                member = archive.getmember(name)
            except KeyError:
                continue
            with archive.extractfile(member) as source:
                value = json.load(source)
            if not isinstance(value, dict):
                raise ValueError(f'native JSON member is not an object: {name}')
            facts[field] = {'archive_kind': kind, 'archive_key': key,
                            'archive_sha256': digest, 'member': name, 'value': value}
    return facts


# Design: ADR-0204 (docs/adr/ADR-0204-one-shot-spot-ci-and-independent-evidence.md)
def teardown_run(config, plan, ecs, oos, oss, github, *, instance_id=None,
                 image_id=None, rust_result=None, tools_check=None, verify=None, now=None, sleep=time.sleep):
    leftovers, terminal = [], 'unknown'
    try:
        instance = None
        if instance_id:
            values = describe_instances(config, ecs, instance_id=instance_id)
            if values:
                instance = values[0]
        else:
            instance = find_run(config, plan, ecs)
            instance_id = instance['InstanceId'] if instance else None
        if instance is not None:
            image_id = image_id or instance.get('ImageId')
        if instance_id:
            expected = {key: value for key, value in plan.tags.items() if key != 'deadline'}
            delete_exact(config, ecs, instance_id, expected, deadline=plan.deadline, now=now, sleep=sleep)
        terminal = 'released'
    except Exception as error:
        leftovers.append({'kind': 'compute', 'id': instance_id, 'error': str(error)})
    operations = [('bootstrap', lambda: delete_bootstrap(config, plan, oos))]
    if plan.execution_kind == 'github-runner':
        operations.insert(0, ('runner', lambda: delete_run_runners(config, plan, github)))
    for kind, operation in operations:
        try:
            operation()
        except Exception as error:
            leftovers.append({'kind': kind, 'error': str(error)})
    if verify is None:
        from .handoff import verify
    archives, native = {}, {}
    for kind in ('final', 'interrupted'):
        try:
            raw = oss.get(plan.oss_keys[kind])
            archives[kind] = verify(raw)
            fact = archives[kind]
            if fact.get('manifest_ok'):
                fact['key_matches_kind'] = fact.get('kind') == kind
                if fact['key_matches_kind']:
                    try:
                        native[kind] = native_archive_facts(raw, kind=kind,
                            key=plan.oss_keys[kind], digest=fact.get('archive_sha256'))
                    except (ValueError, OSError, tarfile.TarError) as error:
                        fact['native_result_error'] = str(error)
        except APIError as error:
            missing = error.code in ('NoSuchKey', 'NotFound', '404')
            archives[kind] = {'present': False if missing else None, 'error': error.code}
        except Exception as error:
            archives[kind] = {'present': True, 'manifest_ok': False, 'error': str(error)}
    for kind in ('final', 'interrupted'):
        facts = native.get(kind, {})
        if rust_result is None and 'execution_index' in facts:
            rust_result = facts['execution_index']
        if tools_check is None and 'tools_check_result' in facts:
            code = facts['tools_check_result']['value'].get('exit_code')
            if type(code) is int:
                tools_check = code == 0
    previous, history_error = None, None
    try:
        previous = json.loads(oss.get(plan.oss_keys['summary']))
    except APIError as error:
        if error.code not in ('NoSuchKey', 'NotFound', '404'):
            history_error = error.code
    except Exception as error:
        history_error = type(error).__name__
    if previous is not None:
        if previous.get('run_id') != plan.run_id or previous.get('run_attempt') != plan.run_attempt:
            raise ValueError('existing summary belongs to another run')
        if rust_result is None:
            rust_result = previous.get('rust_result')
        if tools_check is None:
            tools_check = previous.get('tools_check')
        image_id = image_id or previous.get('image_id')
    summary = run_summary(plan, image_id=image_id, rust_result=rust_result, handoff=archives,
                          compute_terminal=terminal, leftovers=leftovers, tools_check=tools_check)
    if history_error:
        summary['summary_history_error'] = history_error
    try:
        oss.put(plan.oss_keys['summary'], json.dumps(summary, sort_keys=True).encode())
        summary['summary_publication'] = {'published': True}
    except Exception as error:
        summary['summary_publication'] = {'published': False, 'error': getattr(error, 'code', type(error).__name__)}
    return summary
