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

"""GitHub REST projection; token value is never logged or serialized."""
import json
import re
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen
from .ports import APIError


class GitHub:
    def __init__(self, token, *, endpoint='https://api.github.com', opener=urlopen):
        if not token:
            raise ValueError('an explicit GitHub installation token is required')
        self.token, self.endpoint, self.opener = token, endpoint.rstrip('/'), opener

    def call(self, method, path, body=None):
        request = Request(self.endpoint + path,
            data=None if body is None else json.dumps(body).encode(), method=method,
            headers={'Authorization': f'Bearer {self.token}', 'Accept': 'application/vnd.github+json',
                     'X-GitHub-Api-Version': '2022-11-28', 'Content-Type': 'application/json'})
        try:
            with self.opener(request, timeout=30) as response:
                raw = response.read()
                return json.loads(raw) if raw else None
        except HTTPError as error:
            raise APIError(f'GitHubHTTP{error.code}', 'GitHub request failed') from error
        except URLError as error:
            raise APIError('GitHubTransportUnknown', type(error.reason).__name__) from error


def paginate(github, path, key):
    page, result = 1, []
    while True:
        value = github.call('GET', path + ('&' if '?' in path else '?') + f'per_page=100&page={page}')
        rows = value[key] if key else value
        result.extend(rows)
        if len(rows) < 100:
            return result
        page += 1


def validate_target(config, github, sha):
    if not isinstance(sha, str) or not re.fullmatch('[0-9a-fA-F]{40}', sha):
        raise ValueError('sha must be a complete 40-hex commit')
    root = f"/repos/{config['repository']}"
    commit = github.call('GET', f'{root}/commits/{sha}')
    if commit['sha'].lower() != sha.lower():
        raise ValueError('GitHub returned another commit')
    profile = github.call('GET', f'{root}/contents/tools/dev/profiles/aliyun-ubuntu26-amd64-tools.json?ref={sha}')
    if profile.get('type') != 'file':
        raise ValueError('target commit lacks the tools profile')
    return commit


def verify_runner_group(config, github):
    org, repository = config['repository'].split('/')
    group_id = config['github']['runner_group_id']
    group = github.call('GET', f'/orgs/{org}/actions/runner-groups/{group_id}')
    required = f"{config['repository']}/{config['github']['workflow']}@refs/heads/main"
    accepted = (required, required.replace('@refs/heads/main', '@main'))
    if not group.get('restricted_to_workflows') or not (
            set(group.get('selected_workflows', [])) in ({accepted[0]}, {accepted[1]})):
        raise ValueError('runner group must restrict workflow access to the trusted main workflow')
    if not group.get('allows_public_repositories'):
        raise ValueError('runner group must explicitly allow this public repository')
    if group.get('visibility') != 'selected':
        raise ValueError('runner group must select only the deployment repository')
    repositories = paginate(github, f'/orgs/{org}/actions/runner-groups/{group_id}/repositories', 'repositories')
    if {row['full_name'] for row in repositories} != {config['repository']}:
        raise ValueError('runner group repository restriction does not match deployment')
    return group


def runner_tags(runner):
    return {item['name'][4:].split(':', 1)[0]: item['name'][4:].split(':', 1)[1]
            for item in runner.get('labels', []) if item['name'].startswith('g16-') and ':' in item['name'][4:]}


def generate_jit(config, plan, github, *, instance_id):
    verify_runner_group(config, github)
    expected = {key: value for key, value in plan.tags.items() if key != 'deadline'}
    if any(all(runner_tags(row).get(key) == value for key, value in expected.items())
           for row in runners(config, github)):
        raise APIError('RunnerAlreadyRegistered', 'this run already owns a runner registration')
    return github.call('POST', f"/repos/{config['repository']}/actions/runners/generate-jitconfig", {
        'name': plan.label, 'runner_group_id': config['github']['runner_group_id'],
        'labels': [plan.label] + [f'g16-{key}:{value}' for key, value in sorted(
            dict(plan.tags, account_id=str(config['account_id']), region=config['region'], instance_id=instance_id).items())],
        'work_folder': '_work'})['encoded_jit_config']


def runners(config, github):
    return paginate(github, f"/repos/{config['repository']}/actions/runners", 'runners')


def delete_runner(config, github, name):
    removed = []
    for runner in runners(config, github):
        if runner['name'] == name:
            try:
                github.call('DELETE', f"/repos/{config['repository']}/actions/runners/{runner['id']}")
            except APIError as error:
                if error.code != 'GitHubHTTP404':
                    raise
            removed.append(runner['id'])
    return removed
