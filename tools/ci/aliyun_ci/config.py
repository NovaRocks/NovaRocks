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

"""Non-secret deployment inputs. Unknown fields are configuration errors."""
from __future__ import annotations
from dataclasses import dataclass
import json
from pathlib import Path
import re

LOCAL_CARGO_REGISTRIES = ('crates-io', 'rsproxy')
LOCAL_SWAP_GIB = (0, 16)


def fields(value, expected, name):
    if not isinstance(value, dict) or set(value) != set(expected):
        raise ValueError(f"{name} requires exactly these fields: {', '.join(expected)}")


def positive(value, name):
    if type(value) is not int or value <= 0:
        raise ValueError(f"{name} must be a positive integer")


@dataclass(frozen=True)
class DeploymentConfig:
    data: dict

    def __getitem__(self, key):
        return self.data[key]


def validate_config(data: dict) -> DeploymentConfig:
    data = dict(data)
    execution_kind = data.pop('execution_kind', 'github-runner')
    build_jobs = data.pop('build_jobs', None)
    registry_requested = 'cargo_registry' in data
    cargo_registry = data.pop('cargo_registry', 'crates-io')
    swap_requested = 'swap_gib' in data
    swap_gib = data.pop('swap_gib', 0)
    if type(swap_gib) is not int or swap_gib not in LOCAL_SWAP_GIB:
        raise ValueError('swap_gib must be 0 or 16')
    if swap_requested and execution_kind != 'local-rust':
        raise ValueError('swap_gib is a local-rust input')
    if cargo_registry not in LOCAL_CARGO_REGISTRIES:
        raise ValueError('cargo_registry must be crates-io or rsproxy')
    if registry_requested and execution_kind != 'local-rust':
        raise ValueError('cargo_registry is a local-rust input')
    if build_jobs is not None:
        positive(build_jobs, 'build_jobs')
        if execution_kind != 'local-rust':
            raise ValueError('build_jobs is a local-rust input')
    if execution_kind not in ('local-rust', 'github-runner'):
        raise ValueError('unsupported execution_kind')
    if execution_kind == 'local-rust':
        if 'github' in data:
            raise ValueError('local-rust configuration must omit github')
        data['github'] = None
    fields(data, ('deployment_id', 'repository', 'account_id', 'region', 'candidates',
                  'security_group_id', 'instance_ram_role', 'system_disk_gib', 'internet_max_bandwidth_out_mbps',
                  'oss', 'deadline', 'watcher', 'janitor', 'github', 'base_image_id'), 'deployment')
    for key in ('deployment_id', 'region', 'security_group_id', 'instance_ram_role', 'base_image_id'):
        if not isinstance(data[key], str) or not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_.-]*', data[key]):
            raise ValueError(f"invalid {key}")
    for key, limit in (('instance_ram_role', 64), ('deployment_id', 56)):
        # ROS RAM RoleName permits letters, digits, dots and hyphens, at most 64.
        # The deployment also mints the '<deployment>-control' role name.
        if not re.fullmatch(r'[A-Za-z0-9.-]+', data[key]) or len(data[key]) > limit:
            raise ValueError(f'{key} exceeds the RAM role naming contract')
    if not re.fullmatch(r'[0-9]+', str(data['account_id'])):
        raise ValueError('account_id must be numeric')
    if not isinstance(data['repository'], str) or not re.fullmatch(r'[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+', data['repository']):
        raise ValueError('repository must be owner/name')
    if not isinstance(data['candidates'], list) or not data['candidates']:
        raise ValueError('at least one explicit candidate is required')
    for item in data['candidates']:
        fields(item, ('instance_type', 'vswitch_id', 'zone'), 'candidate')
        if any(not isinstance(v, str) or not re.fullmatch(r'[A-Za-z0-9_.-]+', v) for v in item.values()):
            raise ValueError('invalid candidate')
    fields(data['oss'], ('bucket', 'prefix'), 'oss')
    if not re.fullmatch(r'[a-z0-9][a-z0-9-]+[a-z0-9]', data['oss']['bucket']):
        raise ValueError('invalid OSS bucket')
    prefix = data['oss']['prefix']
    if not isinstance(prefix, str) or not re.fullmatch(r'[A-Za-z0-9_-]+(?:/[A-Za-z0-9_-]+)*', prefix):
        raise ValueError('OSS prefix must be an explicit relative path')
    sections = {'deadline': ('work_minutes', 'upload_reserve_minutes', 'teardown_reserve_minutes'),
                'watcher': ('poll_seconds', 'cancel_budget_seconds'), 'janitor': ('grace_minutes',)}
    for key, names in sections.items():
        fields(data[key], names, key)
        for name in names:
            positive(data[key][name], f'{key}.{name}')
    positive(data['system_disk_gib'], 'system_disk_gib')
    positive(data['internet_max_bandwidth_out_mbps'], 'internet_max_bandwidth_out_mbps')
    if sum(data['deadline'].values()) < 30:
        raise ValueError('cloud auto release requires at least 30 minutes')
    if data['watcher']['poll_seconds'] + data['watcher']['cancel_budget_seconds'] >= 300:
        raise ValueError('spot notice must retain time for upload within five minutes')
    if execution_kind == 'github-runner':
        fields(data['github'], ('runner_group_id', 'workflow'), 'github')
        positive(data['github']['runner_group_id'], 'github.runner_group_id')
        if data['github']['workflow'] != '.github/workflows/aliyun-ci.yml':
            raise ValueError('unsupported worker workflow')
    else:
        data.pop('github')
    if execution_kind != 'github-runner':
        data['execution_kind'] = execution_kind
    if build_jobs is not None:
        data['build_jobs'] = build_jobs
    if registry_requested:
        data['cargo_registry'] = cargo_registry
    if swap_requested:
        data['swap_gib'] = swap_gib
    return DeploymentConfig(json.loads(json.dumps(data)))


def load_config(path: str | Path) -> DeploymentConfig:
    return validate_config(json.loads(Path(path).read_text()))
