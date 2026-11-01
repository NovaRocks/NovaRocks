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

"""Offline deployment contract tests, owned by CI maintainers; no cloud resources."""
from copy import deepcopy
from pathlib import Path
import json
import unittest
from aliyun_ci.config import validate_config


def example():
    return json.loads((Path(__file__).parents[1] / 'config.example.json').read_text())


class ConfigTests(unittest.TestCase):
    def test_example_and_unknown_fields(self):
        self.assertEqual('cn-beijing', validate_config(example())['region'])
        for section in (None, 'oss', 'deadline', 'github'):
            data = example()
            (data if section is None else data[section])['unexpected'] = True
            with self.subTest(section=section), self.assertRaises(ValueError):
                validate_config(data)

    def test_deadline_and_notice_leave_upload_time(self):
        data = example()
        data['deadline'] = dict(work_minutes=1, upload_reserve_minutes=1, teardown_reserve_minutes=1)
        with self.assertRaisesRegex(ValueError, '30 minutes'):
            validate_config(data)
        data = example()
        data['watcher']['cancel_budget_seconds'] = 290
        with self.assertRaisesRegex(ValueError, 'upload'):
            validate_config(data)

    def test_cloud_role_names_remain_exact_and_api_valid(self):
        for key, value in (('instance_ram_role', 'bad_role'), ('instance_ram_role', 'r'*65),
                           ('deployment_id', 'd'*57)):
            data = example(); data[key] = value
            with self.subTest(key=key), self.assertRaisesRegex(ValueError, 'RAM role'):
                validate_config(data)

    def test_config_is_detached_from_caller(self):
        data = example()
        config = validate_config(data)
        data['oss']['prefix'] = 'changed'
        self.assertEqual('novarocks-ci', config['oss']['prefix'])

    def test_local_requires_no_github_and_has_explicit_kind(self):
        data=example();data.pop('github');data['execution_kind']='local-rust'
        data['build_jobs']=2
        config=validate_config(data)
        self.assertEqual(2,config['build_jobs'])
        self.assertNotIn('github',config.data)
        from aliyun_ci.plan import derive
        from datetime import datetime,timezone
        plan=derive(config,'local-one',1,datetime.now(timezone.utc))
        self.assertEqual('local-rust',plan.execution_kind)
        self.assertEqual('local-rust',plan.tags['execution_kind'])
        data['github']={'runner_group_id':1,'workflow':'.github/workflows/aliyun-ci.yml'}
        with self.assertRaisesRegex(ValueError,'omit github'):validate_config(data)

    def test_cargo_registry_is_explicit_local_only_and_unknown_values_fail(self):
        data = example(); data.pop('github'); data['execution_kind'] = 'local-rust'
        self.assertNotIn('cargo_registry', validate_config(data).data)
        for selection in ('crates-io', 'rsproxy'):
            self.assertEqual(selection, validate_config(dict(data, cargo_registry=selection))['cargo_registry'])
        for selection in ('auto', '', None, True, []):
            with self.subTest(selection=selection), self.assertRaisesRegex(ValueError, 'cargo_registry'):
                validate_config(dict(data, cargo_registry=selection))
        with self.assertRaisesRegex(ValueError, 'local-rust'):
            validate_config(dict(example(), cargo_registry='rsproxy'))

    def test_swap_is_explicit_local_only_with_legacy_zero_default(self):
        data = example(); data.pop('github'); data['execution_kind'] = 'local-rust'
        self.assertNotIn('swap_gib', validate_config(data).data)
        for size in (0, 16):
            self.assertEqual(size, validate_config(dict(data, swap_gib=size))['swap_gib'])
        for size in (-1, 1, 8, 16.0, True, None, '16'):
            with self.subTest(size=size), self.assertRaisesRegex(ValueError, 'swap_gib'):
                validate_config(dict(data, swap_gib=size))
        with self.assertRaisesRegex(ValueError, 'local-rust'):
            validate_config(dict(example(), swap_gib=16))
