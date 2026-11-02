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

"""Pure identities and cloud deadlines; fixed UTC clock is the independent oracle."""
from datetime import datetime, timezone
import unittest
from aliyun_ci.config import validate_config
from aliyun_ci.plan import derive
from .test_config import example


class PlanTests(unittest.TestCase):
    def test_deterministic_run_and_deadline(self):
        config = validate_config(example())
        plan = derive(config, '42', 2, datetime(2026, 10, 28, tzinfo=timezone.utc), nonce='fixed')
        self.assertEqual('2026-10-28T03:00:00Z', plan.deadline)
        self.assertEqual('g16-novarocks-ci-42-2', plan.client_token)
        self.assertEqual('g16-42-2-fixed', plan.label)
        self.assertEqual('novarocks-ci/runs/42/2/final.tar.gz', plan.oss_keys['final'])
        self.assertEqual('github', plan.tags['origin'])
        self.assertEqual('g16-6e6f7661726f636b732d6369-42-2', plan.secret_name)
        self.assertEqual(165, plan.test_timeout_minutes)

    def test_cloud_minute_precision_never_shortens_deadline(self):
        plan = derive(validate_config(example()), '42', 1,
                      datetime(2026, 10, 28, 0, 0, 37, tzinfo=timezone.utc))
        self.assertEqual('2026-10-28T03:01:00Z', plan.deadline)

    def test_local_identity_and_cloud_token_limit(self):
        config = validate_config(example())
        self.assertEqual('local', derive(config, 'local-20261028-a1', 1, datetime.now(timezone.utc)).tags['origin'])
        with self.assertRaisesRegex(ValueError, 'ClientToken'):
            derive(config, '9' * 64, 1, datetime.now(timezone.utc))
        for run, attempt in (('../42', 1), ('42', 0), ('42', True)):
            with self.assertRaises(ValueError):
                derive(config, run, attempt, datetime.now(timezone.utc))
