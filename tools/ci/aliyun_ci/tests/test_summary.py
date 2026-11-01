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

"""Cleanup success never rewrites product failure or missing evidence."""
from datetime import datetime, timezone
import unittest
from aliyun_ci.config import validate_config
from aliyun_ci.plan import derive
from aliyun_ci.summary import run_summary
from .test_config import example


class SummaryTests(unittest.TestCase):
    def test_independent_facts(self):
        plan = derive(validate_config(example()), '42', 1, datetime.now(timezone.utc))
        value = run_summary(plan, image_id='m-1', rust_result={'exit_code': 100},
                            handoff={'present': False}, compute_terminal='released')
        self.assertEqual({'exit_code': 100}, value['rust_result'])
        self.assertFalse(value['handoff']['present'])
        self.assertEqual('released', value['compute_terminal'])
        self.assertIsNone(value['tools_check'])
