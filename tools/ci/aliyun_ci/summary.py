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

"""Separate product exit, durable handoff, compute terminal and leftovers."""
from .ports import SUMMARY_SCHEMA


def run_summary(plan, *, image_id, rust_result=None, handoff=None,
                compute_terminal='unknown', leftovers=None, tools_check=None):
    if compute_terminal not in ('released', 'unknown', 'present'):
        raise ValueError('invalid compute terminal fact')
    return {'schema': SUMMARY_SCHEMA, 'run_id': plan.run_id, 'run_attempt': plan.run_attempt,
            'image_id': image_id, 'deadline': plan.deadline, 'label': plan.label,
            'rust_result': rust_result, 'handoff': handoff,
            'compute_terminal': compute_terminal, 'leftovers': leftovers or [],
            'tools_check': tools_check}
