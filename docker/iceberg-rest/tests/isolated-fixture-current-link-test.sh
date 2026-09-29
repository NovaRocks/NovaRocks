#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
# `docker/iceberg-rest/runtime/current` is the single documented environment
# entrypoint for a worktree: CLAUDE.md tells every agent and developer to
# `source docker/iceberg-rest/runtime/current/env.sh`.  An isolated system-test
# fixture creates a throwaway environment whose ports die with it, so it must
# leave that link exactly as it found it.  When it did not, every later command
# in the worktree silently targeted the dead fixture's ports and failed with
# errors that pointed nowhere near the cause.

set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
export PYTHONPATH="$SCRIPT_DIR:$SCRIPT_DIR/..${PYTHONPATH:+:$PYTHONPATH}"
exec python3 -m unittest -v test_runtime_entry.IsolatedEntryTests
