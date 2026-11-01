#!/usr/bin/env bash
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

set -euo pipefail
if [ "$#" -ne 1 ]; then
  echo 'Usage: run-rust.sh <absolute-attempt-root>' >&2
  exit 2
fi
attempt=$1
case "$attempt" in
  /*) ;;
  *) echo 'Attempt root must be absolute.' >&2; exit 2 ;;
esac
if [ ! -d "$attempt" ] || [ -e "$attempt/rust" ] || [ -e "$attempt/batch.pid" ]; then
  echo 'Attempt root must exist without prior batch PID or Rust output.' >&2
  exit 2
fi
(umask 077; set -o noclobber; printf '%s\n' "$$" > "$attempt/batch.pid")
exec python3 tools/ci/test-run.py batch --output "$attempt/rust"
