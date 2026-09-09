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

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ENV_FILE="${NOVA_ENV_REST_ENV_FILE:-$SCRIPT_DIR/../iceberg-rest/runtime/current/env.sh}"
args=()
while (($#)); do
  case "$1" in
    --env-file)
      (($# >= 2)) || { echo "--env-file requires a path" >&2; exit 2; }
      ENV_FILE="$2"
      shift 2
      ;;
    --env-file=*)
      ENV_FILE="${1#--env-file=}"
      [[ -n "$ENV_FILE" ]] || { echo "--env-file requires a path" >&2; exit 2; }
      shift
      ;;
    *)
      args+=("$1")
      shift
      ;;
  esac
done

if [[ ! -f "$ENV_FILE" ]]; then
  echo "NovaRocks generated environment is not initialized: $ENV_FILE" >&2
  exit 1
fi

exec python3 "$SCRIPT_DIR/fixture.py" cleanup --env-file "$ENV_FILE" "${args[@]}"
