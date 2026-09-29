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
FIXTURE_STORE="${NOVA_FIXTURE_STORE:-${XDG_CACHE_HOME:-$HOME/.cache}/novarocks/fixture-inputs}"
DRY_RUN="false"
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
    --fixture-store)
      (($# >= 2)) || { echo "--fixture-store requires a path" >&2; exit 2; }
      FIXTURE_STORE="$2"
      shift 2
      ;;
    --fixture-store=*)
      FIXTURE_STORE="${1#--fixture-store=}"
      [[ -n "$FIXTURE_STORE" ]] || { echo "--fixture-store requires a path" >&2; exit 2; }
      shift
      ;;
    --dry-run)
      DRY_RUN="true"
      args+=("$1")
      shift
      ;;
    *)
      args+=("$1")
      shift
      ;;
  esac
done

ENV_FILE="$(python3 -c 'import pathlib, sys; print(pathlib.Path(sys.argv[1]).resolve())' "$ENV_FILE")"
if [[ ! -f "$ENV_FILE" ]]; then
  echo "NovaRocks generated environment is not initialized: $ENV_FILE" >&2
  echo "run docker/iceberg-rest/up.sh first" >&2
  exit 1
fi

if [[ "$DRY_RUN" == "true" ]]; then
  exec python3 "$SCRIPT_DIR/fixture.py" prepare --env-file "$ENV_FILE" "${args[@]}"
fi

"$SCRIPT_DIR/../fixture-inputs/verify.sh" --store "$FIXTURE_STORE"
exec python3 "$SCRIPT_DIR/fixture.py" prepare --env-file "$ENV_FILE" --fixture-bom "$FIXTURE_STORE/bom.json" "${args[@]}"
