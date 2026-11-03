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

script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
fixture_dir=$(cd "$script_dir/.." && pwd)
repo_root=$(cd "$fixture_dir/../.." && pwd)

# Resolve once. The caller owns fixture startup; this driver never changes bindings or inputs.
publication=$(python3 - "$fixture_dir" <<'PY'
import json
import os
from pathlib import Path
import sys

try:
    publication = (Path(sys.argv[1]) / 'runtime/current/published').resolve(strict=True)
    manifest = json.loads((publication / 'manifest.json').read_text())
    if not manifest.get('ready'):
        raise ValueError('publication has no saved configuration')
    if not (publication / 'env.sh').is_file():
        raise ValueError('publication has no immutable environment')
except (OSError, ValueError) as error:
    print(f'BLOCKED: pinned fixture publication required: {error}', file=sys.stderr)
    sys.exit(75)
print(publication)
PY
)
source "$publication/env.sh"
if [[ "$NOVA_ENV_REST_ENV_FILE" != "$publication/env.sh" ]]; then
    echo "FAIL: sourced environment differs from pinned publication" >&2
    exit 1
fi

: "${NOVAROCKS_ICEBERG_REST_URI:?pinned REST URI required}"
: "${NOVAROCKS_ICEBERG_REST_WAREHOUSE:?pinned REST warehouse required}"
: "${AWS_S3_ENDPOINT:?pinned S3 endpoint required}"
: "${AWS_S3_ACCESS_KEY_ID:?pinned S3 access key required}"
: "${AWS_S3_SECRET_ACCESS_KEY:?pinned S3 secret required}"

owns_fixture_run=false
[[ "${NOVA_FIXTURE_RUN_CONTEXT:-null}" == null ]] && owns_fixture_run=true
NOVA_FIXTURE_RUN_CONTEXT="$(python3 "$repo_root/docker/iceberg-rest/fixture_run.py" context --env-file "$publication/env.sh")"
export NOVA_FIXTURE_RUN_CONTEXT
NOVA_FIXTURE_RUN_STORAGE="$(python3 "$repo_root/docker/iceberg-rest/fixture_run.py" storage --env-file "$publication/env.sh")"
export NOVA_FIXTURE_RUN_STORAGE
if [[ "$NOVA_FIXTURE_RUN_CONTEXT" != null && "${NOVA_FIXTURE_JAVA_DRIVER_OWNER:-false}" != true ]]; then
    # The one command owner pins the complete original budget for Cargo and
    # every native test descendant; recursive admission never creates a run.
    export NOVA_FIXTURE_JAVA_DRIVER_OWNER=true
    python3 "$repo_root/docker/iceberg-rest/fixture_run.py" command --env-file "$publication/env.sh" -- bash "$0" "$@" &
    driver_owner_pid=$!
    trap 'kill -TERM "$driver_owner_pid" 2>/dev/null || true; wait "$driver_owner_pid" || true; exit 143' TERM
    trap 'kill -TERM "$driver_owner_pid" 2>/dev/null || true; wait "$driver_owner_pid" || true; exit 130' INT
    driver_status=0
    wait "$driver_owner_pid" || driver_status=$?
    trap - INT TERM
    if [[ "$driver_status" == 0 && "$owns_fixture_run" == true ]]; then
        NOVA_FIXTURE_CLEANUP_MODE=true python3 "$repo_root/docker/iceberg-rest/fixture_run.py" cleanup-run --env-file "$publication/env.sh" >&2 || driver_status=$?
    fi
    exit "$driver_status"
fi
python3 "$repo_root/docker/iceberg-rest/fixture_run.py" sweep-start --env-file "$publication/env.sh" >&2
run_id=$(python3 -c 'import json,sys,uuid; value=json.loads(sys.argv[1]);print(value["identity"]["run_id"] if value else uuid.uuid4().hex)' "$NOVA_FIXTURE_RUN_CONTEXT")
export NR_IRU5_INTEROP_NAMESPACE="${run_id}_iru5"
export NR_IRU5_INTEROP_ARTIFACT_DIR="$NOVA_ENV_RUNTIME_DIR/iru5-java-rest-$run_id"
mkdir "$NR_IRU5_INTEROP_ARTIFACT_DIR"
artifact="$NR_IRU5_INTEROP_ARTIFACT_DIR/runner.log"
python3 - "$publication/manifest.json" "$NR_IRU5_INTEROP_ARTIFACT_DIR/fixture.json" "$publication" <<'PY'
import json
import os
from pathlib import Path
import sys
manifest = json.loads(Path(sys.argv[1]).read_text())
facts = {
    'publication': sys.argv[3],
    'run_context': json.loads(os.environ['NOVA_FIXTURE_RUN_CONTEXT']),
    'run_storage': json.loads(os.environ['NOVA_FIXTURE_RUN_STORAGE']),
    'boundary': 'Java REST metadata interoperability; native 1FE+3BE SQL acceptance is separate',
}
Path(sys.argv[2]).write_text(json.dumps(facts, indent=2) + '\n')
PY

cd "$repo_root"
cargo test --locked --offline --profile dev-opt -p novarocks-connector-iceberg \
    --lib iru5_java_rest_ -- --ignored --test-threads=1 --nocapture 2>&1 | tee "$artifact"

# An unwired ignored module would otherwise produce a misleading zero-test success.
python3 - "$artifact" <<'PY'
from pathlib import Path
import re
import sys
text = Path(sys.argv[1]).read_text()
expected = (
    'iru5_java_rest_three_shapes_historic_spec_and_composition',
    'iru5_java_rest_d12_inheritance_and_physical_rewrite',
    'iru5_java_rest_two_canonical_clients_real_cas',
    'iru5_java_rest_attempt_runner_reloads_and_reprepares_after_real_cas',
)
for name in expected:
    if not re.search(r'^test [^\n]*' + re.escape(name) + r' \.\.\. ok$', text, re.MULTILINE):
        raise SystemExit(f'FAIL: expected ignored interop test did not pass: {name}')
if not re.search(r'test result: ok\. 4 passed; 0 failed; 0 ignored;', text):
    raise SystemExit('FAIL: expected exactly four enabled interop tests')
PY
printf 'IRU-5 Java REST interoperability artifacts: %s\n' "$NR_IRU5_INTEROP_ARTIFACT_DIR"
