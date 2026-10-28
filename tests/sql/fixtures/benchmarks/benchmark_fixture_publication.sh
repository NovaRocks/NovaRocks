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

# Direct S3 publication helpers.  The local storage mode exists only for the
# hermetic lifecycle tests. OSS uses the original machine's supervised SDK
# entrypoint; static MinIO retains its SigV4 transport.

fixture_publication_uri_path() {
  local uri="$1" root="${BENCHMARK_FIXTURE_STORAGE_DIR:?local storage root is required}"
  [[ "$uri" == s3://* ]] || return 64
  printf '%s/%s' "$root" "${uri#s3://}"
}

fixture_publication_curl_capable() {
  curl --help all 2>/dev/null | grep -q -- '--aws-sigv4'
}

fixture_publication_capable() {
  [[ -n "${BENCHMARK_FIXTURE_STORAGE_DIR:-}" ]] && return 0
  if [[ "${NOVA_ENV_OBJECT_STORE_KIND:-minio}" == aliyun-oss ]]; then
    [[ -n "${NOVA_ENV_OBJECT_STORE_CONFIG:-}" && -x "${NOVA_OSS_OPS_PYTHON:-}" && -n "${NOVA_FIXTURE_MACHINE_ROOT:-}" ]]
  else
    fixture_publication_curl_capable
  fi
}

fixture_publication_oss_operation() {
  # No SDK process is launched here. The existing machine owner revalidates
  # this exact benchmark contract, original budget and actual child exit.
  local operation="$1" uri="$2" file="${3:-}"
  python3 - "${resolved_dataset_file:?resolved benchmark contract is required}" "$operation" "$uri" "$file" <<'PYREQUEST' | \
    python3 "$SOURCE_ROOT/docker/iceberg-rest/machine_mc.py" \
      --root "${NOVA_FIXTURE_MACHINE_ROOT:?original machine root is required}" \
      --daemon "${NOVA_FIXTURE_MACHINE_DAEMON:?original machine daemon is required}" \
      --expected-version "${NOVA_FIXTURE_MACHINE_VERSION:?original machine version is required}" \
      --expected-generation "${NOVA_FIXTURE_MACHINE_GENERATION:?original machine generation is required}" \
      --benchmark-oss
import json, sys
resolved, operation, uri, path = sys.argv[1:]
request = dict(resolved=json.load(open(resolved)), operation=operation, uri=uri)
if operation == 'put': request['input'] = path
elif operation == 'get': request['output'] = path
print(json.dumps(request, separators=(',', ':')))
PYREQUEST
}

fixture_publication_s3_url() {
  local uri="$1" endpoint="${AWS_S3_ENDPOINT:?AWS_S3_ENDPOINT is required}"
  [[ "$uri" == s3://* ]] || return 64
  printf '%s/%s' "${endpoint%/}" "${uri#s3://}"
}

fixture_publication_make_curl_config() {
  local config="$1" method="$2" uri="$3" request_header="${4:-}" body="${5:-}" query="${6:-}"
  umask 077
  : > "$config"
  chmod 600 "$config"
  {
    printf 'silent\nshow-error\nfail-with-body\nrequest = "%s"\n' "$method"
    if [[ -n "${NOVA_FIXTURE_RUN_CONTEXT:-}" && "$NOVA_FIXTURE_RUN_CONTEXT" != null ]]; then
      local remaining
      remaining="$(python3 "$SOURCE_ROOT/docker/iceberg-rest/fixture_run.py" remaining --env-file "$ENV_FILE")" || return
      printf 'max-time = %s\nconnect-timeout = %s\n' "$remaining" "$remaining"
    fi
    printf 'url = "%s%s"\n' "$(fixture_publication_s3_url "$uri")" "$query"
    printf 'aws-sigv4 = "aws:amz:%s:s3"\n' "${AWS_REGION:-us-east-1}"
    # curl config keeps this credential out of argv and out of command logs.
    printf 'user = "%s:%s"\n' "${AWS_S3_ACCESS_KEY_ID:?AWS_S3_ACCESS_KEY_ID is required}" "${AWS_S3_SECRET_ACCESS_KEY:?AWS_S3_SECRET_ACCESS_KEY is required}"
    [[ -z "$request_header" ]] || printf 'header = "%s"\n' "$request_header"
    [[ -z "$body" ]] || printf 'data-binary = "@%s"\n' "$body"
  } >> "$config"
}

fixture_publication_list_prefix() {
  # stdout is one s3:// URI per object.  This is a direct S3 ListObjectsV2
  # operation, not a catalog or an mc/FE query.
  local prefix_uri="$1"
  if [[ -n "${BENCHMARK_FIXTURE_STORAGE_DIR:-}" ]]; then
    local path root
    path="$(fixture_publication_uri_path "$prefix_uri")"
    root="${BENCHMARK_FIXTURE_STORAGE_DIR%/}/"
    [[ -d "$path" ]] || return 44
    find "$path" -type f ! -name '*.etag' -print | while IFS= read -r object; do
      printf 's3://%s\n' "${object#"$root"}"
    done
    return
  fi
  if [[ "${NOVA_ENV_OBJECT_STORE_KIND:-minio}" == aliyun-oss ]]; then
    local receipt
    receipt="$(fixture_publication_oss_operation list "$prefix_uri")" || return
    python3 - "$receipt" <<'PYLIST'
import json, sys
value = json.loads(sys.argv[1])
if value.get('ok') is not True: raise SystemExit(1)
for item in value['objects']: print(item['uri'])
PYLIST
    return
  fi
  fixture_publication_curl_capable || return 64
  local bucket key encoded_prefix config headers body status
  bucket="${prefix_uri#s3://}"; bucket="${bucket%%/*}"
  key="${prefix_uri#s3://$bucket/}"
  encoded_prefix="$(python3 - "$key" <<'PY'
import sys
from urllib.parse import quote
print(quote(sys.argv[1], safe='/'))
PY
)"
  config="$(mktemp)"; headers="$(mktemp)"; body="$(mktemp)"
  fixture_publication_make_curl_config "$config" GET "s3://$bucket" "" "" "?list-type=2&prefix=$encoded_prefix"
  status="$(curl --config "$config" --dump-header "$headers" --output "$body" --write-out '%{http_code}' 2>/dev/null || true)"
  if [[ "$status" != 200 ]]; then
    rm -f "$config" "$headers" "$body"
    return 44
  fi
  python3 - "$bucket" "$body" <<'PY'
import sys
from xml.etree import ElementTree
bucket, source = sys.argv[1:]
root = ElementTree.parse(source).getroot()
for node in root.iter():
    if node.tag.rsplit('}', 1)[-1] == 'Key' and node.text:
        print(f's3://{bucket}/{node.text}')
PY
  status=$?
  rm -f "$config" "$headers" "$body"
  return "$status"
}

fixture_publication_get() {
  # Sets FIXTURE_PUBLICATION_BODY and FIXTURE_PUBLICATION_ETAG.  Do not invoke
  # this through command substitution: callers need the ETag from this shell.
  local uri="$1"
  if [[ -n "${BENCHMARK_FIXTURE_STORAGE_DIR:-}" ]]; then
    local path
    path="$(fixture_publication_uri_path "$uri")"
    [[ -f "$path" ]] || return 44
    FIXTURE_PUBLICATION_ETAG="$(cat "$path.etag" 2>/dev/null || sha256sum "$path" | awk '{print $1}')"
    FIXTURE_PUBLICATION_BODY="$(cat "$path")"
    return
  fi
  if [[ "${NOVA_ENV_OBJECT_STORE_KIND:-minio}" == aliyun-oss ]]; then
    local body receipt
    body="$(mktemp)"
    if ! receipt="$(fixture_publication_oss_operation get "$uri" "$body")"; then rm -f "$body"; return 44; fi
    if ! FIXTURE_PUBLICATION_ETAG="$(python3 - "$receipt" <<'PYGET'
import json, sys
value=json.loads(sys.argv[1])
if value.get('ok') is not True or not value.get('etag'): raise SystemExit(1)
print(value['etag'])
PYGET
)"; then rm -f "$body"; return 1; fi
    FIXTURE_PUBLICATION_BODY="$(cat "$body")"
    rm -f "$body"
    return
  fi
  fixture_publication_curl_capable || return 64
  local config headers body status
  config="$(mktemp)"; headers="$(mktemp)"; body="$(mktemp)"
  fixture_publication_make_curl_config "$config" GET "$uri"
  status="$(curl --config "$config" --dump-header "$headers" --output "$body" --write-out '%{http_code}' 2>/dev/null || true)"
  if [[ "$status" != 200 ]]; then
    rm -f "$config" "$headers" "$body"
    return 44
  fi
  FIXTURE_PUBLICATION_ETAG="$(awk 'tolower($1) == "etag:" {gsub("\\r", ""); sub(/^[^:]*:[[:space:]]*/, ""); gsub(/\"/, ""); print; exit}' "$headers")"
  if [[ -z "$FIXTURE_PUBLICATION_ETAG" ]]; then
    rm -f "$config" "$headers" "$body"
    return 1
  fi
  FIXTURE_PUBLICATION_BODY="$(cat "$body")"
  rm -f "$config" "$headers" "$body"
}

fixture_publication_head() {
  local uri="$1"
  if [[ -n "${BENCHMARK_FIXTURE_STORAGE_DIR:-}" ]]; then
    [[ -f "$(fixture_publication_uri_path "$uri")" ]]
    return
  fi
  if [[ "${NOVA_ENV_OBJECT_STORE_KIND:-minio}" == aliyun-oss ]]; then
    local receipt
    receipt="$(fixture_publication_oss_operation stat "$uri")" || return
    python3 - "$receipt" <<'PYHEAD'
import json, sys
value=json.loads(sys.argv[1])
raise SystemExit(0 if value.get('ok') is True and value.get('exists') is True else 1)
PYHEAD
    return
  fi
  fixture_publication_curl_capable || return 64
  local config status
  config="$(mktemp)"
  # Some S3-compatible servers retain keep-alive after an explicit HEAD while
  # reporting the object's non-zero Content-Length.  A one-byte ranged GET is
  # an equally direct existence check and has a bounded response body.
  fixture_publication_make_curl_config "$config" GET "$uri" 'Range: bytes=0-0'
  status="$(curl --config "$config" --output /dev/null --write-out '%{http_code}' 2>/dev/null || true)"
  rm -f "$config"
  # A zero-byte _SUCCESS sentinel is present but its only range is unsatisfiable.
  [[ "$status" == 200 || "$status" == 206 || "$status" == 416 ]]
}

fixture_publication_put() {
  # stdout is the HTTP status of one ordinary object PUT.
  local uri="$1" body="$2"
  if [[ -n "${BENCHMARK_FIXTURE_STORAGE_DIR:-}" ]]; then
    local path
    path="$(fixture_publication_uri_path "$uri")"
    mkdir -p "$(dirname "$path")"
    cp "$body" "$path" || return 1
    sha256sum "$path" | awk '{print $1}' > "$path.etag" || return 1
    printf '200\n'; return
  fi
  if [[ "${NOVA_ENV_OBJECT_STORE_KIND:-minio}" == aliyun-oss ]]; then
    local receipt
    receipt="$(fixture_publication_oss_operation put "$uri" "$body")" || return
    python3 - "$receipt" <<'PYPUT'
import json, sys
value=json.loads(sys.argv[1])
if value.get('ok') is not True or value.get('status') not in (200, 201): raise SystemExit(1)
print(value['status'])
PYPUT
    return
  fi
  fixture_publication_curl_capable || return 64
  local config status
  config="$(mktemp)"
  fixture_publication_make_curl_config "$config" PUT "$uri" "" "$body"
  status="$(curl --config "$config" --output /dev/null --write-out '%{http_code}' 2>/dev/null || true)"
  rm -f "$config"
  printf '%s\n' "$status"
}
