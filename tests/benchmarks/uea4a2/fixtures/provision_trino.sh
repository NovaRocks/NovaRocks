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

if [[ $# -ne 1 ]]; then
  echo "usage: provision_trino.sh OUTPUT_DIR" >&2
  exit 2
fi

script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
repo_root=$(cd "$script_dir/../../../.." && pwd)
output_dir=$1
[[ ! -e "$output_dir" ]] || { echo "output directory already exists" >&2; exit 2; }
command -v docker >/dev/null || { echo "docker is required" >&2; exit 2; }
command -v mc >/dev/null || { echo "MinIO mc is required" >&2; exit 2; }
env_file="${NOVA_ENV_REST_ENV_FILE:-$repo_root/docker/iceberg-rest/runtime/current/env.sh}"
env_file="$(python3 -c 'import pathlib, sys; print(pathlib.Path(sys.argv[1]).resolve(strict=True))' "$env_file")"
source "$env_file"

readonly image_digest=sha256:db58cc93e593a2706553745f276bb119c9810e69918be56ecde088ba7ccb0534
readonly image_ref=$image_digest
actual_image=$(docker image inspect "$image_ref" --format '{{.Id}}' 2>/dev/null) || {
  echo "pinned Trino 483 image is absent; provision it explicitly" >&2
  exit 1
}
[[ "$actual_image" == "$image_digest" ]] || { echo "Trino image digest mismatch" >&2; exit 1; }

mkdir -p "$output_dir"
output_dir=$(cd "$output_dir" && pwd)
container="nr-uea4a2-trino-$$"
schema="uea4a2_writer_$$"
table=sample
mc_config=$(mktemp -d)
cleanup() {
  if docker inspect "$container" >/dev/null 2>&1; then
    docker exec "$container" /usr/bin/trino --server http://localhost:8080 \
      --execute "DROP TABLE IF EXISTS iceberg.$schema.$table" >/dev/null 2>&1 || true
    docker exec "$container" /usr/bin/trino --server http://localhost:8080 \
      --execute "DROP SCHEMA IF EXISTS iceberg.$schema" >/dev/null 2>&1 || true
    docker rm -f "$container" >/dev/null 2>&1 || true
  fi
  rm -rf "$mc_config"
}
trap cleanup EXIT INT TERM

docker run --detach --rm --pull never \
  --name "$container" --label org.novarocks.test=uea4a2-writer \
  --network "${NOVA_ENV_COMPOSE_PROJECT}_iceberg_net" \
  --env "NOVA_TRINO_REST_WAREHOUSE=$NOVA_ENV_REST_WAREHOUSE_URI" \
  --env "NOVA_TRINO_S3_ACCESS_KEY=$AWS_S3_ACCESS_KEY_ID" \
  --env "NOVA_TRINO_S3_SECRET_KEY=$AWS_S3_SECRET_ACCESS_KEY" \
  --mount "type=bind,src=$repo_root/tests/datasketches-tck/interop/trino/rest-catalog,dst=/etc/trino,readonly" \
  "$image_ref" >/dev/null

healthy=false
for _ in $(seq 1 120); do
  if [[ "$(docker inspect "$container" --format '{{if .State.Health}}{{.State.Health.Status}}{{else}}none{{end}}' 2>/dev/null || true)" == healthy ]]; then
    healthy=true
    break
  fi
  sleep 1
done
[[ "$healthy" == true ]] || { docker logs "$container" >&2; exit 1; }
trino() { docker exec "$container" /usr/bin/trino --server http://localhost:8080 "$@"; }
version=$(trino --output-format TSV --execute 'SELECT version()')
[[ "$version" == 483 ]] || { echo "unexpected Trino version: $version" >&2; exit 1; }

sql="CREATE TABLE iceberg.$schema.$table WITH (format = 'PARQUET') AS SELECT CAST(id AS BIGINT) AS id, CAST(id % 17 AS INTEGER) AS category, CAST(id * 0.01 AS DECIMAL(12,2)) AS amount, CAST(ARRAY[CAST(id % 5 AS INTEGER), CAST(id % 7 AS INTEGER)] AS ARRAY(INTEGER)) AS nested FROM UNNEST(sequence(0, 4095)) AS t(id)"
trino --execute "CREATE SCHEMA iceberg.$schema" >/dev/null
trino --execute "$sql" >/dev/null
rows=$(trino --output-format TSV --execute "SELECT count(*) FROM iceberg.$schema.$table")
[[ "$rows" == 4096 ]] || { echo "Trino row oracle failed: $rows" >&2; exit 1; }
object=$(trino --output-format TSV --execute "SELECT file_path FROM iceberg.$schema.\"$table\$files\"")
[[ "$object" == s3://* && "$object" != *$'\n'* ]] || {
  echo "Trino did not publish exactly one S3 Parquet data file" >&2
  exit 1
}
mc --config-dir "$mc_config" alias set task "$AWS_S3_ENDPOINT" \
  "$AWS_S3_ACCESS_KEY_ID" "$AWS_S3_SECRET_ACCESS_KEY" >/dev/null
mc --config-dir "$mc_config" cp "task/${object#s3://}" "$output_dir/trino-483.parquet" >/dev/null

TRINO_OUTPUT_DIR="$output_dir" TRINO_IMAGE_DIGEST="$image_digest" \
TRINO_OBJECT="$object" TRINO_SQL="$sql" python3 - <<'PY'
import hashlib
import json
import os
from pathlib import Path

import pyarrow.parquet as pq

root = Path(os.environ['TRINO_OUTPUT_DIR'])
file = root / 'trino-483.parquet'
table = pq.read_table(file)
metadata = pq.ParquetFile(file).metadata
assert table.num_rows == 4096
receipt = {
    'schema_version': 1,
    'writer': 'Trino',
    'writer_version': '483',
    'image_digest': os.environ['TRINO_IMAGE_DIGEST'],
    'generator': 'provision_trino.sh',
    'sql': os.environ['TRINO_SQL'],
    'source_object': os.environ['TRINO_OBJECT'],
    'file': file.name,
    'sha256': hashlib.sha256(file.read_bytes()).hexdigest(),
    'file_bytes': file.stat().st_size,
    'rows': table.num_rows,
    'row_groups': metadata.num_row_groups,
    'columns': table.column_names,
    'created_by': metadata.created_by,
    'oracle_sha256': hashlib.sha256(
        json.dumps(table.to_pylist(), sort_keys=True, default=str).encode()
    ).hexdigest(),
}
(root / 'trino-manifest.json').write_text(json.dumps(receipt, indent=2, sort_keys=True) + '\n')
print(f"Trino corpus sha256:{receipt['sha256']} rows:{receipt['rows']}")
PY
