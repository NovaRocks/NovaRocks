#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements. See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership. The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License. You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied. See the License for the
# specific language governing permissions and limitations
# under the License.

set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WORKSPACE_ROOT="$(cd "$SCRIPT_DIR/../../../.." && pwd)"
mode=anomalies
namespace=""
prefix=""
output=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    --mode) mode="$2"; shift 2 ;;
    --namespace) namespace="$2"; shift 2 ;;
    --prefix) prefix="$2"; shift 2 ;;
    --output) output="$2"; shift 2 ;;
    *) echo "usage: $0 --output DIR [--mode anomalies|positive|corpus|promotion] [--namespace NAME] [--prefix NAME]" >&2; exit 2 ;;
  esac
done
[[ "$mode" == anomalies || "$mode" == positive || "$mode" == corpus || "$mode" == promotion ]] || { echo "Invalid mode: $mode" >&2; exit 2; }
[[ -n "$output" ]] || { echo "--output is required" >&2; exit 2; }
run_uuid="$(python3 -c 'import uuid; print(uuid.uuid4().hex)')"
namespace="${namespace:-uea4g_${run_uuid}}"
prefix="${prefix:-g02}"
[[ "$namespace" =~ ^[a-zA-Z0-9_]+$ && "$prefix" =~ ^[a-zA-Z0-9_]+$ ]] || { echo "Invalid fixture identifier" >&2; exit 2; }
NOVA_ENV_REST_ENV_FILE="$(python3 -c 'import os,sys; print(os.path.realpath(sys.argv[1]))' \
  "${NOVA_ENV_REST_ENV_FILE:-$WORKSPACE_ROOT/docker/iceberg-rest/runtime/current/env.sh}")"
export NOVA_ENV_REST_ENV_FILE
[[ -f "$NOVA_ENV_REST_ENV_FILE" ]] || { echo "Missing resolved fixture environment" >&2; exit 1; }
mkdir -p "$output"
output="$(cd "$output" && pwd)"
[[ ! -e "$output/receipt.json" && ! -e "$output/spark.log" ]] || { echo "Use a fresh output directory" >&2; exit 1; }
scala_file="$output/fixture.scala"
python3 - "$SCRIPT_DIR" "$scala_file" "$namespace" "$prefix" "$mode" <<'PY'
import json
import pathlib
import sys
source, destination, namespace, prefix, mode = sys.argv[1:]
root = pathlib.Path(source)
body = (root / "generate.scala").read_text()
if mode in ("anomalies", "corpus", "promotion"):
    body += "\n" + (root / (mode + ".scala")).read_text()
body += "\ntry {\n"
body += f"  DeleteApplicabilityFixture.initialize(org.apache.spark.sql.SparkSession.active, {json.dumps(namespace)}, {json.dumps(prefix)})\n"
if mode in ("anomalies", "positive"):
    body += '  val seeds = Seq("position", "equality", "dv").map(kind => DeleteApplicabilityFixture.sameCommit(kind))\n'
if mode == "anomalies":
    body += "  DeleteApplicabilityAnomalies.run(seeds)\n"
elif mode == "corpus":
    body += "  DeleteApplicabilityCorpus.run()\n"
elif mode == "promotion":
    body += "  DeleteApplicabilityPromotion.run()\n"
body += '  println("UEA4G_COMPLETE")\n'
body += "} catch { case failure: Throwable => failure.printStackTrace(); System.exit(1) }\n"
pathlib.Path(destination).write_text(body)
PY
if ! "$WORKSPACE_ROOT/docker/iceberg-rest/spark-shell.sh" "$scala_file" >"$output/spark.log" 2>&1; then
  grep -v '^UEA4G_RECEIPT ' "$output/spark.log" | tail -80 >&2
  exit 1
fi
python3 "$SCRIPT_DIR/validate_receipts.py" "$output/spark.log" --output "$output" --mode "$mode"
if [[ "$mode" == promotion ]]; then
  printf 'UEA4G_OBSERVATION_OK namespace=%s prefix=%s receipt=%s/receipt.json\n' "$namespace" "$prefix" "$output"
else
  printf 'UEA4G_FIXTURE_OK namespace=%s prefix=%s receipt=%s/receipt.json\n' "$namespace" "$prefix" "$output"
fi
