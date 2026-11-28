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

if [[ $# != 4 ]]; then
  echo "usage: $0 recursive|visible initialize|mutate|initial|incremental|full namespace output-dir" >&2
  exit 2
fi
kind=$1
phase=$2
namespace=$3
output_dir=$4
if [[ ! "$namespace" =~ ^[a-z_][a-z0-9_]{0,62}$ ]]; then
  echo 'invalid scoped Iceberg namespace' >&2
  exit 2
fi

case "$kind" in
  recursive)
    fixture=RecursiveTypeFixture
    prefix=recursive_types
    case "$phase" in
      initialize) marker=RECURSIVE_SOURCE_READY ;;
      mutate) marker=RECURSIVE_SOURCE_CHANGED ;;
      *) marker=RECURSIVE_MV_OBSERVED ;;
    esac
    ;;
  visible)
    fixture=VisibleContentEncodingsFixture
    prefix=visible_encodings
    case "$phase" in
      initialize) marker=VISIBLE_CONTENT_ENCODINGS_READY ;;
      mutate) marker=VISIBLE_CONTENT_ENCODINGS_CHANGED ;;
      *) marker=VISIBLE_CONTENT_ENCODINGS_OBSERVED ;;
    esac
    ;;
  *) echo 'unknown content-encoding fixture' >&2; exit 2 ;;
esac

case "$phase" in
  initialize|mutate) call="$fixture.$phase(\"$namespace\")" ;;
  initial|incremental|full) call="$fixture.observe(\"$namespace\", \"$phase\")" ;;
  *) echo 'unknown content-encoding stage' >&2; exit 2 ;;
esac

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
workspace="$(cd "$script_dir/../../../.." && pwd)"
mkdir -p "$output_dir"
evidence="$(mktemp -d "$output_dir/$kind-$phase.XXXXXX")"
scala="$evidence/fixture.scala"
log="$evidence/spark.log"
cat "$script_dir/../iceberg-delete-applicability/generate.scala" "$script_dir/fixture.scala" > "$scala"
cat >> "$scala" <<SCALA
try {
  DeleteApplicabilityFixture.initialize(org.apache.spark.sql.SparkSession.active, "$namespace", "$prefix")
  $call
} catch { case failure: Throwable => failure.printStackTrace(); System.exit(1) }
SCALA
if ! "$workspace/docker/iceberg-rest/spark-shell.sh" "$scala" > "$log" 2>&1; then
  tail -80 "$log" >&2
  exit 1
fi
if ! grep -Fxq "$marker" "$log"; then
  tail -80 "$log" >&2
  exit 1
fi
grep '^UEA4G_RECEIPT ' "$log" > "$evidence/observations.jsonl"
printf '%s\n' "$marker"
