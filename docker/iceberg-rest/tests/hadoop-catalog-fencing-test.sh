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

script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
fixture_dir=$(cd "$script_dir/.." && pwd)
repo_root=$(cd "$fixture_dir/../.." && pwd)

# Bind before reading paths; an old current pointer is not this request's result.
publication="$("$fixture_dir/up.sh" | python3 -c 'import json, sys; print(json.load(sys.stdin)["published_dir"])')"
source "$publication/env.sh"

run_id="$(date +%s)-$$"
export NR_HADOOP_FENCE_MINIO_WAREHOUSE="${NOVAROCKS_ICEBERG_TEST_WAREHOUSE%/}/hadoop-fencing-$run_id"
artifact="$NOVA_ENV_RUNTIME_DIR/hadoop-catalog-fencing-$run_id.log"

cd "$repo_root"
cargo test -p novarocks-connector-iceberg \
    --test hadoop_catalog_fencing minio_ -- \
    --ignored --test-threads=1 --nocapture 2>&1 | tee "$artifact"

echo "Hadoop catalog MinIO fencing artifact: $artifact"
