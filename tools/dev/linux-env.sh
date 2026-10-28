#!/bin/sh
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
set -eu
script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
case "${1:-}" in
    prepare)
        if [ "$(id -u)" != 0 ]; then
            echo 'prepare requires root; use sudo and an explicit --user.' >&2
            exit 2
        fi
        # Check the bootstrap seed before installing Python. The Python owner
        # performs the complete profile and account preflight before other work.
        if [ ! -r /etc/os-release ]; then
            echo 'Ubuntu 26.04 is required.' >&2; exit 2
        fi
        . /etc/os-release
        if [ "$ID" != ubuntu ] || [ "$VERSION_ID" != 26.04 ] || [ "$(uname -m)" != x86_64 ]; then
            echo 'Native Ubuntu 26.04 amd64 is required.' >&2; exit 2
        fi
        selected_user=
        expect_user=no
        for argument in "$@"; do
            if [ "$expect_user" = yes ]; then selected_user=$argument; expect_user=no; fi
            if [ "$argument" = --user ]; then expect_user=yes; fi
        done
        if [ -z "$selected_user" ] || ! id -- "$selected_user" >/dev/null 2>&1; then
            echo 'prepare requires an existing explicit --user account.' >&2; exit 2
        fi
        seed_missing=
        for package in python3 ca-certificates curl git; do
            if ! dpkg-query -W -f='${Status}' "$package" 2>/dev/null | /bin/grep -q '^install ok installed$'; then
                seed_missing="$seed_missing $package"
            fi
        done
        if [ -n "$seed_missing" ]; then
            apt-get update
            # Package names above are fixed, never interpolated user input.
            DEBIAN_FRONTEND=noninteractive apt-get install -y $seed_missing
        fi
        ;;
    check|env|--help|-h)
        ;;
    *)
        echo 'Usage: linux-env.sh {prepare|check|env} --user ACCOUNT --profile FILE --fixture iceberg-rest' >&2
        exit 2
        ;;
esac
if ! command -v python3 >/dev/null 2>&1; then
    echo 'Python 3 is missing; check/env never install prerequisites. Run prepare as root.' >&2
    exit 2
fi
export PYTHONDONTWRITEBYTECODE=1
exec python3 "$script_dir/linux_env.py" "$@"
