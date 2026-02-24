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

if [ -z "${BASH_VERSION:-}" ]; then
    exec bash "$0" "$@"
fi

set -euo pipefail

OS_NAME="$(uname -s)"

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

print_usage() {
    cat <<'EOF'
Usage:
  ./build.sh [cargo subcommand args...]

Examples:
  ./build.sh
  ./build.sh build --release
  ./build.sh test
  STARROCKS_THIRDPARTY=/path/to/starrocks/thirdparty ./build.sh build

Description:
  This wrapper resolves thirdparty root from STARROCKS_THIRDPARTY (or ./thirdparty by default)
  and exports OPENSSL_* / PKG_CONFIG_PATH so openssl-sys can use StarRocks thirdparty libs.
EOF
}

has_thirdparty_layout() {
    local dir="$1"
    [[ -d "${dir}/include" && ( -d "${dir}/lib" || -d "${dir}/lib64" ) ]]
}

normalize_thirdparty_root() {
    local input="$1"
    if has_thirdparty_layout "$input"; then
        echo "$input"
        return 0
    fi
    if has_thirdparty_layout "${input}/installed"; then
        echo "${input}/installed"
        return 0
    fi
    return 1
}

resolve_thirdparty_root() {
    local configured="${STARROCKS_THIRDPARTY:-}"
    if [[ -n "${configured}" ]]; then
        local candidate
        if [[ "${configured}" = /* ]]; then
            candidate="${configured}"
        else
            candidate="${ROOT_DIR}/${configured}"
        fi
        if root="$(normalize_thirdparty_root "${candidate}")"; then
            echo "${root}"
            return 0
        fi
        echo "Error: invalid STARROCKS_THIRDPARTY='${configured}', expected thirdparty root or installed dir with include/ and lib|lib64/" >&2
        return 1
    fi

    local fallback="${ROOT_DIR}/thirdparty"
    if root="$(normalize_thirdparty_root "${fallback}")"; then
        echo "${root}"
        return 0
    fi

    echo "Error: thirdparty directory not found, expected '${fallback}' (or its installed subdir), or set STARROCKS_THIRDPARTY" >&2
    return 1
}

append_path_var() {
    local var_name="$1"
    local value="$2"
    if [[ -z "${value}" ]]; then
        return 0
    fi
    if [[ -n "${!var_name:-}" ]]; then
        export "${var_name}=${value}:${!var_name}"
    else
        export "${var_name}=${value}"
    fi
}

unset_var_if_set() {
    local var_name="$1"
    unset "${var_name}" 2>/dev/null || true
}

detect_starrocks_gcc_home() {
    if [[ -n "${STARROCKS_GCC_HOME:-}" ]]; then
        return 0
    fi

    local candidates=(
        "/opt/rh/gcc-toolset-14/root/usr"
        "/opt/rh/gcc-toolset-13/root/usr"
        "/opt/rh/gcc-toolset-12/root/usr"
        "/opt/rh/gcc-toolset-11/root/usr"
        "/opt/rh/gcc-toolset-10/root/usr"
        "/opt/rh/devtoolset-11/root/usr"
        "/opt/rh/devtoolset-10/root/usr"
    )

    for candidate in "${candidates[@]}"; do
        if [[ -x "${candidate}/bin/g++" ]]; then
            export STARROCKS_GCC_HOME="${candidate}"
            echo "Detected STARROCKS_GCC_HOME=${STARROCKS_GCC_HOME}"
            return 0
        fi
    done
}

setup_starrocks_gcc_runtime() {
    local gcc_home="${STARROCKS_GCC_HOME:-}"
    if [[ -z "${gcc_home}" ]]; then
        return 0
    fi

    local gcc_bin="${gcc_home}/bin"
    local gcc_cmd="${gcc_bin}/gcc"
    local gxx_cmd="${gcc_bin}/g++"
    if [[ ! -x "${gcc_cmd}" || ! -x "${gxx_cmd}" ]]; then
        echo "Warning: STARROCKS_GCC_HOME is set but compiler not found under '${gcc_bin}'" >&2
        return 0
    fi

    export CC="${CC:-${gcc_cmd}}"
    export CXX="${CXX:-${gxx_cmd}}"
    append_path_var PATH "${gcc_bin}"

    local libstdcxx_path
    libstdcxx_path="$("${CXX}" -print-file-name=libstdc++.so 2>/dev/null || true)"
    if [[ -n "${libstdcxx_path}" && "${libstdcxx_path}" != "libstdc++.so" && -f "${libstdcxx_path}" ]]; then
        export LIBSTDCXX_PATH="${libstdcxx_path}"
        append_path_var LIBRARY_PATH "$(dirname "${libstdcxx_path}")"
        if [[ "${OS_NAME}" == "Linux" ]]; then
            append_path_var LD_LIBRARY_PATH "$(dirname "${libstdcxx_path}")"
        elif [[ "${OS_NAME}" == "Darwin" ]]; then
            append_path_var DYLD_LIBRARY_PATH "$(dirname "${libstdcxx_path}")"
        fi
    fi
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
    print_usage
    exit 0
fi

THIRDPARTY_ROOT="$(resolve_thirdparty_root)"

OPENSSL_LIB_ROOT=""
for lib_dir in "${THIRDPARTY_ROOT}/lib" "${THIRDPARTY_ROOT}/lib64"; do
    if [[ ! -d "${lib_dir}" ]]; then
        continue
    fi
    if compgen -G "${lib_dir}/libssl.*" >/dev/null && compgen -G "${lib_dir}/libcrypto.*" >/dev/null; then
        OPENSSL_LIB_ROOT="${lib_dir}"
        break
    fi
done

export STARROCKS_THIRDPARTY="${THIRDPARTY_ROOT}"

if [[ "${OS_NAME}" == "Linux" ]]; then
    detect_starrocks_gcc_home
    setup_starrocks_gcc_runtime
fi

use_thirdparty_openssl=0
if [[ -n "${OPENSSL_LIB_ROOT}" && -f "${THIRDPARTY_ROOT}/include/openssl/opensslv.h" ]]; then
    use_thirdparty_openssl=1
    export OPENSSL_DIR="${THIRDPARTY_ROOT}"
    export OPENSSL_INCLUDE_DIR="${THIRDPARTY_ROOT}/include"
    export OPENSSL_LIB_DIR="${OPENSSL_LIB_ROOT}"

    if [[ -d "${OPENSSL_LIB_ROOT}/pkgconfig" ]]; then
        append_path_var PKG_CONFIG_PATH "${OPENSSL_LIB_ROOT}/pkgconfig"
    fi

    append_path_var LIBRARY_PATH "${OPENSSL_LIB_ROOT}"
    if [[ "${OS_NAME}" == "Linux" ]]; then
        append_path_var LD_LIBRARY_PATH "${OPENSSL_LIB_ROOT}"
    elif [[ "${OS_NAME}" == "Darwin" ]]; then
        append_path_var DYLD_LIBRARY_PATH "${OPENSSL_LIB_ROOT}"
    fi
else
    if [[ "${OS_NAME}" == "Linux" ]]; then
        echo "Error: OpenSSL headers/libs not found under '${THIRDPARTY_ROOT}' (Linux requires thirdparty OpenSSL)" >&2
        exit 1
    fi
    unset_var_if_set OPENSSL_DIR
    unset_var_if_set OPENSSL_INCLUDE_DIR
    unset_var_if_set OPENSSL_LIB_DIR
fi

target="${CARGO_BUILD_TARGET:-$(rustc -vV | sed -n 's/^host: //p')}"
if [[ -n "${target}" ]]; then
    target_env_prefix="$(echo "${target}" | tr '[:lower:]-' '[:upper:]_')"
    if [[ "${use_thirdparty_openssl}" == "1" ]]; then
        export "${target_env_prefix}_OPENSSL_DIR=${OPENSSL_DIR}"
        export "${target_env_prefix}_OPENSSL_INCLUDE_DIR=${OPENSSL_INCLUDE_DIR}"
        export "${target_env_prefix}_OPENSSL_LIB_DIR=${OPENSSL_LIB_DIR}"
    else
        unset_var_if_set "${target_env_prefix}_OPENSSL_DIR"
        unset_var_if_set "${target_env_prefix}_OPENSSL_INCLUDE_DIR"
        unset_var_if_set "${target_env_prefix}_OPENSSL_LIB_DIR"
    fi
    if [[ -n "${CXX:-}" ]]; then
        cargo_linker_var="CARGO_TARGET_${target_env_prefix}_LINKER"
        export "${cargo_linker_var}=${CXX}"
    fi
fi

if [[ $# -eq 0 ]]; then
    set -- build
fi

echo "Using thirdparty root: ${THIRDPARTY_ROOT}"
if [[ "${use_thirdparty_openssl}" == "1" ]]; then
    echo "OPENSSL_DIR=${OPENSSL_DIR}"
    echo "OPENSSL_LIB_DIR=${OPENSSL_LIB_DIR}"
elif [[ "${OS_NAME}" == "Darwin" ]]; then
    echo "Using system OpenSSL on macOS"
fi
if [[ -n "${LIBSTDCXX_PATH:-}" ]]; then
    echo "LIBSTDCXX_PATH=${LIBSTDCXX_PATH}"
fi
if [[ -n "${cargo_linker_var:-}" ]]; then
    echo "${cargo_linker_var}=${!cargo_linker_var}"
fi

exec cargo "$@"
