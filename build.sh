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
  ./build.sh [--output <dir>] [--no-package] [cargo subcommand args...]

Examples:
  ./build.sh
  ./build.sh build --release
  ./build.sh --output ./output/novarocks build --release
  ./build.sh --no-package test
  ./build.sh test
  STARROCKS_THIRDPARTY=./thirdparty ./build.sh build

Description:
  This wrapper resolves thirdparty root from STARROCKS_THIRDPARTY (default: ./thirdparty)
  and exports OPENSSL_* / PKG_CONFIG_PATH so openssl-sys can use bundled thirdparty libs.
  When cargo subcommand is `build`, it also packages runtime files into output/novarocks by default.
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

append_path_if_dir() {
    local var_name="$1"
    local dir="$2"
    if [[ -d "${dir}" ]]; then
        append_path_var "${var_name}" "${dir}"
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

realpath_portable() {
    local path="$1"
    if command -v readlink >/dev/null 2>&1; then
        readlink -f "${path}" 2>/dev/null && return 0
    fi
    python3 - <<'PY' "${path}" 2>/dev/null
import os,sys
print(os.path.realpath(sys.argv[1]))
PY
}

copy_shared_lib() {
    local src="$1"
    local dst_dir="$2"
    if [[ ! -e "${src}" ]]; then
        return 0
    fi
    cp -a "${src}" "${dst_dir}/"
    if [[ -L "${src}" ]]; then
        local real
        real="$(realpath_portable "${src}" || true)"
        if [[ -n "${real}" && -f "${real}" ]]; then
            cp -a "${real}" "${dst_dir}/"
        fi
    fi
}

copy_non_system_linked_libs() {
    local binary="$1"
    local dst_dir="$2"
    if [[ ! -x "${binary}" ]]; then
        return 0
    fi

    if [[ "${OS_NAME}" == "Linux" ]] && command -v ldd >/dev/null 2>&1; then
        ldd "${binary}" 2>/dev/null | while IFS= read -r line; do
            local lib_path
            lib_path="$(echo "${line}" | awk '{for(i=1;i<=NF;i++){if($i=="=>"){print $(i+1); exit}}}')"
            if [[ -z "${lib_path}" || ! -f "${lib_path}" ]]; then
                continue
            fi
            case "${lib_path}" in
                /lib/*|/lib64/*|/usr/lib/*|/usr/lib64/*)
                    continue
                    ;;
            esac
            copy_shared_lib "${lib_path}" "${dst_dir}"
        done
    fi
}

resolve_build_artifact_path() {
    local profile="debug"
    local target_triple="${CARGO_BUILD_TARGET:-}"
    local args=("$@")
    local i=1
    while [[ ${i} -lt ${#args[@]} ]]; do
        local arg="${args[$i]}"
        case "${arg}" in
            --release)
                profile="release"
                ;;
            --profile)
                ((i++))
                profile="${args[$i]}"
                ;;
            --profile=*)
                profile="${arg#--profile=}"
                ;;
            --target)
                ((i++))
                target_triple="${args[$i]}"
                ;;
            --target=*)
                target_triple="${arg#--target=}"
                ;;
        esac
        ((i++))
    done

    local target_root="${ROOT_DIR}/target"
    if [[ -n "${target_triple}" ]]; then
        target_root="${target_root}/${target_triple}"
    fi
    echo "${target_root}/${profile}/novarocks"
}

generate_output_scripts() {
    local out_root="$1"

    cat > "${out_root}/bin/novarocksctl" <<'EOF'
#!/usr/bin/env sh
set -eu

SCRIPT_DIR=$(dirname "$0")
SCRIPT_DIR=$(cd "${SCRIPT_DIR}" && pwd)
ROOT_DIR=$(cd "${SCRIPT_DIR}/.." && pwd)
NOVAROCKS_BIN="${ROOT_DIR}/bin/novarocks"
CONFIG_PATH="${ROOT_DIR}/conf/novarocks.toml"
LOG_DIR="${ROOT_DIR}/log"
PID_FILE="${ROOT_DIR}/bin/novarocks.pid"
LOG_FILE="${LOG_DIR}/novarocks.out"

mkdir -p "${LOG_DIR}" "${ROOT_DIR}/bin"
if [ -n "${LD_LIBRARY_PATH:-}" ]; then
    LD_LIBRARY_PATH="${ROOT_DIR}/lib:${LD_LIBRARY_PATH}"
else
    LD_LIBRARY_PATH="${ROOT_DIR}/lib"
fi
export LD_LIBRARY_PATH

print_usage() {
    cat <<'USAGE'
Usage:
  ./bin/novarocksctl [start] [--daemon] [-- <extra args>...]
  ./bin/novarocksctl stop [--timeout <seconds>]
  ./bin/novarocksctl restart [--timeout <seconds>] [--daemon] [-- <extra args>...]

Examples:
  ./bin/novarocksctl --daemon
  ./bin/novarocksctl start
  ./bin/novarocksctl stop --timeout 30
  ./bin/novarocksctl restart --timeout 30 --daemon
USAGE
}

is_novarocks_process() {
    pid="$1"
    ps -p "${pid}" -o command= 2>/dev/null | grep -q "novarocks"
}

start_novarocks() {
    RUN_DAEMON=0

    while [ $# -gt 0 ]; do
        case "$1" in
            --daemon)
                RUN_DAEMON=1
                shift
                ;;
            --)
                shift
                break
                ;;
            -h|--help)
                print_usage
                exit 0
                ;;
            *)
                break
                ;;
        esac
    done

    if [ ! -x "${NOVAROCKS_BIN}" ]; then
        echo "Error: executable not found: ${NOVAROCKS_BIN}" >&2
        exit 1
    fi

    if [ -f "${PID_FILE}" ]; then
        oldpid=$(cat "${PID_FILE}")
        if is_novarocks_process "${oldpid}"; then
            echo "NovaRocks already running as pid ${oldpid}" >&2
            exit 1
        fi
        rm -f "${PID_FILE}"
    fi

    if [ "${RUN_DAEMON}" -eq 1 ]; then
        if [ $# -gt 0 ]; then
            nohup "${NOVAROCKS_BIN}" run --config "${CONFIG_PATH}" "$@" >> "${LOG_FILE}" 2>&1 < /dev/null &
        else
            nohup "${NOVAROCKS_BIN}" run --config "${CONFIG_PATH}" >> "${LOG_FILE}" 2>&1 < /dev/null &
        fi
        echo $! > "${PID_FILE}"
        echo "NovaRocks started in daemon mode, pid=$(cat "${PID_FILE}")"
    else
        if [ $# -gt 0 ]; then
            exec "${NOVAROCKS_BIN}" run --config "${CONFIG_PATH}" "$@"
        else
            exec "${NOVAROCKS_BIN}" run --config "${CONFIG_PATH}"
        fi
    fi
}

stop_novarocks() {
    STOP_TIMEOUT=30
    while [ $# -gt 0 ]; do
        case "$1" in
            --timeout)
                if [ $# -lt 2 ]; then
                    echo "Error: --timeout requires seconds" >&2
                    exit 1
                fi
                STOP_TIMEOUT="$2"
                shift 2
                ;;
            --)
                shift
                break
                ;;
            *)
                echo "Error: unknown stop option: $1" >&2
                print_usage
                exit 1
                ;;
        esac
    done

    if [ ! -f "${PID_FILE}" ]; then
        echo "NovaRocks pid file not found: ${PID_FILE}"
        exit 0
    fi

    pid=$(cat "${PID_FILE}")
    if ! kill -0 "${pid}" >/dev/null 2>&1; then
        echo "Process ${pid} not found, removing stale pid file"
        rm -f "${PID_FILE}"
        exit 0
    fi

    if ! is_novarocks_process "${pid}"; then
        echo "Refuse to stop pid ${pid}: command is not novarocks"
        exit 1
    fi

    kill -15 "${pid}" >/dev/null 2>&1 || true
    start_ts=$(date +%s)
    while kill -0 "${pid}" >/dev/null 2>&1; do
        if [ "${STOP_TIMEOUT}" -gt 0 ] && [ $(( $(date +%s) - start_ts )) -gt "${STOP_TIMEOUT}" ]; then
            kill -9 "${pid}" >/dev/null 2>&1 || true
            echo "Graceful stop timeout, process ${pid} killed with SIGKILL"
            break
        fi
        sleep 1
    done

    rm -f "${PID_FILE}"
    echo "NovaRocks stopped"
}

restart_novarocks() {
    STOP_TIMEOUT=30
    while [ $# -gt 0 ]; do
        case "$1" in
            --timeout)
                if [ $# -lt 2 ]; then
                    echo "Error: --timeout requires seconds" >&2
                    exit 1
                fi
                STOP_TIMEOUT="$2"
                shift 2
                ;;
            --)
                shift
                break
                ;;
            *)
                break
                ;;
        esac
    done

    stop_novarocks --timeout "${STOP_TIMEOUT}"
    start_novarocks "$@"
}

if [ $# -eq 0 ]; then
    start_novarocks
    exit 0
fi

cmd="$1"
case "${cmd}" in
    -h|--help|help)
        print_usage
        ;;
    start)
        shift
        start_novarocks "$@"
        ;;
    stop)
        shift
        stop_novarocks "$@"
        ;;
    restart)
        shift
        restart_novarocks "$@"
        ;;
    *)
        start_novarocks "$@"
        ;;
esac
EOF
    chmod +x "${out_root}/bin/novarocksctl"

    cat > "${out_root}/README.md" <<'EOF'
# NovaRocks Runtime Package

This directory is generated by `build.sh` in a StarRocks-like runtime layout:

- `bin/`: executable and control script
- `conf/`: runtime configs
- `lib/`: runtime libraries
- `log/`: runtime logs

PID file: `bin/novarocks.pid`

## Start

```bash
cd output/novarocks
./bin/novarocksctl start
```

Daemon mode:

```bash
./bin/novarocksctl start --daemon
```

Stop:

```bash
./bin/novarocksctl stop
```

Restart:

```bash
./bin/novarocksctl restart
```
EOF
}

package_output() {
    local binary_path="$1"
    local out_root="$2"
    local saved_config=""
    local has_saved_config=0

    if [[ ! -x "${binary_path}" ]]; then
        echo "Error: build artifact not found: ${binary_path}" >&2
        exit 1
    fi

    if [[ -f "${out_root}/conf/novarocks.toml" ]]; then
        saved_config="$(mktemp "${TMPDIR:-/tmp}/novarocks.toml.XXXXXX")"
        cp -a "${out_root}/conf/novarocks.toml" "${saved_config}"
        has_saved_config=1
    fi

    echo "Packaging output directory: ${out_root}"
    rm -rf "${out_root}"
    mkdir -p "${out_root}/"{bin,conf,lib,log}

    cp -a "${binary_path}" "${out_root}/bin/novarocks"
    chmod +x "${out_root}/bin/novarocks"

    if [[ -f "${ROOT_DIR}/novarocks.toml.example" ]]; then
        cp -a "${ROOT_DIR}/novarocks.toml.example" "${out_root}/conf/novarocks.toml.example"
    else
        echo "Error: novarocks.toml.example not found" >&2
        exit 1
    fi

    if [[ ${has_saved_config} -eq 1 ]]; then
        cp -a "${saved_config}" "${out_root}/conf/novarocks.toml"
        rm -f "${saved_config}"
    fi

    generate_output_scripts "${out_root}"

    # Bundle runtime libs resolved from current binary linkage.
    copy_non_system_linked_libs "${binary_path}" "${out_root}/lib"

    : > "${out_root}/log/.gitkeep"
}

NOVAROCKS_OUTPUT="${NOVAROCKS_OUTPUT:-${ROOT_DIR}/output/novarocks}"
AUTO_PACKAGE=1
declare -a CARGO_ARGS=()
while [[ $# -gt 0 ]]; do
    case "$1" in
        -h|--help)
            print_usage
            exit 0
            ;;
        --output)
            if [[ $# -lt 2 ]]; then
                echo "Error: --output requires a directory path" >&2
                exit 1
            fi
            NOVAROCKS_OUTPUT="$2"
            shift 2
            ;;
        --no-package)
            AUTO_PACKAGE=0
            shift
            ;;
        *)
            CARGO_ARGS+=("$1")
            shift
            ;;
    esac
done

if [[ ${#CARGO_ARGS[@]} -eq 0 ]]; then
    CARGO_ARGS=(build)
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

append_path_if_dir LIBRARY_PATH "${THIRDPARTY_ROOT}/lib"
append_path_if_dir LIBRARY_PATH "${THIRDPARTY_ROOT}/lib64"
if [[ "${OS_NAME}" == "Linux" ]]; then
    append_path_if_dir LD_LIBRARY_PATH "${THIRDPARTY_ROOT}/lib"
    append_path_if_dir LD_LIBRARY_PATH "${THIRDPARTY_ROOT}/lib64"
elif [[ "${OS_NAME}" == "Darwin" ]]; then
    append_path_if_dir DYLD_LIBRARY_PATH "${THIRDPARTY_ROOT}/lib"
    append_path_if_dir DYLD_LIBRARY_PATH "${THIRDPARTY_ROOT}/lib64"
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

cargo "${CARGO_ARGS[@]}"

if [[ "${CARGO_ARGS[0]}" == "build" && "${AUTO_PACKAGE}" == "1" ]]; then
    artifact_path="$(resolve_build_artifact_path "${CARGO_ARGS[@]}")"
    package_output "${artifact_path}" "${NOVAROCKS_OUTPUT}"
    echo "Packaged runtime output: ${NOVAROCKS_OUTPUT}"
fi
