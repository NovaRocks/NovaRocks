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
  ./build.sh [--output <dir>] [--no-package] [build args...]

Examples:
  ./build.sh
  ./build.sh --release
  ./build.sh --output ./output/novarocks --release
  ./build.sh --no-package --release
  STARROCKS_THIRDPARTY=./thirdparty ./build.sh --release

Description:
  This wrapper resolves thirdparty root from STARROCKS_THIRDPARTY (default: ./thirdparty)
  and exports OPENSSL_* / PKG_CONFIG_PATH so openssl-sys can use bundled thirdparty libs.
  It only runs `cargo build` and packages runtime files into output/novarocks by default.
EOF
}

resolve_thirdparty_install_root() {
    local root="$1"
    local installed="${root}/installed"
    if [[ -d "${installed}/include" && ( -d "${installed}/lib" || -d "${installed}/lib64" ) ]]; then
        echo "${installed}"
        return 0
    fi
    echo "Error: thirdparty artifacts not found under '${root}'. Expected '<thirdparty-root>/installed/{include,lib|lib64}'. Set STARROCKS_THIRDPARTY to thirdparty root." >&2
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
        if [[ -d "${candidate}" ]]; then
            echo "${candidate}"
            return 0
        fi
        echo "Error: invalid STARROCKS_THIRDPARTY='${configured}', expected thirdparty root directory" >&2
        return 1
    fi

    local fallback="${ROOT_DIR}/thirdparty"
    if [[ -d "${fallback}" ]]; then
        echo "${fallback}"
        return 0
    fi

    echo "Error: thirdparty directory not found, expected '${fallback}', or set STARROCKS_THIRDPARTY" >&2
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

setup_starrocks_gcc_runtime() {
    local gcc_home="${STARROCKS_GCC_HOME:-}"
    if [[ -z "${gcc_home}" ]]; then
        echo "Error: STARROCKS_GCC_HOME is required on Linux, e.g. /opt/rh/devtoolset-12/root/usr" >&2
        exit 1
    fi

    local gcc_bin="${gcc_home}/bin"
    local gcc_cmd="${gcc_bin}/gcc"
    local gxx_cmd="${gcc_bin}/g++"
    if [[ ! -x "${gcc_cmd}" || ! -x "${gxx_cmd}" ]]; then
        echo "Error: STARROCKS_GCC_HOME is set but compiler not found under '${gcc_bin}'" >&2
        exit 1
    fi

    local previous_cc="${CC:-}"
    local previous_cxx="${CXX:-}"
    export CC="${gcc_cmd}"
    export CXX="${gxx_cmd}"
    append_path_var PATH "${gcc_bin}"
    if [[ -n "${previous_cc}" && "${previous_cc}" != "${CC}" ]]; then
        echo "Info: override CC='${previous_cc}' with STARROCKS_GCC_HOME compiler '${CC}'"
    fi
    if [[ -n "${previous_cxx}" && "${previous_cxx}" != "${CXX}" ]]; then
        echo "Info: override CXX='${previous_cxx}' with STARROCKS_GCC_HOME compiler '${CXX}'"
    fi

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

print_linux_static_cxx_runtime_info() {
    local cxx_cmd="${CXX:-g++}"
    if [[ ! -x "${cxx_cmd}" ]] && ! command -v "${cxx_cmd}" >/dev/null 2>&1; then
        return 0
    fi

    local libstdcpp_static
    libstdcpp_static="$("${cxx_cmd}" -print-file-name=libstdc++.a 2>/dev/null || true)"
    if [[ -n "${libstdcpp_static}" && "${libstdcpp_static}" != "libstdc++.a" && -f "${libstdcpp_static}" ]]; then
        echo "info: using static libstdc++ archive: ${libstdcpp_static}"
        echo "info: linking C++ runtime statically (-static-libstdc++ -static-libgcc)"
    fi

    local libsupcxx_static
    libsupcxx_static="$("${cxx_cmd}" -print-file-name=libsupc++.a 2>/dev/null || true)"
    if [[ -n "${libsupcxx_static}" && "${libsupcxx_static}" != "libsupc++.a" && -f "${libsupcxx_static}" ]]; then
        echo "info: using static libsupc++ archive: ${libsupcxx_static}"
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
            local lib_name
            lib_name="$(basename "${lib_path}")"
            case "${lib_name}" in
                # libgcc is linked statically for Linux builds; do not bundle toolchain libgcc_s into output.
                libgcc_s.so.1|libgcc_s-*.so.*)
                    continue
                    ;;
            esac
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
    local i=0
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
    local ctl_src="${ROOT_DIR}/bin/novarocksctl"
    if [[ ! -f "${ctl_src}" ]]; then
        echo "Error: novarocksctl script not found: ${ctl_src}" >&2
        exit 1
    fi
    cp -a "${ctl_src}" "${out_root}/bin/novarocksctl"
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
declare -a BUILD_ARGS=()
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
            BUILD_ARGS+=("$1")
            shift
            ;;
    esac
done

if [[ ${#BUILD_ARGS[@]} -gt 0 ]]; then
    if [[ "${BUILD_ARGS[0]}" == "build" ]]; then
        if [[ ${#BUILD_ARGS[@]} -gt 1 ]]; then
            BUILD_ARGS=("${BUILD_ARGS[@]:1}")
        else
            BUILD_ARGS=()
        fi
    elif [[ "${BUILD_ARGS[0]}" != -* ]]; then
        echo "Error: build.sh only supports cargo build, got subcommand '${BUILD_ARGS[0]}'" >&2
        echo "Use 'cargo ${BUILD_ARGS[*]}' directly, and use './bin/novarocksctl' for runtime control." >&2
        exit 1
    fi
fi

THIRDPARTY_ROOT="$(resolve_thirdparty_root)"
THIRDPARTY_INSTALL_ROOT="$(resolve_thirdparty_install_root "${THIRDPARTY_ROOT}")"

OPENSSL_LIB_ROOT=""
for lib_dir in "${THIRDPARTY_INSTALL_ROOT}/lib" "${THIRDPARTY_INSTALL_ROOT}/lib64"; do
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
    setup_starrocks_gcc_runtime
    print_linux_static_cxx_runtime_info
fi

append_path_if_dir LIBRARY_PATH "${THIRDPARTY_INSTALL_ROOT}/lib"
append_path_if_dir LIBRARY_PATH "${THIRDPARTY_INSTALL_ROOT}/lib64"
if [[ "${OS_NAME}" == "Linux" ]]; then
    append_path_if_dir LD_LIBRARY_PATH "${THIRDPARTY_INSTALL_ROOT}/lib"
    append_path_if_dir LD_LIBRARY_PATH "${THIRDPARTY_INSTALL_ROOT}/lib64"
elif [[ "${OS_NAME}" == "Darwin" ]]; then
    append_path_if_dir DYLD_LIBRARY_PATH "${THIRDPARTY_INSTALL_ROOT}/lib"
    append_path_if_dir DYLD_LIBRARY_PATH "${THIRDPARTY_INSTALL_ROOT}/lib64"
fi

use_thirdparty_openssl=0
if [[ -n "${OPENSSL_LIB_ROOT}" && -f "${THIRDPARTY_INSTALL_ROOT}/include/openssl/opensslv.h" ]]; then
    use_thirdparty_openssl=1
    export OPENSSL_DIR="${THIRDPARTY_INSTALL_ROOT}"
    export OPENSSL_INCLUDE_DIR="${THIRDPARTY_INSTALL_ROOT}/include"
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
        echo "Error: OpenSSL headers/libs not found under '${THIRDPARTY_INSTALL_ROOT}' (Linux requires thirdparty OpenSSL)" >&2
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
echo "Using thirdparty install root: ${THIRDPARTY_INSTALL_ROOT}"
if [[ "${use_thirdparty_openssl}" == "1" ]]; then
    echo "OPENSSL_DIR=${OPENSSL_DIR}"
    echo "OPENSSL_LIB_DIR=${OPENSSL_LIB_DIR}"
elif [[ "${OS_NAME}" == "Darwin" ]]; then
    echo "Using system OpenSSL on macOS"
fi
if [[ -n "${LIBSTDCXX_PATH:-}" ]]; then
    echo "LIBSTDCXX_PATH=${LIBSTDCXX_PATH}"
fi
if [[ -n "${CC:-}" ]]; then
    echo "CC=${CC}"
fi
if [[ -n "${CXX:-}" ]]; then
    echo "CXX=${CXX}"
fi
if [[ -n "${cargo_linker_var:-}" ]]; then
    echo "${cargo_linker_var}=${!cargo_linker_var}"
fi

if [[ ${#BUILD_ARGS[@]} -gt 0 ]]; then
    cargo build "${BUILD_ARGS[@]}"
else
    cargo build
fi

if [[ "${AUTO_PACKAGE}" == "1" ]]; then
    if [[ ${#BUILD_ARGS[@]} -gt 0 ]]; then
        artifact_path="$(resolve_build_artifact_path "${BUILD_ARGS[@]}")"
    else
        artifact_path="$(resolve_build_artifact_path)"
    fi
    package_output "${artifact_path}" "${NOVAROCKS_OUTPUT}"
    echo "Packaged runtime output: ${NOVAROCKS_OUTPUT}"
fi
