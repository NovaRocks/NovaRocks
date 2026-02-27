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

#################################################################################
# This script will
# 1. Check prerequisite libraries. Including:
#    cmake byacc flex automake libtool binutils-dev libiberty-dev bison
# 2. Compile and install all thirdparties which are downloaded
#    using *download-thirdparty.sh*.
#
# This script will run *download-thirdparty.sh* once again
# to check if all thirdparties have been downloaded, unpacked and patched.
#################################################################################
set -eo pipefail

curdir=`dirname "$0"`
curdir=`cd "$curdir"; pwd`
OS_NAME="$(uname -s)"

export STARUST_HOME=${STARUST_HOME:-$curdir/..}
export TP_DIR=$curdir

if [ ! -f ${TP_DIR}/vars.sh ]; then
    echo "vars.sh is missing".
    exit 1
fi
. ${TP_DIR}/vars.sh

# Setup macOS environment (similar to StarRocks build-mac/env_macos.sh)
if [[ $(uname) == "Darwin" ]]; then
    export HOMEBREW_PREFIX="${HOMEBREW_PREFIX:-/opt/homebrew}"
    
    # Add Homebrew to PATH if not already there
    if [[ ":$PATH:" != *":$HOMEBREW_PREFIX/bin:"* ]]; then
        export PATH="$HOMEBREW_PREFIX/bin:$HOMEBREW_PREFIX/sbin:$PATH"
    fi
    
    # Use Homebrew LLVM for consistent C++17 support (if available)
    if [[ -d "$HOMEBREW_PREFIX/opt/llvm" ]]; then
        export CC="$HOMEBREW_PREFIX/opt/llvm/bin/clang"
        export CXX="$HOMEBREW_PREFIX/opt/llvm/bin/clang++"
        export AR="$HOMEBREW_PREFIX/opt/llvm/bin/llvm-ar"
        export RANLIB="$HOMEBREW_PREFIX/opt/llvm/bin/llvm-ranlib"
    fi
    
    # Set OpenSSL path
    if [[ -d "$HOMEBREW_PREFIX/opt/openssl@3" ]]; then
        export OPENSSL_ROOT_DIR="$HOMEBREW_PREFIX/opt/openssl@3"
        export PKG_CONFIG_PATH="$OPENSSL_ROOT_DIR/lib/pkgconfig:${PKG_CONFIG_PATH:-}"
        export CPPFLAGS="-I$OPENSSL_ROOT_DIR/include ${CPPFLAGS:-}"
        export LDFLAGS="-L$OPENSSL_ROOT_DIR/lib ${LDFLAGS:-}"
    elif [[ -d "$HOMEBREW_PREFIX/opt/openssl" ]]; then
        export OPENSSL_ROOT_DIR="$HOMEBREW_PREFIX/opt/openssl"
        export PKG_CONFIG_PATH="$OPENSSL_ROOT_DIR/lib/pkgconfig:${PKG_CONFIG_PATH:-}"
        export CPPFLAGS="-I$OPENSSL_ROOT_DIR/include ${CPPFLAGS:-}"
        export LDFLAGS="-L$OPENSSL_ROOT_DIR/lib ${LDFLAGS:-}"
    fi
    
    # Set compiler flags for ARM64
    if [[ $(uname -m) == "arm64" ]]; then
        export CFLAGS="${CFLAGS:-} -march=armv8-a -O3"
        export CXXFLAGS="${CXXFLAGS:-} -march=armv8-a -O3 -std=c++17 -stdlib=libc++"
    fi
    
    # Add Homebrew bison to PATH (required for protobuf/thrift)
    if [[ -d "$HOMEBREW_PREFIX/opt/bison" ]]; then
        export PATH="$HOMEBREW_PREFIX/opt/bison/bin:$PATH"
    fi
    
    # Add Homebrew gnu-getopt to PATH (required for brpc on macOS)
    if [[ -d "$HOMEBREW_PREFIX/opt/gnu-getopt" ]]; then
        export PATH="$HOMEBREW_PREFIX/opt/gnu-getopt/bin:$PATH"
    fi
fi

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

setup_starrocks_gcc_compiler() {
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
    echo "Using STARROCKS_GCC_HOME=${STARROCKS_GCC_HOME}"
}

# Check args
usage() {
    echo "
Usage: $0 [options...]

  Description:
    Build thirdparty dependencies for Starust.

  Optional options:
    -j<num>                Build with <num> parallel jobs (can also use -j <num>)
    --clean                Clean extracted source before building
    -h, --help             Show this help message

  Examples:
    # Build all packages with default parallelism
    $0

    # Build all packages with 8 parallel jobs
    $0 -j8

    # Clean and rebuild everything with 16 parallel jobs
    $0 --clean -j16
  "
    exit 1
}

CLEAN=0
PARALLEL=${PARALLEL:-$default_parallel}

while [[ $# -gt 0 ]]; do
    case $1 in
        -j*)
            PARALLEL="${1#-j}"
            shift
            ;;
        -j)
            PARALLEL="$2"
            shift 2
            ;;
        --clean)
            CLEAN=1
            shift
            ;;
        -h|--help)
            usage
            ;;
        *)
            echo "Unknown option: $1"
            usage
            ;;
    esac
done

echo "Get params:
    PARALLEL            -- ${PARALLEL}
    CLEAN               -- ${CLEAN}
"

cd $TP_DIR

if [[ "${OS_NAME}" == "Linux" ]]; then
    setup_starrocks_gcc_compiler
fi

if [[ "${CLEAN}" -eq 1 ]]; then
    echo "Cleaning source directories..."
    for TP_ARCH in ${TP_ARCHIVES[*]}
    do
        SOURCE=$TP_ARCH"_SOURCE"
        if [ -n "${!SOURCE}" ] && [ -d "$TP_SOURCE_DIR/${!SOURCE}" ]; then
            echo "Removing $TP_SOURCE_DIR/${!SOURCE}"
            rm -rf "$TP_SOURCE_DIR/${!SOURCE}"
        fi
    done
fi

# Download thirdparties.
${TP_DIR}/download-thirdparty.sh

# prepare installed prefix
mkdir -p ${TP_INSTALL_DIR}

check_prerequest() {
    local CMD=$1
    local NAME=$2
    if ! $CMD >/dev/null 2>&1; then
        echo "$NAME is missing"
        exit 1
    else
        echo "$NAME is found"
    fi
}

# Find cmake command (after PATH setup)
CMAKE_CMD=${CMAKE_CMD:-cmake}
if ! command -v "$CMAKE_CMD" >/dev/null 2>&1; then
    # Try to find cmake in common locations
    HOMEBREW_PREFIX="${HOMEBREW_PREFIX:-/opt/homebrew}"
    if [[ -f "$HOMEBREW_PREFIX/bin/cmake" ]]; then
        CMAKE_CMD="$HOMEBREW_PREFIX/bin/cmake"
    elif [[ -f "/opt/homebrew/bin/cmake" ]]; then
        CMAKE_CMD="/opt/homebrew/bin/cmake"
    elif [[ -f "/usr/local/bin/cmake" ]]; then
        CMAKE_CMD="/usr/local/bin/cmake"
    else
        echo "Error: cmake not found. Please install cmake with: brew install cmake"
        exit 1
    fi
    # Add to PATH for subsequent commands
    export PATH="$(dirname "$CMAKE_CMD"):$PATH"
fi

# Ensure CMAKE_CMD is absolute path
if [[ "$CMAKE_CMD" != /* ]]; then
    CMAKE_CMD="$(command -v "$CMAKE_CMD" || echo "$CMAKE_CMD")"
    if [[ "$CMAKE_CMD" != /* ]]; then
        # Still relative, try to resolve
        CMAKE_CMD="$(cd "$(dirname "$CMAKE_CMD")" && pwd)/$(basename "$CMAKE_CMD")"
    fi
fi

# Check prerequisites
check_prerequest "${CMAKE_CMD} --version" "cmake"
check_prerequest "make --version" "make"

BUILD_SYSTEM=${BUILD_SYSTEM:-make}
BUILD_DIR=novarocks_build
MACHINE_TYPE=$(uname -m)

# handle mac m1 platform, change arm64 to aarch64
if [[ "${MACHINE_TYPE}" == "arm64" ]]; then
    MACHINE_TYPE="aarch64"
fi

echo "machine type : $MACHINE_TYPE"

check_if_source_exist() {
    if [ -z $1 ]; then
        echo "dir should specified to check if exist."
        exit 1
    fi

    if [ ! -d $TP_SOURCE_DIR/$1 ];then
        echo "$TP_SOURCE_DIR/$1 does not exist."
        exit 1
    fi
    echo "===== begin build $1"
}

build_zlib() {
    local HOMEBREW_PREFIX="${HOMEBREW_PREFIX:-/opt/homebrew}"

    if [[ $(uname) == "Darwin" ]]; then
        local zlib_prefix=""
        if command -v brew >/dev/null 2>&1; then
            zlib_prefix=$(brew --prefix zlib 2>/dev/null || echo "")
        fi
        if [[ -z "$zlib_prefix" ]]; then
            zlib_prefix="$HOMEBREW_PREFIX/opt/zlib"
        fi

        if [[ ! -d "$zlib_prefix" ]]; then
            echo "Error: Homebrew zlib not found at $zlib_prefix"
            echo "Please install with: brew install zlib"
            exit 1
        fi

        local zlib_lib="$zlib_prefix/lib/libz.a"
        if [[ ! -f "$zlib_lib" ]]; then
            echo "Error: zlib static library not found at $zlib_lib"
            echo "Please install zlib with: brew install zlib"
            exit 1
        fi

        echo "Using Homebrew zlib on macOS: $zlib_prefix"
        mkdir -p $TP_INSTALL_DIR/{lib,include}
        cp "$zlib_lib" "$TP_INSTALL_DIR/lib/" || {
            echo "Error: Failed to copy libz.a from $zlib_lib"
            exit 1
        }
        local zlib_header="$zlib_prefix/include/zlib.h"
        if [[ -f "$zlib_header" ]]; then
            cp "$zlib_header" "$TP_INSTALL_DIR/include/" || {
                echo "Error: Failed to copy zlib.h from $zlib_header"
                exit 1
            }
        else
            echo "Error: zlib.h not found at $zlib_header"
            exit 1
        fi
        if [[ -f "$zlib_prefix/include/zconf.h" ]]; then
            cp "$zlib_prefix/include/zconf.h" "$TP_INSTALL_DIR/include/" 2>/dev/null || true
        fi
        return 0
    else
        check_if_source_exist $ZLIB_SOURCE
        cd $TP_SOURCE_DIR/$ZLIB_SOURCE

        make distclean >/dev/null 2>&1 || true
        CFLAGS="${CFLAGS:-} -fPIC" ./configure --prefix=$TP_INSTALL_DIR --static
        make -j$PARALLEL
        make install
        return 0
    fi
}

build_openssl() {
    local HOMEBREW_PREFIX="${HOMEBREW_PREFIX:-/opt/homebrew}"
    
    if [[ $(uname) == "Darwin" ]]; then
        local openssl_dirs=(
            "$HOMEBREW_PREFIX/opt/openssl@3"
            "$HOMEBREW_PREFIX/opt/openssl@1.1"
            "$HOMEBREW_PREFIX/opt/openssl"
        )
        
        local found_openssl=""
        for openssl_dir in "${openssl_dirs[@]}"; do
            if [[ -d "$openssl_dir" && -f "$openssl_dir/lib/libssl.a" && -f "$openssl_dir/lib/libcrypto.a" ]]; then
                found_openssl="$openssl_dir"
                break
            fi
        done
        
        if [[ -z "$found_openssl" ]]; then
            echo "Error: Homebrew openssl not found."
            echo "Please install with one of:"
            echo "  brew install openssl@3    (recommended)"
            echo "  brew install openssl@1.1"
            echo "  brew install openssl"
            exit 1
        fi
        
        echo "Using Homebrew openssl on macOS: $found_openssl"
        mkdir -p $TP_INSTALL_DIR/{lib,bin,include}
        rm -f "$TP_INSTALL_DIR/lib/libssl.a" "$TP_INSTALL_DIR/lib/libcrypto.a" 2>/dev/null || true
        cp "$found_openssl/lib/libssl.a" "$TP_INSTALL_DIR/lib/" || {
            echo "Error: Failed to copy libssl.a"
            exit 1
        }
        cp "$found_openssl/lib/libcrypto.a" "$TP_INSTALL_DIR/lib/" || {
            echo "Error: Failed to copy libcrypto.a"
            exit 1
        }
        if [[ -d "$found_openssl/include/openssl" ]]; then
            cp -r "$found_openssl/include/openssl" "$TP_INSTALL_DIR/include/" || {
                echo "Error: Failed to copy openssl headers"
                exit 1
            }
        else
            echo "Error: openssl headers not found at $found_openssl/include/openssl"
            exit 1
        fi
        if [[ -f "$found_openssl/bin/openssl" ]]; then
            cp "$found_openssl/bin/openssl" "$TP_INSTALL_DIR/bin/" 2>/dev/null || true
        fi
        return 0
    else
        local ssl_lib_paths=(
            "/usr/lib/x86_64-linux-gnu/libssl.a"
            "/usr/lib64/libssl.a"
            "/usr/lib/libssl.a"
        )
        local crypto_lib_paths=(
            "/usr/lib/x86_64-linux-gnu/libcrypto.a"
            "/usr/lib64/libcrypto.a"
            "/usr/lib/libcrypto.a"
        )
        
        local found_ssl=""
        local found_crypto=""
        
        for path in "${ssl_lib_paths[@]}"; do
            if [[ -f "$path" ]]; then
                found_ssl="$path"
                break
            fi
        done
        
        for path in "${crypto_lib_paths[@]}"; do
            if [[ -f "$path" ]]; then
                found_crypto="$path"
                break
            fi
        done
        
        if [[ -z "$found_ssl" ]] || [[ -z "$found_crypto" ]]; then
            echo "System openssl static libraries not found on Linux, fallback to source build"
            local OPENSSL_PLATFORM="linux-x86_64"
            if [[ "${MACHINE_TYPE}" == "aarch64" ]]; then
                OPENSSL_PLATFORM="linux-aarch64"
            fi
            check_if_source_exist $OPENSSL_SOURCE
            cd $TP_SOURCE_DIR/$OPENSSL_SOURCE

            local old_cflags="${CFLAGS:-}"
            local old_cxxflags="${CXXFLAGS:-}"
            local old_cppflags="${CPPFLAGS:-}"

            unset CXXFLAGS
            unset CPPFLAGS
            export CFLAGS="-O3 -fno-omit-frame-pointer -fPIC"

            make clean >/dev/null 2>&1 || true
            LDFLAGS="-L${TP_LIB_DIR}" LIBDIR="lib" ./Configure --prefix=$TP_INSTALL_DIR -lz -no-shared ${OPENSSL_PLATFORM} --libdir=lib
            make -j$PARALLEL
            make install_sw

            export CFLAGS="$old_cflags"
            export CXXFLAGS="$old_cxxflags"
            export CPPFLAGS="$old_cppflags"
            return 0
        fi
        
        echo "Using system openssl on Linux"
        mkdir -p $TP_INSTALL_DIR/{lib,include}
        cp "$found_ssl" "$TP_INSTALL_DIR/lib/libssl.a" || {
            echo "Error: Failed to copy libssl.a"
            exit 1
        }
        cp "$found_crypto" "$TP_INSTALL_DIR/lib/libcrypto.a" || {
            echo "Error: Failed to copy libcrypto.a"
            exit 1
        }
        if [[ -d "/usr/include/openssl" ]]; then
            cp -r "/usr/include/openssl" "$TP_INSTALL_DIR/include/" || {
                echo "Error: Failed to copy openssl headers"
                exit 1
            }
        else
            echo "Error: openssl headers not found at /usr/include/openssl"
            exit 1
        fi
        return 0
    fi
}

# gflags (dependency of glog)
build_gflags() {
    check_if_source_exist $GFLAGS_SOURCE

    cd $TP_SOURCE_DIR/$GFLAGS_SOURCE
    mkdir -p $BUILD_DIR
    cd $BUILD_DIR
    rm -rf CMakeCache.txt CMakeFiles/
    $CMAKE_CMD -G "${CMAKE_GENERATOR:-Unix Makefiles}" \
        -DCMAKE_INSTALL_PREFIX=$TP_INSTALL_DIR \
        -DBUILD_SHARED_LIBS=OFF \
        -DBUILD_STATIC_LIBS=ON \
        -DINSTALL_SHARED_LIBS=OFF \
        -DINSTALL_STATIC_LIBS=ON \
        -DGFLAGS_BUILD_SHARED_LIBS=OFF \
        -DGFLAGS_BUILD_STATIC_LIBS=ON \
        -DGFLAGS_INSTALL_SHARED_LIBS=OFF \
        -DGFLAGS_INSTALL_STATIC_LIBS=ON \
        -DCMAKE_POSITION_INDEPENDENT_CODE=ON \
        -DCMAKE_POLICY_VERSION_MINIMUM=3.5 \
        ../
    ${BUILD_SYSTEM} -j$PARALLEL
    ${BUILD_SYSTEM} install
}

# glog
build_glog() {
    check_if_source_exist $GLOG_SOURCE
    cd $TP_SOURCE_DIR/$GLOG_SOURCE

    # Apply glog namespace fix patch if available
    if [[ -f "$TP_DIR/patches/glog-0.7.1-namespace-fix.patch" ]]; then
        if ! patch -p1 -N -s --dry-run < "$TP_DIR/patches/glog-0.7.1-namespace-fix.patch" 2>/dev/null | grep -q "previously applied"; then
            echo "Applying glog namespace fix patch..."
            patch -p1 < "$TP_DIR/patches/glog-0.7.1-namespace-fix.patch" || {
                echo "Warning: Failed to apply glog namespace fix patch, continuing anyway..."
            }
        fi
    fi

    mkdir -p $BUILD_DIR
    rm -rf $BUILD_DIR/CMakeCache.txt $BUILD_DIR/CMakeFiles/
    $CMAKE_CMD -G "${CMAKE_GENERATOR:-Unix Makefiles}" \
        -DCMAKE_INSTALL_PREFIX=$TP_INSTALL_DIR \
        -DBUILD_SHARED_LIBS=OFF \
        -DBUILD_TESTING=OFF \
        -DWITH_GTEST=OFF \
        -DCMAKE_POSITION_INDEPENDENT_CODE=ON \
        -DCMAKE_INSTALL_LIBDIR=lib \
        -DCMAKE_POLICY_VERSION_MINIMUM=3.5 \
        -DWITH_GFLAGS=ON \
        -Dgflags_DIR=$TP_INSTALL_DIR/lib/cmake/gflags \
        -S . \
        -B $BUILD_DIR

    cd $BUILD_DIR
    ${BUILD_SYSTEM} -j$PARALLEL
    ${BUILD_SYSTEM} install
}

# Generate hash_memory shim for macOS libc++ compatibility (protobuf 3.14.0)
generate_hash_memory_shim() {
    local src_root="$TP_SOURCE_DIR/$PROTOBUF_SOURCE"
    local shim_dir="$src_root/src/google/protobuf/internal"
    local shim_file="$shim_dir/hash_memory_impl.cc"
    
    # Ensure target directory exists
    mkdir -p "$shim_dir"
    
    # Write shim (overwrite if file already exists)
    cat > "$shim_file" <<'EOF'
#ifndef _LIBCPP_HAS_NO_HASH_MEMORY
#define _LIBCPP_HAS_NO_HASH_MEMORY 1
#endif
#include <cstddef>
namespace std { namespace __1 {
[[gnu::pure]] size_t
__hash_memory(const void* __ptr, size_t __size) noexcept
{
    // Compatible with old libc++ implementation, protobuf only needs the symbol to exist
    size_t h = 0;
    const unsigned char* p = static_cast<const unsigned char*>(__ptr);
    for (size_t i = 0; i < __size; ++i)
        h = h * 31 + p[i];
    return h;
}
} }
EOF
    
    echo "Created hash_memory shim at $shim_file"
}

# protobuf
build_protobuf() {
    check_if_source_exist $PROTOBUF_SOURCE
    cd $TP_SOURCE_DIR/$PROTOBUF_SOURCE
    
    # On macOS, ensure we use Homebrew's bison if available (protobuf needs bison >= 2.5)
    if [[ $(uname) == "Darwin" ]]; then
        HOMEBREW_PREFIX="${HOMEBREW_PREFIX:-/opt/homebrew}"
        if [[ -f "$HOMEBREW_PREFIX/opt/bison/bin/bison" ]]; then
            export PATH="$HOMEBREW_PREFIX/opt/bison/bin:$PATH"
            echo "Using Homebrew bison: $(which bison)"
        else
            echo "Warning: Homebrew bison not found. System bison may be too old."
            echo "  Install with: brew install bison"
            echo "  System bison version: $(bison --version | head -1)"
        fi
    fi
    
    # protobuf 3.14.0 uses autotools, not CMake
    if [[ ! -f configure ]]; then
        ./autogen.sh
    fi
    
    # Generate hash_memory shim for macOS libc++ compatibility
    if [[ $(uname) == "Darwin" ]]; then
        generate_hash_memory_shim
    fi
    
    # Compile the hash memory shim separately and add it to the build
    local shim_obj=""
    if [[ $(uname) == "Darwin" ]]; then
        local shim_src="$TP_SOURCE_DIR/$PROTOBUF_SOURCE/src/google/protobuf/internal/hash_memory_impl.cc"
        shim_obj="$TP_SOURCE_DIR/$PROTOBUF_SOURCE/hash_memory_shim.o"
        if [[ -f "$shim_src" ]]; then
            ${CXX:-c++} ${CXXFLAGS:-} -c "$shim_src" -o "$shim_obj" || true
        fi
    fi
    
    # Configure with zlib support
    local configure_ldflags="-L${TP_LIB_DIR}"
    if [[ -n "$shim_obj" && -f "$shim_obj" ]]; then
        configure_ldflags="$configure_ldflags $shim_obj"
    fi
    
    LDFLAGS="$configure_ldflags" \
    ./configure --prefix=$TP_INSTALL_DIR \
        --disable-shared \
        --enable-static \
        --with-pic \
        --disable-tests \
        --with-zlib \
        CC="${CC:-cc}" \
        CXX="${CXX:-c++}" \
        CFLAGS="${CFLAGS:-}" \
        CXXFLAGS="${CXXFLAGS:-}"
    
    make -j$PARALLEL LDFLAGS="$configure_ldflags"
    make install
    
    # Fix the protobuf library to include our shim
    if [[ -n "$shim_obj" && -f "$shim_obj" && -f "$TP_INSTALL_DIR/lib/libprotobuf.a" ]]; then
        # Create a combined libprotobuf with the shim
        cd "$TP_INSTALL_DIR/lib"
        cp libprotobuf.a libprotobuf.a.backup
        ${AR:-ar} rcs libprotobuf.a "$shim_obj"
        ${RANLIB:-ranlib} libprotobuf.a 2>/dev/null || true
        rm -f libprotobuf.a.backup
        echo "Added hash_memory shim to libprotobuf.a"
    fi
}

# thrift
build_thrift() {
    check_if_source_exist $THRIFT_SOURCE
    cd $TP_SOURCE_DIR/$THRIFT_SOURCE

    if [ ! -f configure ]; then
        ./bootstrap.sh
    fi

    # Set boost include path for thrift (thrift requires boost headers)
    local BOOST_INCLUDE=""
    if [[ -d "$TP_INSTALL_DIR/include/boost" ]]; then
        BOOST_INCLUDE="$TP_INSTALL_DIR/include"
    elif [[ $(uname) == "Darwin" ]]; then
        HOMEBREW_PREFIX="${HOMEBREW_PREFIX:-/opt/homebrew}"
        if [[ -d "$HOMEBREW_PREFIX/include/boost" ]]; then
            BOOST_INCLUDE="$HOMEBREW_PREFIX/include"
        fi
    else
        if [[ -d "/usr/include/boost" ]]; then
            BOOST_INCLUDE="/usr/include"
        elif [[ -d "/usr/local/include/boost" ]]; then
            BOOST_INCLUDE="/usr/local/include"
        fi
    fi
    if [[ -z "$BOOST_INCLUDE" ]]; then
        echo "Error: boost headers not found. Please install boost:"
        if [[ $(uname) == "Darwin" ]]; then
            echo "  brew install boost"
        else
            echo "  Ubuntu/Debian: sudo apt-get install libboost-dev"
            echo "  CentOS/RHEL: sudo yum install boost-devel"
        fi
        exit 1
    fi
    
    echo ${TP_LIB_DIR}
    # Add THRIFT_STATIC_DEFINE for static linking (required for THRIFT_EXPORT macro)
    # Also add boost include path
    # Remove any existing thrift include paths to avoid conflicts with system headers
    local clean_cppflags="${CPPFLAGS:-}"
    clean_cppflags=$(echo "$clean_cppflags" | tr ':' '\n' | grep -v "/opt/homebrew/include/thrift" | tr '\n' ' ' | sed 's/  */ /g')
    export CPPFLAGS="-DTHRIFT_STATIC_DEFINE -I${BOOST_INCLUDE} $clean_cppflags"
    export CFLAGS="${CFLAGS:-} -fPIC"
    export CXXFLAGS="${CXXFLAGS:-} -fPIC"
    export LDFLAGS="-L${TP_LIB_DIR}"
    export LIBS="-lssl -lcrypto -ldl -lz"
    ./configure \
    --prefix=$TP_INSTALL_DIR \
    --docdir=$TP_INSTALL_DIR/doc \
    --enable-static \
    --disable-shared \
    --disable-tests \
    --disable-tutorial \
    --without-qt4 \
    --without-qt5 \
    --without-csharp \
    --without-erlang \
    --without-nodejs \
    --without-lua \
    --without-perl \
    --without-php \
    --without-php_extension \
    --without-dart \
    --without-ruby \
    --without-haskell \
    --without-go \
    --without-haxe \
    --without-d \
    --without-python \
    --without-java \
    --without-rs \
    --without-swift \
    --with-cpp \
    --with-openssl=$TP_INSTALL_DIR \
    --with-boost="${BOOST_INCLUDE%/include}"

    if [ -f compiler/cpp/thrifty.hh ];then
        mv compiler/cpp/thrifty.hh compiler/cpp/thrifty.h
    fi

    # Patch TOutput.cpp to include thrift_export.h before using THRIFT_EXPORT
    # This ensures THRIFT_EXPORT is defined when TOutput.cpp uses it
    # TOutput.cpp includes Thrift.h, but Thrift.h includes TOutput.h (circular dependency)
    # So we need to include thrift_export.h directly in TOutput.cpp
    if [[ -f "lib/cpp/src/thrift/TOutput.cpp" ]]; then
        if ! grep -q "#include <thrift/thrift_export.h>" lib/cpp/src/thrift/TOutput.cpp; then
            echo "Patching TOutput.cpp to include thrift_export.h..."
            if [[ $(uname) == "Darwin" ]]; then
                # Insert after the license comment, before other includes
                sed -i '' '/^#include <thrift\/Thrift.h>/i\
#include <thrift/thrift_export.h>
' lib/cpp/src/thrift/TOutput.cpp
            else
                sed -i '/^#include <thrift\/Thrift.h>/i#include <thrift/thrift_export.h>' lib/cpp/src/thrift/TOutput.cpp
            fi
        fi
    fi

    # Ensure CPPFLAGS with THRIFT_STATIC_DEFINE is in Makefile
    # Modify Makefile to include THRIFT_STATIC_DEFINE in AM_CPPFLAGS (used by libtool)
    # Also remove system thrift headers from include path to avoid conflicts
    if [[ -f lib/cpp/Makefile ]]; then
        if [[ $(uname) == "Darwin" ]]; then
            sed -i '' 's/^AM_CPPFLAGS =/AM_CPPFLAGS = -DTHRIFT_STATIC_DEFINE /' lib/cpp/Makefile || true
            # Remove duplicate if added
            sed -i '' 's/AM_CPPFLAGS = -DTHRIFT_STATIC_DEFINE -DTHRIFT_STATIC_DEFINE/AM_CPPFLAGS = -DTHRIFT_STATIC_DEFINE/' lib/cpp/Makefile || true
            # Remove system thrift include paths from BOOST_CPPFLAGS and INCLUDES
            sed -i '' 's|-I/opt/homebrew/include/thrift[^ ]*||g' lib/cpp/Makefile || true
            sed -i '' 's|-I/usr/local/include/thrift[^ ]*||g' lib/cpp/Makefile || true
            # Ensure local src directory is searched before system paths
            sed -i '' 's|INCLUDES =|INCLUDES = -I./src |' lib/cpp/Makefile || true
        else
            sed -i 's/^AM_CPPFLAGS =/AM_CPPFLAGS = -DTHRIFT_STATIC_DEFINE /' lib/cpp/Makefile || true
            sed -i 's/AM_CPPFLAGS = -DTHRIFT_STATIC_DEFINE -DTHRIFT_STATIC_DEFINE/AM_CPPFLAGS = -DTHRIFT_STATIC_DEFINE/' lib/cpp/Makefile || true
            sed -i 's|-I/usr/include/thrift[^ ]*||g' lib/cpp/Makefile || true
            sed -i 's|INCLUDES =|INCLUDES = -I./src |' lib/cpp/Makefile || true
        fi
    fi

    # Build C++ library first (this is what we need)
    echo "Building thrift C++ library..."
    (cd lib/cpp && make -j$PARALLEL CXXFLAGS="${CXXFLAGS:-} -DTHRIFT_STATIC_DEFINE -Wno-error" CFLAGS="${CFLAGS:-} -Wno-error" AM_CPPFLAGS="-DTHRIFT_STATIC_DEFINE ${AM_CPPFLAGS:-}") || {
        echo "Error: Failed to build thrift C++ library"
        exit 1
    }
    
    # Try to build compiler (needed for code generation, but can fail)
    echo "Building thrift compiler..."
    (cd compiler/cpp && make -j$PARALLEL CXXFLAGS="${CXXFLAGS:-} -DTHRIFT_STATIC_DEFINE -Wno-error" CFLAGS="${CFLAGS:-} -Wno-error" AM_CPPFLAGS="-DTHRIFT_STATIC_DEFINE ${AM_CPPFLAGS:-}") 2>&1 | grep -v -E "(error|Error|undefined|symbol.*not found)" || {
        echo "Warning: Failed to build thrift compiler, but C++ library was built successfully"
        echo "  This is acceptable - we only need the C++ library for linking"
    }
    
    # Try to install, but ignore errors for optional components
    make install 2>&1 | grep -v -E "(Swift|Error|error)" || true
    
    # Manually install required files (in case make install didn't install everything)
    mkdir -p $TP_INSTALL_DIR/{bin,lib,include}
    
    # Install binary
    if [ -f compiler/cpp/thrift ]; then
        cp compiler/cpp/thrift $TP_INSTALL_DIR/bin/ 2>/dev/null || true
    fi
    
    # Install libraries - check multiple possible locations
    local thrift_lib_found=0
    for lib_path in "lib/cpp/.libs/libthrift.a" "lib/libthrift.a" "$TP_INSTALL_DIR/lib/libthrift.a"; do
        if [ -f "$lib_path" ]; then
            cp "$lib_path" "$TP_INSTALL_DIR/lib/libthrift.a" 2>/dev/null && {
                thrift_lib_found=1
                break
            }
        fi
    done
    
    # Try to find libthriftnb.a
    for lib_path in "lib/cpp/.libs/libthriftnb.a" "lib/libthriftnb.a" "$TP_INSTALL_DIR/lib/libthriftnb.a"; do
        if [ -f "$lib_path" ]; then
            cp "$lib_path" "$TP_INSTALL_DIR/lib/libthriftnb.a" 2>/dev/null || true
            break
        fi
    done
    
    # Install headers - critical for C++ compilation
    if [ -d lib/cpp/src/thrift ]; then
        mkdir -p $TP_INSTALL_DIR/include/thrift
        cp -r lib/cpp/src/thrift/* $TP_INSTALL_DIR/include/thrift/ 2>/dev/null || true
    fi
    
    # Verify installation
    if [ ! -f "$TP_INSTALL_DIR/include/thrift/Thrift.h" ]; then
        echo "Warning: Thrift.h not found, trying to install manually..."
        if [ -f "lib/cpp/src/thrift/Thrift.h" ]; then
            mkdir -p $TP_INSTALL_DIR/include/thrift
            cp -r lib/cpp/src/thrift/* $TP_INSTALL_DIR/include/thrift/ 2>/dev/null || true
        fi
    fi
    
    # Verify libraries
    if [ ! -f "$TP_INSTALL_DIR/lib/libthrift.a" ]; then
        echo "Error: libthrift.a not found after build"
        echo "  Searched in: lib/cpp/.libs/libthrift.a, lib/libthrift.a, $TP_INSTALL_DIR/lib/libthrift.a"
        echo "  Build directory contents:"
        ls -la lib/cpp/.libs/*.a 2>/dev/null || echo "    lib/cpp/.libs/ not found or empty"
        exit 1
    fi
    
    # Create a minimal libthriftnb.a if it doesn't exist (brpc config_brpc.sh requires it)
    # libthriftnb requires libevent which may not be available, but brpc's config script needs it
    if [[ ! -f "$TP_INSTALL_DIR/lib/libthriftnb.a" ]]; then
        echo "Note: Creating minimal libthriftnb.a placeholder (libevent not available, non-blocking thrift not built)"
        touch "$TP_INSTALL_DIR/lib/libthriftnb_dummy.c"
        ${CC:-gcc} -c -o "$TP_INSTALL_DIR/lib/libthriftnb_dummy.o" "$TP_INSTALL_DIR/lib/libthriftnb_dummy.c" 2>/dev/null || true
        ${AR:-ar} rcs "$TP_INSTALL_DIR/lib/libthriftnb.a" "$TP_INSTALL_DIR/lib/libthriftnb_dummy.o" 2>/dev/null || true
        rm -f "$TP_INSTALL_DIR/lib/libthriftnb_dummy.c" "$TP_INSTALL_DIR/lib/libthriftnb_dummy.o" 2>/dev/null || true
    fi
}

# brpc
build_brpc() {
    check_if_source_exist $BRPC_SOURCE

    cd $TP_SOURCE_DIR/$BRPC_SOURCE
    
    # Apply brpc-1.9.0 patch if available (fixes C++17 requirement)
    local PATCHED_MARK="patched_mark"
    if [[ ! -f "$PATCHED_MARK" ]]; then
        # Apply config_brpc.sh patch to use C++17
        if [[ -f "$TP_DIR/patches/brpc-1.9.0.patch" ]]; then
            patch -p1 < "$TP_DIR/patches/brpc-1.9.0.patch" || true
        else
            # Manually patch config_brpc.sh to use C++17 instead of C++0x
            if [[ -f config_brpc.sh ]]; then
                if [[ $(uname) == "Darwin" ]]; then
                    sed -i '' 's/CXXFLAGS="-std=c++0x"/CXXFLAGS="-std=c++17"/' config_brpc.sh || true
                else
                    sed -i 's/CXXFLAGS="-std=c++0x"/CXXFLAGS="-std=c++17"/' config_brpc.sh || true
                fi
            fi
        fi
        touch "$PATCHED_MARK"
    fi
    
    # Use CMake for macOS (more reliable), config_brpc.sh for Linux
    if [[ $(uname) == "Darwin" ]]; then
        # Remove pre-generated protobuf files to force regeneration with our protoc 3.14.0
        # This ensures compatibility with our compiled protobuf version
        echo "Removing pre-generated protobuf files to force regeneration..."
        find . -name "*.pb.h" -type f ! -path "./cmake_build/*" -delete 2>/dev/null || true
        find . -name "*.pb.cc" -type f ! -path "./cmake_build/*" -delete 2>/dev/null || true
        # Also remove from any build directories
        rm -rf cmake_build novarocks_build output 2>/dev/null || true
        
        # Force C++17 in CMakeLists.txt using sed for robustness (similar to StarRocks)
        if [[ -f CMakeLists.txt ]]; then
            echo "Forcing C++17 in CMakeLists.txt..."
            if [[ $(uname) == "Darwin" ]]; then
                # Replace C++11 with C++17 in multiple places
                sed -i '' 's/-std=c++11/-std=c++17/g' CMakeLists.txt || true
                sed -i '' 's/set(CMAKE_CXX_STANDARD 11)/set(CMAKE_CXX_STANDARD 17)/g' CMakeLists.txt || true
                sed -i '' 's/set(CMAKE_CXX_STANDARD_REQUIRED ON)/set(CMAKE_CXX_STANDARD_REQUIRED ON)\n\n# Force C++17 for glog 0.7.1 compatibility on macOS\nset(CMAKE_CXX_STANDARD 17)\nset(CMAKE_CXX_STANDARD_REQUIRED ON)/' CMakeLists.txt || true
            else
                sed -i 's/-std=c++11/-std=c++17/g' CMakeLists.txt || true
                sed -i 's/set(CMAKE_CXX_STANDARD 11)/set(CMAKE_CXX_STANDARD 17)/g' CMakeLists.txt || true
                sed -i 's/set(CMAKE_CXX_STANDARD_REQUIRED ON)/set(CMAKE_CXX_STANDARD_REQUIRED ON)\n\n# Force C++17 for glog 0.7.1 compatibility\nset(CMAKE_CXX_STANDARD 17)\nset(CMAKE_CXX_STANDARD_REQUIRED ON)/' CMakeLists.txt || true
            fi
        fi
        
        mkdir -p cmake_build && cd cmake_build
        rm -rf CMakeCache.txt CMakeFiles/
        
        # CRITICAL: Ensure our bin directory is first in PATH to use our protoc 3.14.0
        # Store absolute path to cmake before modifying PATH
        if [[ "$CMAKE_CMD" != /* ]]; then
            # Convert to absolute path
            CMAKE_CMD="$(which "$CMAKE_CMD" || echo "$CMAKE_CMD")"
        fi
        # Remove Homebrew's protoc from PATH temporarily, but keep cmake accessible via absolute path
        export PATH="$TP_INSTALL_DIR/bin:$(echo $PATH | tr ':' '\n' | grep -v '/opt/homebrew/bin' | tr '\n' ':' | sed 's/:$//')"
        export PKG_CONFIG_PATH="$TP_INSTALL_DIR/lib/pkgconfig:${PKG_CONFIG_PATH:-}"
        export PROTOBUF_ROOT="$TP_INSTALL_DIR"
        
        # Verify we're using the correct protoc and cmake
        echo "Using protoc: $(which protoc) - $(protoc --version 2>&1 | head -1)"
        if [[ -x "$CMAKE_CMD" ]]; then
            echo "Using cmake: $CMAKE_CMD - $($CMAKE_CMD --version 2>&1 | head -1)"
        else
            echo "Error: cmake not found at $CMAKE_CMD"
            exit 1
        fi
        
        # Set CPLUS_INCLUDE_PATH and C_INCLUDE_PATH to ensure local headers are found first
        export CPLUS_INCLUDE_PATH="$TP_INSTALL_DIR/include${CPLUS_INCLUDE_PATH:+:$CPLUS_INCLUDE_PATH}"
        export C_INCLUDE_PATH="$TP_INSTALL_DIR/include${C_INCLUDE_PATH:+:$C_INCLUDE_PATH}"
        
        # Prepend our include directory to ensure local headers are used first
        # Use -isystem like StarRocks does
        brpc_include_flag="-isystem $TP_INSTALL_DIR/include"
        CXX_FLAGS="$brpc_include_flag"
        C_FLAGS="$brpc_include_flag"
        if [[ -n "${CXXFLAGS:-}" ]]; then
            CXX_FLAGS="$CXXFLAGS $CXX_FLAGS"
        fi
        if [[ -n "${CFLAGS:-}" ]]; then
            C_FLAGS="$CFLAGS $C_FLAGS"
        fi
        
        # Use OpenSSL from thirdparty/installed (unified for both macOS and Linux)
        # build_openssl() has already copied system OpenSSL to $TP_INSTALL_DIR
        OPENSSL_ROOT="${TP_INSTALL_DIR}"
        
        # Add GLOG_USE_GLOG_EXPORT to CXX_FLAGS for glog compatibility
        # Ensure C++17 is used (required for glog 0.7.1 and std::exchange, std::make_unique)
        CXX_FLAGS="$CXX_FLAGS -DGLOG_USE_GLOG_EXPORT -DGLOG_STATIC_DEFINE -std=c++17"
        
        $CMAKE_CMD -G "${CMAKE_GENERATOR:-Unix Makefiles}" \
            -DCMAKE_BUILD_TYPE=Release \
            -DCMAKE_INSTALL_PREFIX="$TP_INSTALL_DIR" \
            -DCMAKE_PREFIX_PATH="$TP_INSTALL_DIR" \
            -DCMAKE_CXX_FLAGS="$CXX_FLAGS" \
            -DCMAKE_C_FLAGS="$C_FLAGS" \
            -DCMAKE_CXX_STANDARD=17 \
            -DCMAKE_CXX_STANDARD_REQUIRED=ON \
            -DCMAKE_CXX_EXTENSIONS=OFF \
            -DCMAKE_POSITION_INDEPENDENT_CODE=ON \
            -DBUILD_SHARED_LIBS=OFF \
            -DGFLAGS_STATIC=ON \
            -DWITH_GLOG=ON \
            -DBRPC_WITH_GLOG=ON \
            -DWITH_THRIFT=OFF \
            -DOPENSSL_ROOT_DIR="$OPENSSL_ROOT" \
            -DOPENSSL_INCLUDE_DIR="$OPENSSL_ROOT/include" \
            -DOPENSSL_SSL_LIBRARY="$OPENSSL_ROOT/lib/libssl.a" \
            -DOPENSSL_CRYPTO_LIBRARY="$OPENSSL_ROOT/lib/libcrypto.a" \
            -Dgflags_DIR="$TP_INSTALL_DIR/lib/cmake/gflags" \
            -DProtobuf_DIR="$TP_INSTALL_DIR/lib/cmake/protobuf" \
            -DProtobuf_PROTOC_EXECUTABLE="$TP_INSTALL_DIR/bin/protoc" \
            -DProtobuf_INCLUDE_DIR="$TP_INSTALL_DIR/include" \
            -DProtobuf_LIBRARY="$TP_INSTALL_DIR/lib/libprotobuf.a" \
            -DProtobuf_VERSION="3.14.0" \
            -Dglog_DIR="$TP_INSTALL_DIR/lib/cmake/glog" \
            -DGLOG_INCLUDE_PATH="$TP_INSTALL_DIR/include" \
            -DGLOG_LIB="$TP_INSTALL_DIR/lib/libglog.a" \
            -DCMAKE_POLICY_VERSION_MINIMUM=3.5 \
            ..
        
        # After CMake configuration, modify the generated CMakeCache.txt or CMakeFiles
        # to ensure GLOG_USE_GLOG_EXPORT is defined for all targets
        # This is a workaround since we can't modify brpc's CMakeLists.txt
        if [[ -f CMakeCache.txt ]]; then
            # Try to add compile definitions to all targets by modifying CMakeFiles
            find . -name "flags.make" -exec sed -i '' 's/CXX_DEFINES =/CXX_DEFINES = -DGLOG_USE_GLOG_EXPORT -DGLOG_STATIC_DEFINE /' {} \; 2>/dev/null || true
        fi
        
        ${BUILD_SYSTEM} -j$PARALLEL
        ${BUILD_SYSTEM} install
    else
        # On Linux, use config_brpc.sh
        # On macOS, ensure we use Homebrew's gnu-getopt
        if [[ $(uname) == "Darwin" ]]; then
            local HOMEBREW_PREFIX="${HOMEBREW_PREFIX:-/opt/homebrew}"
            if [[ -f "$HOMEBREW_PREFIX/opt/gnu-getopt/bin/getopt" ]]; then
                export PATH="$HOMEBREW_PREFIX/opt/gnu-getopt/bin:$PATH"
                echo "Using Homebrew gnu-getopt: $(which getopt)"
            else
                echo "Warning: Homebrew gnu-getopt not found. Install with: brew install gnu-getopt"
            fi
        fi
        
        # Ensure our bin directory is first in PATH to use our protoc
        PATH=$TP_INSTALL_DIR/bin:$PATH ./config_brpc.sh \
            --headers="$TP_INSTALL_DIR/include" \
            --libs="$TP_INSTALL_DIR/bin $TP_INSTALL_DIR/lib" \
            --with-glog \
            --with-thrift
        
        # Modify Makefile to skip shared library build (only build static library)
        # This avoids -fPIC issues with system libraries like zlib
        if [[ -f Makefile ]]; then
            # Remove libbrpc.$(SOEXT) from 'all' target
            if [[ $(uname) == "Darwin" ]]; then
                sed -i '' 's/^all:.*libbrpc\.$(SOEXT).*/all:  protoc-gen-mcpack libbrpc.a output\/include output\/lib output\/bin/' Makefile || true
                # Remove libbrpc.$(SOEXT) dependency from output/lib target
                sed -i '' 's/^output\/lib:libbrpc\.a libbrpc\.$(SOEXT)/output\/lib:libbrpc.a/' Makefile || true
            else
                sed -i 's/^all:.*libbrpc\.$(SOEXT).*/all:  protoc-gen-mcpack libbrpc.a output\/include output\/lib output\/bin/' Makefile || true
                # Remove libbrpc.$(SOEXT) dependency from output/lib target
                sed -i 's/^output\/lib:libbrpc\.a libbrpc\.$(SOEXT)/output\/lib:libbrpc.a/' Makefile || true
            fi
        fi
        
        # Add GLOG_USE_GLOG_EXPORT and GLOG_STATIC_DEFINE for glog compatibility
        # On Linux, modify config.mk directly (brpc uses config.mk, not output/Makefile)
        if [[ $(uname) == "Linux" ]] && [[ -f config.mk ]]; then
            # Add glog compatibility flags to CPPFLAGS in config.mk
            if ! grep -q "DGLOG_USE_GLOG_EXPORT" config.mk; then
                sed -i 's/^CPPFLAGS=\(.*\)/CPPFLAGS=\1 -DGLOG_USE_GLOG_EXPORT -DGLOG_STATIC_DEFINE/' config.mk || true
            fi
        fi
        
        # Modify Makefile to add glog compatibility flags (for macOS or if output/Makefile exists)
        if [ -f output/Makefile ]; then
            if [[ $(uname) == "Darwin" ]]; then
                sed -i '' 's/^\(CXXFLAGS[[:space:]]*=[[:space:]]*\)/\1-DGLOG_USE_GLOG_EXPORT -DGLOG_STATIC_DEFINE /' output/Makefile || true
                sed -i '' 's/\(CXXFLAGS[[:space:]]*+=[[:space:]]*\)/\1-DGLOG_USE_GLOG_EXPORT -DGLOG_STATIC_DEFINE /' output/Makefile || true
                sed -i '' 's/^\(CPPFLAGS[[:space:]]*=[[:space:]]*\)/\1-DGLOG_USE_GLOG_EXPORT -DGLOG_STATIC_DEFINE /' output/Makefile || true
            else
                sed -i 's/^\(CXXFLAGS[[:space:]]*=[[:space:]]*\)/\1-DGLOG_USE_GLOG_EXPORT -DGLOG_STATIC_DEFINE /' output/Makefile || true
                sed -i 's/\(CXXFLAGS[[:space:]]*+=[[:space:]]*\)/\1-DGLOG_USE_GLOG_EXPORT -DGLOG_STATIC_DEFINE /' output/Makefile || true
                sed -i 's/^\(CPPFLAGS[[:space:]]*=[[:space:]]*\)/\1-DGLOG_USE_GLOG_EXPORT -DGLOG_STATIC_DEFINE /' output/Makefile || true
            fi
        fi
        
        # Build brpc using the Makefile
        # On Linux, config.mk is already modified, so just run make
        # On macOS, use CMake build which is handled above
        # Only build static library (libbrpc.a), skip shared library to avoid -fPIC issues with system libraries
        if [[ $(uname) == "Linux" ]]; then
            make -j$PARALLEL libbrpc.a protoc-gen-mcpack output/include output/lib output/bin || \
            make -j$PARALLEL libbrpc.a
        else
            make -j$PARALLEL libbrpc.a CXXFLAGS="${CXXFLAGS}" CPPFLAGS="${CPPFLAGS}" || \
            make -j$PARALLEL libbrpc.a
        fi
        
        # Install brpc libraries and headers
        # brpc 1.9.0 may install to different locations, check common locations
        if [ -d output ]; then
            cp -rf output/* ${TP_INSTALL_DIR}/
        fi
        
        # Also check if libraries are in the source directory
        if [ -f libbrpc.a ]; then
            mkdir -p ${TP_INSTALL_DIR}/lib
            cp libbrpc.a ${TP_INSTALL_DIR}/lib/ 2>/dev/null || true
        fi
        
        # Install headers if they exist in include directory
        if [ -d include ]; then
            mkdir -p ${TP_INSTALL_DIR}/include
            cp -r include/* ${TP_INSTALL_DIR}/include/ 2>/dev/null || true
        fi
    fi
}

#########################
# build all thirdparties
#########################

# leveldb (required by brpc)
build_leveldb() {
    check_if_source_exist $LEVELDB_SOURCE
    cd $TP_SOURCE_DIR/$LEVELDB_SOURCE
    
    # Build using Makefile (leveldb 1.20 doesn't use CMake)
    # On macOS ARM64, disable SSE flags
    # Add -fPIC for shared library support (required when linking into libbrpc.so)
    # Use environment variable to pass CXXFLAGS (leveldb Makefile respects CXXFLAGS env var)
    if [[ $(uname) == "Darwin" ]]; then
        # Disable SSE on non-x86 architectures
        CXXFLAGS="-fPIC" LDFLAGS="-L ${TP_LIB_DIR} -static-libstdc++ -static-libgcc" \
        make -j$PARALLEL PLATFORM_SSEFLAGS="" out-static/libleveldb.a
    else
        CXXFLAGS="-fPIC" LDFLAGS="-L ${TP_LIB_DIR} -static-libstdc++ -static-libgcc" \
        make -j$PARALLEL out-static/libleveldb.a
    fi
    
    # Install manually
    mkdir -p $TP_INSTALL_DIR/include/leveldb
    mkdir -p $TP_INSTALL_DIR/lib
    
    # Copy headers
    cp -r include/leveldb/* $TP_INSTALL_DIR/include/leveldb/
    
    # Copy the static library
    if [[ -f "out-static/libleveldb.a" ]]; then
        cp out-static/libleveldb.a $TP_INSTALL_DIR/lib/
    else
        echo "Error: leveldb static library not found in out-static/"
        exit 1
    fi
}

# Create runtime_version.h shim for protobuf 3.14.0 compatibility
# This file is required by brpc but doesn't exist in protobuf 3.14.0
create_protobuf_runtime_version_shim() {
    mkdir -p $TP_INSTALL_DIR/include/google/protobuf
    if [[ ! -f "$TP_INSTALL_DIR/include/google/protobuf/runtime_version.h" ]]; then
        cat > "$TP_INSTALL_DIR/include/google/protobuf/runtime_version.h" <<'EOF'
#ifndef GOOGLE_PROTOBUF_RUNTIME_VERSION_H__
#define GOOGLE_PROTOBUF_RUNTIME_VERSION_H__

// Shim for protobuf 3.14.0 compatibility
// This file doesn't exist in protobuf 3.14.0 but is required by brpc
// We don't define PROTOBUF_VERSION here to avoid conflicts with protobuf's own definitions

#endif  // GOOGLE_PROTOBUF_RUNTIME_VERSION_H__
EOF
        echo "Created runtime_version.h shim for protobuf 3.14.0"
    fi
}

# Build in dependency order
build_zlib
build_openssl
build_gflags
build_glog
build_protobuf
create_protobuf_runtime_version_shim
build_thrift
build_leveldb
build_brpc

echo "===== All thirdparties built successfully!"
