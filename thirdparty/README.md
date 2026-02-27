# Starust Thirdparty Management

This directory contains scripts to download and build third-party dependencies for Starust.

## Quick Start

1. **Download thirdparty sources:**
   ```bash
   ./thirdparty/download-thirdparty.sh
   ```

2. **Build thirdparty libraries:**
   ```bash
   export STARROCKS_GCC_HOME=/opt/rh/devtoolset-12/root/usr
   ./thirdparty/build-thirdparty.sh
   ```

   Or with parallel jobs:
   ```bash
   export STARROCKS_GCC_HOME=/opt/rh/devtoolset-12/root/usr
   ./thirdparty/build-thirdparty.sh -j8
   ```

3. **Build Starust:**
   ```bash
   cargo build
   ```

## Dependencies

### Libraries Built from Source (6)

These libraries are **always built from source** and downloaded automatically:

1. **protobuf 3.14.0** - Protocol buffer library (required version for brpc compatibility)
2. **gflags 2.2.2** - Command-line flags library (dependency of glog)
3. **glog 0.7.1** - Logging library
4. **thrift 0.20.0** - Thrift RPC framework (required version for StarRocks FE compatibility)
5. **leveldb 1.20** - Key-value storage library (required by brpc)
6. **brpc 1.9.0** - Baidu RPC framework

### System Libraries (4)

These libraries **must be installed on your system** before building. The build script will use them directly and **fail if they are not found** (no source build fallback):

1. **zlib** - Compression library
   - **macOS**: `brew install zlib`
   - **Linux**: `sudo apt-get install zlib1g-dev` (Ubuntu/Debian) or `sudo yum install zlib-devel` (CentOS/RHEL)

2. **openssl** - SSL/TLS library
   - **macOS**: `brew install openssl@3` (recommended) or `brew install openssl@1.1` or `brew install openssl`
   - **Linux**: `sudo apt-get install libssl-dev` (Ubuntu/Debian) or `sudo yum install openssl-devel` (CentOS/RHEL)
   - **Note**: Some Linux distributions only provide shared libraries. You may need to build openssl from source separately.

3. **boost** - C++ libraries (required by thrift)
   - **macOS**: `brew install boost`
   - **Linux**: `sudo apt-get install libboost-dev` (Ubuntu/Debian) or `sudo yum install boost-devel` (CentOS/RHEL)

4. **gperftools** - Performance tools (optional dependency of brpc)
   - **macOS**: `brew install gperftools`
   - **Linux**: Install via package manager or build from source

## Directory Structure

```
thirdparty/
├── src/              # Downloaded source archives (created by download script)
├── installed/        # Built libraries and headers (created by build script)
│   ├── bin/         # Executables (thrift, protoc)
│   ├── include/     # Header files
│   └── lib/         # Static libraries
├── patches/         # Patches for thirdparty sources (if needed)
├── vars.sh          # Configuration file defining versions and download URLs
├── download-thirdparty.sh  # Script to download sources
└── build-thirdparty.sh     # Script to build libraries
```

## Build Requirements

The build script requires:
- `cmake` - Build system
- `make` - Build tool
- `gcc` or `clang` - C/C++ compiler
- `wget` or `curl` - Download tool
- `tar` - Archive extraction
- `autoconf`, `automake`, `libtool` - For some packages (usually pre-installed)

### macOS Specific Requirements

**Required system libraries** (install via Homebrew):
- `zlib` - `brew install zlib`
- `openssl` - `brew install openssl@3` (recommended)
- `boost` - `brew install boost`
- `bison` >= 2.5 (required by protobuf) - `brew install bison`
- `gperftools` (optional) - `brew install gperftools`

**Homebrew must be installed**: https://brew.sh

The build script will **fail immediately** if any required system library is missing (no automatic source build fallback).

### Linux Specific Requirements

**Required system libraries** (install via package manager):
- `zlib` - `sudo apt-get install zlib1g-dev` (Ubuntu/Debian) or `sudo yum install zlib-devel` (CentOS/RHEL)
- `openssl` - `sudo apt-get install libssl-dev` (Ubuntu/Debian) or `sudo yum install openssl-devel` (CentOS/RHEL)
  - **Note**: Some distributions only provide shared libraries. You may need to build openssl from source separately.
- `boost` - `sudo apt-get install libboost-dev` (Ubuntu/Debian) or `sudo yum install boost-devel` (CentOS/RHEL)
- `STARROCKS_GCC_HOME` is required and must point to your GCC toolchain root (must contain `bin/gcc` and `bin/g++`), for example:
  ```bash
  export STARROCKS_GCC_HOME=/opt/rh/devtoolset-12/root/usr
  ```

## Notes

- The build process will take some time (10-30 minutes depending on your machine)
- Built libraries are static and linked into the final binary
- If you need to rebuild, use `--clean` flag:
  ```bash
  ./thirdparty/build-thirdparty.sh --clean -j8
  ```

## Troubleshooting

### Build fails with "command not found"
Make sure all build requirements are installed. On macOS:
```bash
brew install cmake
```

On Ubuntu/Debian:
```bash
sudo apt-get install cmake build-essential
```

### Download fails
Check your internet connection. The script will retry downloads automatically.

### Build fails for a specific package
Check the error message. Some packages may require additional system libraries.
You can try building individual packages by modifying the build script.
