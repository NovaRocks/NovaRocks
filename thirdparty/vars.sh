#!/bin/bash
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

############################################################
# You may have to set variables bellow,
# which are used for compiling thirdparties and starust itself.
############################################################

# --job param for *make*
# support macos
if [[ $(uname) == "Darwin" ]]; then
    default_parallel=$[$(sysctl -n hw.physicalcpu)/4+1]
else
    default_parallel=$[$(nproc)/4+1]
fi

# use the value if $PARALLEL is already set, otherwise use $default_parallel
PARALLEL=${PARALLEL:-$default_parallel}

###################################################
# DO NOT change variables bellow unless you known
# what you are doing.
###################################################

# thirdparties will be downloaded and unpacked here
export TP_SOURCE_DIR=$TP_DIR/src

# thirdparties will be installed to here
export TP_INSTALL_DIR=$TP_DIR/installed

# patches for all thirdparties
export TP_PATCH_DIR=$TP_DIR/patches

# header files of all thirdparties will be intalled to here
export TP_INCLUDE_DIR=$TP_INSTALL_DIR/include

# libraries of all thirdparties will be intalled to here
export TP_LIB_DIR=$TP_INSTALL_DIR/lib

#####################################################
# Download url, filename and unpacked filename
# of all thirdparties
#####################################################

# thrift
THRIFT_DOWNLOAD="http://archive.apache.org/dist/thrift/0.20.0/thrift-0.20.0.tar.gz"
THRIFT_NAME=thrift-0.20.0.tar.gz
THRIFT_SOURCE=thrift-0.20.0
THRIFT_MD5SUM="aadebde599e1f5235acd3c730721b873"

# protobuf
PROTOBUF_DOWNLOAD="https://github.com/google/protobuf/archive/v3.14.0.tar.gz"
PROTOBUF_NAME=protobuf-3.14.0.tar.gz
PROTOBUF_SOURCE=protobuf-3.14.0
PROTOBUF_MD5SUM="0c9d2a96f3656ba7ef3b23b533fb6170"

# gflags (dependency of glog)
GFLAGS_DOWNLOAD="https://github.com/gflags/gflags/archive/v2.2.2.tar.gz"
GFLAGS_NAME=gflags-2.2.2.tar.gz
GFLAGS_SOURCE=gflags-2.2.2
GFLAGS_MD5SUM="1a865b93bacfa963201af3f75b7bd64c"

# glog
GLOG_DOWNLOAD="https://github.com/google/glog/archive/v0.7.1.tar.gz"
GLOG_NAME=glog-0.7.1.tar.gz
GLOG_SOURCE=glog-0.7.1
GLOG_MD5SUM="128e2995cc33d794ff24f785a3060346"

# leveldb (required by brpc)
LEVELDB_DOWNLOAD="https://github.com/google/leveldb/archive/v1.20.tar.gz"
LEVELDB_NAME=leveldb-1.20.tar.gz
LEVELDB_SOURCE=leveldb-1.20
LEVELDB_MD5SUM="298b5bddf12c675d6345784261302252"

# brpc
BRPC_DOWNLOAD="https://github.com/apache/brpc/archive/refs/tags/1.9.0.tar.gz"
BRPC_NAME=brpc-1.9.0.tar.gz
BRPC_SOURCE=brpc-1.9.0
BRPC_MD5SUM="a2b626d96a5b017f2a6701ffa594530c"

# Libraries to download and build from source
TP_ARCHIVES="GFLAGS GLOG THRIFT PROTOBUF LEVELDB BRPC"
