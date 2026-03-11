// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.novarocks.connector.iceberg;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import org.apache.iceberg.Table;
import org.apache.iceberg.io.FileIO;

import java.util.concurrent.TimeUnit;

final class TableFileIOCache {
    private final Cache<String, FileIO> cache;

    TableFileIOCache(long expireSeconds, long capacity) {
        this.cache = CacheBuilder.newBuilder()
                .expireAfterWrite(expireSeconds, TimeUnit.SECONDS)
                .maximumSize(capacity)
                .build();
    }

    synchronized FileIO get(Table table) {
        String cacheKey = table.name() + "_" + table.uuid();
        FileIO fileIO = cache.getIfPresent(cacheKey);
        if (fileIO == null) {
            fileIO = table.io();
            cache.put(cacheKey, fileIO);
        }
        return fileIO;
    }
}
