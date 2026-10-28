// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

package org.novarocks.fixture.auth;

import java.lang.reflect.InvocationTargetException;
import java.util.jar.JarFile;

/** Keeps the upstream main class while adding a real provider classpath. */
public final class RestLauncher {
  private RestLauncher() {}

  public static void main(String[] args) throws Throwable {
    String adapter = System.getenv("NOVA_OSS_REST_ADAPTER_JAR");
    if (adapter == null) {
      adapter = "/usr/lib/iceberg-rest/iceberg-rest-adapter.jar";
    }
    String main;
    try (JarFile jar = new JarFile(adapter)) {
      main = jar.getManifest().getMainAttributes().getValue("Main-Class");
    }
    if (main == null || main.trim().isEmpty()) {
      throw new IllegalStateException("REST adapter has no manifest main class");
    }
    try {
      Class.forName(main).getMethod("main", String[].class).invoke(null, (Object) args);
    } catch (InvocationTargetException failure) {
      throw failure.getCause();
    }
  }
}
