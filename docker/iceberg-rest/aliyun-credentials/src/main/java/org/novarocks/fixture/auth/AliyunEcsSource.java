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

import com.aliyun.credentials.models.CredentialModel;
import com.aliyun.credentials.provider.EcsRamRoleCredentialProvider;
import java.util.Objects;

final class AliyunEcsSource implements SessionCredentialSource {
  private final EcsRamRoleCredentialProvider provider;
  private boolean closed;

  AliyunEcsSource(String roleName) {
    this(providerFor(roleName));
  }

  AliyunEcsSource(EcsRamRoleCredentialProvider provider) {
    this.provider = Objects.requireNonNull(provider);
  }

  private static EcsRamRoleCredentialProvider providerFor(String roleName) {
    if (roleName == null || roleName.trim().isEmpty()) {
      throw new IllegalArgumentException("An explicit ECS RAM role name is required");
    }
    return EcsRamRoleCredentialProvider.builder()
        .roleName(roleName)
        .disableIMDSv1(true)
        .asyncCredentialUpdateEnabled(false)
        .build();
  }

  @Override
  public synchronized CredentialModel credentials() {
    if (closed) {
      throw new IllegalStateException("ECS credential provider is closed");
    }
    // The SDK owns cache validity and renewal; adapters never retain a token.
    return provider.getCredentials();
  }

  @Override
  public synchronized void close() {
    if (!closed) {
      closed = true;
      provider.close();
    }
  }
}
