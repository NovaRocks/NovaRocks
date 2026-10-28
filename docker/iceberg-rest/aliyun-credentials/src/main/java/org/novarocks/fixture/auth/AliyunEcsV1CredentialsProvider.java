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
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.BasicSessionCredentials;
import java.io.Closeable;
import java.util.Objects;

/** Hadoop 3.3 S3A's AWS SDK v1 extension, selected explicitly in configuration. */
public final class AliyunEcsV1CredentialsProvider implements AWSCredentialsProvider, Closeable {
  private final SessionCredentialSource source;

  public AliyunEcsV1CredentialsProvider() {
    this(new AliyunEcsSource(System.getenv("NOVA_OSS_ROLE_NAME")));
  }

  AliyunEcsV1CredentialsProvider(SessionCredentialSource source) {
    this.source = Objects.requireNonNull(source);
  }

  @Override
  public AWSCredentials getCredentials() {
    CredentialModel value = source.credentials();
    return new BasicSessionCredentials(
        value.getAccessKeyId(), value.getAccessKeySecret(), value.getSecurityToken());
  }

  @Override
  public void refresh() {
    // Refresh decisions stay in the SDK's supplier rather than a second cache.
    source.credentials();
  }

  @Override
  public void close() {
    source.close();
  }
}
