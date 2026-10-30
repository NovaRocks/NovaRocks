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
import java.util.Map;
import java.util.Objects;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.utils.SdkAutoCloseable;

/** Iceberg's AWS SDK v2 extension. It does not load any AWS SDK v1 class. */
public final class AliyunEcsCredentialsProvider implements AwsCredentialsProvider, SdkAutoCloseable {
  private final SessionCredentialSource source;

  AliyunEcsCredentialsProvider(SessionCredentialSource source) {
    this.source = Objects.requireNonNull(source);
  }

  public static AliyunEcsCredentialsProvider create(Map<String, String> properties) {
    return new AliyunEcsCredentialsProvider(new AliyunEcsSource(properties.get("role-name")));
  }

  @Override
  public AwsCredentials resolveCredentials() {
    CredentialModel value = source.credentials();
    return AwsSessionCredentials.create(
        value.getAccessKeyId(), value.getAccessKeySecret(), value.getSecurityToken());
  }

  @Override
  public void close() {
    source.close();
  }
}
