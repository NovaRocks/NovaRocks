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
import com.aliyun.credentials.provider.RefreshResult;
import com.amazonaws.auth.AWSSessionCredentials;
import java.util.Map;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;

/** Offline adapter contract tests; no metadata address is contacted. */
public final class ProviderContractTest {
  private static void check(boolean condition, String message) {
    if (!condition) {
      throw new AssertionError(message);
    }
  }

  private static final class RotatingProvider extends EcsRamRoleCredentialProvider {
    private int fetches;

    @SuppressWarnings("deprecation")
    RotatingProvider() {
      super("not-requested");
    }

    @Override
    public RefreshResult<CredentialModel> refreshCredentials() {
      int generation = ++fetches;
      CredentialModel value = CredentialModel.builder()
          .accessKeyId("id-" + generation)
          .accessKeySecret("secret-" + generation)
          .securityToken("token-" + generation)
          .build();
      // Make the official cache demand a fresh atomic value on the next call.
      return RefreshResult.builder(value).staleTime(0).build();
    }
  }

  public static void main(String[] args) {
    RotatingProvider officialV2 = new RotatingProvider();
    AliyunEcsSource sourceV2 = new AliyunEcsSource(officialV2);
    try (AliyunEcsCredentialsProvider adapter = new AliyunEcsCredentialsProvider(sourceV2)) {
      for (int generation = 1; generation <= 2; generation++) {
        AwsSessionCredentials value = (AwsSessionCredentials) adapter.resolveCredentials();
        check(value.accessKeyId().equals("id-" + generation), "AWSv2 key generation");
        check(value.secretAccessKey().equals("secret-" + generation), "AWSv2 secret generation");
        check(value.sessionToken().equals("token-" + generation), "AWSv2 token generation");
      }
    }
    check(officialV2.fetches == 2, "AWSv2 must delegate renewal to the official provider");
    try {
      sourceV2.credentials();
      throw new AssertionError("closed source accepted an acquisition");
    } catch (IllegalStateException expected) {
      // Closing the adapter closes its source and prevents later acquisitions.
    }

    RotatingProvider officialV1 = new RotatingProvider();
    try (AliyunEcsV1CredentialsProvider adapter =
        new AliyunEcsV1CredentialsProvider(new AliyunEcsSource(officialV1))) {
      for (int generation = 1; generation <= 2; generation++) {
        AWSSessionCredentials value = (AWSSessionCredentials) adapter.getCredentials();
        check(value.getAWSAccessKeyId().equals("id-" + generation), "AWSv1 key generation");
        check(value.getAWSSecretKey().equals("secret-" + generation), "AWSv1 secret generation");
        check(value.getSessionToken().equals("token-" + generation), "AWSv1 token generation");
      }
    }
    check(officialV1.fetches == 2, "AWSv1 must delegate renewal to the official provider");

    try {
      AliyunEcsCredentialsProvider.create(Map.of());
      throw new AssertionError("missing explicit role accepted");
    } catch (IllegalArgumentException expected) {
      // Configuration validation must finish before metadata I/O.
    }
    try (AliyunEcsCredentialsProvider ignored =
        AliyunEcsCredentialsProvider.create(Map.of("role-name", "not-requested"))) {
      // The documented static factory is sufficient for Iceberg reflection.
    }
    System.out.println("Java ECS provider contracts passed");
  }
}
