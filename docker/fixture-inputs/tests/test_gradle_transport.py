# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements. See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership. The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License. You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied. See the License for the
# specific language governing permissions and limitations
# under the License.

"""Optional offline Gradle 8.14.3 repository lifecycle contract.

The ordinary fixture unit suite does not require host Gradle. Select its exact
local executable with NOVA_FIXTURE_TEST_GRADLE to exercise the real init script.
No dependencies are resolved, Docker builds run or verification metadata changed.
"""
import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest

REPO = Path(__file__).resolve().parents[3]
GRADLE = os.environ.get("NOVA_FIXTURE_TEST_GRADLE")


@unittest.skipUnless(GRADLE, "explicit local Gradle 8.14.3 is required")
class GradleTransportContractTests(unittest.TestCase):
    def test_existing_and_later_repositories_change_only_matching_transport(self):
        with tempfile.TemporaryDirectory() as temporary:
            project = Path(temporary)
            (project / "settings.gradle").write_text("""pluginManagement {
  repositories {
    gradlePluginPortal()
    maven { name = 'unmatched'; url = 'https://unmatched.invalid/plugins' }
  }
}
dependencyResolutionManagement { repositories { mavenCentral() } }
rootProject.name = 'transport-contract'
include 'child'
gradle.settingsEvaluated { settings ->
  assert settings.pluginManagement.repositories.collect { it.url.toString() } ==
      ['https://mirror.invalid/plugins/', 'https://unmatched.invalid/plugins']
  assert settings.dependencyResolutionManagement.repositories.collect { it.url.toString() } ==
      ['https://mirror.invalid/maven/']
}
""")
            (project / "build.gradle").write_text("""buildscript { repositories { gradlePluginPortal() } }
repositories {
  mavenCentral()
  maven { name = 'unmatched'; url = 'https://unmatched.invalid/maven/' }
  mavenLocal()
}
subprojects { repositories { mavenCentral() } }
tasks.register('transportContract') {
  doLast {
    assert buildscript.repositories.collect { it.url.toString() } == ['https://mirror.invalid/plugins/']
    assert repositories.size() == 3
    assert repositories[0].url.toString() == 'https://mirror.invalid/maven/'
    assert repositories[1].url.toString() == 'https://unmatched.invalid/maven/'
    assert repositories[2].url.scheme == 'file'
    assert project(':child').repositories.collect { it.url.toString() } == ['https://mirror.invalid/maven/']
    assert gradle.gradleVersion == '8.14.3'
    println 'TRANSPORT_CONTRACT_PASS'
  }
}
""")
            environment = dict(os.environ, GRADLE_USER_HOME=str(project / "gradle-home"),
                NOVA_FIXTURE_URL_PREFIXES=json.dumps({
                    "https://repo.maven.apache.org/maven2/": "https://mirror.invalid/maven/",
                    "https://plugins.gradle.org/m2/": "https://mirror.invalid/plugins/"}))
            result = subprocess.run([GRADLE, "-p", str(project), "--offline", "--no-daemon",
                "--console=plain", "--init-script",
                str(REPO / "docker/iceberg-rest/rest-mv/build/transport.init.gradle"), "transportContract"],
                env=environment, capture_output=True, text=True, timeout=120)
            self.assertEqual(0, result.returncode, result.stdout + result.stderr)
            self.assertIn("TRANSPORT_CONTRACT_PASS", result.stdout)


if __name__ == "__main__":
    unittest.main()
