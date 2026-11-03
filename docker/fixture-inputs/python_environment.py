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

"""Build a missing SDK interpreter; existing environments are used directly."""
from pathlib import Path
from fixture_inputs import require_relative, run


def prepare_environment(item, inputs, store, directory):
    directory = Path(directory)
    # The command itself reports a missing or unusable interpreter.
    run([f"python{item['python_version']}", "-I", "-m", "venv", "--without-pip", str(directory)])
    python = directory / "bin/python"
    artifacts = Path(store) / "artifacts"
    pip = artifacts / require_relative(item["pip_artifact"])
    runner = ("import sys,runpy; wheel=sys.argv.pop(1); sys.path.insert(0,wheel); "
              "sys.argv[0]='pip'; runpy.run_module('pip',run_name='__main__')")
    bootstrap = [str(artifacts / require_relative(value)) for value in item["bootstrap_artifacts"]]
    run([str(python), "-I", "-c", runner, str(pip), "--isolated", "install",
         "--no-index", "--no-deps", "--no-compile", *bootstrap])
    requirements = directory / "requirements.txt"
    requirements.write_text("".join(f"{name}=={version} --hash=sha256:{inputs['artifacts'][item['package_artifacts'][name]]['sha256']}\n"
                                    for name, version in sorted(item["packages"].items())))
    run([str(python), "-I", "-m", "pip", "--isolated", "install", "--no-index",
         "--find-links", str(artifacts / require_relative(item["wheelhouse"])),
         "--no-deps", "--no-build-isolation", "--no-compile", "--require-hashes", "-r", str(requirements)])
