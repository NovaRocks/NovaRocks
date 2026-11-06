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

"""Strict CREATE oracle for the shared typed query tool's original failure."""
import json
from pathlib import Path
import sys


def require_already_exists(report):
    cause = report.get("failure")
    failure = cause.get("Trino") if isinstance(cause, dict) else None
    if not isinstance(failure, dict) or (
        failure.get("errorCode"), failure.get("errorName"), failure.get("errorType")
    ) != (12, "ALREADY_EXISTS", "USER_ERROR"):
        raise ValueError("strict CREATE did not return typed ALREADY_EXISTS/12/USER_ERROR")
    if report.get("worker_joined") is not True or report.get("cleanup_failures") != []:
        raise ValueError("strict CREATE query worker or cleanup did not settle")


def main():
    if len(sys.argv) != 3 or sys.argv[1] != "already-exists":
        raise ValueError("expected already-exists ERROR_JSON")
    require_already_exists(json.loads(Path(sys.argv[2]).read_text()))


if __name__ == "__main__":
    try:
        main()
    except (OSError, ValueError, TypeError, AttributeError) as error:
        sys.exit("error: " + str(error))
