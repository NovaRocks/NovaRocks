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

"""Strict-create identity never accepts candidate text or transport failures."""
import importlib.util
from pathlib import Path
import unittest

spec = importlib.util.spec_from_file_location("hadoop_trino_query",Path(__file__).with_name("hadoop-catalog-trino-query.py"))
owner = importlib.util.module_from_spec(spec); spec.loader.exec_module(owner)


class StrictCreateTests(unittest.TestCase):
    def report(self):
        return {"failure":{"Trino":{"errorCode":12,"errorName":"ALREADY_EXISTS","errorType":"USER_ERROR","message":"arbitrary text"}},"worker_joined":True,"cleanup_failures":[]}

    def test_only_typed_original_identity_and_actual_join_satisfy_oracle(self):
        owner.require_already_exists(self.report())
        for key,value in [("errorCode",65536),("errorName","GENERIC_INTERNAL_ERROR"),("errorType","INTERNAL_ERROR")]:
            report = self.report(); report["failure"]["Trino"][key]=value
            report["failure"]["Trino"]["message"]="AlreadyExists"
            with self.assertRaises(ValueError): owner.require_already_exists(report)
        for failure in [{"Transport":"AlreadyExists"},{"Http":{"status":500,"body":"AlreadyExists"}},"Deadline"]:
            report=self.report(); report["failure"]=failure
            with self.assertRaises(ValueError): owner.require_already_exists(report)
        for key,value in [("worker_joined",False),("cleanup_failures",[{"Deadline":None}])]:
            report=self.report();report[key]=value
            with self.assertRaises(ValueError):owner.require_already_exists(report)



if __name__ == "__main__":
    unittest.main()
