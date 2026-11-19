-- Licensed to the Apache Software Foundation (ASF) under one
-- or more contributor license agreements.  See the NOTICE file
-- distributed with this work for additional information
-- regarding copyright ownership.  The ASF licenses this file
-- to you under the Apache License, Version 2.0 (the
-- "License"); you may not use this file except in compliance
-- with the License.  You may obtain a copy of the License at
--
--   http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing,
-- software distributed under the License is distributed on an
-- "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
-- KIND, either express or implied.  See the License for the
-- specific language governing permissions and limitations
-- under the License.

-- @tags=low-cardinality,dictionary,null
-- Verify nullable runtime dictionary carriers preserve plain NULL semantics:
-- GROUP BY keeps a NULL group with the correct count.
CREATE TABLE ${case_db}.dict_null_t (
  k INT,
  s STRING
) TBLPROPERTIES ("format-version" = "3");
INSERT INTO ${case_db}.dict_null_t VALUES
  (1, 'a'), (2, NULL), (3, 'a'), (4, NULL), (5, 'b');
SELECT s,
  CASE WHEN COUNT(s) = 0 THEN 'true' ELSE 'false' END AS is_null,
  COUNT() AS c
FROM ${case_db}.dict_null_t
GROUP BY s
ORDER BY is_null DESC, s;
