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

-- @tags=complex,exchange,dictionary
-- Test Objective:
-- Preserve repeated string values and SQL NULL under independent exchange
-- units in both join placements. Actual dictionary messages and IDs are
-- separately checked by the real Driver/decoder regression.

-- query 1
-- @skip_result_check=true
CREATE TABLE ${case_db}.dictionary_seed (id INT NOT NULL, label STRING)
TBLPROPERTIES ("format-version" = "3");
INSERT INTO ${case_db}.dictionary_seed VALUES (7,'a'),(19,'a'),(27,'b'),(35,NULL);

-- query 2
-- @order_sensitive=true
-- @explain_contains=HASH JOIN (PARTITIONED, INNER
SELECT l.id, r.label
FROM ${case_db}.dictionary_seed l JOIN [shuffle] ${case_db}.dictionary_seed r ON l.id = r.id
ORDER BY l.id;

-- query 3
-- @order_sensitive=true
-- @explain_contains=HASH JOIN (BROADCAST, INNER
SELECT l.id, r.label
FROM ${case_db}.dictionary_seed l JOIN [broadcast] ${case_db}.dictionary_seed r ON l.id = r.id
ORDER BY l.id;
