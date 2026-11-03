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

-- @tags=complex,exchange,depth64
-- Test Objective:
-- Keep a non-NULL logical-depth-64 value across a multi-consumer CTE and
-- explicit shuffle/broadcast joins under the real distributed topology.
-- Storage holds only the shallow id seed; no deep Provider schema is used.
-- Expected rows are two ids, each wrapped exactly 63 times. Row children
-- retain their positional value; Map keys remain the string "k" at every level.

-- query 1
-- @skip_result_check=true
CREATE TABLE ${case_db}.deep_seed (id INT NOT NULL)
TBLPROPERTIES ("format-version" = "3");
INSERT INTO ${case_db}.deep_seed VALUES (7), (19);

-- query 2
-- @order_sensitive=true
-- @explain_contains=HASH JOIN (PARTITIONED, INNER
WITH deep AS (SELECT id, ROW(ROW(ROW(ROW(ROW(ROW(ROW(ROW(ROW(ROW(ROW(ROW(ROW(ROW(ROW(ROW(ROW(ROW(ROW(ROW(ROW(ROW(ROW(ROW(ROW(ROW(ROW(ROW(ROW(ROW(ROW(ROW(ROW(ROW(ROW(ROW(ROW(ROW(ROW(ROW(ROW(ROW(ROW(ROW(ROW(ROW(ROW(ROW(ROW(ROW(ROW(ROW(ROW(ROW(ROW(ROW(ROW(ROW(ROW(ROW(ROW(ROW(ROW(id))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))) AS value FROM ${case_db}.deep_seed)
SELECT l.id, r.value AS deep_value
FROM deep l JOIN [shuffle] deep r ON l.id = r.id
ORDER BY l.id;

-- query 3
-- @order_sensitive=true
-- @explain_contains=HASH JOIN (BROADCAST, INNER
WITH deep AS (SELECT id, ROW(ROW(ROW(ROW(ROW(ROW(ROW(ROW(ROW(ROW(ROW(ROW(ROW(ROW(ROW(ROW(ROW(ROW(ROW(ROW(ROW(ROW(ROW(ROW(ROW(ROW(ROW(ROW(ROW(ROW(ROW(ROW(ROW(ROW(ROW(ROW(ROW(ROW(ROW(ROW(ROW(ROW(ROW(ROW(ROW(ROW(ROW(ROW(ROW(ROW(ROW(ROW(ROW(ROW(ROW(ROW(ROW(ROW(ROW(ROW(ROW(ROW(ROW(id))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))) AS value FROM ${case_db}.deep_seed)
SELECT l.id, r.value AS deep_value
FROM deep l JOIN [broadcast] deep r ON l.id = r.id
ORDER BY l.id;
