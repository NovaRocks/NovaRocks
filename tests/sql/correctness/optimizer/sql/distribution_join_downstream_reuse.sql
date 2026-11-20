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

-- @tags=optimizer,oq8,distribution
DROP TABLE IF EXISTS ${case_db}.oq8_down_l;
DROP TABLE IF EXISTS ${case_db}.oq8_down_r;
CREATE TABLE ${case_db}.oq8_down_l (k INT, v INT);
CREATE TABLE ${case_db}.oq8_down_r (k INT, w INT);
INSERT INTO ${case_db}.oq8_down_l VALUES ROW(1, 10), ROW(2, 20), ROW(3, 30);
INSERT INTO ${case_db}.oq8_down_r VALUES ROW(1, 100), ROW(2, 200), ROW(3, 300);
ANALYZE ${case_db}.oq8_down_l;
ANALYZE ${case_db}.oq8_down_r;

-- @explain_contains=HASH JOIN (PARTITIONED
-- @explain_contains=WINDOW [
-- @explain_contains=HASH EXCHANGE
-- @explain_contains=PARTITION: HASH_PARTITIONED
SELECT l.k,
       r.k AS rk,
       ROW_NUMBER() OVER (PARTITION BY l.k, r.k ORDER BY l.v) AS rn
FROM ${case_db}.oq8_down_l l
INNER JOIN ${case_db}.oq8_down_r r ON l.k = r.k
ORDER BY l.k, r.k;
