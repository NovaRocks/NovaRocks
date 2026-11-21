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

WITH samples(k) AS (VALUES
    ARRAY[TIMESTAMP '2024-01-01 00:00:00.000000000001 UTC'],
    ARRAY[TIMESTAMP '2024-01-01 08:00:00.000000000001 Asia/Shanghai'])
SELECT count(*) AS grouped_rows FROM samples GROUP BY k;

WITH samples(k) AS (VALUES
    ROW(TIME '00:15:00.000000000001 +00:00'),
    ROW(TIME '08:15:00.000000000001 +08:00'))
SELECT count(*) AS distinct_keys FROM (SELECT DISTINCT k FROM samples) d;

WITH samples(k) AS (VALUES
    ARRAY[TIMESTAMP '2024-01-01 08:00:00.000000000001 Asia/Shanghai'])
SELECT CAST(element_at(k, 1) AS VARCHAR) AS representative
FROM samples GROUP BY k;
