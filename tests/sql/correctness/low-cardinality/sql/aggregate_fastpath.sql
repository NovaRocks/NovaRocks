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

-- @tags=low-cardinality,dictionary,aggregate
-- Verify aggregate results over runtime dictionary carriers match flat Utf8
-- semantics, including NULL group handling.
CREATE TABLE ${case_db}.dict_agg_fastpath_t (
  k INT,
  status VARCHAR,
  region VARCHAR,
  v INT
) WITH (format_version = 3);
INSERT INTO ${case_db}.dict_agg_fastpath_t VALUES
  ROW(1, 'PAID', 'east', 10),
  ROW(2, 'NEW', 'west', 20),
  ROW(3, 'PAID', 'west', 30),
  ROW(4, NULL, 'east', 40),
  ROW(5, 'CANCELLED', 'east', 50),
  ROW(6, NULL, 'west', 60);

SELECT status, COUNT(*) AS c, SUM(v) AS total
FROM ${case_db}.dict_agg_fastpath_t
GROUP BY status
ORDER BY status IS NOT NULL, status;

-- Mixed string grouping keeps plain result semantics.
SELECT status, region, COUNT(*) AS c
FROM ${case_db}.dict_agg_fastpath_t
GROUP BY status, region
ORDER BY status IS NOT NULL, status, region;

-- min/max on low-cardinality strings remains value-domain correct.
SELECT MIN(status), MAX(status)
FROM ${case_db}.dict_agg_fastpath_t;
